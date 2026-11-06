package fdbdriver

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

const (
	managedCreatedColumnName = "created"
	managedUpdatedColumnName = "updated"
	managedVersionColumnName = db.ColumnNameVersion
)

// FdbTable is a table compiled for FoundationDB: the shared TableCore plus the row codec's column
// IDs and the TypeDelta indexes, the one index kind this driver keeps entries for so far.
type FdbTable struct {
	db.TableCore
	// storedColumns are the columns a row holds (rowcodec.go), in column order.
	storedColumns []storedColumn
	// storedColumnPositionByID resolves a row entry back to its column's position in storedColumns.
	storedColumnPositionByID map[uint16]int
	deltaIndexes             []fdbDeltaIndex
	// unservedIndexes are the declared indexes this driver keeps no entries for yet. Reads never use
	// them (a read scans the partition and filters exactly), so they cost speed, not correctness.
	unservedIndexes []string
}

// storedColumn is one column of the row codec: its 16-bit ID and how it is read and written.
type storedColumn struct {
	id     uint16
	column *db.ColumnInfo
}

// fdbDeltaIndex is a TypeDelta index and the ID its entries and moves are keyed by.
type fdbDeltaIndex struct {
	db.DeltaIndexInfo
	id uint16
}

type fdbTableCacheEntry struct {
	once  sync.Once
	table *FdbTable
}

var fdbTableCache sync.Map

// getOrCompileFdbTable compiles a bound table struct once per type. It takes `any` for the same
// reason the Scylla driver does: one compiled body serves every table type.
func getOrCompileFdbTable(schemaStruct any) *FdbTable {
	schemaType := reflect.TypeOf(schemaStruct).Elem()
	cacheEntryAny, _ := fdbTableCache.LoadOrStore(schemaType, &fdbTableCacheEntry{})
	cacheEntry := cacheEntryAny.(*fdbTableCacheEntry)
	cacheEntry.once.Do(func() {
		cacheEntry.table = compileTable(schemaStruct.(db.TableHandle).GetSchema(), reflect.ValueOf(schemaStruct).Elem())
	})
	return cacheEntry.table
}

// compiledTableOf compiles (or returns) the table of a record type, for the write paths that get no
// bound table struct.
func compiledTableOf[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]]() *FdbTable {
	return getOrCompileFdbTable(db.InitStructTable[TableT, RecordT](new(TableT)))
}

// compileTable turns a schema into an FdbTable. It follows the Scylla driver's makeTable for
// everything the shared TableCore means (columns, keys, managed columns, packing, FixedValues), so
// both drivers agree on what a record is.
func compileTable(schema db.TableSchema, structRefValue reflect.Value) *FdbTable {
	if len(schema.Keys) == 0 {
		panic(fmt.Sprintf(`Table "%v": no Keys declared`, schema.Name))
	}
	db.ClaimTableID(schema.ID, schema.Name)

	table := &FdbTable{
		TableCore: db.TableCore{
			Namespace:     schema.Namespace,
			Name:          schema.Name,
			ID:            schema.ID,
			CacheByIDs:    schema.CacheByIDs,
			ColumnsMap:    map[string]db.IColInfo{},
			ColumnsIdxMap: map[int16]db.IColInfo{},
			UseSequences:  schema.UseSequences,
			MaxColIdx:     int16(structRefValue.NumField()) + 1,
		},
		storedColumnPositionByID: map[uint16]int{},
	}
	if table.Namespace == "" {
		table.Namespace = connParams.Keyspace
	}
	if schema.SequenceColumn != nil || schema.CounterColumn != nil {
		panic(fmt.Sprintf(`Table "%v": SequenceColumn / CounterColumn are not implemented on FoundationDB yet`, schema.Name))
	}

	computedPendingColumns := map[string]db.ColumnInfo{}
	for fieldIndex := 0; fieldIndex < structRefValue.NumField(); fieldIndex++ {
		field := structRefValue.Field(fieldIndex)
		columnHandle, isColumn := field.Addr().Interface().(db.Coln)
		if !isColumn {
			continue
		}
		column := columnHandle.GetInfo()
		if column.Name == "" {
			panic(fmt.Sprintf(`Table "%v": a column has no name`, schema.Name))
		}
		if column.IsComputed {
			computedPendingColumns[column.GetName()] = column
			continue
		}
		assertColumnIsEncodable(schema.Name, &column)
		if _, isRepeated := table.ColumnsMap[column.GetName()]; isRepeated {
			panic(fmt.Sprintf(`Table "%v": repeated column name "%v"`, schema.Name, column.GetName()))
		}
		column.Idx = int16(column.FieldIdx) + 1
		table.MaxColIdx = max(table.MaxColIdx, column.Idx+1)
		table.ColumnsMap[column.GetName()] = &column
	}

	bindManagedAuditColumns(table, schema)
	table.ConfigureComputedColumns(schema, structRefValue, computedPendingColumns)
	table.FixedValueRanges = table.ResolveFixedValueRanges(schema.FixedValues)

	if schema.Partition != nil {
		table.PartKey = table.ColumnsMap[schema.Partition.GetInfo().Name]
		if table.PartKey != nil && !table.PartKey.IsNil() {
			table.KeysIdx = append(table.KeysIdx, table.PartKey.GetInfo().Idx)
		}
	}
	compileKeys(table, schema)
	if schema.AutoincrementPart != nil {
		table.AutoincrementPart = table.ColumnsMap[schema.AutoincrementPart.GetInfo().Name]
	}
	table.ConfigureTextSearchIndex(schema)

	for _, indexCfg := range schema.Indexes {
		if indexCfg.Type == db.TypeDelta {
			compileDeltaIndex(table, indexCfg)
			continue
		}
		keyNames := []string{}
		for _, key := range indexCfg.Keys {
			keyNames = append(keyNames, key.GetInfo().Name)
		}
		table.unservedIndexes = append(table.unservedIndexes,
			fmt.Sprintf("%v(%v)", db.IndexTypeNames[indexCfg.Type], strings.Join(keyNames, ",")))
	}

	for _, column := range table.ColumnsMap {
		if columnInfo, isColumnInfo := column.(*db.ColumnInfo); isColumnInfo {
			columnInfo.CompileFastAccessors()
		}
	}
	for _, column := range table.ColumnsMap {
		table.ColumnsIdxMap[column.GetInfo().Idx] = column
		if !column.GetInfo().IsComputed {
			table.Columns = append(table.Columns, column)
		}
	}
	slices.SortFunc(table.Columns, func(left, right db.IColInfo) int { return int(left.GetInfo().Idx - right.GetInfo().Idx) })
	compileStoredColumns(table)
	if db.ShouldLog() && len(table.unservedIndexes) > 0 {
		fmt.Printf("fdbdriver: table %v keeps no entries for %v (reads scan the partition)\n", table.Name, table.unservedIndexes)
	}
	return table
}

// compileKeys resolves the key columns, autoincrement and key packing, as the Scylla driver does.
func compileKeys(table *FdbTable, schema db.TableSchema) {
	for _, key := range schema.Keys {
		column := table.ColumnsMap[key.GetInfo().Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": key column "%v" was not found`, table.Name, key.GetInfo().Name))
		}
		table.Keys = append(table.Keys, column)
		table.KeysIdx = append(table.KeysIdx, column.GetInfo().Idx)
		keyInfo := key.GetInfo()
		if keyInfo.AutoincrementRandBits != 0 {
			column.SetAutoincrementRandSize(keyInfo.AutoincrementRandBits)
			if keyInfo.AutoincrementFirstID > 0 {
				column.(*db.ColumnInfo).AutoincrementFirstID = keyInfo.AutoincrementFirstID
			}
			if table.AutoincrementCol != nil && table.AutoincrementCol != column {
				panic(fmt.Sprintf(`Table "%v": several autoincrement columns`, table.Name))
			}
			table.AutoincrementCol = column
		}
	}
	for _, column := range table.ColumnsMap {
		if columnInfo, isColumnInfo := column.(*db.ColumnInfo); isColumnInfo && columnInfo.AutoincrementRandBits != 0 && table.AutoincrementCol == nil {
			table.AutoincrementCol = column
		}
	}

	if len(schema.KeyIntPacking) > 0 {
		if len(table.Keys) != 1 || table.Keys[0].GetType().Type != 2 {
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking requires exactly one int64 key`, table.Name))
		}
		for _, declared := range schema.KeyIntPacking {
			packedColumn := table.ColumnsMap[declared.GetName()]
			declaredInfo := declared.GetInfo()
			if packedColumn == nil {
				if declaredInfo.AutoincrementRandBits < 0 {
					panic(fmt.Sprintf(`Table "%v": KeyIntPacking column "%v" not found`, table.Name, declared.GetName()))
				}
				// An autoincrement placeholder: a slot filled from the sequence, with no record field.
				placeholder := &db.ColumnInfo{
					ColInfo:               db.ColInfo{Name: "autoincrement_placeholder", IsVirtual: true},
					AutoincrementRandBits: declaredInfo.AutoincrementRandBits,
					SlotBits:              declaredInfo.SlotBits,
				}
				packedColumn = placeholder
				table.AutoincrementCol = placeholder
			} else if declaredInfo.SlotBits > 0 {
				packedColumn.SetSlotBits(declaredInfo.SlotBits)
			}
			table.KeyIntPacking = append(table.KeyIntPacking, packedColumn)
		}
		table.KeyIntPackingSlotBits = db.KeyIntPackingSlotBits(table.Name, schema.KeyIntPacking)
	}

	if len(schema.KeyConcatenated) > 0 {
		if len(table.Keys) != 1 || table.Keys[0].GetType().Type != 1 {
			panic(fmt.Sprintf(`Table "%v": KeyConcatenated requires exactly one string key`, table.Name))
		}
		keyColumn := table.Keys[0].(*db.ColumnInfo)
		concatenatedColumns := []db.IColInfo{}
		for _, declared := range schema.KeyConcatenated {
			concatenatedColumns = append(concatenatedColumns, table.ColumnsMap[declared.GetName()])
		}
		table.KeyConcatenated = concatenatedColumns
		keyColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
			values := make([]any, 0, len(concatenatedColumns))
			for _, column := range concatenatedColumns {
				values = append(values, column.GetRawValue(ptr))
			}
			return db.MakeKeyConcat(values...)
		}
		keyColumn.GetValueFn = keyColumn.GetRawValueFn
	}
}

// bindManagedAuditColumns adds created / updated (and version on a TypeDelta table), exactly as the
// Scylla driver does: a column the record does not declare is stored all the same.
func bindManagedAuditColumns(table *FdbTable, schema db.TableSchema) {
	hasDelta := slices.ContainsFunc(schema.Indexes, func(index db.Index) bool { return index.Type == db.TypeDelta })
	if _, isDeclared := table.ColumnsMap[managedUpdatedColumnName]; (hasDelta || schema.CacheByIDs) && !isDeclared {
		panic(fmt.Sprintf(`Table "%v": TypeDelta and CacheByIDs need the record field "Updated int32" (json "upd")`, table.Name))
	}
	if _, isDeclared := table.ColumnsMap[managedVersionColumnName]; hasDelta && !isDeclared {
		panic(fmt.Sprintf(`Table "%v": TypeDelta needs the record field "Version int16" (json "ver,omitempty")`, table.Name))
	}
	if !schema.DisableDefaultColumns || schema.CacheByIDs {
		table.CreatedCol = ensureManagedIntColumn(table, managedCreatedColumnName)
		table.UpdatedCol = ensureManagedIntColumn(table, managedUpdatedColumnName)
	}
	if hasDelta {
		table.VersionCol = ensureManagedIntColumn(table, managedVersionColumnName)
	}
}

func ensureManagedIntColumn(table *FdbTable, columnName string) db.IColInfo {
	if declared := table.ColumnsMap[columnName]; declared != nil {
		if declared.GetType().IsSlice || !isIntegerFieldType(declared.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": managed column "%v" must be an integer scalar`, table.Name, columnName))
		}
		return declared
	}
	table.MaxColIdx++
	// No record field: the write path stamps it (managedStamp) and the row stores it.
	managedColumn := &db.ColumnInfo{
		ColInfo: db.ColInfo{Name: columnName, FieldName: columnName, Idx: table.MaxColIdx, RefType: reflect.TypeOf(int32(0))},
		ColType: db.GetColTypeByID(3),
	}
	managedColumn.GetRawValueFn = func(ptr unsafe.Pointer) any { return nil }
	table.ColumnsMap[columnName] = managedColumn
	return managedColumn
}

func isIntegerFieldType(fieldType string) bool {
	switch fieldType {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

// compileStoredColumns assigns every stored column its 16-bit row ID (low bits of the CRC32 of its
// name). Virtual columns are derived on write and never stored.
func compileStoredColumns(table *FdbTable) {
	for _, column := range table.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		columnID := stableIndexID(column.GetName())
		if clashingPosition, isTaken := table.storedColumnPositionByID[columnID]; isTaken {
			panic(fmt.Sprintf(`Table "%v": columns "%v" and "%v" hash to the same row ID; rename one`,
				table.Name, table.storedColumns[clashingPosition].column.GetName(), column.GetName()))
		}
		table.storedColumnPositionByID[columnID] = len(table.storedColumns)
		table.storedColumns = append(table.storedColumns, storedColumn{id: columnID, column: column.(*db.ColumnInfo)})
	}
}

// compileDeltaIndex registers a TypeDelta index: the same validation and packed layout as the Scylla
// driver (delta_view.go), with entries instead of a materialized view.
func compileDeltaIndex(table *FdbTable, indexCfg db.Index) {
	if partitionColumn := table.GetPartKey(); partitionColumn == nil || partitionColumn.IsNil() || !isIntegerFieldType(partitionColumn.GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires an integer partition column`, table.Name))
	}
	if len(table.Keys) != 1 || !isIntegerFieldType(table.Keys[0].GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires exactly one integer key column, the record ID`, table.Name))
	}
	replaceThreshold := indexCfg.DeltaReplaceThreshold
	if replaceThreshold <= 0 {
		replaceThreshold = cache.DefaultDeltaReplaceThreshold
	}
	deltaIndex := db.DeltaIndexInfo{Tag: "all", ReplaceThreshold: replaceThreshold}
	declaredNames := []string{}
	for _, declaredKey := range indexCfg.Keys {
		column := table.ColumnsMap[declaredKey.GetInfo().Name]
		if column == nil || column.GetType().IsSlice || column.GetType().IsComplexType ||
			!db.IsSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" must be a scalar integer column`, table.Name, declaredKey.GetInfo().Name))
		}
		if column.GetName() == table.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" is the record ID`, table.Name, column.GetName()))
		}
		deltaIndex.Pinned = append(deltaIndex.Pinned, column)
		declaredNames = append(declaredNames, column.GetName())
	}
	if len(declaredNames) > 0 {
		deltaIndex.Tag = strings.Join(declaredNames, "_")
	}
	deltaIndex.PinnedSlotBits = table.PlanDeltaPinnedSlots(deltaIndex.Pinned, indexCfg)
	// The packed column only names the layout for PackRowValues' messages: entries carry the value
	// in their key, so it is never stored.
	deltaIndex.PackedColumn = &db.ColumnInfo{
		ColInfo: db.ColInfo{Name: "zz_dlt_" + deltaIndex.Tag, IsVirtual: true},
		ColType: db.GetColTypeByID(2),
	}
	deltaID := stableIndexID("delta:" + deltaIndex.Tag)
	for _, registered := range table.deltaIndexes {
		if registered.Tag == deltaIndex.Tag {
			panic(fmt.Sprintf(`Table "%v": two TypeDelta indexes declare the same Keys (%v)`, table.Name, deltaIndex.Tag))
		}
		if registered.id == deltaID {
			panic(fmt.Sprintf(`Table "%v": TypeDelta indexes %v and %v hash to the same ID`, table.Name, registered.Tag, deltaIndex.Tag))
		}
	}
	table.DeltaIndexes = append(table.DeltaIndexes, deltaIndex)
	table.deltaIndexes = append(table.deltaIndexes, fdbDeltaIndex{DeltaIndexInfo: deltaIndex, id: deltaID})
}

// assertColumnIsEncodable refuses at compile a column colbin cannot carry, like the Scylla driver.
func assertColumnIsEncodable(tableName string, column *db.ColumnInfo) {
	if !column.IsComplexType || column.Field == nil || isByteSliceField(column) || isUnsignedKind(column.Field.Type.Kind()) {
		return
	}
	if _, err := colbin.Marshal(reflect.New(column.Field.Type).Elem().Interface()); err != nil {
		panic(fmt.Sprintf("table %s: column %q (%s) cannot be serialized: %v", tableName, column.GetName(), column.Field.Type, err))
	}
}

func isUnsignedKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return true
	}
	return false
}
