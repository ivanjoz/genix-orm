package fdbdriver

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

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

// compileTable turns a schema into an FdbTable. The table-level compile is the shared one
// (db.TableCore.CompileFromSchema), so every driver agrees on what a record is; below are the parts
// only this driver has.
func compileTable(schema db.TableSchema, structRefValue reflect.Value) *FdbTable {
	if schema.SequenceColumn != nil || schema.CounterColumn != nil {
		panic(fmt.Sprintf(`Table "%v": SequenceColumn / CounterColumn are not implemented on FoundationDB yet`, schema.Name))
	}
	table := &FdbTable{storedColumnPositionByID: map[uint16]int{}}
	table.CompileFromSchema(schema, structRefValue)
	if table.Namespace == "" {
		table.Namespace = connParams.Keyspace
	}
	for _, column := range table.ColumnsMap {
		if columnMetadata, isColumnInfo := column.(*db.ColumnInfo); isColumnInfo {
			assertColumnIsEncodable(table.Name, columnMetadata)
		}
	}

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

	table.FinalizeColumns()
	compileStoredColumns(table)
	if db.ShouldLog() && len(table.unservedIndexes) > 0 {
		fmt.Printf("fdbdriver: table %v keeps no entries for %v (reads scan the partition)\n", table.Name, table.unservedIndexes)
	}
	return table
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
