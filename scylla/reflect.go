package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

var rangeOperators = []string{">", "<", ">=", "<="}

const (
	managedCreatedColumnName        = "created"
	managedUpdatedColumnName        = "updated"
	managedUpdatedVersionColumnName = db.ColumnNameUpdatedVersion
)

var makeStatementWith string = `	WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
	and compaction = {'class': 'SizeTieredCompactionStrategy'}
	and compression = {'compression_level': '3', 'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor'}
	and dclocal_read_repair_chance = 0
	and speculative_retry = '99.0PERCENTILE'`

// https://forum.scylladb.com/t/what-is-the-difference-between-clustering-primary-partition-and-composite-or-compound-keys-in-scylladb/41
type statementRangeGroup struct {
	from      *ColumnStatement
	betweenTo *ColumnStatement
}

func normalizeCompositeBucketSizes(rawSizes []int8) []int8 {
	// Normalize and validate bucket sizes once so read/write logic can rely on deterministic order.
	bucketSizeSeen := map[int8]bool{}
	bucketSizes := make([]int8, 0, len(rawSizes))
	for _, bucketSize := range rawSizes {
		if bucketSize <= 0 {
			panic(fmt.Sprintf("CompositeBucketing requires bucket sizes > 0. Got: %v", bucketSize))
		}
		if bucketSizeSeen[bucketSize] {
			continue
		}
		bucketSizeSeen[bucketSize] = true
		bucketSizes = append(bucketSizes, bucketSize)
	}
	if len(bucketSizes) == 0 {
		panic("CompositeBucketing requires at least one bucket size.")
	}
	slices.Sort(bucketSizes)
	return bucketSizes
}

func isCompositeNumericFieldType(fieldType string) bool {
	// Composite bucket hashing currently supports integer scalars/slices only, matching query planner assumptions.
	switch fieldType {
	case "int", "int8", "int16", "int32", "int64", "[]int", "[]int8", "[]int16", "[]int32", "[]int64":
		return true
	}
	return false
}

func isManagedIntFieldType(fieldType string) bool {
	// Managed audit columns are shared scalar values written back into one numeric column.
	switch fieldType {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}

func ensureManagedIntColumn(dbTable *ScyllaTable, columnName string) IColInfo {
	if currentColumn := dbTable.ColumnsMap[columnName]; currentColumn != nil {
		if currentColumn.GetType().IsSlice || !isManagedIntFieldType(currentColumn.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": managed column "%v" must be an integer scalar. Found: %v`,
				dbTable.Name, columnName, currentColumn.GetType().FieldType))
		}
		return currentColumn
	}

	dbTable.MaxColIdx++
	managedColumn := &columnInfo{
		ColInfo: colInfo{
			Name:      columnName,
			FieldName: columnName,
			Idx:       dbTable.MaxColIdx,
			RefType:   reflect.TypeOf(int32(0)),
		},
		ColType: db.GetColTypeByID(3),
		// DB-only managed columns exist in Scylla even when a record struct does not expose them.
		GetRawValueFn:       func(ptr unsafe.Pointer) any { return nil },
		GetStatementValueFn: func(ptr unsafe.Pointer) any { return nil },
		GetValueFn:          func(ptr unsafe.Pointer) any { return nil },
	}
	dbTable.ColumnsMap[columnName] = managedColumn
	return managedColumn
}

// consumesUpdatedVersion reports whether the table has a reader for the write sequence number: a
// delta index watermarks on it, and the by-IDs cache compares slot versions with it. A table struct
// that declares the column counts too, so declaring the field is never silently ignored.
func consumesUpdatedVersion(dbTable *ScyllaTable, schema TableSchema) bool {
	if schema.SaveUpdatedVersion {
		return true
	}
	for _, index := range schema.Indexes {
		if index.Type == TypeDelta {
			return true
		}
	}
	_, isDeclaredByTableStruct := dbTable.ColumnsMap[managedUpdatedVersionColumnName]
	return isDeclaredByTableStruct
}

func bindManagedAuditColumns(dbTable *ScyllaTable, schema TableSchema) {
	dbTable.CreatedCol = ensureManagedIntColumn(dbTable, managedCreatedColumnName)
	dbTable.UpdatedCol = ensureManagedIntColumn(dbTable, managedUpdatedColumnName)

	// The version costs a counter read per write, so a table that has no reader for it keeps neither
	// the column nor that cost.
	if !consumesUpdatedVersion(dbTable, schema) {
		return
	}

	// A column already in ColumnsMap was declared by the table struct, so it is backed by a record
	// field and reaches the client — which is required, since the client sends it back as a watermark.
	if _, isDeclaredByTableStruct := dbTable.ColumnsMap[managedUpdatedVersionColumnName]; !isDeclaredByTableStruct {
		panic(fmt.Sprintf(`Table "%v": SaveUpdatedVersion and TypeDelta need the version to reach the client. `+
			`Declare the field "UpdatedVersion int32" with the json tag "upv,omitempty" in the record struct, `+
			`and "UpdatedVersion db.Col[...]" in the table struct.`, dbTable.Name))
	}

	dbTable.UpdatedVersionCol = ensureManagedIntColumn(dbTable, managedUpdatedVersionColumnName)
}

func flattenCompositeInt64Values(rawValue any) []int64 {
	// Treat scalar values as a single-item list so composite hashing works for both scalar and slice numeric columns.
	if rawValue == nil {
		return nil
	}

	rv := reflect.ValueOf(rawValue)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return []int64{convertToInt64(rv.Interface())}
	}

	values := make([]int64, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		values = append(values, convertToInt64(rv.Index(i).Interface()))
	}
	return values
}

func getCompositeBucketValues(rawValue any, bucketSize int8, isWeek bool) []int64 {
	// Composite buckets index bucket IDs (week/size), not raw week values.
	flatValues := flattenCompositeInt64Values(rawValue)
	bucketValues := make([]int64, 0, len(flatValues))
	for _, value := range flatValues {
		bucketValues = append(bucketValues, makeCompositeBucketID(value, bucketSize, isWeek))
	}
	return bucketValues
}

func computeCompositeHashSet(ptr unsafe.Pointer, sourceColumns []IColInfo, bucketColumn IColInfo, bucketSize int8, bucketIsWeek bool) []int32 {
	// Build the cartesian product across up to 3 numeric source columns, then hash each tuple for set<int> indexing.
	combinations := [][]int64{{}}

	for _, sourceColumn := range sourceColumns {
		var columnValues []int64
		if sourceColumn.GetName() == bucketColumn.GetName() {
			columnValues = getCompositeBucketValues(sourceColumn.GetRawValue(ptr), bucketSize, bucketIsWeek)
		} else {
			columnValues = flattenCompositeInt64Values(sourceColumn.GetRawValue(ptr))
		}

		if len(columnValues) == 0 {
			return []int32{}
		}

		nextCombinations := make([][]int64, 0, len(combinations)*len(columnValues))
		for _, combination := range combinations {
			for _, columnValue := range columnValues {
				combinationExpanded := append(append([]int64{}, combination...), columnValue)
				nextCombinations = append(nextCombinations, combinationExpanded)
			}
		}
		combinations = nextCombinations
	}

	uniqueHashes := map[int32]struct{}{}
	// Deduplicate hashes per record to keep generated set values compact and stable.
	for _, combination := range combinations {
		uniqueHashes[HashInt64(combination...)] = struct{}{}
	}

	hashValues := make([]int32, 0, len(uniqueHashes))
	for hashValue := range uniqueHashes {
		hashValues = append(hashValues, hashValue)
	}
	slices.Sort(hashValues)
	return hashValues
}

func makeTable[T TableSchemaInterface[T]](structType *T) ScyllaTable {

	schema := (*structType).GetSchema()
	structRefValue := reflect.ValueOf(structType).Elem()

	if len(schema.Keys) == 0 {
		panic("No se ha especificado una PrimaryKey")
	}

	// Claiming the ID here validates its range and rejects duplicates the first time each table is
	// compiled, which is well before any cached slot version could be written under a wrong key.
	db.ClaimTableID(schema.ID, schema.Name)

	dbTable := ScyllaTable{
		TableCore: db.TableCore{
			Namespace:          schema.Namespace,
			Name:               schema.Name,
			ID:                 schema.ID,
			SaveUpdatedVersion: schema.SaveUpdatedVersion,
			ColumnsMap:         map[string]IColInfo{},
			ColumnsIdxMap:      map[int16]IColInfo{},
			UseSequences:       schema.UseSequences,
			MaxColIdx:          int16(structRefValue.NumField()) + 1,
		},
		indexes:              map[string]*viewInfo{},
		views:                map[string]*viewInfo{},
		selectStatementCache: newSelectPlanCache(),
	}

	if dbTable.Namespace == "" {
		dbTable.Namespace = connParams.Keyspace
	}

	sequenceColumn := ""
	if schema.SequenceColumn != nil {
		sequenceColumn = schema.SequenceColumn.GetName()
	}

	// New implementation: iterate over struct fields and process Col[T,E] fields
	for i := 0; i < structRefValue.NumField(); i++ {
		field := structRefValue.Field(i)

		// Skip if field cannot be addressed or interfaced
		if !field.CanAddr() || !field.Addr().CanInterface() {
			panic("No es un interface!: " + field.Type().Name())
		}

		// Check if field implements Coln interface
		fieldAddr := field.Addr()
		colInterface, ok := fieldAddr.Interface().(Coln)
		fieldName := field.Type().Name()

		if !ok {
			if !(len(fieldName) > 12 && fieldName[0:12] == "TableStruct[") {
				fmt.Println("No es una columna:", fieldName)
			}
			continue
		}

		// Get column info from the field
		column := colInterface.GetInfo()
		// fmt.Printf("DEBUG: makeTable field=%s, GetInfo().Name=%s, Type=%T\n", fieldName, column.Name, colInterface)
		if column.Name == "" {
			fmt.Printf("DEBUG: makeTable EMPTY NAME for field=%s! columnInfo=%+v\n", fieldName, column)
			panic("El nombre de la columna no está seteada.")
		}

		if DebugFull {
			offset := uintptr(0)
			if column.GetInfo().Field != nil {
				offset = column.GetInfo().Field.Offset
			}
			fmt.Printf("Mapped Table Column: %-20s | Field: %-20s | Type: %-10s | Offset: %d\n", column.GetName(), column.GetInfo().FieldName, column.FieldType, offset)
		}

		if sequenceColumn == column.GetName() {
			column.DBType = "counter"
		} /* else if column.DBType == "" {
			column.GetType().IsComplexType = true
			column.Type = 9
			column.GetType().IsSlice = false
			column.DBType = "blob"
		} else if column.GetType().IsSlice && column.DBType[0:3] != "set" {
			column.DBType = fmt.Sprintf("set<%v>", column.DBType)
		} */

		if _, ok := dbTable.ColumnsMap[column.GetName()]; ok {
			panic("The following column name is repeated:" + column.GetName())
		} else {
			column.GetInfo().Idx = int16(column.GetInfo().FieldIdx) + 1
			dbTable.ColumnsMap[column.GetName()] = &column
			if DebugFull {
				// fmt.Printf("Mapped Col: %s, Field: %s, Offset: %d\n", column.GetName(), column.GetInfo().FieldName, column.GetInfo().Field.Offset)
			}
		}
	}

	// Every table keeps the same audit columns in Scylla, even when a struct hides them.
	bindManagedAuditColumns(&dbTable, schema)

	// Resolve declared value ranges before any index compiles, since TypeDelta sizes its digit
	// slots from them.
	dbTable.fixedValueRanges = resolveFixedValueRanges(&dbTable, schema.FixedValues)

	if schema.Partition != nil {
		dbTable.PartKey = dbTable.ColumnsMap[schema.Partition.GetInfo().Name]
		if dbTable.PartKey != nil && !dbTable.PartKey.IsNil() {
			dbTable.KeysIdx = append(dbTable.KeysIdx, dbTable.PartKey.GetInfo().Idx)
		}
	}

	for _, key := range schema.Keys {
		col := dbTable.ColumnsMap[key.GetInfo().Name]
		dbTable.Keys = append(dbTable.Keys, col)
		dbTable.KeysIdx = append(dbTable.KeysIdx, col.GetInfo().Idx)

		// Transfer AutoincrementRandDigits from Key to the actual column in columnsMap
		// This enables autoincrement functionality when a Key is marked with .Autoincrement()
		// Valid values: -1 (no random suffix) or >0 (with random suffix)
		// Default value 0 means .Autoincrement() was never called
		keyInfo := key.GetInfo()
		if keyInfo.AutoincrementRandDigits != 0 {
			col.SetAutoincrementRandSize(keyInfo.AutoincrementRandDigits)

			// Set autoincrementCol if not already set
			if dbTable.AutoincrementCol == nil {
				dbTable.AutoincrementCol = col
			} else if dbTable.AutoincrementCol != col {
				panic(fmt.Sprintf(`Table "%v": Multiple autoincrement columns are not supported. Found autoincrement on both "%v" and "%v"`,
					dbTable.Name, dbTable.AutoincrementCol.GetName(), col.GetName()))
			}
		}
	}

	if schema.AutoincrementPart != nil {
		dbTable.AutoincrementPart = dbTable.ColumnsMap[schema.AutoincrementPart.GetInfo().Name]
	}

	configureTextSearchIndex(&dbTable, schema)

	// Identify autoincrement column from direct column definitions (backward compatibility)
	// Only set if not already set during Keys processing
	for _, col := range dbTable.ColumnsMap {
		if c, ok := col.(*columnInfo); ok {
			// Important: default is 0 (meaning "not autoincrement"). Valid autoincrement values are:
			// -1  : autoincrement with no random suffix (Col.Autoincrement(0) normalizes to -1)
			// > 0 : autoincrement with random suffix of that size
			// The previous condition `>= 0` incorrectly treated the default 0 as autoincrement and
			// caused tables without Autoincrement() to still query `sequences` on insert.
			if c.AutoincrementRandDigits != 0 && dbTable.AutoincrementCol == nil {
				dbTable.AutoincrementCol = col
			}
		}
	}

	if len(schema.KeyIntPacking) > 0 {
		if len(dbTable.Keys) != 1 {
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking requires exactly one column in Keys. Found: %v`, dbTable.Name, len(dbTable.Keys)))
		}

		keyCol := dbTable.Keys[0].(*columnInfo)
		if keyCol.Type != 2 { // 2 = int64
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking requires the key column to be an int64. Found: %v`, dbTable.Name, keyCol.FieldType))
		}

		for _, col := range schema.KeyIntPacking {
			packedCol := dbTable.ColumnsMap[col.GetName()]
			if packedCol == nil {
				info := col.GetInfo()
				if info.AutoincrementRandDigits >= 0 {
					// It's an autoincrement placeholder
					placeholder := &columnInfo{
						ColInfo: colInfo{
							Name:      "autoincrement_placeholder",
							IsVirtual: true,
						},
						AutoincrementRandDigits: info.AutoincrementRandDigits,
						DecimalDigits:           info.DecimalDigits,
					}
					packedCol = placeholder
					dbTable.AutoincrementCol = placeholder
				} else {
					panic(fmt.Sprintf(`Table "%v": Column "%v" in KeyIntPacking not found`, dbTable.Name, col.GetName()))
				}
			} else {
				// If it's a real column, ensure DecimalDigits is transferred if set in KeyIntPacking
				info := col.GetInfo()
				if info.DecimalDigits > 0 {
					packedCol.SetDecimalSize(info.DecimalDigits)
				}
			}
			dbTable.keyIntPacking = append(dbTable.keyIntPacking, packedCol)
		}
	}

	if len(schema.KeyConcatenated) > 0 {
		if len(dbTable.Keys) != 1 {
			panic(fmt.Sprintf(`Table "%v": KeyConcatenated requires exactly one column in Keys. Found: %v`, dbTable.Name, len(dbTable.Keys)))
		}
		keyCol := dbTable.Keys[0].(*columnInfo)
		if keyCol.Type != 1 { // 1 = string
			panic(fmt.Sprintf(`Table "%v": KeyConcatenated requires the key column to be a string. Found: %v`, dbTable.Name, keyCol.FieldType))
		}

		concatCols := []IColInfo{}
		for _, col := range schema.KeyConcatenated {
			concatCol := dbTable.ColumnsMap[col.GetName()]
			concatCols = append(concatCols, concatCol)
			dbTable.keyConcatenated = append(dbTable.keyConcatenated, concatCol)
		}

		keyCol.GetRawValueFn = func(ptr unsafe.Pointer) any {
			values := []any{}
			for _, col := range concatCols {
				values = append(values, col.GetRawValue(ptr))
			}
			return MakeKeyConcat(values...)
		}

		keyCol.GetValueFn = func(ptr unsafe.Pointer) any {
			return "'" + strings.ReplaceAll(keyCol.GetRawValueFn(ptr).(string), "'", "''") + "'"
		}
	}

	idxCount := int8(1)

	if schema.SequencePartCol != nil {
		gi := schema.SequencePartCol.GetInfo()
		dbTable.SequencePartCol = &gi
	}

	// Compile schema indexes in one pass to keep the public API and compiler flow simple.
	for _, indexCfg := range schema.Indexes {
		// TypeDelta is the one kind that means something with no keys: it appends "updated_version"
		// itself, so a keyless entry declares a watermark-only sync.
		if len(indexCfg.Keys) == 0 && resolveSchemaIndexType(indexCfg) != TypeDelta {
			panic(fmt.Sprintf(`Table "%v": Indexes entry must not be empty`, dbTable.Name))
		}
		if indexCfg.Type == TypeInheritFromKey && !indexCfg.UseIndexGroup {
			panic(fmt.Sprintf(`Table "%v": TypeInheritFromKey requires UseIndexGroup: true`, dbTable.Name))
		}
		// Only views materialize their own partition, so an override is meaningless elsewhere.
		schemaIndexType := resolveSchemaIndexType(indexCfg)
		if indexPartitionColumnName(indexCfg) != "" && schemaIndexType != TypeView && schemaIndexType != TypeDelta {
			panic(fmt.Sprintf(`Table "%v": Partition is only supported on TypeView and TypeDelta indexes`, dbTable.Name))
		}
		if indexCfg.UseIndexGroup {
			registerIndexGroup(&dbTable, &idxCount, indexCfg)
			continue
		}
		if hasCompositeBucketing(indexCfg) {
			indexColumns := indexCfg.Keys
			if len(indexColumns) < 2 || len(indexColumns) > 3 {
				panic(fmt.Sprintf(`Table "%v": composite-bucketing index entries must have 2 to 3 columns. Found: %v`, dbTable.Name, len(indexColumns)))
			}

			sourceColumns := make([]IColInfo, 0, len(indexColumns))
			var bucketColumn IColInfo
			bucketIsWeek := false
			var bucketSizes []int8
			sourceColumnNames := make([]string, 0, len(indexColumns))

			for _, indexColumn := range indexColumns {
				indexColumnInfo := indexColumn.GetInfo()
				column := dbTable.ColumnsMap[indexColumnInfo.Name]
				if column == nil {
					panic(fmt.Sprintf(`Table "%v": composite-bucketing column "%v" was not found`, dbTable.Name, indexColumnInfo.Name))
				}
				if !isCompositeNumericFieldType(column.GetType().FieldType) {
					panic(fmt.Sprintf(`Table "%v": composite-bucketing column "%v" must be integer scalar/slice. Found: %v`, dbTable.Name, column.GetName(), column.GetType().FieldType))
				}
				sourceColumns = append(sourceColumns, column)
				sourceColumnNames = append(sourceColumnNames, column.GetName())

				bucketDefs := indexColumnInfo.CompositeBucketSizes
				if len(bucketDefs) > 0 {
					if bucketColumn != nil {
						panic(fmt.Sprintf(`Table "%v": composite-bucketing supports exactly one CompositeBucketing column per index`, dbTable.Name))
					}
					if column.GetType().IsSlice {
						panic(fmt.Sprintf(`Table "%v": CompositeBucketing column "%v" must be numeric (not a slice)`, dbTable.Name, column.GetName()))
					}
					bucketColumn = column
					bucketIsWeek = indexColumnInfo.IsWeek
					bucketSizes = normalizeCompositeBucketSizes(bucketDefs)
				}
			}

			if bucketColumn == nil {
				panic(fmt.Sprintf(`Table "%v": composite-bucketing requires one column marked with CompositeBucketing`, dbTable.Name))
			}

			compositeIndex := compositeBucketIndex{
				name:                 strings.Join(sourceColumnNames, "_"),
				sourceColumns:        sourceColumns,
				bucketColumn:         bucketColumn,
				bucketIsWeek:         bucketIsWeek,
				bucketSizes:          bucketSizes,
				virtualColumnsBySize: map[int8]IColInfo{},
			}

			for _, bucketSize := range bucketSizes {
				virtualColName := fmt.Sprintf("zz_hb_%s_b%d", strings.Join(sourceColumnNames, "_"), bucketSize)
				if _, exists := dbTable.ColumnsMap[virtualColName]; exists {
					panic(fmt.Sprintf(`Table "%v": generated virtual composite bucket column already exists: %v`, dbTable.Name, virtualColName))
				}

				bucketSizeLocal := bucketSize
				sourceColumnsLocal := sourceColumns
				bucketColumnLocal := bucketColumn

				virtualColumn := &columnInfo{
					ColInfo: colInfo{
						Name:      virtualColName,
						FieldName: virtualColName,
						IsVirtual: true,
						Idx:       dbTable.MaxColIdx,
					},
					ColType: colType{
						Type:      13,
						FieldType: "[]int32",
						DBType:    "set<int>",
						IsSlice:   true,
					},
				}

				bucketIsWeekLocal := compositeIndex.bucketIsWeek
				virtualColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
					return computeCompositeHashSet(ptr, sourceColumnsLocal, bucketColumnLocal, bucketSizeLocal, bucketIsWeekLocal)
				}
				virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
					hashValues := computeCompositeHashSet(ptr, sourceColumnsLocal, bucketColumnLocal, bucketSizeLocal, bucketIsWeekLocal)
					return makeSignedIntCollectionLiteral(virtualColumn.DBType, hashValues)
				}

				dbTable.MaxColIdx++
				dbTable.ColumnsMap[virtualColumn.GetName()] = virtualColumn
				compositeIndex.virtualColumnsBySize[bucketSize] = virtualColumn

				index := &viewInfo{
					Type:    1,
					name:    fmt.Sprintf(`%v__%v_index_0`, dbTable.Name, virtualColumn.GetName()),
					idx:     idxCount,
					column:  virtualColumn,
					columns: []string{virtualColumn.GetName()},
				}
				index.getCreateScript = func() string {
					return fmt.Sprintf(`CREATE INDEX %v ON %v (VALUES(%v))`, index.name, dbTable.GetFullName(), virtualColumn.GetName())
				}

				idxCount++
				dbTable.indexes[index.name] = index
			}

			dbTable.compositeBucketIndexes = append(dbTable.compositeBucketIndexes, compositeIndex)
			fmt.Printf("CompositeBucketing index registered: table=%s index=%s bucketSizes=%v\n", dbTable.Name, compositeIndex.name, compositeIndex.bucketSizes)
			continue
		}

		switch schemaIndexType {
		case TypeGlobalIndex:
			registerSchemaGlobalIndex(&dbTable, &idxCount, indexCfg)
		case TypeLocalIndex:
			registerSchemaLocalIndex(&dbTable, &idxCount, indexCfg)
		case TypeViewTable:
			dbTable.hasTableBackedViews = true
			compileSchemaViewTable(&dbTable, indexCfg)
		case TypeView:
			compileSchemaView(&dbTable, indexCfg, nil)
		case TypeDelta:
			compileSchemaDeltaView(&dbTable, indexCfg)
		default:
			panic(fmt.Sprintf(`Table "%v": unsupported index type %d`, dbTable.Name, schemaIndexType))
		}
	}

	for _, col := range dbTable.ColumnsMap {
		if columnMetadata, isColumnInfo := col.(*columnInfo); isColumnInfo {
			// Compile fast accessors once per table build so row scan/write paths avoid generic branching.
			columnMetadata.CompileFastAccessors()
		}
	}

	for _, col := range dbTable.ColumnsMap {
		dbTable.Columns = append(dbTable.Columns, col)
		dbTable.ColumnsIdxMap[col.GetInfo().Idx] = col
	}

	for _, e := range dbTable.indexes {
		dbTable.indexViews = append(dbTable.indexViews, e)
	}
	for _, e := range dbTable.views {
		dbTable.indexViews = append(dbTable.indexViews, e)
	}
	// Both sources above are maps, so their iteration order changes every process. Capability
	// matching breaks priority ties by first-seen, which would make a query plan differ between
	// runs whenever two sources advertise the same signature at the same priority. Sorting by name
	// pins the tie-break to something stable.
	slices.SortFunc(dbTable.indexViews, func(leftView, rightView *viewInfo) int {
		return strings.Compare(leftView.name, rightView.name)
	})

	for _, idxview := range dbTable.indexViews {
		for _, colname := range idxview.columns {
			col := dbTable.ColumnsMap[colname]
			idxview.columnsIdx = append(idxview.columnsIdx, col.GetInfo().Idx)
		}
	}

	dbTable.capabilities = dbTable.ComputeCapabilities()

	// Initializes and validates by-IDs slot metadata once per table build, not per query/write call.
	configureSlotVersionFields(&dbTable)
	// Precompiles the generic by-IDs access plan; depends on the slot metadata above.
	configureGenericRecordFields(&dbTable, schema)

	return dbTable
}
