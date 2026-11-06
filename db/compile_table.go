package db

import (
	"fmt"
	"reflect"
	"slices"
	"unsafe"
)

// Table half of the two-phase compile (indexes are describe_indexes.go): what a table IS to every
// storage engine — columns, managed columns, keys, partition, autoincrement, key packing and
// concatenation. A driver calls CompileFromSchema, adjusts what only it means (Scylla: the counter
// type of a sequence column, the quoted literal of a concatenated key), describes and materializes
// the indexes, then calls FinalizeColumns.

// CompileFromSchema fills an empty TableCore from a schema and the bound table struct. Namespace is
// the schema's; a driver defaults an empty one to its connection's keyspace.
func (core *TableCore) CompileFromSchema(schema TableSchema, structRefValue reflect.Value) {
	if len(schema.Keys) == 0 {
		panic("No se ha especificado una PrimaryKey")
	}
	// Claiming the ID here validates its range and rejects duplicates the first time each table is
	// compiled, which is well before any cached slot version could be written under a wrong key.
	ClaimTableID(schema.ID, schema.Name)

	core.Namespace = schema.Namespace
	core.Name = schema.Name
	core.ID = schema.ID
	core.CacheByIDs = schema.CacheByIDs
	core.ColumnsMap = map[string]IColInfo{}
	core.ColumnsIdxMap = map[int16]IColInfo{}
	core.UseSequences = schema.UseSequences
	core.MaxColIdx = int16(structRefValue.NumField()) + 1

	computedPendingColumns := map[string]ColumnInfo{}
	for fieldIndex := 0; fieldIndex < structRefValue.NumField(); fieldIndex++ {
		field := structRefValue.Field(fieldIndex)
		if !field.CanAddr() || !field.Addr().CanInterface() {
			panic("No es un interface!: " + field.Type().Name())
		}
		columnHandle, isColumn := field.Addr().Interface().(Coln)
		fieldTypeName := field.Type().Name()
		if !isColumn {
			if !(len(fieldTypeName) > 12 && fieldTypeName[0:12] == "TableStruct[") {
				fmt.Println("No es una columna:", fieldTypeName)
			}
			continue
		}

		column := columnHandle.GetInfo()
		if column.Name == "" {
			fmt.Printf("DEBUG: makeTable EMPTY NAME for field=%s! columnInfo=%+v\n", fieldTypeName, column)
			panic("El nombre de la columna no está seteada.")
		}
		// No record field: ConfigureComputedColumns binds it to its TableSchema.Computed declaration.
		if column.IsComputed {
			computedPendingColumns[column.GetName()] = column
			continue
		}
		if _, isRepeated := core.ColumnsMap[column.GetName()]; isRepeated {
			panic("The following column name is repeated:" + column.GetName())
		}
		column.GetInfo().Idx = int16(column.GetInfo().FieldIdx) + 1
		// Idx follows the record's field order, which can hold more fields than the table struct: the
		// generated columns numbered from MaxColIdx must start past every one.
		core.MaxColIdx = max(core.MaxColIdx, column.GetInfo().Idx+1)
		core.ColumnsMap[column.GetName()] = &column
	}

	// Default audit columns are added unless the schema explicitly opts out.
	core.bindManagedAuditColumns(schema)
	core.ConfigureComputedColumns(schema, structRefValue, computedPendingColumns)

	// Resolve declared value ranges before any index compiles, since TypeDelta sizes its bit slots from them.
	core.FixedValueRanges = core.ResolveFixedValueRanges(schema.FixedValues)

	if schema.Partition != nil {
		core.PartKey = core.ColumnsMap[schema.Partition.GetInfo().Name]
		if core.PartKey != nil && !core.PartKey.IsNil() {
			core.KeysIdx = append(core.KeysIdx, core.PartKey.GetInfo().Idx)
		}
	}
	core.compileKeys(schema)
	if schema.AutoincrementPart != nil {
		core.AutoincrementPart = core.ColumnsMap[schema.AutoincrementPart.GetInfo().Name]
	}
	core.ConfigureTextSearchIndex(schema)
	if schema.SequencePartCol != nil {
		sequencePartInfo := schema.SequencePartCol.GetInfo()
		core.SequencePartCol = &sequencePartInfo
	}
}

// FinalizeColumns runs once every column exists, generated ones included: it compiles the fast
// accessors and fills Columns (in column order) and ColumnsIdxMap.
func (core *TableCore) FinalizeColumns() {
	for _, column := range core.ColumnsMap {
		if columnMetadata, isColumnInfo := column.(*ColumnInfo); isColumnInfo {
			// Compiled once per table build so row scan/write paths avoid generic branching.
			columnMetadata.CompileFastAccessors()
		}
	}
	for _, column := range core.ColumnsMap {
		core.ColumnsIdxMap[column.GetInfo().Idx] = column
		// Columns is what every write, read and deploy walks: a computed column has no storage.
		if !column.GetInfo().IsComputed {
			core.Columns = append(core.Columns, column)
		}
	}
	slices.SortFunc(core.Columns, func(left, right IColInfo) int {
		return int(left.GetInfo().Idx - right.GetInfo().Idx)
	})
}

// compileKeys resolves the key columns, the autoincrement column, KeyIntPacking and KeyConcatenated.
func (core *TableCore) compileKeys(schema TableSchema) {
	for _, key := range schema.Keys {
		column := core.ColumnsMap[key.GetInfo().Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": key column "%v" was not found`, core.Name, key.GetInfo().Name))
		}
		core.Keys = append(core.Keys, column)
		core.KeysIdx = append(core.KeysIdx, column.GetInfo().Idx)

		// A Key marked .Autoincrement() carries its random-suffix size: -1 (no suffix) or > 0. The
		// default 0 means .Autoincrement() was never called.
		keyInfo := key.GetInfo()
		if keyInfo.AutoincrementRandBits != 0 {
			column.SetAutoincrementRandSize(keyInfo.AutoincrementRandBits)
			if keyInfo.AutoincrementFirstID > 0 {
				column.(*ColumnInfo).AutoincrementFirstID = keyInfo.AutoincrementFirstID
			}
			if core.AutoincrementCol == nil {
				core.AutoincrementCol = column
			} else if core.AutoincrementCol != column {
				panic(fmt.Sprintf(`Table "%v": Multiple autoincrement columns are not supported. Found autoincrement on both "%v" and "%v"`,
					core.Name, core.AutoincrementCol.GetName(), column.GetName()))
			}
		}
	}
	// A column declared .Autoincrement() directly, not through Keys. AutoincrementRandBits 0 means
	// "not autoincrement"; treating it as one made tables without Autoincrement() query sequences.
	for _, column := range core.ColumnsMap {
		if columnMetadata, isColumnInfo := column.(*ColumnInfo); isColumnInfo && columnMetadata.AutoincrementRandBits != 0 && core.AutoincrementCol == nil {
			core.AutoincrementCol = column
		}
	}

	if len(schema.KeyIntPacking) > 0 {
		if len(core.Keys) != 1 {
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking requires exactly one column in Keys. Found: %v`, core.Name, len(core.Keys)))
		}
		if keyColumn := core.Keys[0].(*ColumnInfo); keyColumn.Type != 2 { // 2 = int64
			panic(fmt.Sprintf(`Table "%v": KeyIntPacking requires the key column to be an int64. Found: %v`, core.Name, keyColumn.FieldType))
		}
		for _, declared := range schema.KeyIntPacking {
			packedColumn := core.ColumnsMap[declared.GetName()]
			declaredInfo := declared.GetInfo()
			if packedColumn == nil {
				if declaredInfo.AutoincrementRandBits < 0 {
					panic(fmt.Sprintf(`Table "%v": Column "%v" in KeyIntPacking not found`, core.Name, declared.GetName()))
				}
				// An autoincrement placeholder: a slot filled from the sequence, with no record field.
				placeholder := &ColumnInfo{
					ColInfo:               ColInfo{Name: "autoincrement_placeholder", IsVirtual: true},
					AutoincrementRandBits: declaredInfo.AutoincrementRandBits,
					SlotBits:              declaredInfo.SlotBits,
				}
				packedColumn = placeholder
				core.AutoincrementCol = placeholder
			} else if declaredInfo.SlotBits > 0 {
				// A real column takes the SlotBits set in KeyIntPacking.
				packedColumn.SetSlotBits(declaredInfo.SlotBits)
			}
			core.KeyIntPacking = append(core.KeyIntPacking, packedColumn)
		}
		core.KeyIntPackingSlotBits = KeyIntPackingSlotBits(core.Name, schema.KeyIntPacking)
	}

	if len(schema.KeyConcatenated) > 0 {
		if len(core.Keys) != 1 {
			panic(fmt.Sprintf(`Table "%v": KeyConcatenated requires exactly one column in Keys. Found: %v`, core.Name, len(core.Keys)))
		}
		keyColumn := core.Keys[0].(*ColumnInfo)
		if keyColumn.Type != 1 { // 1 = string
			panic(fmt.Sprintf(`Table "%v": KeyConcatenated requires the key column to be a string. Found: %v`, core.Name, keyColumn.FieldType))
		}
		concatenatedColumns := []IColInfo{}
		for _, declared := range schema.KeyConcatenated {
			concatenatedColumns = append(concatenatedColumns, core.ColumnsMap[declared.GetName()])
		}
		core.KeyConcatenated = concatenatedColumns
		// The neutral value is the concatenated string; a driver that binds literals wraps GetValueFn.
		keyColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
			values := make([]any, 0, len(concatenatedColumns))
			for _, column := range concatenatedColumns {
				values = append(values, column.GetRawValue(ptr))
			}
			return MakeKeyConcat(values...)
		}
		keyColumn.GetValueFn = keyColumn.GetRawValueFn
	}
}

// bindManagedAuditColumns adds created / updated (and version on a TypeDelta table). A column the
// record does not declare is stored all the same; a column the cache protocols hand to the client
// must be declared.
func (core *TableCore) bindManagedAuditColumns(schema TableSchema) {
	hasDeltaIndex := slices.ContainsFunc(schema.Indexes, func(index Index) bool { return index.Type == TypeDelta })
	// A column already in ColumnsMap was declared by the table struct, so it is backed by a record
	// field and reaches the client: the cache protocols need both of these there.
	if _, isDeclared := core.ColumnsMap[ColumnNameUpdated]; schema.CacheByIDs && !isDeclared {
		panic(fmt.Sprintf(`Table "%v": CacheByIDs returns each record's slot tick in its Updated. `+
			`Declare the field "Updated int32" with the json tag "upd,omitempty" in the record struct, `+
			`and "Updated db.Col[...]" in the table struct.`, core.Name))
	}
	// The delta key packs the record's Updated and a Delta() read hands it to the client as the
	// cursor: a DB-only column reads nil from the record, so every key would pack updated = 0.
	if _, isDeclared := core.ColumnsMap[ColumnNameUpdated]; hasDeltaIndex && !isDeclared {
		panic(fmt.Sprintf(`Table "%v": TypeDelta packs each record's Updated into its key and returns it as the cursor. `+
			`Declare the field "Updated int32" with the json tag "upd" in the record struct, `+
			`and "Updated db.Col[...]" in the table struct.`, core.Name))
	}
	if _, isDeclared := core.ColumnsMap[ColumnNameVersion]; hasDeltaIndex && !isDeclared {
		panic(fmt.Sprintf(`Table "%v": TypeDelta logs each write's Version and the client compares it. `+
			`Declare the field "Version int16" with the json tag "ver,omitempty" in the record struct, `+
			`and "Version db.Col[...]" in the table struct.`, core.Name))
	}

	if !schema.DisableDefaultColumns || schema.CacheByIDs {
		core.CreatedCol = core.ensureManagedIntColumn(ColumnNameCreated)
		core.UpdatedCol = core.ensureManagedIntColumn(ColumnNameUpdated)
	}
	// The version costs a read per write, so only a table with a delta log keeps it.
	if hasDeltaIndex {
		core.VersionCol = core.ensureManagedIntColumn(ColumnNameVersion)
	}
}

// ensureManagedIntColumn returns the declared managed column, or adds a DB-only one: it exists in the
// table even when the record struct does not expose it, and the write path stamps its value.
func (core *TableCore) ensureManagedIntColumn(columnName string) IColInfo {
	if currentColumn := core.ColumnsMap[columnName]; currentColumn != nil {
		if currentColumn.GetType().IsSlice || !isManagedIntFieldType(currentColumn.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": managed column "%v" must be an integer scalar. Found: %v`,
				core.Name, columnName, currentColumn.GetType().FieldType))
		}
		return currentColumn
	}
	core.MaxColIdx++
	managedColumn := &ColumnInfo{
		ColInfo:             ColInfo{Name: columnName, FieldName: columnName, Idx: core.MaxColIdx, RefType: reflect.TypeOf(int32(0))},
		ColType:             GetColTypeByID(3),
		GetRawValueFn:       func(ptr unsafe.Pointer) any { return nil },
		GetStatementValueFn: func(ptr unsafe.Pointer) any { return nil },
		GetValueFn:          func(ptr unsafe.Pointer) any { return nil },
	}
	core.ColumnsMap[columnName] = managedColumn
	return managedColumn
}

func isManagedIntFieldType(fieldType string) bool {
	switch fieldType {
	case "int", "int8", "int16", "int32", "int64":
		return true
	}
	return false
}
