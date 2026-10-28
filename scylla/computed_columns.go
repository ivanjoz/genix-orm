package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// computedColumnInfo is one TableSchema.Computed column: fromIdx are the stored columns it is
// derived from, so an update that writes any of them rehashes its index groups.
type computedColumnInfo struct {
	column  IColInfo
	fromIdx []int16
}

// configureComputedColumns binds the table-struct columns that have no record field
// (pendingColumns, by name) to their TableSchema.Computed declaration. The column goes into
// ColumnsMap, where the index groups and the query planner find it by name, but never into
// Columns, so no insert, update, select or deploy sees it.
func configureComputedColumns(dbTable *ScyllaTable, schema TableSchema, structRefValue reflect.Value,
	pendingColumns map[string]columnInfo,
) {
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": `+format, append([]any{dbTable.Name}, args...)...))
	}

	var recordType reflect.Type
	if recordTyped, isTyped := structRefValue.Addr().Interface().(interface{ BaseStructType() reflect.Type }); isTyped {
		recordType = recordTyped.BaseStructType()
	}

	for _, computed := range schema.Computed {
		columnName := computed.Col.GetName()
		column, isPending := pendingColumns[columnName]
		if !isPending {
			fail(`Computed column "%v" must be a table-struct column with no field in the record struct`, columnName)
		}
		delete(pendingColumns, columnName)

		if computed.Value == nil {
			fail(`Computed column "%v" needs a Value: db.Compute((*Record).Method)`, columnName)
		}
		if recordType != nil && computed.Value.RecordType != recordType {
			fail(`Computed column "%v" reads a method of %v, the table's record is %v`,
				columnName, computed.Value.RecordType, recordType)
		}
		if valueType := db.GetColTypeByGoType(computed.Value.ValueType); valueType.Type != column.Type {
			fail(`Computed column "%v" is declared as %v but its method returns %v`,
				columnName, column.FieldType, computed.Value.ValueType)
		}
		if len(computed.From) == 0 {
			fail(`Computed column "%v" needs From: the stored columns its value is derived from`, columnName)
		}

		fromIdx := make([]int16, 0, len(computed.From))
		for _, fromColumn := range computed.From {
			storedColumn := dbTable.ColumnsMap[fromColumn.GetName()]
			if storedColumn == nil || storedColumn.IsNil() || storedColumn.GetInfo().Field == nil {
				fail(`Computed column "%v": From column "%v" is not a stored column`, columnName, fromColumn.GetName())
			}
			fromIdx = append(fromIdx, storedColumn.GetInfo().Idx)
		}

		// IsVirtual keeps it out of every projection that walks ColumnsMap (views, view tables).
		column.IsVirtual = true
		column.IsComputed = true
		// The managed columns take MaxColIdx after incrementing it and the generated index columns
		// before, so stepping past it on both sides keeps this slot from colliding with either.
		dbTable.MaxColIdx++
		column.Idx = dbTable.MaxColIdx
		dbTable.MaxColIdx++
		readValue := computed.Value.Read
		column.GetRawValueFn = func(ptr unsafe.Pointer) any { return readValue(ptr) }
		column.GetStatementValueFn = column.GetRawValueFn
		column.GetValueFn = column.GetRawValueFn
		// A scan never carries it; anything handed to it is dropped.
		column.SetValueFn = func(ptr unsafe.Pointer, value any) {}

		computedColumn := &column
		dbTable.ColumnsMap[columnName] = computedColumn
		dbTable.computedColumns = append(dbTable.computedColumns, computedColumnInfo{column: computedColumn, fromIdx: fromIdx})
	}

	for columnName := range pendingColumns {
		fail(`column "%v" has no field in the record struct. Add the field, or declare it in Computed`, columnName)
	}

	rejectComputedColumnUses(dbTable, schema)
}

// rejectComputedColumnUses fails the compile when a computed column is named anywhere that needs it
// stored. A multi-column UseIndexGroup is the one use allowed: it stores hashes computed from the
// record in memory. A single-column group indexes the column itself.
func rejectComputedColumnUses(dbTable *ScyllaTable, schema TableSchema) {
	if len(dbTable.computedColumns) == 0 {
		return
	}
	storedUses := []Coln{schema.Partition, schema.SequenceColumn, schema.CounterColumn, schema.SequencePartCol,
		schema.AutoincrementPart, schema.GenericRecord.Name, schema.GenericRecord.S1, schema.GenericRecord.S2,
		schema.GenericRecord.N1, schema.GenericRecord.N2}
	storedUses = slices.Concat(storedUses, schema.Keys, schema.KeyConcatenated, schema.KeyIntPacking)
	for _, fixedValues := range schema.FixedValues {
		storedUses = append(storedUses, fixedValues.Col)
	}
	if schema.TextSearch != nil {
		storedUses = slices.Concat(storedUses, schema.TextSearch.Columns, []Coln{schema.TextSearch.Status})
	}
	for _, frame := range schema.DataFrames {
		storedUses = slices.Concat(storedUses, frame.Keys, frame.Rows, frame.Sums)
	}
	for _, index := range schema.Indexes {
		storedUses = slices.Concat(storedUses, index.Cols, []Coln{index.Partition})
		isHashedIndexGroup := index.UseIndexGroup && len(index.Keys) > 1 && index.Type != TypeInheritFromKey
		if !isHashedIndexGroup {
			storedUses = append(storedUses, index.Keys...)
		}
	}
	for _, computed := range schema.Computed {
		storedUses = append(storedUses, computed.From...)
	}

	for _, use := range storedUses {
		if use == nil {
			continue
		}
		if column := dbTable.ColumnsMap[use.GetName()]; column != nil && !column.IsNil() && column.GetInfo().IsComputed {
			panic(fmt.Sprintf(`Table "%v": computed column "%v" has no storage: only a UseIndexGroup of two or more Keys can name it`,
				dbTable.Name, use.GetName()))
		}
	}
}

// includeComputedColumns adds to an update's included columns every computed column whose From
// columns the update writes, so the index groups hashed from it are rewritten too.
func includeComputedColumns(scyllaTable *ScyllaTable, columnsIncluded []int16) []int16 {
	for _, computed := range scyllaTable.computedColumns {
		if slices.ContainsFunc(computed.fromIdx, func(idx int16) bool { return slices.Contains(columnsIncluded, idx) }) {
			columnsIncluded = append(columnsIncluded, computed.column.GetInfo().Idx)
		}
	}
	return columnsIncluded
}

// storedColumnsOf is what has to be read from the table to know a column's value: the column itself,
// or the From columns of a computed one.
func storedColumnsOf(scyllaTable *ScyllaTable, column IColInfo) []IColInfo {
	if !column.GetInfo().IsComputed {
		return []IColInfo{column}
	}
	for _, computed := range scyllaTable.computedColumns {
		if computed.column.GetName() != column.GetName() {
			continue
		}
		storedColumns := make([]IColInfo, 0, len(computed.fromIdx))
		for _, fromIdx := range computed.fromIdx {
			storedColumns = append(storedColumns, scyllaTable.ColumnsIdxMap[fromIdx])
		}
		return storedColumns
	}
	return nil
}

// rejectComputedStatements fails a query whose predicates on a computed column were not taken by an
// index group: there is no stored column for a WHERE to filter.
func rejectComputedStatements(scyllaTable ScyllaTable, remainingStatements []ColumnStatement) error {
	for _, statement := range remainingStatements {
		if column := scyllaTable.ColumnsMap[statement.Col]; column != nil && !column.IsNil() && column.GetInfo().IsComputed {
			return fmt.Errorf(`table "%v": computed column "%v" can only be queried through a UseIndexGroup that names it, with every Key of that index`,
				scyllaTable.Name, statement.Col)
		}
	}
	return nil
}
