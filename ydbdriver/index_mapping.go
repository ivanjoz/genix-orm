package ydbdriver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// How each described index (db.IndexSource) is stored on YDB (docs/YDB_PLAN.md §8.2–§8.4):
//   - an index keyed on scalars is a native GLOBAL ASYNC index named ix_<source>. A packed, hashed
//     or delta index keys on its generated column, which the main table then stores;
//   - an index keyed on a slice (a global/local index on a slice column, a cache group over a
//     slice) is a key-only side table <table>__sx_<source>, one row per element.
//
// A view (Type 6/7/8, TypeDelta included) covers the columns it serves, so a read through it needs
// no join back to the main table.

// ydbScalarTypeByFieldType is the YDB type of one element of a slice column.
var ydbScalarTypeByFieldType = map[string]string{
	"string": "Utf8", "int64": "Int64", "int32": "Int32", "int16": "Int16", "int8": "Int8", "int": "Int64",
	"float32": "Float", "float64": "Double", "bool": "Bool",
}

// mapTableObjects builds the main table (columns, primary key, native indexes) and its side tables.
func mapTableObjects(table *YdbTable, hasDataFrames bool) (tableSpec, []tableSpec) {
	partitionColumn := table.GetPartKey()
	if partitionColumn != nil && partitionColumn.IsNil() {
		partitionColumn = nil
	}
	primaryKey := []string{}
	if partitionColumn != nil {
		primaryKey = append(primaryKey, partitionColumn.GetName())
	}
	for _, keyColumn := range table.Keys {
		primaryKey = append(primaryKey, keyColumn.GetName())
	}

	// The real columns, in column order; generated columns join them when a native index keys on one.
	mainTable := tableSpec{path: table.path, primaryKey: primaryKey}
	storedColumnNames := []string{}
	for _, column := range table.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		mainTable.columns = append(mainTable.columns, columnSpec{column.GetName(), column.GetType().DBType, slices.Contains(primaryKey, column.GetName())})
		storedColumnNames = append(storedColumnNames, column.GetName())
	}

	sideTables := []tableSpec{}
	for _, source := range table.IndexSources {
		switch source.Kind {
		case db.IndexKindViewTable, db.IndexKindCompositeBucket, db.IndexKindUnique:
			panic(fmt.Sprintf(`ydbdriver: table "%v" index "%v": %v indexes are not implemented on YDB yet`,
				table.Name, source.Name, kindName(source.Kind)))
		}
		if sourceKeysOnSlice(table, source) {
			sideTables = append(sideTables, sliceSideTable(table, source, partitionColumn))
			continue
		}

		index := nativeIndexSpec{name: "ix_" + source.Name, keyColumns: source.Columns}
		if source.Column != nil && !source.Column.IsNil() && source.Column.GetInfo().IsVirtual {
			// A generated key column: the source's partition (none for a global index), then the column.
			index.keyColumns = nil
			if sourcePartition := sourcePartitionColumn(source, partitionColumn); sourcePartition != nil {
				index.keyColumns = append(index.keyColumns, sourcePartition.GetName())
			}
			index.keyColumns = append(index.keyColumns, source.Column.GetName())
			if !slices.ContainsFunc(mainTable.columns, func(column columnSpec) bool { return column.name == source.Column.GetName() }) {
				mainTable.columns = append(mainTable.columns, columnSpec{source.Column.GetName(), generatedColumnType(source), false})
			}
		}
		if source.Kind == db.IndexKindView || source.Kind == db.IndexKindDelta {
			index.coverColumns = viewCoverColumns(source, storedColumnNames, index.keyColumns, primaryKey)
		}
		mainTable.indexes = append(mainTable.indexes, index)
	}

	if len(table.DeltaIndexes) > 0 {
		sideTables = append(sideTables, deltaMovesTableSpec(table.path, table.Keys[0].GetType().DBType))
	}
	if len(table.IndexGroups) > 0 {
		sideTables = append(sideTables, indexUpdatedTableSpec(table.path))
	}
	if hasDataFrames {
		sideTables = append(sideTables, dataFrameLogTableSpec(table.path, table.Keys[0].GetType().DBType))
	}
	return mainTable, sideTables
}

// sourceKeysOnSlice: an index whose key holds a slice column (or whose generated column is a set of
// hashes) cannot be a YDB index, which keys on scalars only.
func sourceKeysOnSlice(table *YdbTable, source *db.IndexSource) bool {
	if source.Column != nil && !source.Column.IsNil() && source.Column.GetType().IsSlice {
		return true
	}
	return slices.ContainsFunc(source.Columns, func(columnName string) bool {
		column := table.ColumnsMap[columnName]
		return column != nil && !column.GetInfo().IsVirtual && column.GetType().IsSlice
	})
}

// sliceSideTable is the side table of an index keyed on a slice: (partition, element, main key) for
// a partitioned index, (element, partition, main key) for a global one. A cache group over a slice
// stores its group hashes as the elements.
func sliceSideTable(table *YdbTable, source *db.IndexSource, partitionColumn db.IColInfo) tableSpec {
	elementType := "Int32" // a cache group's hash
	if source.IndexGroup == nil {
		sliceColumn := source.Column
		if sliceColumn == nil || sliceColumn.IsNil() || !sliceColumn.GetType().IsSlice {
			for _, columnName := range source.Columns {
				if column := table.ColumnsMap[columnName]; column.GetType().IsSlice {
					sliceColumn = column
				}
			}
		}
		elementType = ydbScalarTypeByFieldType[strings.TrimPrefix(sliceColumn.GetType().FieldType, "[]")]
		if elementType == "" {
			panic(fmt.Sprintf(`ydbdriver: table "%v" index "%v": slice column "%v" of %v has no YDB element type`,
				table.Name, source.Name, sliceColumn.GetName(), sliceColumn.GetType().FieldType))
		}
	}

	mainKeyColumns := []columnSpec{}
	var sidePartition *columnSpec
	if sourcePartition := sourcePartitionColumn(source, partitionColumn); sourcePartition != nil {
		sidePartition = &columnSpec{sourcePartition.GetName(), sourcePartition.GetType().DBType, true}
	} else if partitionColumn != nil {
		// A global index keeps the record's partition in its key, after the element.
		mainKeyColumns = append(mainKeyColumns, columnSpec{partitionColumn.GetName(), partitionColumn.GetType().DBType, true})
	}
	for _, keyColumn := range table.Keys {
		mainKeyColumns = append(mainKeyColumns, columnSpec{keyColumn.GetName(), keyColumn.GetType().DBType, true})
	}
	// Y8: no index here orders its elements by another key column, so there is nothing to refuse yet;
	// the check stays where an order column would be added.
	assertSliceOrderColumns(table.Name, source.Name, nil)
	return sliceSideTableSpec(table.path, source.Name, sidePartition, elementType, nil, mainKeyColumns)
}

// sourcePartitionColumn is the partition an index is keyed under: none for a global index (Type 1),
// the view's own partition when it overrides the table's, the table's otherwise.
func sourcePartitionColumn(source *db.IndexSource, partitionColumn db.IColInfo) db.IColInfo {
	if source.Type == 1 {
		return nil
	}
	if source.PartitionColumn != nil && !source.PartitionColumn.IsNil() {
		return source.PartitionColumn
	}
	return partitionColumn
}

// generatedColumnType: a packed column (range view, delta view, packed index) holds the unsigned
// layout db packs, which YDB compares unsigned as it is; a hash column keeps its integer type.
func generatedColumnType(source *db.IndexSource) string {
	switch {
	case source.PackedIndex != nil && source.PackedIndex.IsInt32Packed, len(source.PackedSourceColumns) > 0 && source.PackedIsInt32:
		return "Uint32"
	case source.PackedIndex != nil, len(source.PackedSourceColumns) > 0:
		return "Uint64"
	}
	return source.Column.GetType().DBType
}

// viewCoverColumns are the columns a view serves without the main table: its projection, or every
// stored column, minus what YDB already holds in the index (its key and the primary key).
func viewCoverColumns(source *db.IndexSource, storedColumnNames, indexKey, primaryKey []string) []string {
	servedColumnNames := storedColumnNames
	if len(source.ProjectedColumns) > 0 {
		servedColumnNames = nil
		for _, projectedColumn := range source.ProjectedColumns {
			servedColumnNames = append(servedColumnNames, projectedColumn.GetName())
		}
	}
	coverColumns := []string{}
	for _, columnName := range servedColumnNames {
		if !slices.Contains(indexKey, columnName) && !slices.Contains(primaryKey, columnName) {
			coverColumns = append(coverColumns, columnName)
		}
	}
	return coverColumns
}

func kindName(kind db.IndexKind) string {
	switch kind {
	case db.IndexKindViewTable:
		return "TypeViewTable"
	case db.IndexKindCompositeBucket:
		return "composite-bucketing"
	case db.IndexKindUnique:
		return "TypeUniqueIndex"
	}
	return fmt.Sprintf("kind %d", kind)
}
