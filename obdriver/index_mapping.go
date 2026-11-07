package obdriver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// How each described index (db.IndexSource) is stored on OceanBase (docs/OCEANBASE_PLAN.md §8.1–§8.4).
// Every index kind is a native synchronous index named ix_<source>; there are no slice side tables:
//   - an index keyed on scalars is a plain index. A packed, hashed or delta index keys on its
//     generated column, which the main table then stores;
//   - an index keyed on a slice (a global/local index on a slice column, a cache group over a slice)
//     stores that slice as JSON and is a multi-value index over its elements (O11). OceanBase only
//     builds multi-value indexes LOCAL;
//   - an index whose first key part is not the table's partition column is GLOBAL (O9);
//   - a view or delta index carries its projection with STORING only when every projected column
//     can be stored in an index, which excludes LONGTEXT, LONGBLOB and JSON (D2).

// multiValueCastByElementType is the CAST of a slice's elements in a multi-value index. A string
// slice is refused (D3: the server misses case-variant elements of one record), and so is a float
// slice (no exact element equality to index).
var multiValueCastByElementType = map[string]string{
	"int64": "SIGNED", "int32": "SIGNED", "int16": "SIGNED", "int8": "SIGNED", "int": "SIGNED",
}

// unstorableColumnTypes are the types STORING refuses (§18b: LOBs and JSON).
var unstorableColumnTypes = map[string]bool{"LONGTEXT": true, "LONGBLOB": true, "JSON": true}

// mapTableObjects builds the main table (columns, primary key, native indexes) and its side tables.
func mapTableObjects(table *ObTable, hasDataFrames bool, partitionCount int) (tableSpec, []tableSpec) {
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

	// First pass: the key parts of every index, which also decides which columns are keyed (their
	// strings become VARCHAR) and which slices are JSON.
	indexKeyParts := make([][]indexKeyPart, len(table.IndexSources))
	keyedColumnNames := map[string]bool{}
	multiValueSourceByColumn := map[string]string{}
	for sourceIndex, source := range table.IndexSources {
		switch source.Kind {
		case db.IndexKindViewTable, db.IndexKindCompositeBucket:
			panic(fmt.Sprintf(`obdriver: table "%v" index "%v": %v indexes are not implemented on OceanBase yet`,
				table.Name, source.Name, kindName(source.Kind)))
		}
		keyColumnNames := source.Columns
		if isGeneratedColumn(source.Column) {
			// A generated key column: the source's partition (none for a global index), then the column.
			keyColumnNames = nil
			if sourcePartition := sourcePartitionColumn(source, partitionColumn); sourcePartition != nil {
				keyColumnNames = append(keyColumnNames, sourcePartition.GetName())
			}
			keyColumnNames = append(keyColumnNames, source.Column.GetName())
		}
		for _, columnName := range keyColumnNames {
			part := indexKeyPart{column: columnName}
			if column := columnByName(table, source, columnName); column != nil && column.GetType().IsSlice {
				part.multiValueCast = multiValueCast(table, source, column)
				// O18: a second multi-value index on the same JSON column makes the server use neither.
				if otherSource, isTaken := multiValueSourceByColumn[columnName]; isTaken {
					panic(fmt.Sprintf(`obdriver: table "%v" column "%v": indexes "%v" and "%v" both key on it; OceanBase uses no multi-value index of a column that has two`,
						table.Name, columnName, otherSource, source.Name))
				}
				multiValueSourceByColumn[columnName] = source.Name
			}
			keyedColumnNames[columnName] = true
			indexKeyParts[sourceIndex] = append(indexKeyParts[sourceIndex], part)
		}
	}

	// The real columns, in column order, then the generated columns an index keys on.
	mainTable := tableSpec{
		database: table.Namespace, name: table.Name, primaryKey: primaryKey, partitionCount: partitionCount,
	}
	if partitionColumn != nil {
		mainTable.partitionColumn = partitionColumn.GetName()
	}
	columnTypeByName := map[string]string{}
	storedColumnNames := []string{}
	addColumn := func(columnName, sqlType string) {
		isKey := slices.Contains(primaryKey, columnName)
		switch {
		case multiValueSourceByColumn[columnName] != "":
			sqlType = "JSON"
		case sqlType == "LONGTEXT" && (isKey || keyedColumnNames[columnName]):
			sqlType = keyedStringType
		}
		mainTable.columns = append(mainTable.columns, columnSpec{columnName, sqlType, isKey})
		columnTypeByName[columnName] = sqlType
	}
	for _, column := range table.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		addColumn(column.GetName(), column.GetType().DBType)
		storedColumnNames = append(storedColumnNames, column.GetName())
	}
	for _, source := range table.IndexSources {
		if isGeneratedColumn(source.Column) && columnTypeByName[source.Column.GetName()] == "" {
			addColumn(source.Column.GetName(), generatedColumnType(source))
		}
	}

	for sourceIndex, source := range table.IndexSources {
		index := indexSpec{name: boundedIdentifier("ix_" + source.Name), keyParts: indexKeyParts[sourceIndex]}
		// O9: an index not led by the partition column spans partitions. Multi-value indexes can only
		// be LOCAL (§18b), so a global slice index stays local and a lookup visits every partition.
		index.isGlobal = partitionColumn != nil && index.keyParts[0].column != partitionColumn.GetName() && !index.isMultiValue()
		if source.Kind == db.IndexKindView || source.Kind == db.IndexKindDelta {
			keyColumnNames := make([]string, len(index.keyParts))
			for partIndex, part := range index.keyParts {
				keyColumnNames[partIndex] = part.column
			}
			storingColumns := viewStoringColumns(source, storedColumnNames, keyColumnNames, primaryKey)
			// D2: covering only when nothing in the projection is a LOB or JSON column.
			if !slices.ContainsFunc(storingColumns, func(columnName string) bool { return unstorableColumnTypes[columnTypeByName[columnName]] }) {
				index.storingColumns = storingColumns
			}
		}
		mainTable.indexes = append(mainTable.indexes, index)
	}

	sideTables := []tableSpec{}
	if len(table.DeltaIndexes) > 0 {
		sideTables = append(sideTables, deltaMovesTableSpec(table.Namespace, table.Name, columnTypeByName[table.Keys[0].GetName()], partitionCount))
	}
	if len(table.IndexGroups) > 0 {
		sideTables = append(sideTables, indexUpdatedTableSpec(table.Namespace, table.Name, partitionCount))
	}
	if hasDataFrames {
		sideTables = append(sideTables, dataFrameLogTableSpec(table.Namespace, table.Name, columnTypeByName[table.Keys[0].GetName()], partitionCount))
	}
	return mainTable, sideTables
}

func isGeneratedColumn(column db.IColInfo) bool {
	return column != nil && !column.IsNil() && column.GetInfo().IsVirtual
}

// columnByName resolves a key column: the source's generated column, or a stored column.
func columnByName(table *ObTable, source *db.IndexSource, columnName string) db.IColInfo {
	if isGeneratedColumn(source.Column) && source.Column.GetName() == columnName {
		return source.Column
	}
	return table.ColumnsMap[columnName]
}

// multiValueCast is the element CAST of a slice key column: a cache group's hash set holds int32
// hashes; any other slice casts by its element type, and D3 refuses string (and float) slices.
func multiValueCast(table *ObTable, source *db.IndexSource, sliceColumn db.IColInfo) string {
	if source.IndexGroup != nil {
		return "SIGNED"
	}
	elementType := strings.TrimPrefix(strings.TrimPrefix(sliceColumn.GetType().FieldType, "*"), "[]")
	cast := multiValueCastByElementType[elementType]
	if cast == "" {
		panic(fmt.Sprintf(`obdriver: table "%v" index "%v": slice column "%v" of %v cannot be indexed on OceanBase (only integer slices: plan D3)`,
			table.Name, source.Name, sliceColumn.GetName(), sliceColumn.GetType().FieldType))
	}
	return cast
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
// layout db packs, which OceanBase compares unsigned as it is (no Scylla sign flip). A group hash
// set is a JSON array; a hash column keeps its integer type.
func generatedColumnType(source *db.IndexSource) string {
	switch {
	case source.PackedIndex != nil && source.PackedIndex.IsInt32Packed, len(source.PackedSourceColumns) > 0 && source.PackedIsInt32:
		return "INT UNSIGNED"
	case source.PackedIndex != nil, len(source.PackedSourceColumns) > 0:
		return "BIGINT UNSIGNED"
	case source.Column.GetType().IsSlice:
		return "JSON"
	}
	return source.Column.GetType().DBType
}

// viewStoringColumns are the columns a view serves without the main table: its projection, or every
// stored column, minus what the index row already holds (its key and the primary key).
func viewStoringColumns(source *db.IndexSource, storedColumnNames, indexKey, primaryKey []string) []string {
	servedColumnNames := storedColumnNames
	if len(source.ProjectedColumns) > 0 {
		servedColumnNames = nil
		for _, projectedColumn := range source.ProjectedColumns {
			servedColumnNames = append(servedColumnNames, projectedColumn.GetName())
		}
	}
	storingColumns := []string{}
	for _, columnName := range servedColumnNames {
		if !slices.Contains(indexKey, columnName) && !slices.Contains(primaryKey, columnName) {
			storingColumns = append(storingColumns, columnName)
		}
	}
	return storingColumns
}

func kindName(kind db.IndexKind) string {
	switch kind {
	case db.IndexKindViewTable:
		return "TypeViewTable"
	case db.IndexKindCompositeBucket:
		return "composite-bucketing"
	}
	return fmt.Sprintf("kind %d", kind)
}
