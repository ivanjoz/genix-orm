package pgdriver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// How each described index (db.IndexSource) is stored on PostgreSQL. Every index kind is a native
// synchronous index named <table>__ix_<source> (index names are per schema here); there are no slice
// side tables:
//   - an index keyed on scalars is a B-tree index. A packed, hashed or delta index keys on its
//     generated column, which the main table then stores;
//   - an index keyed on a slice (a global/local index on a slice column, a cache group over a slice)
//     stores that slice as JSONB and is a GIN index over its elements;
//   - a view or delta index carries its projection with INCLUDE when every projected column fits a
//     B-tree entry, which excludes TEXT, BYTEA and JSONB.

// elementSetTypes are the slice element types a GIN index serves: integers only (a string slice
// would need its own exactness rules, a float one has no exact element equality).
var elementSetTypes = map[string]bool{"int64": true, "int32": true, "int16": true, "int8": true, "int": true}

// unincludableColumnTypes are the types an INCLUDE leaves out: a large value would break the B-tree
// entry size limit.
var unincludableColumnTypes = map[string]bool{"TEXT": true, "BYTEA": true, "JSONB": true}

// mapTableObjects builds the main table (columns, primary key, native indexes) and its side tables.
func mapTableObjects(table *PgTable) (tableSpec, []tableSpec) {
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
	// strings compare bytes) and which slices are JSONB.
	indexKeyParts := make([][]indexKeyPart, len(table.IndexSources))
	keyedColumnNames := map[string]bool{}
	elementSetColumns := map[string]bool{}
	for sourceIndex, source := range table.IndexSources {
		switch source.Kind {
		case db.IndexKindViewTable, db.IndexKindCompositeBucket:
			panic(fmt.Sprintf(`pgdriver: table "%v" index "%v": %v indexes are not implemented on PostgreSQL yet`,
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
				assertElementSetType(table, source, column)
				part.isElementSet = true
				elementSetColumns[columnName] = true
			}
			keyedColumnNames[columnName] = true
			indexKeyParts[sourceIndex] = append(indexKeyParts[sourceIndex], part)
		}
	}

	// The real columns, in column order, then the generated columns an index keys on.
	mainTable := tableSpec{database: table.Namespace, name: table.Name, primaryKey: primaryKey}
	columnTypeByName := map[string]string{}
	storedColumnNames := []string{}
	addColumn := func(columnName, sqlType string) {
		isKey := slices.Contains(primaryKey, columnName)
		switch {
		case elementSetColumns[columnName]:
			sqlType = "JSONB"
		case sqlType == "TEXT" && (isKey || keyedColumnNames[columnName]):
			sqlType = keyedStringType()
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
		index := indexSpec{name: indexNameOf(table, source), keyParts: indexKeyParts[sourceIndex], isUnique: source.Kind == db.IndexKindUnique}
		if (source.Kind == db.IndexKindView || source.Kind == db.IndexKindDelta) && !index.isElementSet() {
			keyColumnNames := make([]string, len(index.keyParts))
			for partIndex, part := range index.keyParts {
				keyColumnNames[partIndex] = part.column
			}
			includedColumns := viewIncludedColumns(source, storedColumnNames, keyColumnNames, primaryKey)
			// Covering only when nothing in the projection can outgrow a B-tree entry.
			if !slices.ContainsFunc(includedColumns, func(columnName string) bool {
				return unincludableColumnTypes[strings.TrimSuffix(columnTypeByName[columnName], ` COLLATE "C"`)]
			}) {
				index.includedColumns = includedColumns
			}
		}
		mainTable.indexes = append(mainTable.indexes, index)
	}

	sideTables := []tableSpec{}
	if len(table.DeltaIndexes) > 0 {
		sideTables = append(sideTables, deltaMovesTableSpec(table.Namespace, table.Name, columnTypeByName[table.Keys[0].GetName()]))
	}
	if len(table.IndexGroups) > 0 {
		sideTables = append(sideTables, indexUpdatedTableSpec(table.Namespace, table.Name), groupDataTableSpec(table.Namespace, table.Name))
	}
	if table.dataFrames != nil {
		sideTables = append(sideTables, table.dataFrames.aggregateTables...)
	}
	return mainTable, sideTables
}

func isGeneratedColumn(column db.IColInfo) bool {
	return column != nil && !column.IsNil() && column.GetInfo().IsVirtual
}

// columnByName resolves a key column: the source's generated column, or a stored column.
func columnByName(table *PgTable, source *db.IndexSource, columnName string) db.IColInfo {
	if isGeneratedColumn(source.Column) && source.Column.GetName() == columnName {
		return source.Column
	}
	return table.ColumnsMap[columnName]
}

// assertElementSetType refuses a slice key column a GIN index cannot serve exactly. A cache group's
// hash set holds int32 hashes; any other slice must hold integers.
func assertElementSetType(table *PgTable, source *db.IndexSource, sliceColumn db.IColInfo) {
	if source.IndexGroup != nil {
		return
	}
	elementType := strings.TrimPrefix(strings.TrimPrefix(sliceColumn.GetType().FieldType, "*"), "[]")
	if !elementSetTypes[elementType] {
		panic(fmt.Sprintf(`pgdriver: table "%v" index "%v": slice column "%v" of %v cannot be indexed on PostgreSQL (only integer slices)`,
			table.Name, source.Name, sliceColumn.GetName(), sliceColumn.GetType().FieldType))
	}
}

// indexNameOf is the native index an IndexSource is stored as. Index names are per schema in
// PostgreSQL, so the table's name leads unless the source's name already does.
func indexNameOf(table *PgTable, source *db.IndexSource) string {
	if strings.HasPrefix(source.Name, table.Name+"__") {
		return boundedIdentifier(table.Name + "__ix_" + strings.TrimPrefix(source.Name, table.Name+"__"))
	}
	return boundedIdentifier(table.Name + "__ix_" + source.Name)
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
// layout db packs, stored in a BIGINT with its top bit flipped (bindArguments). A group hash set is a
// JSONB array; a hash column keeps its integer type.
func generatedColumnType(source *db.IndexSource) string {
	switch {
	case source.PackedIndex != nil, len(source.PackedSourceColumns) > 0:
		return "BIGINT"
	case source.Column.GetType().IsSlice:
		return "JSONB"
	}
	return source.Column.GetType().DBType
}

// viewIncludedColumns are the columns a view serves without the main table: its projection, or every
// stored column, minus what the index row already holds (its key and the primary key).
func viewIncludedColumns(source *db.IndexSource, storedColumnNames, indexKey, primaryKey []string) []string {
	servedColumnNames := storedColumnNames
	if len(source.ProjectedColumns) > 0 {
		servedColumnNames = nil
		for _, projectedColumn := range source.ProjectedColumns {
			servedColumnNames = append(servedColumnNames, projectedColumn.GetName())
		}
	}
	includedColumns := []string{}
	for _, columnName := range servedColumnNames {
		if !slices.Contains(indexKey, columnName) && !slices.Contains(primaryKey, columnName) {
			includedColumns = append(includedColumns, columnName)
		}
	}
	return includedColumns
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
