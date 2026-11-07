package mariadbdriver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// How each described index (db.IndexSource) is stored on MariaDB. Every index is synchronous, written
// in the record's transaction:
//   - an index keyed on scalars is a native index named ix_<source>. A packed, hashed or delta index
//     keys on its generated column, which the main table then stores;
//   - an index on a slice column stores that slice as JSON in the main table, and its elements in an
//     element side table (ddl.go, elementIndexTableSpec): MariaDB has no multi-value index;
//   - the cache groups over a slice share one table of raw values per slice (group_elements.go);
//   - no index is covering: MariaDB has no included columns;
//   - a scalar cache group over several keys is a composite index on its real columns, not a hash
//     column: an equality per key, no generated column to write, no hash collision.
//
// There are no partitions: the partition column leads the primary key and every index, so a
// partition's rows sit together in each B-tree.

// indexableElementTypes are the slice element types an element side table holds (a BIGINT). A string
// or float slice is refused, as on OceanBase.
var indexableElementTypes = map[string]bool{"int64": true, "int32": true, "int16": true, "int8": true, "int": true}

// mapTableObjects builds the main table (columns, primary key, native indexes), its side tables, and
// the element indexes of its slices.
func mapTableObjects(table *MariaTable) (tableSpec, []tableSpec, []elementIndex, []groupElementTable) {
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
	elementSourceByColumn := map[string]string{}
	for sourceIndex, source := range table.IndexSources {
		switch source.Kind {
		case db.IndexKindViewTable, db.IndexKindCompositeBucket:
			panic(fmt.Sprintf(`mariadbdriver: table "%v" index "%v": %v indexes are not implemented on MariaDB`,
				table.Name, source.Name, kindName(source.Kind)))
		}
		if isGroupOverSlice(source) {
			continue
		}
		keyColumnNames := source.Columns
		if isScalarGroupHash(source) {
			keyColumnNames = append([]string{partitionColumn.GetName()}, source.Columns...)
		} else if isGeneratedColumn(source.Column) {
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
				assertIndexableElements(table, source, column)
				part.isElementSet = true
				// A CONTAINS read finds its element table by the slice column, so a slice has one.
				if otherSource, isTaken := elementSourceByColumn[columnName]; isTaken {
					panic(fmt.Sprintf(`mariadbdriver: table "%v" column "%v": indexes "%v" and "%v" both key on it; MariaDB supports one element index per slice`,
						table.Name, columnName, otherSource, source.Name))
				}
				elementSourceByColumn[columnName] = source.Name
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
		case elementSourceByColumn[columnName] != "":
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
		if isGeneratedColumn(source.Column) && !isGroupOverSlice(source) && !isScalarGroupHash(source) && columnTypeByName[source.Column.GetName()] == "" {
			addColumn(source.Column.GetName(), generatedColumnType(source))
		}
	}

	elementIndexes := []elementIndex{}
	for sourceIndex, source := range table.IndexSources {
		if isGroupOverSlice(source) {
			continue
		}
		index := indexSpec{name: boundedIdentifier("ix_" + source.Name), keyParts: indexKeyParts[sourceIndex]}
		if index.isElementSet() {
			built := elementIndexTableSpec(table.Namespace, table.Name, index, primaryKey, columnTypeByName)
			built.sourceName = source.Name
			elementIndexes = append(elementIndexes, built)
			continue
		}
		mainTable.indexes = append(mainTable.indexes, index)
	}

	sideTables := []tableSpec{}
	for _, built := range elementIndexes {
		sideTables = append(sideTables, built.sideTable)
	}
	elementTables := groupElementTables(table, columnTypeByName)
	for _, built := range elementTables {
		sideTables = append(sideTables, built.sideTable)
	}
	if len(table.DeltaIndexes) > 0 {
		sideTables = append(sideTables, deltaMovesTableSpec(table.Namespace, table.Name, columnTypeByName[table.Keys[0].GetName()]))
	}
	if len(table.IndexGroups) > 0 {
		sideTables = append(sideTables, indexUpdatedTableSpec(table.Namespace, table.Name), groupDataTableSpec(table.Namespace, table.Name))
	}
	if table.dataFrames != nil {
		sideTables = append(sideTables, table.dataFrames.aggregateTables...)
	}
	return mainTable, sideTables, elementIndexes, elementTables
}

func isGeneratedColumn(column db.IColInfo) bool {
	return column != nil && !column.IsNil() && column.GetInfo().IsVirtual
}

// columnByName resolves a key column: the source's generated column, or a stored column.
func columnByName(table *MariaTable, source *db.IndexSource, columnName string) db.IColInfo {
	if isGeneratedColumn(source.Column) && source.Column.GetName() == columnName {
		return source.Column
	}
	return table.ColumnsMap[columnName]
}

// assertIndexableElements: a cache group's hash set holds int32 hashes; any other slice must hold
// integers.
func assertIndexableElements(table *MariaTable, source *db.IndexSource, sliceColumn db.IColInfo) {
	if source.IndexGroup != nil {
		return
	}
	elementType := strings.TrimPrefix(strings.TrimPrefix(sliceColumn.GetType().FieldType, "*"), "[]")
	if !indexableElementTypes[elementType] {
		panic(fmt.Sprintf(`mariadbdriver: table "%v" index "%v": slice column "%v" of %v cannot be indexed on MariaDB (only integer slices)`,
			table.Name, source.Name, sliceColumn.GetName(), sliceColumn.GetType().FieldType))
	}
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
// layout db packs, which MariaDB compares unsigned as it is (no Scylla sign flip). A group hash
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

func kindName(kind db.IndexKind) string {
	switch kind {
	case db.IndexKindViewTable:
		return "TypeViewTable"
	case db.IndexKindCompositeBucket:
		return "composite-bucketing"
	}
	return fmt.Sprintf("kind %d", kind)
}

// isScalarGroupHash: a cache group over several scalar keys, which Scylla serves through a hash column.
func isScalarGroupHash(source *db.IndexSource) bool {
	return source.Kind == db.IndexKindGroupHash && source.IndexGroup != nil && !source.IndexGroup.UsesCollectionValues
}
