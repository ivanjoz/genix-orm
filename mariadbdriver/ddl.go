package mariadbdriver

import (
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
)

// Pure DDL text generation. No connection, no ORM types: callers describe a table with tableSpec and
// get statements back.

// tableOptions are written on every table, even where they equal the server defaults: the server's
// default collation is case-insensitive, which would break key parity with Scylla. The storage engine
// is the server's default_storage_engine (InnoDB, or ROCKSDB for MyRocks), so one build runs on both.
const tableOptions = "DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin"

// tableCollation is what homologation expects to read back.
const tableCollation = "utf8mb4_bin"

type columnSpec struct {
	name    string
	sqlType string
	notNull bool
}

// indexKeyPart is one key part: a column, or (isElementSet) the elements of a JSON array column, which
// MariaDB cannot index natively, so that index lives in an element side table (elementIndexTableSpec).
type indexKeyPart struct {
	column       string
	isElementSet bool
}

// indexSpec is a native, synchronous secondary index of the main table, or (isElementSet) the key of
// an element side table. MariaDB has no included columns, so a view index is not covering: the
// projection is read from the table through the primary key the index row holds.
type indexSpec struct {
	name     string
	keyParts []indexKeyPart
	// isUnique is a TypeUniqueIndex: the database refuses two rows with the same key parts.
	isUnique bool
}

type tableSpec struct {
	database   string
	name       string
	columns    []columnSpec
	primaryKey []string
	indexes    []indexSpec
}

func quoteName(name string) string { return "`" + name + "`" }

// maxIdentifierLength is MariaDB's limit on table, column and index names.
const maxIdentifierLength = 64

// boundedIdentifier keeps a generated name (ix_<source>, <table>__<side table>) within 64
// characters: a longer one keeps its first 55 characters and ends in "_" + the 8-hex FNV-1a hash of
// the full name, so it stays readable, deterministic and unique per full name.
func boundedIdentifier(name string) string {
	if len(name) <= maxIdentifierLength {
		return name
	}
	nameHash := fnv.New32a()
	nameHash.Write([]byte(name))
	return fmt.Sprintf("%s_%08x", name[:maxIdentifierLength-9], nameHash.Sum32())
}

func quotedNames(names []string) string {
	quoted := make([]string, len(names))
	for index, name := range names {
		quoted[index] = quoteName(name)
	}
	return strings.Join(quoted, ", ")
}

func (spec tableSpec) qualifiedName() string {
	return quoteName(spec.database) + "." + quoteName(spec.name)
}

func (index indexSpec) isElementSet() bool {
	for _, part := range index.keyParts {
		if part.isElementSet {
			return true
		}
	}
	return false
}

func (index indexSpec) columnNames() []string {
	names := make([]string, len(index.keyParts))
	for partIndex, part := range index.keyParts {
		names[partIndex] = part.column
	}
	return names
}

// indexDefinition is one index as CREATE TABLE and ADD INDEX both write it.
func indexDefinition(index indexSpec) string {
	indexKeyword := "INDEX"
	if index.isUnique {
		indexKeyword = "UNIQUE INDEX"
	}
	return fmt.Sprintf("%s %s (%s)", indexKeyword, quoteName(index.name), quotedNames(index.columnNames()))
}

func columnDefinition(column columnSpec) string {
	definition := quoteName(column.name) + " " + column.sqlType
	if column.notNull {
		definition += " NOT NULL"
	}
	return definition
}

func createTableStatement(spec tableSpec) string {
	definitions := make([]string, 0, len(spec.columns)+len(spec.indexes)+1)
	for _, column := range spec.columns {
		definitions = append(definitions, columnDefinition(column))
	}
	definitions = append(definitions, fmt.Sprintf("PRIMARY KEY (%s)", quotedNames(spec.primaryKey)))
	for _, index := range spec.indexes {
		definitions = append(definitions, indexDefinition(index))
	}
	return fmt.Sprintf("CREATE TABLE %s (\n    %s\n) %s", spec.qualifiedName(), strings.Join(definitions, ",\n    "), tableOptions)
}

func createDatabaseStatement(database string) string {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin", quoteName(database))
}

func addIndexStatement(spec tableSpec, index indexSpec) string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", spec.qualifiedName(), indexDefinition(index))
}

func dropIndexStatement(spec tableSpec, indexName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", spec.qualifiedName(), quoteName(indexName))
}

// addColumnStatement: an added column is always nullable, so a populated table accepts it.
func addColumnStatement(spec tableSpec, column columnSpec) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", spec.qualifiedName(), quoteName(column.name), column.sqlType)
}

// elementIndex is an index over a slice's elements, stored as its own table: one row per element and
// record. Its primary key is the index's key parts (the slice part holding one element) and then the
// record's primary key, so an element lookup is a key range; recordIndex finds a record's rows to
// rewrite them.
type elementIndex struct {
	sourceName  string
	sliceColumn string
	keyParts    []indexKeyPart
	// recordKeyColumns are the record's primary-key columns the key parts do not already hold.
	recordKeyColumns []string
	sideTable        tableSpec
}

// elementColumnType is the element column of an element side table: every indexed slice holds
// integers (index_mapping.go refuses the others).
const elementColumnType = "BIGINT"

// elementIndexTableSpec is `<table>__<ix name>`: the key parts, then the record's primary key.
func elementIndexTableSpec(database, tableName string, index indexSpec, primaryKey []string, columnTypeByName map[string]string) elementIndex {
	spec := tableSpec{database: database, name: boundedIdentifier(tableName + "__" + index.name)}
	built := elementIndex{keyParts: index.keyParts, sideTable: spec}
	for _, part := range index.keyParts {
		columnType := columnTypeByName[part.column]
		if part.isElementSet {
			columnType = elementColumnType
			built.sliceColumn = part.column
		}
		built.sideTable.columns = append(built.sideTable.columns, columnSpec{part.column, columnType, true})
		built.sideTable.primaryKey = append(built.sideTable.primaryKey, part.column)
	}
	for _, keyColumn := range primaryKey {
		if !slices.Contains(built.sideTable.primaryKey, keyColumn) {
			built.sideTable.columns = append(built.sideTable.columns, columnSpec{keyColumn, columnTypeByName[keyColumn], true})
			built.sideTable.primaryKey = append(built.sideTable.primaryKey, keyColumn)
			built.recordKeyColumns = append(built.recordKeyColumns, keyColumn)
		}
	}
	recordIndexParts := []indexKeyPart{}
	for _, keyColumn := range primaryKey {
		recordIndexParts = append(recordIndexParts, indexKeyPart{column: keyColumn})
	}
	built.sideTable.indexes = []indexSpec{{name: "ix_record", keyParts: recordIndexParts}}
	return built
}

// deltaMovesTableSpec mirrors Scylla's `<table>__delta_moves`.
func deltaMovesTableSpec(database, tableName, idType string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__delta_moves"),
		columns: []columnSpec{
			{"partition_id", "INT", true}, {"index_tag", keyedStringType, true}, {"pinned_prefix", "BIGINT UNSIGNED", true},
			{"updated", "INT", true}, {"id", idType, true},
		},
		primaryKey: []string{"partition_id", "index_tag", "pinned_prefix", "updated", "id"},
	}
}

// indexUpdatedTableSpec mirrors Scylla's `<table>__index_updated` (cache-group marks).
func indexUpdatedTableSpec(database, tableName string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__index_updated"),
		columns: []columnSpec{
			{"partition_id", "INT", true}, {"index_id", "SMALLINT", true}, {"index_value", "BIGINT UNSIGNED", true}, {"last_write", "INT", false},
		},
		primaryKey: []string{"partition_id", "index_id", "index_value"},
	}
}

// groupDataTableSpec is `<table>__group_data`: the record snapshot of each settled cache group,
// labelled built_write (the last_write it was read at), served while its group's mark still holds that
// value. InnoDB as every table: MariaDB cannot drop the redo log of one table, and a snapshot is saved
// rarely (once per settled group that changed and was read), while the reads, frequent, never wait.
func groupDataTableSpec(database, tableName string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__group_data"),
		columns: []columnSpec{
			{"partition_id", "INT", true}, {"index_id", "SMALLINT", true}, {"index_value", "BIGINT UNSIGNED", true},
			{"built_write", "INT", true}, {"data", "LONGBLOB", true},
		},
		primaryKey: []string{"partition_id", "index_id", "index_value"},
	}
}

// cacheSlotsTableName holds the by-IDs cache slots of every CacheByIDs table of a database: per table
// partition, slot uint8(ID) → the Updated of its latest write (as Scylla's cache_by_ids_slots).
const cacheSlotsTableName = "_cache_by_ids_slots"

func cacheSlotsTableSpec(database string) tableSpec {
	return tableSpec{
		database: database, name: cacheSlotsTableName,
		columns: []columnSpec{
			{"table_id", "SMALLINT", true}, {"partition_id", "BIGINT", true}, {"slot", "TINYINT UNSIGNED", true}, {"updated", "INT", true},
		},
		primaryKey: []string{"table_id", "partition_id", "slot"},
	}
}

// sequencesTableSpec is `<database>._sequences`: a handful of rows.
func sequencesTableSpec(database string) tableSpec {
	return tableSpec{
		database: database, name: "_sequences",
		columns:    []columnSpec{{"name", keyedStringType, true}, {"value", "BIGINT", true}},
		primaryKey: []string{"name"},
	}
}
