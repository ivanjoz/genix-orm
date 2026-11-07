package pgdriver

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// Pure DDL text generation. No connection, no ORM types: callers describe a table with tableSpec and
// get statements back. A keyspace is a PostgreSQL schema; its tables are "schema"."table".

type columnSpec struct {
	name    string
	sqlType string
	notNull bool
}

// indexKeyPart is one key part: a column, or the elements of a JSONB array column when isElementSet,
// which makes the index a GIN index.
type indexKeyPart struct {
	column       string
	isElementSet bool
}

// indexSpec is a native, synchronous secondary index. includedColumns exclude the key and the
// primary key, which every index row already holds.
type indexSpec struct {
	name            string
	keyParts        []indexKeyPart
	includedColumns []string
}

type tableSpec struct {
	database   string
	name       string
	columns    []columnSpec
	primaryKey []string
	indexes    []indexSpec
	// isUnlogged creates the table without WAL: for data any read rebuilds (CockroachDB has no such tables).
	isUnlogged bool
}

func quoteName(name string) string { return `"` + name + `"` }

// maxIdentifierLength is PostgreSQL's limit on table, column and index names (NAMEDATALEN - 1).
const maxIdentifierLength = 63

// boundedIdentifier keeps a generated name within 63 characters: a longer one keeps its first 54
// characters and ends in "_" + the 8-hex FNV-1a hash of the full name, so it stays readable,
// deterministic and unique per full name.
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

// createIndexStatement creates one index. An index over a JSONB element set is a GIN index on that
// column alone (jsonb_path_ops serves @>); the other key parts stay in the WHERE as plain conditions.
// Index names are per schema in PostgreSQL, so they carry the table's name (index_mapping.go).
func createIndexStatement(spec tableSpec, index indexSpec) string {
	if index.isElementSet() {
		for _, part := range index.keyParts {
			if part.isElementSet {
				// CockroachDB's inverted index takes no operator class nor storage parameter; it serves @> as it is.
				if isCockroachDB() {
					return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN (%s)", quoteName(index.name), spec.qualifiedName(), quoteName(part.column))
				}
				// fastupdate: a write appends its entries to the GIN pending list instead of inserting
				// each into the tree; vacuum (or the list passing gin_pending_list_limit) merges them.
				return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING GIN (%s jsonb_path_ops) WITH (fastupdate = on)",
					quoteName(index.name), spec.qualifiedName(), quoteName(part.column))
			}
		}
	}
	columns := make([]string, len(index.keyParts))
	for partIndex, part := range index.keyParts {
		columns[partIndex] = part.column
	}
	statement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", quoteName(index.name), spec.qualifiedName(), quotedNames(columns))
	if len(index.includedColumns) > 0 {
		statement += fmt.Sprintf(" INCLUDE (%s)", quotedNames(index.includedColumns))
	}
	return statement
}

func columnDefinition(column columnSpec) string {
	definition := quoteName(column.name) + " " + column.sqlType
	if column.notNull {
		definition += " NOT NULL"
	}
	return definition
}

// createTableStatements renders CREATE TABLE and then each of its indexes.
func createTableStatements(spec tableSpec) []string {
	definitions := make([]string, 0, len(spec.columns)+1)
	for _, column := range spec.columns {
		definitions = append(definitions, columnDefinition(column))
	}
	definitions = append(definitions, fmt.Sprintf("PRIMARY KEY (%s)", quotedNames(spec.primaryKey)))
	tableKind := "TABLE"
	if spec.isUnlogged && !isCockroachDB() {
		tableKind = "UNLOGGED TABLE"
	}
	statements := []string{fmt.Sprintf("CREATE %s IF NOT EXISTS %s (\n    %s\n)", tableKind, spec.qualifiedName(), strings.Join(definitions, ",\n    "))}
	for _, index := range spec.indexes {
		statements = append(statements, createIndexStatement(spec, index))
	}
	return statements
}

func createSchemaStatement(database string) string {
	return fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteName(database))
}

// addColumnStatement: an added column is always nullable, so a populated table accepts it.
func addColumnStatement(spec tableSpec, column columnSpec) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", spec.qualifiedName(), quoteName(column.name), column.sqlType)
}

func dropIndexStatement(spec tableSpec, indexName string) string {
	return fmt.Sprintf("DROP INDEX IF EXISTS %s.%s", quoteName(spec.database), quoteName(indexName))
}

// deltaMovesTableSpec mirrors Scylla's `<table>__delta_moves`. pinned_prefix is a packed key: a
// BIGINT with its top bit flipped (bindArguments).
func deltaMovesTableSpec(database, tableName, idType string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__delta_moves"),
		columns: []columnSpec{
			{"partition_id", "INTEGER", true}, {"index_tag", keyedStringType(), true}, {"pinned_prefix", "BIGINT", true},
			{"updated", "INTEGER", true}, {"id", idType, true},
		},
		primaryKey: []string{"partition_id", "index_tag", "pinned_prefix", "updated", "id"},
	}
}

// indexUpdatedTableSpec mirrors Scylla's `<table>__index_updated` (cache-group marks).
func indexUpdatedTableSpec(database, tableName string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__index_updated"),
		columns: []columnSpec{
			{"partition_id", "INTEGER", true}, {"index_id", "SMALLINT", true}, {"index_value", "BIGINT", true}, {"last_write", "INTEGER", false},
		},
		primaryKey: []string{"partition_id", "index_id", "index_value"},
	}
}

// groupDataTableSpec is `<table>__group_data`: the record snapshot of each settled cache group,
// labelled built_write (the last_write it was read at), served while its group's mark still holds that
// value. It is a cache any read rebuilds, so the table is UNLOGGED: no WAL, emptied after a crash.
func groupDataTableSpec(database, tableName string) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__group_data"), isUnlogged: true,
		columns: []columnSpec{
			{"partition_id", "INTEGER", true}, {"index_id", "SMALLINT", true}, {"index_value", "BIGINT", true},
			{"built_write", "INTEGER", true}, {"data", "BYTEA", true},
		},
		primaryKey: []string{"partition_id", "index_id", "index_value"},
	}
}

// cacheSlotsTableName holds the by-IDs cache slots of every CacheByIDs table of a schema: per table
// partition, slot uint8(ID) → the Updated of its latest write (as Scylla's cache_by_ids_slots).
const cacheSlotsTableName = "_cache_by_ids_slots"

func cacheSlotsTableSpec(database string) tableSpec {
	return tableSpec{
		database: database, name: cacheSlotsTableName,
		columns: []columnSpec{
			{"table_id", "SMALLINT", true}, {"partition_id", "BIGINT", true}, {"slot", "SMALLINT", true}, {"updated", "INTEGER", true},
		},
		primaryKey: []string{"table_id", "partition_id", "slot"},
	}
}

// sequencesTableSpec is `<schema>._sequences`.
func sequencesTableSpec(database string) tableSpec {
	return tableSpec{
		database: database, name: "_sequences",
		columns:    []columnSpec{{"name", keyedStringType(), true}, {"value", "BIGINT", true}},
		primaryKey: []string{"name"},
	}
}
