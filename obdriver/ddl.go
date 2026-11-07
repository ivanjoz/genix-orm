package obdriver

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// Pure DDL text generation (docs/OCEANBASE_PLAN.md §8). No connection, no ORM types: callers describe
// a table with tableSpec and get statements back, so every string is golden-testable.

// tableOptions are written on every table (O15, O16), even where they equal the server defaults:
// the server's default collation is case-insensitive, which would break key parity with Scylla.
const tableOptions = "DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'"

// tableCompression and tableCollation are what homologation expects to read back.
const tableCompression, tableCollation = "zstd_1.3.8", "utf8mb4_bin"

// dataFrameLogTTL is the DataFrame log's retention (Scylla: 48 h TTL).
const dataFrameLogTTL = "INTERVAL 48 HOUR"

type columnSpec struct {
	name    string
	sqlType string
	notNull bool
}

// indexKeyPart is one key part: a column, or the elements of a JSON array column when
// multiValueCast is set ("SIGNED" / "UNSIGNED"), which makes the index a multi-value index (O11).
type indexKeyPart struct {
	column         string
	multiValueCast string
}

// indexSpec is a native, synchronous secondary index. storingColumns exclude the key and the
// primary key, which every index row already holds.
type indexSpec struct {
	name           string
	keyParts       []indexKeyPart
	storingColumns []string
	isGlobal       bool
}

type tableSpec struct {
	database   string
	name       string
	columns    []columnSpec
	primaryKey []string
	indexes    []indexSpec
	// partitionColumn and partitionCount render PARTITION BY KEY(…) when partitionCount > 1 (O8).
	partitionColumn string
	partitionCount  int
	// ttlColumn, when set, deletes a row once ttlColumn + dataFrameLogTTL has passed (D4).
	ttlColumn string
	// isColumnStore adds the column store next to the row store (TableSchema.ColumnStore).
	isColumnStore bool
}

// columnGroupClause is the hybrid row + column store.
const columnGroupClause = "WITH COLUMN GROUP(all columns, each column)"

func quoteName(name string) string { return "`" + name + "`" }

// maxIdentifierLength is OceanBase's (MySQL mode) limit on table, column and index names.
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

// keyPartDefinition renders a key part; the JSON path must be written exactly as the reads write it
// (`col->'$'`), or the optimizer does not match the index (§18b).
func keyPartDefinition(part indexKeyPart) string {
	if part.multiValueCast == "" {
		return quoteName(part.column)
	}
	return fmt.Sprintf("(CAST(%s->'$' AS %s ARRAY))", quoteName(part.column), part.multiValueCast)
}

func (index indexSpec) isMultiValue() bool {
	for _, part := range index.keyParts {
		if part.multiValueCast != "" {
			return true
		}
	}
	return false
}

// indexDefinition is one index as CREATE TABLE and ADD INDEX both write it.
func indexDefinition(index indexSpec) string {
	parts := make([]string, len(index.keyParts))
	for partIndex, part := range index.keyParts {
		parts[partIndex] = keyPartDefinition(part)
	}
	definition := fmt.Sprintf("INDEX %s (%s)", quoteName(index.name), strings.Join(parts, ", "))
	if len(index.storingColumns) > 0 {
		definition += fmt.Sprintf(" STORING (%s)", quotedNames(index.storingColumns))
	}
	if index.isGlobal {
		return definition + " GLOBAL"
	}
	return definition + " LOCAL"
}

func columnDefinition(column columnSpec) string {
	definition := quoteName(column.name) + " " + column.sqlType
	if column.notNull {
		definition += " NOT NULL"
	}
	return definition
}

// createTableStatement renders CREATE TABLE with the O15/O16 options, the TTL (D4) and the KEY
// partitioning (O8) when the table has a partition column and more than one partition.
func createTableStatement(spec tableSpec) string {
	definitions := make([]string, 0, len(spec.columns)+len(spec.indexes)+1)
	for _, column := range spec.columns {
		definitions = append(definitions, columnDefinition(column))
	}
	definitions = append(definitions, fmt.Sprintf("PRIMARY KEY (%s)", quotedNames(spec.primaryKey)))
	for _, index := range spec.indexes {
		definitions = append(definitions, indexDefinition(index))
	}
	statement := fmt.Sprintf("CREATE TABLE %s (\n    %s\n) %s", spec.qualifiedName(), strings.Join(definitions, ",\n    "), tableOptions)
	if spec.ttlColumn != "" {
		statement += fmt.Sprintf(" TTL (%s + %s)", quoteName(spec.ttlColumn), dataFrameLogTTL)
	}
	if spec.partitionColumn != "" && spec.partitionCount > 1 {
		statement += fmt.Sprintf(" PARTITION BY KEY(%s) PARTITIONS %d", quoteName(spec.partitionColumn), spec.partitionCount)
	}
	if spec.isColumnStore {
		statement += " " + columnGroupClause
	}
	return statement
}

func createDatabaseStatement(database string) string {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin", quoteName(database))
}

// addIndexStatement adds an index to a populated table; OceanBase builds it online and the
// statement returns once the build is done (§18b: writers keep going meanwhile).
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

func alterCompressionStatement(spec tableSpec) string {
	return fmt.Sprintf("ALTER TABLE %s COMPRESSION = '%s'", spec.qualifiedName(), tableCompression)
}

// addColumnGroupStatement turns an existing row-store table into the hybrid store (an offline DDL:
// OceanBase rewrites the table).
func addColumnGroupStatement(spec tableSpec) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN GROUP(all columns, each column)", spec.qualifiedName())
}

func alterTTLStatement(spec tableSpec) string {
	return fmt.Sprintf("ALTER TABLE %s TTL (%s + %s)", spec.qualifiedName(), quoteName(spec.ttlColumn), dataFrameLogTTL)
}

// deltaMovesTableSpec mirrors Scylla's `<table>__delta_moves`.
func deltaMovesTableSpec(database, tableName, idType string, partitionCount int) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__delta_moves"),
		columns: []columnSpec{
			{"partition_id", "INT", true}, {"index_tag", keyedStringType, true}, {"pinned_prefix", "BIGINT UNSIGNED", true},
			{"updated", "INT", true}, {"id", idType, true},
		},
		primaryKey:      []string{"partition_id", "index_tag", "pinned_prefix", "updated", "id"},
		partitionColumn: "partition_id", partitionCount: partitionCount,
	}
}

// indexUpdatedTableSpec mirrors Scylla's `<table>__index_updated` (cache-group marks).
func indexUpdatedTableSpec(database, tableName string, partitionCount int) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__index_updated"),
		columns: []columnSpec{
			{"partition_id", "INT", true}, {"index_id", "SMALLINT", true}, {"index_hash", "INT", true}, {"last_write", "INT", false},
		},
		primaryKey:      []string{"partition_id", "index_id", "index_hash"},
		partitionColumn: "partition_id", partitionCount: partitionCount,
	}
}

// dataFrameLogTableSpec mirrors Scylla's `<table>__dtf_log`. written_at is a TIMESTAMP (time-zone
// independent) from the ORM clock; the table TTL deletes the row 48 h later (D4).
func dataFrameLogTableSpec(database, tableName, recordIDType string, partitionCount int) tableSpec {
	return tableSpec{
		database: database, name: boundedIdentifier(tableName + "__dtf_log"),
		columns: []columnSpec{
			{"half_hour", "INT", true}, {"partition_id", "INT", true}, {"ts", "INT", true},
			{"record_id", recordIDType, true}, {"changes", "LONGBLOB", false}, {"written_at", "TIMESTAMP", true},
		},
		primaryKey:      []string{"half_hour", "partition_id", "ts", "record_id"},
		partitionColumn: "partition_id", partitionCount: partitionCount,
		ttlColumn: "written_at",
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

// sequencesTableSpec is `<database>._sequences` (O14). Never partitioned: a handful of rows.
func sequencesTableSpec(database string) tableSpec {
	return tableSpec{
		database: database, name: "_sequences",
		columns:    []columnSpec{{"name", keyedStringType, true}, {"value", "BIGINT", true}},
		primaryKey: []string{"name"},
	}
}
