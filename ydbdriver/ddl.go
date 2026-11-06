package ydbdriver

import (
	"fmt"
	"strings"
)

// Pure DDL text generation (docs/YDB_PLAN.md §8). No connection, no ORM types: callers describe a
// table with tableSpec and get statements back, so every string is golden-testable.

// maxNativeIndexesPerTable is YDB's limit of secondary indexes per table (D5: exceeding it is an error).
const maxNativeIndexesPerTable = 20

// partitioningSettings is Y20, written explicitly even where it equals YDB's defaults (load
// splitting is off by default). An index table does not inherit it, see alterIndexPartitioningStatement.
const partitioningSettings = "AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, " +
	"AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, " +
	"AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50"

type columnSpec struct {
	name    string
	ydbType string
	notNull bool
}

// nativeIndexSpec is a GLOBAL ASYNC secondary index (Y6). coverColumns exclude the index key and the
// primary key: YDB adds those implicitly.
type nativeIndexSpec struct {
	name         string
	keyColumns   []string
	coverColumns []string
}

type tableSpec struct {
	// path is relative to the database: `<keyspace>/<table>`.
	path       string
	columns    []columnSpec
	primaryKey []string
	indexes    []nativeIndexSpec
	// expireAtColumn, when set, adds a TTL on that Uint32 column (seconds).
	expireAtColumn string
}

// createTableStatement renders CREATE TABLE with the lz4 family (Y19) and the partitioning settings (Y20).
func createTableStatement(spec tableSpec) string {
	definitions := make([]string, 0, len(spec.columns)+len(spec.indexes)+2)
	for _, column := range spec.columns {
		definition := fmt.Sprintf("%s %s", column.name, column.ydbType)
		if column.notNull {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
	}
	definitions = append(definitions, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(spec.primaryKey, ", ")))
	for _, index := range spec.indexes {
		definitions = append(definitions, indexDefinition(index))
	}
	definitions = append(definitions, `FAMILY default (COMPRESSION = "lz4")`)

	tableOptions := partitioningSettings
	if spec.expireAtColumn != "" {
		tableOptions += fmt.Sprintf(`, TTL = Interval("PT0S") ON %s AS SECONDS`, spec.expireAtColumn)
	}
	return fmt.Sprintf("CREATE TABLE `%s` (\n    %s\n) WITH (%s)", spec.path, strings.Join(definitions, ",\n    "), tableOptions)
}

// indexDefinition is one GLOBAL ASYNC index (Y6), as CREATE TABLE and ADD INDEX both write it.
func indexDefinition(index nativeIndexSpec) string {
	definition := fmt.Sprintf("INDEX %s GLOBAL ASYNC ON (%s)", index.name, strings.Join(index.keyColumns, ", "))
	if len(index.coverColumns) > 0 {
		definition += fmt.Sprintf(" COVER (%s)", strings.Join(index.coverColumns, ", "))
	}
	return definition
}

// addIndexStatement adds an index to a populated table; YDB builds it in the background and the
// statement returns once the build is done.
func addIndexStatement(tablePath string, index nativeIndexSpec) string {
	return fmt.Sprintf("ALTER TABLE `%s` ADD %s", tablePath, indexDefinition(index))
}

func dropIndexStatement(tablePath, indexName string) string {
	return fmt.Sprintf("ALTER TABLE `%s` DROP INDEX %s", tablePath, indexName)
}

// alterIndexPartitioningStatement applies Y20 to an index table, which does not inherit the table's
// settings and does not accept them at index creation.
func alterIndexPartitioningStatement(tablePath, indexName string) string {
	return fmt.Sprintf("ALTER TABLE `%s` ALTER INDEX %s SET (%s)", tablePath, indexName, partitioningSettings)
}

func alterTablePartitioningStatement(tablePath string) string {
	return fmt.Sprintf("ALTER TABLE `%s` SET (%s)", tablePath, partitioningSettings)
}

func alterFamilyCompressionStatement(tablePath string) string {
	return fmt.Sprintf("ALTER TABLE `%s` ALTER FAMILY default SET COMPRESSION \"lz4\"", tablePath)
}

func addColumnStatement(tablePath string, column columnSpec) string {
	// An added column is always nullable: YDB cannot add a NOT NULL column to a populated table.
	return fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN %s %s", tablePath, column.name, column.ydbType)
}

// assertNativeIndexLimit is D5: a table with more than 20 native indexes is a compile error.
func assertNativeIndexLimit(tableName string, indexes []nativeIndexSpec) {
	if len(indexes) <= maxNativeIndexesPerTable {
		return
	}
	indexNames := make([]string, 0, len(indexes))
	for _, index := range indexes {
		indexNames = append(indexNames, index.name)
	}
	panic(fmt.Sprintf(`ydbdriver: table "%s" needs %d YDB indexes, the limit is %d: %s`,
		tableName, len(indexes), maxNativeIndexesPerTable, strings.Join(indexNames, ", ")))
}

// assertSliceOrderColumns is Y8: the order columns after a slice column in a side table cannot be the
// managed `updated` or `version` columns, which the ORM rewrites on every update.
func assertSliceOrderColumns(tableName, indexName string, orderColumns []string) {
	for _, orderColumn := range orderColumns {
		if orderColumn == "updated" || orderColumn == "version" {
			panic(fmt.Sprintf(`ydbdriver: table "%s" index "%s": a slice index cannot order by managed column "%s"`,
				tableName, indexName, orderColumn))
		}
	}
}

// sliceSideTableSpec is `<table>__sx_<source>` (§8.4): key only. partitionColumn is empty for a
// global index; orderColumns are the index keys after the slice column.
func sliceSideTableSpec(tablePath, sourceName string, partitionColumn *columnSpec, elementType string, orderColumns, mainKeyColumns []columnSpec) tableSpec {
	spec := tableSpec{path: fmt.Sprintf("%s__sx_%s", tablePath, sourceName)}
	if partitionColumn != nil {
		spec.columns = append(spec.columns, columnSpec{partitionColumn.name, partitionColumn.ydbType, true})
	}
	spec.columns = append(spec.columns, columnSpec{"element", elementType, true})
	for _, orderColumn := range orderColumns {
		spec.columns = append(spec.columns, columnSpec{orderColumn.name, orderColumn.ydbType, true})
	}
	for _, keyColumn := range mainKeyColumns {
		spec.columns = append(spec.columns, columnSpec{keyColumn.name, keyColumn.ydbType, true})
	}
	for _, column := range spec.columns {
		spec.primaryKey = append(spec.primaryKey, column.name)
	}
	return spec
}

// deltaMovesTableSpec mirrors Scylla's `<table>__delta_moves`.
func deltaMovesTableSpec(tablePath, idType string) tableSpec {
	return tableSpec{
		path: tablePath + "__delta_moves",
		columns: []columnSpec{
			{"partition_id", "Int32", true}, {"index_tag", "Utf8", true}, {"pinned_prefix", "Uint64", true},
			{"updated", "Int32", true}, {"id", idType, true},
		},
		primaryKey: []string{"partition_id", "index_tag", "pinned_prefix", "updated", "id"},
	}
}

// indexUpdatedTableSpec mirrors Scylla's `<table>__index_updated` (cache-group marks).
func indexUpdatedTableSpec(tablePath string) tableSpec {
	return tableSpec{
		path: tablePath + "__index_updated",
		columns: []columnSpec{
			{"partition_id", "Int32", true}, {"index_id", "Int16", true}, {"index_hash", "Int32", true}, {"last_write", "Int32", false},
		},
		primaryKey: []string{"partition_id", "index_id", "index_hash"},
	}
}

// dataFrameLogTableSpec mirrors Scylla's `<table>__dtf_log`; expire_at is the write time plus 48 h.
func dataFrameLogTableSpec(tablePath, recordIDType string) tableSpec {
	return tableSpec{
		path: tablePath + "__dtf_log",
		columns: []columnSpec{
			{"half_hour", "Int32", true}, {"partition_id", "Int32", true}, {"ts", "Int32", true},
			{"record_id", recordIDType, true}, {"changes", "String", false}, {"expire_at", "Uint32", false},
		},
		primaryKey:     []string{"half_hour", "partition_id", "ts", "record_id"},
		expireAtColumn: "expire_at",
	}
}

// sequencesTableSpec is `<keyspace>/_sequences` (Y11).
func sequencesTableSpec(keyspace string) tableSpec {
	return tableSpec{
		path:       keyspace + "/_sequences",
		columns:    []columnSpec{{"name", "Utf8", true}, {"value", "Int64", false}},
		primaryKey: []string{"name"},
	}
}
