package ydbdriver

import (
	"strings"
	"testing"
)

const expectedPartitioning = `AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 2000, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50`

func TestCreatePlainTable(t *testing.T) {
	statement := createTableStatement(tableSpec{
		path: "genix/products",
		columns: []columnSpec{
			{"company_id", "Int32", true}, {"id", "Int64", true}, {"name", "Utf8", false},
		},
		primaryKey: []string{"company_id", "id"},
	})
	expected := "CREATE TABLE `genix/products` (\n" +
		"    company_id Int32 NOT NULL,\n    id Int64 NOT NULL,\n    name Utf8,\n" +
		"    PRIMARY KEY (company_id, id),\n" +
		"    FAMILY default (COMPRESSION = \"lz4\")\n" +
		") WITH (" + expectedPartitioning + ")"
	if statement != expected {
		t.Fatalf("got:\n%s\nwant:\n%s", statement, expected)
	}
}

func TestCreateTableWithNativeAndDeltaIndexes(t *testing.T) {
	statement := createTableStatement(tableSpec{
		path: "genix/products",
		columns: []columnSpec{
			{"company_id", "Int32", true}, {"id", "Int64", true}, {"category_id", "Int32", false},
			{"name", "Utf8", false}, {"updated", "Int32", false}, {"zz_dlt_ss", "Uint64", false},
		},
		primaryKey: []string{"company_id", "id"},
		indexes: []nativeIndexSpec{
			{name: "ix_category", keyColumns: []string{"company_id", "category_id"}, coverColumns: []string{"name"}},
			{name: "ix_delta", keyColumns: []string{"company_id", "zz_dlt_ss"}, coverColumns: []string{"category_id", "name", "updated"}},
			{name: "ix_bare", keyColumns: []string{"name"}},
		},
	})
	for _, fragment := range []string{
		"INDEX ix_category GLOBAL ASYNC ON (company_id, category_id) COVER (name),",
		"INDEX ix_delta GLOBAL ASYNC ON (company_id, zz_dlt_ss) COVER (category_id, name, updated),",
		"INDEX ix_bare GLOBAL ASYNC ON (name),",
		"FAMILY default (COMPRESSION = \"lz4\")",
	} {
		if !strings.Contains(statement, fragment) {
			t.Errorf("missing %q in:\n%s", fragment, statement)
		}
	}
}

func TestTableWithoutPartitionUsesKeysAlone(t *testing.T) {
	statement := createTableStatement(tableSpec{
		path: "genix/settings", columns: []columnSpec{{"id", "Int64", true}}, primaryKey: []string{"id"},
	})
	if !strings.Contains(statement, "PRIMARY KEY (id)") {
		t.Fatal(statement)
	}
}

func TestSliceSideTableWithAndWithoutOrderColumns(t *testing.T) {
	partition := columnSpec{"company_id", "Int32", true}
	mainKey := []columnSpec{{"id", "Int64", true}}
	local := sliceSideTableSpec("genix/products", "tags", &partition, "Int32", nil, mainKey)
	if got := strings.Join(local.primaryKey, ","); got != "company_id,element,id" {
		t.Fatalf("local key = %s", got)
	}
	ordered := sliceSideTableSpec("genix/products", "tags", &partition, "Int32", []columnSpec{{"status", "Int16", true}}, mainKey)
	if got := strings.Join(ordered.primaryKey, ","); got != "company_id,element,status,id" {
		t.Fatalf("ordered key = %s", got)
	}
	global := sliceSideTableSpec("genix/products", "tags", nil, "Utf8", nil, []columnSpec{partition, mainKey[0]})
	if got := strings.Join(global.primaryKey, ","); got != "element,company_id,id" {
		t.Fatalf("global key = %s", got)
	}
	if local.path != "genix/products__sx_tags" {
		t.Fatalf("path = %s", local.path)
	}
}

func TestSideTablesAndSequences(t *testing.T) {
	moves := createTableStatement(deltaMovesTableSpec("genix/products", "Int64"))
	if !strings.Contains(moves, "PRIMARY KEY (partition_id, index_tag, pinned_prefix, updated, id)") {
		t.Fatal(moves)
	}
	log := createTableStatement(dataFrameLogTableSpec("genix/products", "Int64"))
	if !strings.Contains(log, `TTL = Interval("PT0S") ON expire_at AS SECONDS`) {
		t.Fatal(log)
	}
	sequences := createTableStatement(sequencesTableSpec("genix"))
	if !strings.Contains(sequences, "CREATE TABLE `genix/_sequences`") || !strings.Contains(sequences, expectedPartitioning) {
		t.Fatal(sequences)
	}
	marks := createTableStatement(indexUpdatedTableSpec("genix/products"))
	if !strings.Contains(marks, "last_write Int32,") {
		t.Fatal(marks)
	}
}

func TestAlterStatements(t *testing.T) {
	if got := alterIndexPartitioningStatement("genix/products", "ix_delta"); got != "ALTER TABLE `genix/products` ALTER INDEX ix_delta SET ("+expectedPartitioning+")" {
		t.Fatal(got)
	}
	if got := alterFamilyCompressionStatement("genix/products"); got != "ALTER TABLE `genix/products` ALTER FAMILY default SET COMPRESSION \"lz4\"" {
		t.Fatal(got)
	}
}

func TestMoreThanTwentyIndexesPanics(t *testing.T) {
	indexes := make([]nativeIndexSpec, 21)
	for position := range indexes {
		indexes[position] = nativeIndexSpec{name: "ix_" + string(rune('a'+position))}
	}
	defer func() {
		message, _ := recover().(string)
		if !strings.Contains(message, `table "genix/products"`) || !strings.Contains(message, "21 YDB indexes") || !strings.Contains(message, "ix_u") {
			t.Fatalf("panic message = %q", message)
		}
	}()
	assertNativeIndexLimit("genix/products", indexes)
	t.Fatal("expected a panic")
}

func TestTwentyIndexesIsAllowed(t *testing.T) {
	assertNativeIndexLimit("genix/products", make([]nativeIndexSpec, 20))
}

func TestSliceOrderByManagedColumnPanics(t *testing.T) {
	for _, managed := range []string{"updated", "version"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s should panic", managed)
				}
			}()
			assertSliceOrderColumns("genix/products", "ix_tags", []string{"status", managed})
		}()
	}
	assertSliceOrderColumns("genix/products", "ix_tags", []string{"status"})
}
