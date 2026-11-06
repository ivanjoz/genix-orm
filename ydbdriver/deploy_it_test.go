package ydbdriver

import (
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// Homologation brings a drifted table back to its declaration and is idempotent afterwards: a
// dropped index and a changed cover are rebuilt, a new column added, compression and partitioning
// reset. A changed column type or primary key fails instead.
func TestDeployHomologatesADriftedTable(t *testing.T) {
	connection := requireYdbIntegration(t)
	tablePath := spikeDirectory(t, connection) + "/products"
	declared := tableSpec{
		path: tablePath,
		columns: []columnSpec{
			{"company_id", "Int32", true}, {"id", "Int64", true}, {"category_id", "Int32", false}, {"name", "Utf8", false},
		},
		primaryKey: []string{"company_id", "id"},
		indexes: []nativeIndexSpec{
			{name: "ix_category", keyColumns: []string{"company_id", "category_id"}, coverColumns: []string{"name"}},
			{name: "ix_name", keyColumns: []string{"company_id", "name"}},
		},
	}
	if wasCreated, _, err := homologateTable(connection, declared, false); err != nil || !wasCreated {
		t.Fatalf("create: created=%v err=%v", wasCreated, err)
	}
	assertHomologated(t, connection, declared)

	// Drift the table away from its declaration.
	spikeMustExec(t, connection, dropIndexStatement(tablePath, "ix_name"))
	spikeMustExec(t, connection, "ALTER TABLE `"+tablePath+"` ALTER FAMILY default SET COMPRESSION \"off\"")
	spikeMustExec(t, connection, "ALTER TABLE `"+tablePath+"` SET (AUTO_PARTITIONING_BY_LOAD = DISABLED)")
	spikeMustExec(t, connection, "ALTER TABLE `"+tablePath+"` ADD INDEX ix_stale GLOBAL ASYNC ON (company_id, category_id)")
	declared.columns = append(declared.columns, columnSpec{"price", "Double", false})
	declared.indexes[0].coverColumns = []string{"name", "price"}

	if _, wasAltered, err := homologateTable(connection, declared, true); err != nil || !wasAltered {
		t.Fatalf("homologate: altered=%v err=%v", wasAltered, err)
	}
	assertHomologated(t, connection, declared)

	retyped := declared
	retyped.columns = append([]columnSpec{}, declared.columns...)
	retyped.columns[2] = columnSpec{"category_id", "Int64", false}
	if _, _, err := homologateTable(connection, retyped, true); err == nil || !strings.Contains(err.Error(), "wipe and reseed") {
		t.Fatalf("a changed column type must fail, got %v", err)
	}
	rekeyed := declared
	rekeyed.primaryKey = []string{"company_id", "category_id", "id"}
	if _, _, err := homologateTable(connection, rekeyed, true); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("a changed primary key must fail, got %v", err)
	}
	spikeLog(t, "deploy: drifted table homologated (index re-added, cover rebuilt, column added, lz4 and Y20 reset); type and key changes refused")
}

// assertHomologated: a described table that matches its spec needs no statement.
func assertHomologated(t *testing.T, connection *ydb.Driver, spec tableSpec) {
	t.Helper()
	description, err := describeTablePath(connection, spec.path)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := homologationStatements(connection, spec, description)
	if err != nil || len(statements) != 0 {
		t.Fatalf("expected no change, got %v (err %v)", statements, err)
	}
}
