package obdriver

import (
	"database/sql"
	"slices"
	"strings"
	"testing"
)

// Homologation brings a drifted table back to its declaration and is idempotent afterwards: a
// dropped index and a changed STORING are rebuilt, a stray index dropped, a new column added,
// compression reset. A changed column type, primary key or partition count fails instead.
func TestDeployHomologatesADriftedTable(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	database := spikeDatabase(t, pool)
	declared := productsSpec(1)
	declared.database = database
	if wasCreated, _, err := homologateTable(pool, declared, nil); err != nil || !wasCreated {
		t.Fatalf("create: created=%v err=%v", wasCreated, err)
	}
	assertHomologated(t, pool, declared)

	// Drift the table away from its declaration.
	spikeMustExec(t, pool, dropIndexStatement(declared, "ix_code"))
	spikeMustExec(t, pool, "ALTER TABLE "+declared.qualifiedName()+" COMPRESSION = 'lz4_1.0'")
	spikeMustExec(t, pool, "ALTER TABLE "+declared.qualifiedName()+" ADD INDEX `ix_stale` (`company_id`, `category_id`) LOCAL")
	declared.columns = append(declared.columns, columnSpec{"price", "DOUBLE", false})
	declared.indexes[0].storingColumns = []string{"code", "price"}

	if _, wasAltered, err := homologateTable(pool, declared, describedTableOf(t, pool, declared)); err != nil || !wasAltered {
		t.Fatalf("homologate: altered=%v err=%v", wasAltered, err)
	}
	assertHomologated(t, pool, declared)

	refusals := map[string]func(spec *tableSpec){
		"wipe and reseed": func(spec *tableSpec) {
			spec.columns = slices.Clone(spec.columns)
			spec.columns[2] = columnSpec{"category_id", "BIGINT", false}
		},
		"primary key": func(spec *tableSpec) { spec.primaryKey = []string{"company_id", "category_id", "id"} },
		"partitions":  func(spec *tableSpec) { spec.partitionCount = 8 },
	}
	for wantFragment, changeDeclaration := range refusals {
		changed := declared
		changeDeclaration(&changed)
		if _, _, err := homologateTable(pool, changed, describedTableOf(t, pool, changed)); err == nil || !strings.Contains(err.Error(), wantFragment) {
			t.Errorf("a %s change must fail, got %v", wantFragment, err)
		}
	}
	spikeLog(t, "deploy: drifted table homologated (index re-added, STORING rebuilt, stray index dropped, column added, compression reset); type, key and partition-count changes refused")
}

// A partitioned table with a GLOBAL index, and a TTL side table, read back as declared.
func TestDeployReadsBackPartitionedAndTTLTables(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	database := spikeDatabase(t, pool)
	partitioned := productsSpec(8)
	partitioned.database = database
	frameLog := dataFrameLogTableSpec(database, "products", "BIGINT", 8)
	for _, spec := range []tableSpec{partitioned, frameLog} {
		if _, _, err := homologateTable(pool, spec, nil); err != nil {
			t.Fatal(err)
		}
		assertHomologated(t, pool, spec)
	}
	spikeLog(t, "deploy: 8-partition table with LOCAL, multi-value and GLOBAL indexes, and the TTL frame log, read back unchanged")
}

func describedTableOf(t *testing.T, pool *sql.DB, spec tableSpec) *describedTable {
	t.Helper()
	describedTables, err := describeDatabase(pool, spec.database)
	if err != nil {
		t.Fatal(err)
	}
	return describedTables[spec.name]
}

// assertHomologated: a described table that matches its spec needs no statement.
func assertHomologated(t *testing.T, pool *sql.DB, spec tableSpec) {
	t.Helper()
	details, err := readCreateTableDetails(pool, spec)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := homologationStatements(spec, describedTableOf(t, pool, spec), details)
	if err != nil || len(statements) != 0 {
		t.Fatalf("expected no change, got %v (err %v)", statements, err)
	}
}
