package ydbdriver

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestSpikePing(t *testing.T) {
	requireYdbIntegration(t)
	if err := Ping(); err != nil {
		t.Fatal(err)
	}
}

// Item 1: GLOBAL ASYNC ... COVER is accepted. Item 2: Stale RO reads covered columns, Snapshot RO may not.
func TestSpikeAsyncCoveringIndex(t *testing.T) {
	connection := requireYdbIntegration(t)
	directory := spikeDirectory(t, connection)
	table := directory + "/items"
	spikeMustExec(t, connection, "CREATE TABLE `"+table+"` (company_id Int32 NOT NULL, id Int64 NOT NULL, updated Int32, name Utf8, "+
		"PRIMARY KEY (company_id, id), "+
		"INDEX ix_delta GLOBAL ASYNC ON (company_id, updated) COVER (name))")
	spikeLog(t, "item1 CREATE TABLE ... INDEX GLOBAL ASYNC ON (...) COVER (...): accepted")

	spikeMustExec(t, connection, "UPSERT INTO `"+table+"` (company_id, id, updated, name) VALUES (1, 1, 10, 'a'), (1, 2, 20, 'b'), (1, 3, 30, 'c')")

	// Wait for the async index to catch up.
	_ = waitForIndexRows(t, connection, table, 3)

	staleRows, err := spikeInt64Rows(connection, "SELECT id FROM `"+table+"` VIEW ix_delta WHERE company_id = 1 AND updated > 10 ORDER BY updated", query.StaleReadOnlyTxControl())
	spikeLog(t, "item2 VIEW ix under StaleRO: rows=%v err=%v", staleRows, err)
	if err != nil {
		t.Fatalf("stale read of an async index failed: %v", err)
	}
	snapshotRows, err := spikeInt64Rows(connection, "SELECT id FROM `"+table+"` VIEW ix_delta WHERE company_id = 1 AND updated > 10 ORDER BY updated", query.SnapshotReadOnlyTxControl())
	spikeLog(t, "item2 VIEW ix under SnapshotRO: rows=%v err=%v", snapshotRows, err)
	serializableRows, err := spikeInt64Rows(connection, "SELECT id FROM `"+table+"` VIEW ix_delta WHERE company_id = 1 AND updated > 10 ORDER BY updated", nil)
	spikeLog(t, "item2 VIEW ix under default (serializable RW): rows=%v err=%v", serializableRows, err)

	// Item 3: ORDER BY DESC LIMIT on the index is a reverse read, not a sort.
	plan := spikePlan(t, connection, "SELECT id, name FROM `"+table+"` VIEW ix_delta WHERE company_id = 1 ORDER BY updated DESC LIMIT 2")
	spikeLog(t, "item3 plan has Sort=%v Reverse=%v\n%s", strings.Contains(plan, "\"Sort\""), strings.Contains(plan, "\"Reverse\":true"), plan)
}

// waitForIndexRows polls the async index until it holds the expected row count and returns the lag.
func waitForIndexRows(t *testing.T, connection *ydb.Driver, table string, expected int) time.Duration {
	t.Helper()
	startedAt := time.Now()
	var lastCounts []int64
	var lastErr error
	for time.Since(startedAt) < 60*time.Second {
		counts, err := spikeInt64Rows(connection, "SELECT CAST(COUNT(*) AS Int64) FROM `"+table+"` VIEW ix_delta WHERE company_id = 1", query.StaleReadOnlyTxControl())
		lastCounts, lastErr = counts, err
		if err == nil && len(counts) == 1 && int(counts[0]) == expected {
			return time.Since(startedAt)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("index of %s did not reach %d rows in 60s: counts=%v err=%v", table, expected, lastCounts, lastErr)
	return 0
}

// TestSpikePersistence checks that data survives `podman restart`: run with GENIX_YDB_PERSIST=write,
// restart the container, then run with GENIX_YDB_PERSIST=check (which drops the table).
func TestSpikePersistence(t *testing.T) {
	phase := os.Getenv("GENIX_YDB_PERSIST")
	if phase == "" {
		t.Skip("set GENIX_YDB_PERSIST=write|check")
	}
	connection := requireYdbIntegration(t)
	if phase == "write" {
		spikeMustExec(t, connection, "CREATE TABLE `spike_persistence` (id Int64 NOT NULL, PRIMARY KEY (id))")
		spikeMustExec(t, connection, "UPSERT INTO `spike_persistence` (id) VALUES (42)")
		return
	}
	rows, err := spikeInt64Rows(connection, "SELECT id FROM `spike_persistence`", nil)
	spikeLog(t, "persistence after restart: rows=%v err=%v", rows, err)
	spikeMustExec(t, connection, "DROP TABLE `spike_persistence`")
	if err != nil || len(rows) != 1 || rows[0] != 42 {
		t.Fatal("data did not survive the restart")
	}
}
