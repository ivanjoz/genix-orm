package obdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Step 0 spike helpers (docs/OCEANBASE_PLAN.md §16). Every spike test runs in its own throwaway
// database, dropped at the end.

func requireOceanbaseIntegration(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("GENIX_OCEANBASE_IT") != "1" {
		t.Skip("set GENIX_OCEANBASE_IT=1 to run against the OceanBase server (docs/OCEANBASE_PLAN.md §5)")
	}
	SetConnection(ParamsFromEnvironment())
	pool, err := sharedConnection()
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

// ParamsFromEnvironment reads the server address, defaulting to the homelab node over Tailscale (§5).
func ParamsFromEnvironment() ConnParams {
	host := os.Getenv("GENIX_OCEANBASE_HOST")
	if host == "" {
		host = "100.64.0.4"
	}
	port, _ := strconv.Atoi(os.Getenv("GENIX_OCEANBASE_PORT"))
	if port == 0 {
		port = 2881
	}
	user := os.Getenv("GENIX_OCEANBASE_USER")
	if user == "" {
		user = "root@test"
	}
	return ConnParams{Host: host, Port: port, User: user, Password: os.Getenv("GENIX_OCEANBASE_PASSWORD"), Database: "genix"}
}

// spikeDatabase creates a uniquely named database and drops it when the test ends.
func spikeDatabase(t *testing.T, pool *sql.DB) string {
	t.Helper()
	databaseName := fmt.Sprintf("spike_%d_%d", time.Now().UnixNano(), rand.IntN(1000))
	spikeMustExec(t, pool, "CREATE DATABASE `"+databaseName+"`")
	t.Cleanup(func() {
		_, _ = pool.Exec("DROP DATABASE IF EXISTS `" + databaseName + "`")
	})
	return databaseName
}

func spikeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 300*time.Second)
}

func spikeMustExec(t *testing.T, pool *sql.DB, statement string, args ...any) {
	t.Helper()
	statementContext, cancel := spikeContext()
	defer cancel()
	if _, err := pool.ExecContext(statementContext, statement, args...); err != nil {
		t.Fatalf("%v\nstatement: %s", err, statement)
	}
}

// spikeInt64Rows runs a statement that selects ONE integer column and returns every row in order.
func spikeInt64Rows(pool *sql.DB, statement string, args ...any) ([]int64, error) {
	statementContext, cancel := spikeContext()
	defer cancel()
	rows, err := pool.QueryContext(statementContext, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []int64{}
	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

// spikeExplain returns the EXPLAIN text of a statement written with literal values.
func spikeExplain(t *testing.T, pool *sql.DB, statement string) string {
	t.Helper()
	rows, err := pool.Query("EXPLAIN " + statement)
	if err != nil {
		t.Fatalf("explain: %v\nstatement: %s", err, statement)
	}
	defer rows.Close()
	lines := []string{}
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// auditedExecution is what OceanBase recorded for one execution of a prepared statement: the
// plan it actually ran (with the bound parameters, not an EXPLAIN of literals) and the rows the
// storage layer read to answer it.
type auditedExecution struct {
	ReturnedRows  int64
	RowsRead      int64 // memstore + sstable rows touched by the scan
	PlanOperators []string
}

// spikeAuditedQuery runs a prepared statement on a pinned connection, takes its LAST_TRACE_ID() and
// reads the matching GV$OB_SQL_AUDIT row and plan-cache plan. The tenant records an empty
// query_sql, so the trace id is the only handle. SQL audit is asynchronous, so it polls briefly.
func spikeAuditedQuery(t *testing.T, pool *sql.DB, statement string, args ...any) auditedExecution {
	t.Helper()
	statementContext, cancel := spikeContext()
	defer cancel()
	connection, err := pool.Conn(statementContext)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	rows, err := connection.QueryContext(statementContext, statement, args...)
	if err != nil {
		t.Fatalf("%v\nstatement: %s", err, statement)
	}
	returnedRows := int64(0)
	for rows.Next() {
		returnedRows++
	}
	rows.Close()
	var traceID string
	if err := connection.QueryRowContext(statementContext, "SELECT LAST_TRACE_ID()").Scan(&traceID); err != nil {
		t.Fatalf("last trace id: %v", err)
	}
	execution := auditedExecution{}
	var planID int64
	for attempt := 0; attempt < 50; attempt++ {
		err := pool.QueryRow(`SELECT plan_id, return_rows, total_memstore_read_row_count + total_ssstore_read_row_count
			FROM oceanbase.GV$OB_SQL_AUDIT WHERE trace_id = ? AND is_inner_sql = 0 AND return_rows = ?
			ORDER BY request_time DESC LIMIT 1`, traceID, returnedRows).Scan(&planID, &execution.ReturnedRows, &execution.RowsRead)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatalf("sql audit: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if planID == 0 {
		t.Fatalf("no SQL audit row for trace %s", traceID)
	}
	planRows, err := pool.Query(`SELECT operator, name FROM oceanbase.GV$OB_PLAN_CACHE_PLAN_EXPLAIN
		WHERE plan_id = ? ORDER BY plan_line_id`, planID)
	if err != nil {
		t.Fatalf("plan explain: %v", err)
	}
	defer planRows.Close()
	for planRows.Next() {
		var operatorName, objectName sql.NullString
		if err := planRows.Scan(&operatorName, &objectName); err != nil {
			t.Fatal(err)
		}
		execution.PlanOperators = append(execution.PlanOperators, strings.TrimSpace(operatorName.String+" "+objectName.String))
	}
	return execution
}

func spikeLog(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("SPIKE-RESULT %s", strings.TrimSpace(fmt.Sprintf(format, args...)))
}

// placeholderRows renders "(?,?,…),(?,?,…)…" for a multi-row INSERT.
func placeholderRows(rowCount, columnCount int) string {
	oneRow := "(" + strings.TrimSuffix(strings.Repeat("?,", columnCount), ",") + ")"
	return strings.TrimSuffix(strings.Repeat(oneRow+",", rowCount), ",")
}

func percentile(sortedLatencies []time.Duration, fraction float64) time.Duration {
	if len(sortedLatencies) == 0 {
		return 0
	}
	return sortedLatencies[int(float64(len(sortedLatencies)-1)*fraction)]
}

func sortedDurations(latencies []time.Duration) []time.Duration {
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	return latencies
}
