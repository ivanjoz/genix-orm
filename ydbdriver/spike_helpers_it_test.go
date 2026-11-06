package ydbdriver

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// Step 0 spike helpers (docs/YDB_PLAN.md §16). Every spike test runs on its own throwaway directory.

func requireYdbIntegration(t *testing.T) *ydb.Driver {
	t.Helper()
	if os.Getenv("GENIX_YDB_IT") != "1" {
		t.Skip("set GENIX_YDB_IT=1 to run against the YDB container (docs/YDB_PLAN.md §5)")
	}
	SetConnection(ParamsFromEnvironment())
	connection, err := sharedConnection()
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

// ParamsFromEnvironment reads the container endpoint, defaulting to the §5 container.
func ParamsFromEnvironment() ConnParams {
	endpoint := os.Getenv("GENIX_YDB_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:2136"
	}
	return ConnParams{Endpoint: endpoint, Database: "/local", Keyspace: "genix"}
}

// spikeDirectory returns a unique directory name (relative to the database) and removes its tables at the end.
func spikeDirectory(t *testing.T, connection *ydb.Driver) string {
	t.Helper()
	directoryName := fmt.Sprintf("spike_%d_%d", time.Now().UnixNano(), rand.IntN(1000))
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		listing, err := connection.Scheme().ListDirectory(cleanupContext, connection.Name()+"/"+directoryName)
		if err != nil {
			return
		}
		for _, child := range listing.Children {
			_ = connection.Query().Exec(cleanupContext, fmt.Sprintf("DROP TABLE `%s/%s`", directoryName, child.Name))
		}
	})
	return directoryName
}

func spikeContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 120*time.Second)
}

func spikeExec(t *testing.T, connection *ydb.Driver, statement string, options ...query.ExecuteOption) error {
	t.Helper()
	statementContext, cancel := spikeContext()
	defer cancel()
	return connection.Query().Exec(statementContext, statement, options...)
}

func spikeMustExec(t *testing.T, connection *ydb.Driver, statement string, options ...query.ExecuteOption) {
	t.Helper()
	if err := spikeExec(t, connection, statement, options...); err != nil {
		t.Fatalf("%v\nstatement: %s", err, statement)
	}
}

// spikeInt64Rows runs a statement that selects ONE Int64 column and returns every row in order.
func spikeInt64Rows(connection *ydb.Driver, statement string, txControl *query.TransactionControl) ([]int64, error) {
	statementContext, cancel := spikeContext()
	defer cancel()
	options := []query.ExecuteOption{}
	if txControl != nil {
		options = append(options, query.WithTxControl(txControl))
	}
	result, err := connection.Query().Query(statementContext, statement, options...)
	if err != nil {
		return nil, err
	}
	defer result.Close(statementContext)
	values := []int64{}
	for resultSet, setErr := range result.ResultSets(statementContext) {
		if setErr != nil {
			return nil, setErr
		}
		for row, rowErr := range resultSet.Rows(statementContext) {
			if rowErr != nil {
				return nil, rowErr
			}
			var value int64
			if err := row.Scan(&value); err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

// spikePlan returns the EXPLAIN plan text of a statement.
func spikePlan(t *testing.T, connection *ydb.Driver, statement string) string {
	t.Helper()
	statementContext, cancel := spikeContext()
	defer cancel()
	planText := ""
	result, err := connection.Query().Query(statementContext, statement,
		query.WithExecMode(query.ExecModeExplain),
		query.WithStatsMode(query.StatsModeNone, func(stats query.Stats) { planText = stats.QueryPlan() }))
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	for _, setErr := range result.ResultSets(statementContext) {
		_ = setErr
	}
	_ = result.Close(statementContext)
	return planText
}

func spikeLog(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("SPIKE-RESULT %s", strings.TrimSpace(fmt.Sprintf(format, args...)))
}
