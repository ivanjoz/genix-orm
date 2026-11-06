package ydbdriver

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// Step 0 microbenchmark (docs/YDB_PLAN.md §16), gated by GENIX_YDB_BENCH=1 on top of GENIX_YDB_IT=1.
// Same workloads as FDB plan §14: one point get, a 1,000-row range, a 1,000-row commit.
func TestSpikeMicrobenchmark(t *testing.T) {
	if os.Getenv("GENIX_YDB_BENCH") != "1" {
		t.Skip("set GENIX_YDB_BENCH=1 to run the microbenchmark")
	}
	connection := requireYdbIntegration(t)
	tablePath := spikeCreateItems(t, connection)

	seedRows := make([]types.Value, 0, 5000)
	for id := int64(1); id <= 5000; id++ {
		seedRows = append(seedRows, types.StructValue(
			types.StructFieldValue("company_id", types.Int32Value(1)), types.StructFieldValue("id", types.Int64Value(id)),
			types.StructFieldValue("updated", types.OptionalValue(types.Int32Value(int32(id)))),
			types.StructFieldValue("name", types.OptionalValue(types.TextValue("benchmark-row-name")))))
	}
	spikeMustExec(t, connection, "DECLARE $rows AS List<Struct<company_id: Int32, id: Int64, updated: Int32?, name: Utf8?>>;\nUPSERT INTO `"+tablePath+"` SELECT company_id, id, updated, name FROM AS_TABLE($rows)", spikeListParameter("$rows", seedRows))

	var commitIDCounter int64 = 1_000_000
	var counterMutex sync.Mutex
	workloads := map[string]func(worker, iteration int) error{
		"point get": func(worker, iteration int) error {
			_, err := spikeInt64Rows(connection, fmt.Sprintf("SELECT id FROM `%s` WHERE company_id = 1 AND id = %d", tablePath, 1+(worker*37+iteration)%5000), query.SnapshotReadOnlyTxControl())
			return err
		},
		"range 1000": func(worker, iteration int) error {
			_, err := spikeInt64Rows(connection, fmt.Sprintf("SELECT id FROM `%s` WHERE company_id = 1 AND id > %d ORDER BY id LIMIT 1000", tablePath, (worker*13+iteration)%3000), query.SnapshotReadOnlyTxControl())
			return err
		},
		"commit 1000 rows": func(worker, iteration int) error {
			counterMutex.Lock()
			firstID := commitIDCounter
			commitIDCounter += 1000
			counterMutex.Unlock()
			commitRows := make([]types.Value, 0, 1000)
			for offset := int64(0); offset < 1000; offset++ {
				commitRows = append(commitRows, types.StructValue(
					types.StructFieldValue("company_id", types.Int32Value(2)), types.StructFieldValue("id", types.Int64Value(firstID+offset)),
					types.StructFieldValue("updated", types.OptionalValue(types.Int32Value(1))),
					types.StructFieldValue("name", types.OptionalValue(types.TextValue("commit-row")))))
			}
			statementContext, cancel := spikeContext()
			defer cancel()
			return connection.Query().Exec(statementContext, "DECLARE $rows AS List<Struct<company_id: Int32, id: Int64, updated: Int32?, name: Utf8?>>;\nUPSERT INTO `"+tablePath+"` SELECT company_id, id, updated, name FROM AS_TABLE($rows)", spikeListParameter("$rows", commitRows))
		},
	}
	for _, workloadName := range []string{"point get", "range 1000", "commit 1000 rows"} {
		for _, concurrency := range []int{1, 64} {
			iterations := 400
			if workloadName == "commit 1000 rows" {
				iterations = 60
			}
			latencies := []time.Duration{}
			var latencyMutex sync.Mutex
			var workerGroup sync.WaitGroup
			startedAt := time.Now()
			for worker := 0; worker < concurrency; worker++ {
				workerGroup.Add(1)
				go func(worker int) {
					defer workerGroup.Done()
					for iteration := 0; iteration < iterations; iteration++ {
						operationStart := time.Now()
						if err := workloads[workloadName](worker, iteration); err != nil {
							t.Errorf("%s: %v", workloadName, err)
							return
						}
						latencyMutex.Lock()
						latencies = append(latencies, time.Since(operationStart))
						latencyMutex.Unlock()
					}
				}(worker)
			}
			workerGroup.Wait()
			elapsed := time.Since(startedAt)
			sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
			spikeLog(t, "microbench %s concurrency=%d ops=%d ops/s=%.0f p50=%v p99=%v",
				workloadName, concurrency, len(latencies), float64(len(latencies))/elapsed.Seconds(), percentile(latencies, 0.5), percentile(latencies, 0.99))
		}
	}
}
