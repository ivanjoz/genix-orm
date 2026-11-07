package obdriver

import (
	"os"
	"sync"
	"testing"
	"time"
)

// Step 0 microbenchmark (docs/OCEANBASE_PLAN.md §16), gated by GENIX_OCEANBASE_BENCH=1 on top of
// GENIX_OCEANBASE_IT=1. Same workloads as YDB Step 0: one point get, a 1,000-row range, a 1,000-row
// commit, at concurrency 1 and 64. Every statement is parameterized (server-side prepared).
func TestSpikeMicrobenchmark(t *testing.T) {
	if os.Getenv("GENIX_OCEANBASE_BENCH") != "1" {
		t.Skip("set GENIX_OCEANBASE_BENCH=1 to run the microbenchmark")
	}
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`bench`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, updated INT, name VARCHAR(512),"+
		" PRIMARY KEY (company_id, id), INDEX ix_updated (company_id, updated) LOCAL) COLLATE = utf8mb4_bin")
	const seededRows, statementRows = 5000, 1000
	for firstID := 1; firstID <= seededRows; firstID += statementRows {
		args := make([]any, 0, statementRows*4)
		for id := firstID; id < firstID+statementRows; id++ {
			args = append(args, 1, id, id, "benchmark-row-name")
		}
		spikeMustExec(t, pool, "INSERT INTO "+table+" VALUES "+placeholderRows(statementRows, 4), args...)
	}

	pointGet, err := pool.Prepare("SELECT id FROM " + table + " WHERE company_id = 1 AND id = ?")
	if err != nil {
		t.Fatal(err)
	}
	rangeRead, err := pool.Prepare("SELECT id FROM " + table + " WHERE company_id = 1 AND id > ? ORDER BY company_id, id LIMIT 1000")
	if err != nil {
		t.Fatal(err)
	}
	commitStatement := "INSERT INTO " + table + " VALUES " + placeholderRows(statementRows, 4) +
		" ON DUPLICATE KEY UPDATE updated = VALUES(updated), name = VALUES(name)"
	var commitIDCounter int64 = 1_000_000
	var counterMutex sync.Mutex

	workloads := map[string]func(worker, iteration int) error{
		"point get": func(worker, iteration int) error {
			var id int64
			return pointGet.QueryRow(1 + (worker*37+iteration)%seededRows).Scan(&id)
		},
		"range 1000": func(worker, iteration int) error {
			rows, err := rangeRead.Query((worker*13 + iteration) % 3000)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			rows.Close()
			return rows.Err()
		},
		"commit 1000 rows": func(worker, iteration int) error {
			counterMutex.Lock()
			firstID := commitIDCounter
			commitIDCounter += statementRows
			counterMutex.Unlock()
			args := make([]any, 0, statementRows*4)
			for offset := int64(0); offset < statementRows; offset++ {
				args = append(args, 2, firstID+offset, 1, "commit-row")
			}
			transaction, err := pool.Begin()
			if err != nil {
				return err
			}
			if _, err := transaction.Exec(commitStatement, args...); err != nil {
				_ = transaction.Rollback()
				return err
			}
			return transaction.Commit()
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
			for worker := range concurrency {
				workerGroup.Add(1)
				go func() {
					defer workerGroup.Done()
					for iteration := range iterations {
						operationStartedAt := time.Now()
						if err := workloads[workloadName](worker, iteration); err != nil {
							t.Errorf("%s: %v", workloadName, err)
							return
						}
						latencyMutex.Lock()
						latencies = append(latencies, time.Since(operationStartedAt))
						latencyMutex.Unlock()
					}
				}()
			}
			workerGroup.Wait()
			elapsed := time.Since(startedAt)
			latencies = sortedDurations(latencies)
			spikeLog(t, "microbench %s concurrency=%d ops=%d ops/s=%.0f p50=%v p99=%v",
				workloadName, concurrency, len(latencies), float64(len(latencies))/elapsed.Seconds(), percentile(latencies, 0.5), percentile(latencies, 0.99))
		}
	}
}
