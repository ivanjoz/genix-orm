package obdriver

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Step 0 spike (docs/OCEANBASE_PLAN.md §16): connection, the §8.1 DDL, and every §10 read shape
// executed as a prepared statement, checked through the plan OceanBase actually ran and the rows
// its storage layer read.

func TestSpikeConnection(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	if err := Ping(); err != nil {
		t.Fatal(err)
	}
	var version, collation, queryTimeout string
	if err := pool.QueryRow("SELECT version(), @@collation_connection, @@ob_query_timeout").Scan(&version, &collation, &queryTimeout); err != nil {
		t.Fatal(err)
	}
	spikeLog(t, "connection version=%s collation_connection=%s ob_query_timeout=%s", version, collation, queryTimeout)
	if collation != "utf8mb4_bin" {
		t.Errorf("collation_connection = %s, want utf8mb4_bin", collation)
	}
	if queryTimeout != fmt.Sprint(sessionQueryTimeout.Microseconds()) {
		t.Errorf("ob_query_timeout = %s, want %d", queryTimeout, sessionQueryTimeout.Microseconds())
	}
	latencies := []time.Duration{}
	for range 200 {
		startedAt := time.Now()
		if _, err := spikeInt64Rows(pool, "SELECT 1"); err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(startedAt))
	}
	latencies = sortedDurations(latencies)
	spikeLog(t, "round trip SELECT 1 p50=%v p99=%v", percentile(latencies, 0.5), percentile(latencies, 0.99))
}

// itemsTableDDL is the §8.1 shape: every index kind the driver emits, in one statement.
func itemsTableDDL(databaseName string, partitionCount int) string {
	partitionClause := ""
	if partitionCount > 1 {
		partitionClause = fmt.Sprintf(" PARTITION BY KEY(company_id) PARTITIONS %d", partitionCount)
	}
	// STORING refuses LOB and JSON columns, and a row's declared width is capped at ~1.5 MB (§18b), so
	// ix_delta can only store the non-LOB columns. ix_delta_lean stores nothing: every column is read
	// back from the table.
	return "CREATE TABLE `" + databaseName + "`.`items` (" +
		" company_id INT NOT NULL, id BIGINT NOT NULL, category_id INT, name VARCHAR(512), notes LONGTEXT," +
		" price DOUBLE, photos LONGBLOB, tags JSON, updated INT, zz_dlt BIGINT UNSIGNED," +
		" PRIMARY KEY (company_id, id)," +
		" INDEX ix_cat (company_id, category_id) STORING (name, price) LOCAL," +
		" INDEX ix_delta (company_id, zz_dlt) STORING (category_id, name, price, updated) LOCAL," +
		" INDEX ix_delta_lean (company_id, zz_dlt, id) LOCAL," +
		" INDEX ix_tags (company_id, (CAST(tags->'$' AS SIGNED ARRAY))) LOCAL," +
		" INDEX ix_gcat (category_id) GLOBAL" +
		") DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMPRESSION = 'zstd_1.3.8'" + partitionClause
}

const spikeCompanies, spikeRowsPerCompany = 4, 5000

// seedItems writes spikeCompanies × spikeRowsPerCompany rows. category = id % 20; tags hold id % 10
// and 10 + id % 7, so every record has two elements and a record can match both 3 and 13.
func seedItems(t *testing.T, pool *sql.DB, databaseName string) {
	t.Helper()
	const batchSize = 1000
	for companyID := 1; companyID <= spikeCompanies; companyID++ {
		for firstID := 1; firstID <= spikeRowsPerCompany; firstID += batchSize {
			args := []any{}
			for id := firstID; id < firstID+batchSize; id++ {
				args = append(args, companyID, id, id%20, fmt.Sprintf("item-%d", id), strings.Repeat("note ", 40), id%1000,
					[]byte(strings.Repeat("p", 120)), fmt.Sprintf("[%d,%d]", id%10, 10+id%7), id, uint64(id)<<20)
			}
			spikeMustExec(t, pool, "INSERT INTO `"+databaseName+"`.`items` (company_id, id, category_id, name, notes, price, photos, tags, updated, zz_dlt) VALUES "+
				placeholderRows(batchSize, 10), args...)
		}
	}
	// Push the rows from the memstore to an SSTable so reads exercise the on-disk layout too.
	if _, err := pool.Exec("ALTER SYSTEM MINOR FREEZE"); err != nil {
		spikeLog(t, "minor freeze refused: %v", err)
	}
}

func TestSpikeReadShapes(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	for _, partitionCount := range []int{1, 8} {
		t.Run(fmt.Sprintf("partitions=%d", partitionCount), func(t *testing.T) {
			databaseName := spikeDatabase(t, pool)
			spikeMustExec(t, pool, itemsTableDDL(databaseName, partitionCount))
			seedItems(t, pool, databaseName)
			table := "`" + databaseName + "`.`items`"
			targetCompany := 2

			readShapes := []struct {
				name      string
				statement string
				args      []any
			}{
				{"pk range 100", "SELECT id FROM " + table + " WHERE company_id = ? AND id >= ? AND id < ? ORDER BY company_id, id LIMIT ?", []any{targetCompany, 1000, 1100, 1000}},
				{"pk points 3", "SELECT id FROM " + table + " WHERE (company_id, id) IN ((?, ?), (?, ?), (?, ?))", []any{targetCompany, 10, targetCompany, 20, targetCompany, 30}},
				{"local index eq", "SELECT /*+ INDEX(items ix_cat) */ id FROM " + table + " WHERE company_id = ? AND category_id = ?", []any{targetCompany, 7}},
				{"local index desc limit 10", "SELECT /*+ INDEX(items ix_cat) */ id FROM " + table + " WHERE company_id = ? AND category_id = ? ORDER BY category_id DESC, id DESC LIMIT 10", []any{targetCompany, 7}},
				{"local index IN fan-out", "SELECT /*+ INDEX(items ix_cat) */ id FROM " + table + " WHERE company_id = ? AND category_id IN (?, ?, ?)", []any{targetCompany, 1, 2, 3}},
				{"delta range desc limit 50", "SELECT /*+ INDEX(items ix_delta) */ id FROM " + table + " WHERE company_id = ? AND zz_dlt >= ? AND zz_dlt <= ? ORDER BY zz_dlt DESC LIMIT 50", []any{targetCompany, uint64(100) << 20, uint64(4000) << 20}},
				{"multi-value one element", "SELECT /*+ INDEX(items ix_tags) */ id FROM " + table + " WHERE company_id = ? AND ? MEMBER OF (tags->'$')", []any{targetCompany, 3}},
				{"multi-value overlaps 2", "SELECT /*+ INDEX(items ix_tags) */ id FROM " + table + " WHERE company_id = ? AND JSON_OVERLAPS(tags->'$', CAST(? AS JSON))", []any{targetCompany, "[3,13]"}},
				{"global index (cross-company by design)", "SELECT /*+ INDEX(items ix_gcat) */ id FROM " + table + " WHERE category_id = ?", []any{7}},
			}
			for _, shape := range readShapes {
				execution := spikeAuditedQuery(t, pool, shape.statement, shape.args...)
				spikeLog(t, "partitions=%d %s: returned=%d rowsRead=%d plan=[%s]", partitionCount, shape.name,
					execution.ReturnedRows, execution.RowsRead, strings.Join(execution.PlanOperators, " | "))
				// A company-scoped read must not touch the neighbouring companies' rows: whatever it
				// reads beyond what it returns stays far below one company's row count.
				if !strings.HasPrefix(shape.name, "global") && execution.RowsRead > 2*execution.ReturnedRows+100 {
					t.Errorf("%s read %d rows to return %d", shape.name, execution.RowsRead, execution.ReturnedRows)
				}
			}

			// Results of the multi-value shapes: one element, and the engine-side dedupe of a record
			// matching both elements (id % 10 == 3 and id % 7 == 3 → counted once).
			oneElement, _ := spikeInt64Rows(pool, readShapes[6].statement, readShapes[6].args...)
			overlapping, _ := spikeInt64Rows(pool, readShapes[7].statement, readShapes[7].args...)
			expectedOverlap := 0
			for id := 1; id <= spikeRowsPerCompany; id++ {
				if id%10 == 3 || id%7 == 3 {
					expectedOverlap++
				}
			}
			spikeLog(t, "partitions=%d multi-value results: one element=%d (want %d), overlaps=%d (want %d, no duplicates)",
				partitionCount, len(oneElement), spikeRowsPerCompany/10, len(overlapping), expectedOverlap)
			if len(oneElement) != spikeRowsPerCompany/10 || len(overlapping) != expectedOverlap {
				t.Error("multi-value index results differ from the expected counts")
			}

			// Covering: a projection inside the index key + STORING never goes back to the table.
			coveredPlan := spikeExplain(t, pool, "SELECT /*+ INDEX(items ix_cat) */ id, name, price FROM "+table+" WHERE company_id = 2 AND category_id = 7")
			deltaPlan := spikeExplain(t, pool, "SELECT /*+ INDEX(items ix_delta) */ id, name, price, updated FROM "+table+" WHERE company_id = 2 AND zz_dlt >= 1")
			spikeLog(t, "partitions=%d covered ix_cat index_back=%t, ix_delta non-LOB projection index_back=%t",
				partitionCount, strings.Contains(coveredPlan, "is_index_back=true"), strings.Contains(deltaPlan, "is_index_back=true"))

			// A first sync reads a whole company through the delta index. Compare: covered projection
			// (no table lookup), full projection through the covering index (lookup for the LOBs), and
			// full projection through the lean index (lookup for everything), against a PK scan.
			deltaReads := []struct{ label, hint, projection string }{
				{"ix_delta covered non-LOB projection", "/*+ INDEX(items ix_delta) */", "id, category_id, name, price, updated"},
				{"ix_delta full projection (lookup)", "/*+ INDEX(items ix_delta) */", "id, category_id, name, notes, price, photos, tags, updated"},
				{"ix_delta_lean full projection (lookup)", "/*+ INDEX(items ix_delta_lean) */", "id, category_id, name, notes, price, photos, tags, updated"},
				{"primary key full projection (baseline)", "", "id, category_id, name, notes, price, photos, tags, updated"},
			}
			for _, deltaRead := range deltaReads {
				orderBy := " ORDER BY zz_dlt DESC"
				if deltaRead.hint == "" {
					orderBy = " ORDER BY company_id, id"
				}
				statement := "SELECT " + deltaRead.hint + " " + deltaRead.projection + " FROM " + table +
					" WHERE company_id = ? AND zz_dlt >= ?" + orderBy + " LIMIT ?"
				latencies := []time.Duration{}
				for range 30 {
					startedAt := time.Now()
					rows, err := pool.Query(statement, targetCompany, 0, spikeRowsPerCompany)
					if err != nil {
						t.Fatal(err)
					}
					rowCount := 0
					for rows.Next() {
						rowCount++
					}
					rows.Close()
					latencies = append(latencies, time.Since(startedAt))
					if rowCount != spikeRowsPerCompany {
						t.Fatalf("%s returned %d rows", deltaRead.label, rowCount)
					}
				}
				latencies = sortedDurations(latencies)
				spikeLog(t, "partitions=%d full-company read (%d rows) %s: p50=%v p99=%v",
					partitionCount, spikeRowsPerCompany, deltaRead.label, percentile(latencies, 0.5), percentile(latencies, 0.99))
			}
		})
	}
}

func TestSpikeMultiValueEdgeCases(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	databaseName := spikeDatabase(t, pool)
	table := "`" + databaseName + "`.`slices`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, tags JSON, names JSON,"+
		" PRIMARY KEY (company_id, id),"+
		" INDEX ix_tags (company_id, (CAST(tags->'$' AS SIGNED ARRAY))),"+
		" INDEX ix_names (company_id, (CAST(names->'$' AS CHAR(512) ARRAY)))) COLLATE = utf8mb4_bin")
	spikeMustExec(t, pool, "INSERT INTO "+table+" VALUES (1, 1, '[]', '[]'), (1, 2, NULL, NULL), (1, 3, '[-5, 2147483647]', '[\"Ab\", \"ab\"]'), (1, 4, '[7, 7]', '[\"ab\"]')")
	count := func(statement string, args ...any) int {
		ids, err := spikeInt64Rows(pool, statement, args...)
		if err != nil {
			t.Fatalf("%v\nstatement: %s", err, statement)
		}
		return len(ids)
	}
	emptyRow := count("SELECT id FROM " + table + " WHERE company_id = 1 AND id = 1 AND JSON_LENGTH(tags) = 0")
	negative := count("SELECT /*+ INDEX(slices ix_tags) */ id FROM "+table+" WHERE company_id = 1 AND ? MEMBER OF (tags->'$')", -5)
	duplicateElement := count("SELECT /*+ INDEX(slices ix_tags) */ id FROM "+table+" WHERE company_id = 1 AND ? MEMBER OF (tags->'$')", 7)
	caseSensitive := count("SELECT /*+ INDEX(slices ix_names) */ id FROM "+table+" WHERE company_id = 1 AND ? MEMBER OF (names->'$')", "Ab")
	lowercase := count("SELECT /*+ INDEX(slices ix_names) */ id FROM "+table+" WHERE company_id = 1 AND ? MEMBER OF (names->'$')", "ab")
	spikeLog(t, "multi-value edges: '[]' stored and read=%d, -5 matches=%d, [7,7] matched once=%d, 'Ab' matches=%d (want 1), 'ab' matches=%d (want 2)",
		emptyRow, negative, duplicateElement, caseSensitive, lowercase)
	if emptyRow != 1 || negative != 1 || duplicateElement != 1 || caseSensitive != 1 {
		t.Error("a multi-value edge case differs from the expected result")
	}
	// Known server defect (plan §18b): two elements of ONE record that differ only by case get one
	// index entry, so the other spelling misses that record. Recorded, not failed, so a server
	// upgrade that fixes it shows up here.
	spikeLog(t, "string multi-value case-variant defect present=%t", lowercase != 2)
	stringPlan := spikeExplain(t, pool, "SELECT /*+ INDEX(slices ix_names) */ id FROM "+table+" WHERE company_id = 1 AND 'ab' MEMBER OF (names->'$')")
	spikeLog(t, "string multi-value index used=%t", strings.Contains(stringPlan, "ix_names"))
}
