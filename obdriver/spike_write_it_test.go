package obdriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Step 0 spike, write side (docs/OCEANBASE_PLAN.md §16): upsert semantics, statement limits, row
// locks, transaction sizes, TTL, online DDL and the per-partition overhead (O8).

func mysqlErrorText(err error) string {
	var serverError *mysql.MySQLError
	if errors.As(err, &serverError) {
		return fmt.Sprintf("code=%d state=%s message=%q", serverError.Number, string(serverError.SQLState[:]), serverError.Message)
	}
	if err == nil {
		return "no error"
	}
	return err.Error()
}

func TestSpikeUpsertSubset(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`upserts`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, category_id INT, name VARCHAR(512), PRIMARY KEY (company_id, id))")
	spikeMustExec(t, pool, "INSERT INTO "+table+" VALUES (1, 1, 10, 'kept')")
	spikeMustExec(t, pool, "INSERT INTO "+table+" (company_id, id, category_id) VALUES (?, ?, ?), (?, ?, ?) ON DUPLICATE KEY UPDATE category_id = VALUES(category_id)", 1, 1, 20, 1, 2, 30)
	var existingName, createdName sql.NullString
	var existingCategory, createdCategory int
	_ = pool.QueryRow("SELECT category_id, name FROM "+table+" WHERE company_id = 1 AND id = 1").Scan(&existingCategory, &existingName)
	_ = pool.QueryRow("SELECT category_id, name FROM "+table+" WHERE company_id = 1 AND id = 2").Scan(&createdCategory, &createdName)
	spikeLog(t, "upsert subset: existing row category=%d name=%q (kept), missing row created category=%d name valid=%t",
		existingCategory, existingName.String, createdCategory, createdName.Valid)
	if existingCategory != 20 || existingName.String != "kept" || createdCategory != 30 || createdName.Valid {
		t.Error("ON DUPLICATE KEY UPDATE does not match Scylla's upsert semantics")
	}
}

func TestSpikeStatementLimits(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`limits`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, payload LONGBLOB, PRIMARY KEY (company_id, id))")
	insertRows := func(firstID, rowCount int) error {
		args := make([]any, 0, rowCount*2)
		for id := firstID; id < firstID+rowCount; id++ {
			args = append(args, 1, id)
		}
		_, err := pool.Exec("INSERT INTO "+table+" (company_id, id) VALUES "+placeholderRows(rowCount, 2), args...)
		return err
	}
	spikeLog(t, "placeholders 65534 (32767 rows × 2): %s", mysqlErrorText(insertRows(1, 32767)))
	spikeLog(t, "placeholders 65536 (32768 rows × 2): %s", mysqlErrorText(insertRows(100_000, 32768)))
	for _, payloadSize := range []int{8 << 20, 15 << 20, 17 << 20} {
		_, err := pool.Exec("INSERT INTO "+table+" (company_id, id, payload) VALUES (?, ?, ?)", 2, payloadSize, make([]byte, payloadSize))
		spikeLog(t, "one statement with a %d MB value: %s", payloadSize>>20, mysqlErrorText(err))
	}
}

// lockingTransaction opens a pinned connection with a transaction, and a short lock-wait timeout so
// a blocked statement fails instead of waiting for the 600 s query timeout.
func lockingTransaction(t *testing.T, pool *sql.DB, lockWait time.Duration) (*sql.Conn, *sql.Tx) {
	t.Helper()
	connection, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), fmt.Sprintf("SET ob_trx_lock_timeout = %d", lockWait.Microseconds())); err != nil {
		t.Fatal(err)
	}
	transaction, err := connection.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = transaction.Rollback()
		// The session variable must not leak into the pool.
		_, _ = connection.ExecContext(context.Background(), "SET ob_trx_lock_timeout = -1")
		_ = connection.Close()
	})
	return connection, transaction
}

func TestSpikeRowLocking(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`locks`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, counter INT, PRIMARY KEY (company_id, id))")
	spikeMustExec(t, pool, "INSERT INTO "+table+" VALUES (1, 1, 0), (1, 2, 0)")
	lockRow := func(transaction *sql.Tx, id int) error {
		var counter int
		return transaction.QueryRow("SELECT counter FROM "+table+" WHERE company_id = 1 AND id = ? FOR UPDATE", id).Scan(&counter)
	}

	// Blocking: B waits on A's lock and proceeds once A commits.
	_, holder := lockingTransaction(t, pool, 10*time.Second)
	_, waiter := lockingTransaction(t, pool, 10*time.Second)
	if err := lockRow(holder, 1); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan error, 1)
	waitStartedAt := time.Now()
	go func() { waitResult <- lockRow(waiter, 1) }()
	time.Sleep(500 * time.Millisecond)
	_, _ = holder.Exec("UPDATE " + table + " SET counter = counter + 1 WHERE company_id = 1 AND id = 1")
	if err := holder.Commit(); err != nil {
		t.Fatal(err)
	}
	waitErr := <-waitResult
	_ = waiter.Rollback() // it now holds row 1; release it for the deadlock case below
	spikeLog(t, "FOR UPDATE on a locked row: waited %v, then %s", time.Since(waitStartedAt).Round(time.Millisecond), mysqlErrorText(waitErr))

	// Lock-wait timeout.
	_, longHolder := lockingTransaction(t, pool, 10*time.Second)
	_, impatient := lockingTransaction(t, pool, time.Second)
	if err := lockRow(longHolder, 2); err != nil {
		t.Fatal(err)
	}
	timeoutStartedAt := time.Now()
	timeoutErr := lockRow(impatient, 2)
	spikeLog(t, "lock wait timeout (1 s): after %v %s", time.Since(timeoutStartedAt).Round(time.Millisecond), mysqlErrorText(timeoutErr))
	_ = longHolder.Rollback()

	// Deadlock: A holds 1 and wants 2, B holds 2 and wants 1.
	_, first := lockingTransaction(t, pool, 30*time.Second)
	_, second := lockingTransaction(t, pool, 30*time.Second)
	if err := lockRow(first, 1); err != nil {
		t.Fatal(err)
	}
	if err := lockRow(second, 2); err != nil {
		t.Fatal(err)
	}
	deadlockStartedAt := time.Now()
	results := make(chan string, 2)
	go func() { results <- "A: " + mysqlErrorText(lockRow(first, 2)) }()
	go func() { results <- "B: " + mysqlErrorText(lockRow(second, 1)) }()
	firstResult := <-results
	_ = first.Rollback()
	_ = second.Rollback()
	secondResult := <-results
	spikeLog(t, "deadlock resolved after %v: %s; %s", time.Since(deadlockStartedAt).Round(time.Millisecond), firstResult, secondResult)
}

func TestSpikeTransactionSizes(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`tx_sizes`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, category_id INT, name VARCHAR(512), price DOUBLE, updated INT,"+
		" PRIMARY KEY (company_id, id), INDEX ix_cat (company_id, category_id) LOCAL)")
	const statementRows = 1000
	nextID := 1
	for _, transactionRows := range []int{1000, 5000, 20000} {
		startedAt := time.Now()
		transaction, err := pool.Begin()
		if err != nil {
			t.Fatal(err)
		}
		var writeErr error
		for written := 0; written < transactionRows && writeErr == nil; written += statementRows {
			args := make([]any, 0, statementRows*6)
			for range statementRows {
				args = append(args, 1, nextID, nextID%50, fmt.Sprintf("row-%d", nextID), 1.5, nextID)
				nextID++
			}
			_, writeErr = transaction.Exec("INSERT INTO "+table+" VALUES "+placeholderRows(statementRows, 6)+
				" ON DUPLICATE KEY UPDATE category_id = VALUES(category_id), name = VALUES(name), price = VALUES(price), updated = VALUES(updated)", args...)
		}
		if writeErr == nil {
			writeErr = transaction.Commit()
		} else {
			_ = transaction.Rollback()
		}
		spikeLog(t, "one transaction, %d rows (+1 index entry each) in %d-row statements: %v, %s",
			transactionRows, statementRows, time.Since(startedAt).Round(time.Millisecond), mysqlErrorText(writeErr))
	}
}

func TestSpikeTTL(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`frame_log`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (half_hour INT NOT NULL, partition_id INT NOT NULL, ts INT NOT NULL, record_id BIGINT NOT NULL,"+
		" changes LONGBLOB, written_at DATETIME NOT NULL, PRIMARY KEY (half_hour, partition_id, ts, record_id)) TTL (written_at + INTERVAL 48 HOUR)")
	var parameterName, enableTTL string
	rows, err := pool.Query("SHOW PARAMETERS LIKE 'enable_ttl'")
	if err != nil {
		t.Fatal(err)
	}
	columns, _ := rows.Columns()
	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		_ = rows.Scan(pointers...)
		for index, column := range columns {
			switch column {
			case "name":
				parameterName = string(values[index])
			case "value":
				enableTTL = string(values[index])
			}
		}
	}
	rows.Close()
	_, triggerErr := pool.Exec("ALTER SYSTEM TRIGGER TTL")
	spikeLog(t, "TTL table accepted; tenant %s=%s; ALTER SYSTEM TRIGGER TTL: %s", parameterName, enableTTL, mysqlErrorText(triggerErr))
}

func TestSpikeOnlineDDL(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	table := "`" + spikeDatabase(t, pool) + "`.`ddl`"
	spikeMustExec(t, pool, "CREATE TABLE "+table+" (company_id INT NOT NULL, id BIGINT NOT NULL, category_id INT, name VARCHAR(512), PRIMARY KEY (company_id, id))")
	const seededRows, statementRows = 200_000, 2000
	for firstID := 1; firstID <= seededRows; firstID += statementRows {
		args := make([]any, 0, statementRows*4)
		for id := firstID; id < firstID+statementRows; id++ {
			args = append(args, 1+id%10, id, id%100, fmt.Sprintf("name-%d", id))
		}
		spikeMustExec(t, pool, "INSERT INTO "+table+" VALUES "+placeholderRows(statementRows, 4), args...)
	}

	// Writers keep inserting while the index builds; their errors show whether the build blocks DML.
	var stopWriters atomic.Bool
	var writerErrors, writerInserts atomic.Int64
	var writersDone sync.WaitGroup
	for worker := range 4 {
		writersDone.Add(1)
		go func() {
			defer writersDone.Done()
			for id := 10_000_000 + worker*1_000_000; !stopWriters.Load(); id++ {
				if _, err := pool.Exec("INSERT INTO "+table+" VALUES (?, ?, ?, ?)", 1, id, id%100, "concurrent"); err != nil {
					writerErrors.Add(1)
					continue
				}
				writerInserts.Add(1)
			}
		}()
	}
	buildStartedAt := time.Now()
	_, buildErr := pool.Exec("ALTER TABLE " + table + " ADD INDEX ix_cat (company_id, category_id) LOCAL")
	buildDuration := time.Since(buildStartedAt)
	stopWriters.Store(true)
	writersDone.Wait()
	spikeLog(t, "ADD INDEX on %d rows: %v, %s; concurrent single-row inserts meanwhile: %d ok, %d failed",
		seededRows, buildDuration.Round(time.Millisecond), mysqlErrorText(buildErr), writerInserts.Load(), writerErrors.Load())

	indexedCount, _ := spikeInt64Rows(pool, "SELECT /*+ INDEX(ddl ix_cat) */ COUNT(*) FROM "+table+" WHERE company_id = 1 AND category_id = 7")
	plainCount, _ := spikeInt64Rows(pool, "SELECT /*+ FULL(ddl) */ COUNT(*) FROM "+table+" WHERE company_id = 1 AND category_id = 7")
	spikeLog(t, "index after concurrent build: count via index=%v, via table=%v (must match)", indexedCount, plainCount)

	renameStartedAt := time.Now()
	_, renameErr := pool.Exec("ALTER TABLE " + table + " RENAME COLUMN category_id TO family_id")
	var indexColumns []string
	indexRows, _ := pool.Query("SELECT column_name FROM information_schema.statistics WHERE table_schema = ? AND table_name = 'ddl' AND index_name = 'ix_cat' ORDER BY seq_in_index",
		strings.Trim(strings.Split(table, ".")[0], "`"))
	for indexRows != nil && indexRows.Next() {
		var columnName string
		_ = indexRows.Scan(&columnName)
		indexColumns = append(indexColumns, columnName)
	}
	if indexRows != nil {
		indexRows.Close()
	}
	spikeLog(t, "RENAME COLUMN of an indexed column: %v, %s; ix_cat columns now %v", time.Since(renameStartedAt).Round(time.Millisecond), mysqlErrorText(renameErr), indexColumns)

	compressionStartedAt := time.Now()
	_, compressionErr := pool.Exec("ALTER TABLE " + table + " COMPRESSION = 'lz4_1.0'")
	spikeLog(t, "ALTER TABLE COMPRESSION on %d rows: %v, %s", seededRows, time.Since(compressionStartedAt).Round(time.Millisecond), mysqlErrorText(compressionErr))
}

// tenantMemoryAndTablets reads the tenant's held memory and its tablet count.
func tenantMemoryAndTablets(t *testing.T, pool *sql.DB) (heldMemory int64, tabletCount int64) {
	t.Helper()
	if err := pool.QueryRow("SELECT SUM(hold) FROM oceanbase.GV$OB_TENANT_MEMORY").Scan(&heldMemory); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow("SELECT COUNT(*) FROM oceanbase.DBA_OB_TABLET_REPLICAS").Scan(&tabletCount); err != nil {
		t.Fatal(err)
	}
	return heldMemory, tabletCount
}

// TestSpikePartitionOverhead deploys 132 tables shaped like genix's (primary key, 4 local indexes,
// 1 global index) with 1 and with 8 partitions, and records tablets, memory and deploy time (O8).
func TestSpikePartitionOverhead(t *testing.T) {
	pool := requireOceanbaseIntegration(t)
	const tableCount = 132
	for _, partitionCount := range []int{1, 8} {
		databaseName := spikeDatabase(t, pool)
		memoryBefore, tabletsBefore := tenantMemoryAndTablets(t, pool)
		partitionClause := ""
		if partitionCount > 1 {
			partitionClause = fmt.Sprintf(" PARTITION BY KEY(company_id) PARTITIONS %d", partitionCount)
		}
		deployStartedAt := time.Now()
		for tableIndex := range tableCount {
			spikeMustExec(t, pool, fmt.Sprintf("CREATE TABLE `%s`.`t%03d` (company_id INT NOT NULL, id BIGINT NOT NULL, a INT, b INT, c INT, d BIGINT UNSIGNED, e INT,"+
				" name VARCHAR(512), notes LONGTEXT, PRIMARY KEY (company_id, id),"+
				" INDEX ix_a (company_id, a) LOCAL, INDEX ix_b (company_id, b) LOCAL, INDEX ix_c (company_id, c) LOCAL,"+
				" INDEX ix_d (company_id, d) STORING (a, b, name) LOCAL, INDEX ix_e (e) GLOBAL) COLLATE = utf8mb4_bin%s", databaseName, tableIndex, partitionClause))
		}
		deployDuration := time.Since(deployStartedAt)
		time.Sleep(10 * time.Second)
		memoryAfter, tabletsAfter := tenantMemoryAndTablets(t, pool)
		spikeLog(t, "partitions=%d: %d tables deployed in %v; tablets +%d (%d total); tenant memory held +%d MB (%d MB total)",
			partitionCount, tableCount, deployDuration.Round(time.Millisecond), tabletsAfter-tabletsBefore, tabletsAfter,
			(memoryAfter-memoryBefore)>>20, memoryAfter>>20)
		dropStartedAt := time.Now()
		spikeMustExec(t, pool, "DROP DATABASE `"+databaseName+"`")
		spikeLog(t, "partitions=%d: DROP DATABASE took %v", partitionCount, time.Since(dropStartedAt).Round(time.Millisecond))
	}
}
