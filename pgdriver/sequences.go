package pgdriver

import (
	"context"
	"fmt"
	"sync"
)

// Sequences (obdriver's, docs/OCEANBASE_PLAN.md O14): PostgreSQL owns them. One row per counter in
// `<database>._sequences`, advanced under its row lock, so two writers never mint the same ID. A write
// reserves its IDs before its own transaction, so a retried write chunk does not burn IDs.

// GetAutoincrementID reserves recordsSize consecutive IDs of the default database and returns the
// first one.
func GetAutoincrementID(key string, recordsSize int) (int64, error) {
	return reserveSequence(connParams.Database, key, recordsSize)
}

// reserveSequence reserves count consecutive IDs of a counter and returns the first one.
func reserveSequence(database, counterName string, count int) (int64, error) {
	if count <= 0 {
		return 0, fmt.Errorf("pgdriver: reserving IDs on %q: the count must be positive, got %d", counterName, count)
	}
	pool, err := sharedConnection()
	if err != nil {
		return 0, err
	}
	// One statement: the upsert takes the row lock, so concurrent callers queue on it and never get
	// overlapping ranges.
	var lastValue int64
	statement := "INSERT INTO " + sequencesTableSpec(database).qualifiedName() + ` AS "sequence" ("name", "value") VALUES (?, ?)
		ON CONFLICT ("name") DO UPDATE SET "value" = "sequence"."value" + EXCLUDED."value" RETURNING "value"`
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := queryRowStatement(pool, ctx, statement, counterName, int64(count)).Scan(&lastValue); err != nil {
		return 0, fmt.Errorf("pgdriver: reserving %d IDs on %q: %w", count, counterName, err)
	}
	return lastValue - int64(count) + 1, nil
}

// tableKeyBlockSize is how many autoincrement keys one sequence transaction reserves for a table: the
// process hands them out from memory until the block runs out (reserveTableKeys).
const tableKeyBlockSize = 64

// tableKeyBlock is the unused rest of a counter's last reserved block: [next, end).
type tableKeyBlock struct {
	mutex     sync.Mutex
	next, end int64
}

// tableKeyBlocks holds a block per database + counter name.
var tableKeyBlocks sync.Map

// useTableKeyBlocks turns the blocks on (UseTableKeyBlocks). Off, every write reserves exactly its
// keys: a short-lived process (fn-init, a script) would otherwise strand most of a block per counter.
var useTableKeyBlocks bool

// UseTableKeyBlocks makes this process reserve table keys in blocks: for the long-lived server, the
// one process that writes most and whose gaps are at most one block per counter per restart.
func UseTableKeyBlocks() { useTableKeyBlocks = true }

// reserveTableKeys reserves count consecutive autoincrement keys of a table, from this process's block
// of the counter when it holds enough, otherwise from a new block of max(count, tableKeyBlockSize). A
// sale then commits one sequence transaction per ~64 keys instead of one per write. The database
// stays the only source of the counter, so blocks never overlap across processes; what a block leaves
// unused (a restart, a request larger than its rest) is a gap, harmless for record keys. Counters that
// must be gapless (invoice correlativos) go through GetAutoincrementID, which reserves exactly.
func reserveTableKeys(database, counterName string, count int) (int64, error) {
	if !useTableKeyBlocks {
		return reserveSequence(database, counterName, count)
	}
	blockAny, _ := tableKeyBlocks.LoadOrStore(database+"\x00"+counterName, &tableKeyBlock{})
	block := blockAny.(*tableKeyBlock)
	block.mutex.Lock()
	defer block.mutex.Unlock()
	if block.end-block.next < int64(count) {
		reservedCount := max(count, tableKeyBlockSize)
		firstKey, err := reserveSequence(database, counterName, reservedCount)
		if err != nil {
			return 0, err
		}
		block.next, block.end = firstKey, firstKey+int64(reservedCount)
	}
	firstKey := block.next
	block.next += int64(count)
	return firstKey, nil
}

// SetCounter moves a sequence of the default database to an absolute value and returns what it held
// (0 when it did not exist). This process's block of the counter is dropped: its keys belong to the
// old value.
func SetCounter(counterName string, value int64) (int64, error) {
	tableKeyBlocks.Delete(connParams.Database + "\x00" + counterName)
	previousValue, err := advanceSequence(connParams.Database, counterName, func(int64) int64 { return value })
	if err != nil {
		return 0, fmt.Errorf("pgdriver: SetCounter %q: %w", counterName, err)
	}
	return previousValue, nil
}

// ListCounters returns the names of every sequence of the default database.
func ListCounters() ([]string, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	rows, err := pool.Query(fmt.Sprintf(`SELECT "name" FROM %s ORDER BY "name"`, sequencesTableSpec(connParams.Database).qualifiedName()))
	if err != nil {
		return nil, fmt.Errorf("pgdriver: ListCounters: %w", err)
	}
	defer rows.Close()
	counterNames := []string{}
	for rows.Next() {
		var counterName string
		if err := rows.Scan(&counterName); err != nil {
			return nil, fmt.Errorf("pgdriver: ListCounters: %w", err)
		}
		counterNames = append(counterNames, counterName)
	}
	return counterNames, rows.Err()
}

// advanceSequence writes nextValue(previous) and returns previous, in one transaction: the counter
// row is created at 0 when missing, then locked by the read, so concurrent callers queue on it.
func advanceSequence(database, counterName string, nextValue func(previous int64) int64) (int64, error) {
	sequencesTable := sequencesTableSpec(database).qualifiedName()
	var previousValue int64
	err := runTransaction(func(transaction *writeTransaction) error {
		if err := transaction.queue("INSERT INTO "+sequencesTable+` ("name", "value") VALUES (?, 0) ON CONFLICT DO NOTHING`, counterName); err != nil {
			return err
		}
		rows, closeRows, err := transaction.queryRows(transaction.ctx, "SELECT \"value\" FROM "+sequencesTable+` WHERE "name" = ? FOR UPDATE`, counterName)
		if err != nil {
			return err
		}
		defer closeRows()
		if !rows.Next() {
			return fmt.Errorf("counter %q: no row after creating it: %w", counterName, rows.Err())
		}
		if err := rows.Scan(&previousValue); err != nil {
			return err
		}
		closeRows()
		return transaction.queue("UPDATE "+sequencesTable+` SET "value" = ? WHERE "name" = ?`, nextValue(previousValue), counterName)
	})
	return previousValue, err
}
