// Package mariadbdriver is the MariaDB (InnoDB) implementation of db.Executor: a port of obdriver
// (the OceanBase driver, same MySQL dialect) for the sale-flow benchmarks (backend/tests/loadtest).
// It is its own Go module so the default Scylla build does not carry the MySQL client.
//
// What differs from obdriver:
//   - no partitions, column groups, table TTL (the DataFrame log keeps its rows), compression, index
//     hints or STORING (no index is covering);
//   - MariaDB has no multi-value index: an indexed slice is JSON in the main table plus an element
//     side table, rebuilt from that JSON in the record's transaction (write.go, writeElementIndexes).
package mariadbdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/go-sql-driver/mysql"
	"github.com/ivanjoz/genix-orm/db"
)

// ConnParams is what Connect needs. Database is the keyspace tables default to (a MariaDB database).
type ConnParams struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	MaxConns int
}

var (
	connParams      ConnParams
	connectionMutex sync.Mutex
	sharedPool      *sql.DB
	// sharedWritePool serves the write transactions (runTransaction): its sessions run with autocommit
	// off, so a transaction opens with its first statement and needs no START TRANSACTION.
	sharedWritePool *sql.DB
	connectTimeout  = 15 * time.Second
)

// SetConnection records the parameters; the pools open on first use.
func SetConnection(params ConnParams) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	connParams = params
	for _, pool := range []*sql.DB{sharedPool, sharedWritePool} {
		if pool != nil {
			_ = pool.Close()
		}
	}
	sharedPool, sharedWritePool = nil, nil
	// db.RunTransaction runs its body in one of this driver's transactions.
	db.SetTransactionRunner(func(body func(driverTransaction any) error) error {
		return runTransaction(func(transaction *writeTransaction) error { return body(transaction) })
	})
}

// Database is the default database (keyspace) SetConnection received.
func Database() string { return connParams.Database }

// connectionConfig builds the client config. No database is selected: every statement names its
// table as `database`.`table`, so the pool can create the database at deploy.
func connectionConfig(params ConnParams) *mysql.Config {
	config := mysql.NewConfig()
	config.Net = "tcp"
	config.Addr = net.JoinHostPort(params.Host, strconv.Itoa(params.Port))
	config.User = params.User
	config.Passwd = params.Password
	// utf8mb4_bin compares bytes like Scylla (plan O16); the server default is case-insensitive.
	config.Collation = "utf8mb4_bin"
	config.Timeout = connectTimeout
	// The client inlines the arguments: one round trip per statement instead of the driver's
	// prepare + execute + close. The server's fast parser pulls the literals back out and reuses the
	// cached plan, so nothing is re-optimized; every scan target is typed, so the text protocol's
	// results decode the same.
	config.InterpolateParams = true
	// Read committed is set once per connection: a transaction that asks for it instead makes the
	// client send SET TRANSACTION ISOLATION LEVEL before every START TRANSACTION, one more round trip
	// per transaction.
	//
	// in_predicate_conversion_threshold=0 keeps a list of primary-key tuples (`(pk) IN ((…), (…))`) as
	// key lookups. By default 1000 values or more become a join against a derived table, which on a
	// freshly created table's statistics is planned as a full scan: harmless for InnoDB at read
	// committed, which unlocks the rows that do not match, but MyRocks keeps every scanned row locked,
	// and concurrent writers of different partitions lock each other out.
	config.Params = map[string]string{
		"transaction_isolation":             "'READ-COMMITTED'",
		"in_predicate_conversion_threshold": "0",
		// Caps a row-lock wait (1205, retried): a cache-group mark stamped before a long wait would
		// otherwise commit past the settle window (db.SQLIndexGroupSettleTicks).
		"innodb_lock_wait_timeout": "3",
	}
	// A write transaction sends its queued statements and the COMMIT as one multi-statement packet
	// (writeTransaction). With interpolated arguments every statement of the packet may carry them.
	config.MultiStatements = true
	return config
}

// primaryKeyLookup pins a read of listed primary keys to the primary key: a new table's statistics
// can make the optimizer pick a secondary index that covers only the partition, which reads (and,
// FOR UPDATE, locks) the whole partition.
const primaryKeyLookup = "FORCE INDEX (PRIMARY)"

// sharedConnection opens the process-wide autocommit pool once (plan §6): reads and single
// statements.
func sharedConnection() (*sql.DB, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if sharedPool == nil {
		var err error
		if sharedPool, err = openPool(false); err != nil {
			return nil, err
		}
	}
	return sharedPool, nil
}

// sharedWriteConnection opens the process-wide pool of the write transactions once.
func sharedWriteConnection() (*sql.DB, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if sharedWritePool == nil {
		var err error
		if sharedWritePool, err = openPool(true); err != nil {
			return nil, err
		}
	}
	return sharedWritePool, nil
}

func openPool(isTransactional bool) (*sql.DB, error) {
	if connParams.Host == "" || connParams.User == "" {
		return nil, errors.New("mariadbdriver: no host/user configured (SetConnection)")
	}
	config := connectionConfig(connParams)
	if isTransactional {
		config.Params["autocommit"] = "0"
	}
	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, fmt.Errorf("mariadbdriver: connector for %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	maxConns := connParams.MaxConns
	if maxConns <= 0 {
		maxConns = 64
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(maxConns)
	pool.SetMaxIdleConns(maxConns)
	return pool, nil
}

// Ping opens the pool and runs a trivial query, so a wrong address or user fails before any work.
func Ping() error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	pingContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	if err := pool.PingContext(pingContext); err != nil {
		return fmt.Errorf("mariadbdriver: ping %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	return nil
}

// requestTimeout bounds one transaction attempt.
const requestTimeout = 300 * time.Second

// transactionAttempts bounds the retries of a transaction that lost a deadlock or a lock wait.
const transactionAttempts = 5

// runTransaction runs body in one read-committed transaction (the connection's level) and
// commits it. A deadlock (1213) or lock-wait timeout (1205) rolls back and runs body again, which
// re-reads whatever it locks, so the retry works on the state that won.
func runTransaction(body func(transaction *writeTransaction) error) error {
	pool, err := sharedWriteConnection()
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		err := runTransactionOnce(pool, body)
		var serverError *mysql.MySQLError
		if err == nil || attempt >= transactionAttempts || !errors.As(err, &serverError) || (serverError.Number != 1213 && serverError.Number != 1205) {
			return err
		}
	}
}

func runTransactionOnce(pool *sql.DB, body func(transaction *writeTransaction) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	connection, err := pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	transaction := &writeTransaction{connection: connection, ctx: ctx}
	err = body(transaction)
	if err == nil {
		err = transaction.queueGroupMarks()
	}
	if err == nil {
		if err = transaction.queue("COMMIT"); err == nil && transaction.holdsTableLocks {
			err = transaction.queue("UNLOCK TABLES")
		}
		if err == nil {
			err = transaction.flush()
		}
	}
	if err != nil {
		// The session has autocommit off: whatever the failed attempt did stays open until rolled back.
		// ROLLBACK keeps the table locks: the session must not go back to the pool holding them.
		_, rollbackError := connection.ExecContext(ctx, "ROLLBACK")
		if rollbackError == nil && transaction.holdsTableLocks {
			_, rollbackError = connection.ExecContext(ctx, "UNLOCK TABLES")
		}
		if rollbackError != nil {
			_ = connection.Raw(func(any) error { return driver.ErrBadConn })
		}
	}
	return err
}

// writeTransaction is a transaction on one session of the write pool. The statements that return
// nothing (every write) are queued and sent together, in one multi-statement round trip: before the
// next read, which must see them, or with the COMMIT. A sale's write is then two round trips (its
// locking read, then its writes and the commit) instead of one per statement plus START TRANSACTION
// and COMMIT. A statement's error stops the packet there; the caller rolls back.
type writeTransaction struct {
	connection   *sql.Conn
	ctx          context.Context
	pendingSQL   strings.Builder
	pendingArgs  []any
	pendingBytes int
	// holdsTableLocks: the transaction ran LOCK TABLES (lockTables), released after its COMMIT or ROLLBACK.
	holdsTableLocks bool
	// deferredMarks are the transaction's cache-group marks by mark table, written last
	// (queueGroupMarks).
	deferredMarks map[string][]groupMark
	// lockedRecords are the rows LockByKeys read under lock, by table and primaryKeyToken: a later
	// write of the same rows in this transaction reuses them instead of reading them again
	// (storedRecordsForChunk). A written row leaves the map, its stored state having changed.
	lockedRecords map[string]map[string]unsafe.Pointer
}

// lockTables runs a LOCK TABLES statement as the transaction's first: with autocommit off it locks
// InnoDB tables until UNLOCK TABLES, which runTransactionOnce sends after the COMMIT or the ROLLBACK.
func (transaction *writeTransaction) lockTables(statement string) error {
	transaction.holdsTableLocks = true
	return transaction.queue(statement)
}

// queue adds a statement without result to the next packet. A packet nearing maxStatementBytes is
// sent first, so it stays under max_allowed_packet.
func (transaction *writeTransaction) queue(statement string, args ...any) error {
	statementBytes := len(statement)
	for _, value := range args {
		statementBytes += argumentBytes(value)
	}
	if transaction.pendingBytes > 0 && transaction.pendingBytes+statementBytes > maxStatementBytes {
		if err := transaction.flush(); err != nil {
			return err
		}
	}
	if transaction.pendingSQL.Len() > 0 {
		transaction.pendingSQL.WriteString(";\n")
	}
	transaction.pendingSQL.WriteString(statement)
	transaction.pendingArgs = append(transaction.pendingArgs, args...)
	transaction.pendingBytes += statementBytes
	return nil
}

// flush sends the queued statements.
func (transaction *writeTransaction) flush() error {
	if transaction.pendingSQL.Len() == 0 {
		return nil
	}
	_, err := transaction.connection.ExecContext(transaction.ctx, transaction.pendingSQL.String(), transaction.pendingArgs...)
	transaction.pendingSQL.Reset()
	transaction.pendingArgs, transaction.pendingBytes = nil, 0
	return err
}

// QueryContext reads inside the transaction, after sending what is queued.
func (transaction *writeTransaction) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	if err := transaction.flush(); err != nil {
		return nil, err
	}
	return transaction.connection.QueryContext(ctx, statement, args...)
}

// errNotImplemented marks the operations this driver does not serve. Each one fails loudly instead
// of answering wrong.
func errNotImplemented(operation string) error {
	return fmt.Errorf("mariadbdriver: %s is not implemented on MariaDB", operation)
}
