// Package pgdriver is the PostgreSQL implementation of db.Executor: a port of obdriver (the
// OceanBase driver, docs/OCEANBASE_PLAN.md) to PostgreSQL's dialect, for the sale-flow benchmarks
// (backend/tests/loadtest/plan.md). The ORM's keyspace is a PostgreSQL schema. It is its own Go module
// so the default Scylla build does not carry the PostgreSQL client in genix-orm/go.mod.
//
// What differs from obdriver:
//   - statements are written with `?` placeholders and rebound to $n (bindStatement);
//   - an upsert is INSERT … ON CONFLICT (primary key) DO UPDATE SET c = EXCLUDED.c;
//   - an indexed slice is a JSONB array with a GIN index, matched with @> (no multi-value index);
//   - a covering index lists its projection with INCLUDE; there are no global indexes, hints,
//     partitions, column groups or table TTL (the DataFrame log keeps its rows);
//   - PostgreSQL has no unsigned integers: a uint64 (a packed key) is stored in a BIGINT with its top
//     bit flipped, as Scylla stores it, which keeps the unsigned order.
package pgdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	// The pgx database/sql driver registers itself as "pgx".
	"github.com/jackc/pgx/v5/stdlib"
)

// ConnParams is what Connect needs. Database is the keyspace: the PostgreSQL schema tables default
// to. DatabaseName is the PostgreSQL database the connection opens.
type ConnParams struct {
	Host         string
	Port         int
	User         string
	Password     string
	DatabaseName string
	Database     string
	MaxConns     int
}

var (
	connParams      ConnParams
	connectionMutex sync.Mutex
	sharedPool      *sql.DB
	connectTimeout  = 15 * time.Second
)

// SetConnection records the parameters; the pool opens on first use.
func SetConnection(params ConnParams) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	connParams = params
	if sharedPool != nil {
		_ = sharedPool.Close()
		sharedPool = nil
	}
	// db.RunTransaction runs its body in one of this driver's transactions.
	db.SetTransactionRunner(func(body func(driverTransaction any) error) error {
		return runTransaction(func(transaction *writeTransaction) error { return body(transaction) })
	})
}

// Database is the default schema (keyspace) SetConnection received.
func Database() string { return connParams.Database }

// sharedConnection opens the process-wide pool once.
func sharedConnection() (*sql.DB, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if sharedPool != nil {
		return sharedPool, nil
	}
	if connParams.Host == "" || connParams.User == "" {
		return nil, errors.New("pgdriver: no host/user configured (SetConnection)")
	}
	databaseName := connParams.DatabaseName
	if databaseName == "" {
		databaseName = "postgres"
	}
	// Statements are cached per connection (pgx's default): a write repeats few shapes. Values are
	// quoted: an empty password (YugabyteDB's default user) would otherwise swallow the next key.
	quoted := func(value string) string {
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
	}
	// lock_timeout caps a lock wait (55P03, retried): a cache-group mark stamped before a long wait
	// would otherwise commit past the settle window (db.SQLIndexGroupSettleTicks).
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=%d lock_timeout=%d",
		quoted(connParams.Host), connParams.Port, quoted(connParams.User), quoted(connParams.Password), quoted(databaseName), int(connectTimeout.Seconds()),
		lockTimeout.Milliseconds())
	pool, err := sql.Open("pgx", connectionString)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: opening %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	maxConns := connParams.MaxConns
	// 16 is what the load tests measured on 2 vCPUs: more connections only queue on the server's cores.
	if maxConns <= 0 {
		maxConns = 16
	}
	pool.SetMaxOpenConns(maxConns)
	pool.SetMaxIdleConns(maxConns)
	sharedPool = pool
	serverIsCockroach = nil
	return sharedPool, nil
}

// serverIsCockroach caches whether the server is CockroachDB (nil: not asked yet on this pool).
var serverIsCockroach *bool

// isCockroachDB tells whether the server speaks CockroachDB's dialect of the PostgreSQL wire protocol.
// Its DDL differs in two places: a STRING already compares byte by byte (it has no "C" collation),
// and a GIN index takes no operator class.
func isCockroachDB() bool {
	pool, err := sharedConnection()
	if err != nil {
		return false
	}
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if serverIsCockroach == nil {
		var serverVersion string
		if err := pool.QueryRow("SELECT version()").Scan(&serverVersion); err != nil {
			return false
		}
		isCockroach := strings.Contains(serverVersion, "CockroachDB")
		serverIsCockroach = &isCockroach
	}
	return *serverIsCockroach
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
		return fmt.Errorf("pgdriver: ping %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	return nil
}

// lockTimeout is the session's lock_timeout: well under the 8 s settle window of a cache group.
const lockTimeout = 3 * time.Second

// requestTimeout bounds one transaction attempt.
const requestTimeout = 300 * time.Second

// transactionAttempts bounds the retries of a transaction that lost a deadlock or a serialization.
const transactionAttempts = 5

// retriedErrorCodes are the SQLSTATEs a transaction runs again on: serialization_failure,
// deadlock_detected and lock_not_available.
var retriedErrorCodes = map[string]bool{"40001": true, "40P01": true, "55P03": true}

// runTransaction runs body in one read-committed transaction (the server's default level) and
// commits it. A deadlock rolls back and runs body again, which re-reads whatever it locks, so the
// retry works on the state that won.
func runTransaction(body func(transaction *writeTransaction) error) error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		err := runTransactionOnce(pool, body)
		var serverError *pgconn.PgError
		if err == nil || attempt >= transactionAttempts || !errors.As(err, &serverError) || !retriedErrorCodes[serverError.Code] {
			return err
		}
	}
}

// runTransactionOnce runs one attempt on the pgx connection under a pooled database/sql one: a pgx
// batch is a pipeline, which database/sql cannot send.
func runTransactionOnce(pool *sql.DB, body func(transaction *writeTransaction) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	connection, err := pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	var bodyError error
	err = connection.Raw(func(driverConnection any) error {
		transaction := &writeTransaction{connection: driverConnection.(*stdlib.Conn).Conn(), ctx: ctx, pending: &pgx.Batch{}}
		transaction.pending.Queue("BEGIN")
		bodyError = body(transaction)
		if bodyError == nil {
			bodyError = transaction.queueGroupMarks()
		}
		if bodyError == nil {
			transaction.pending.Queue("COMMIT")
			bodyError = transaction.flush()
		}
		if bodyError == nil {
			return nil
		}
		// An attempt that failed after its BEGIN went out leaves the session in a transaction.
		if _, rollbackError := transaction.connection.Exec(ctx, "ROLLBACK"); rollbackError != nil {
			return driver.ErrBadConn
		}
		return nil
	})
	if bodyError != nil {
		return bodyError
	}
	return err
}

// writeTransaction is a transaction on one pgx connection. BEGIN and the statements that return
// nothing (every write) are queued and pipelined in one pgx batch: with the next read, which must see
// them and rides in the same batch, or with the COMMIT. A sale's write is then one or two round trips
// (its locking read with the BEGIN, then its writes with the COMMIT) instead of one per statement
// plus BEGIN and COMMIT. An error stops the batch there; the caller rolls back.
type writeTransaction struct {
	connection   *pgx.Conn
	ctx          context.Context
	pending      *pgx.Batch
	pendingBytes int
	// deferredMarks are the transaction's cache-group marks by mark table, written last
	// (queueGroupMarks).
	deferredMarks map[string][]groupMark
	// lockedRecords are the rows LockByKeys read under lock, by table and primaryKeyToken: a later
	// write of the same rows in this transaction reuses them instead of reading them again
	// (storedRecordsForChunk). A written row leaves the map, its stored state having changed.
	lockedRecords map[string]map[string]unsafe.Pointer
}

// queue adds a statement without result to the next batch. A batch nearing maxStatementBytes is sent
// first.
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
	transaction.pending.Queue(bindStatement(statement), bindArguments(args)...)
	transaction.pendingBytes += statementBytes
	return nil
}

// flush sends the queued statements.
func (transaction *writeTransaction) flush() error {
	if transaction.pending.Len() == 0 {
		return nil
	}
	results := transaction.connection.SendBatch(transaction.ctx, transaction.pending)
	transaction.pending, transaction.pendingBytes = &pgx.Batch{}, 0
	return results.Close()
}

// queryRows reads inside the transaction, in one batch with what is queued.
func (transaction *writeTransaction) queryRows(ctx context.Context, statement string, args ...any) (scannedRows, func(), error) {
	queuedCount := transaction.pending.Len()
	transaction.pending.Queue(bindStatement(statement), bindArguments(args)...)
	results := transaction.connection.SendBatch(ctx, transaction.pending)
	transaction.pending, transaction.pendingBytes = &pgx.Batch{}, 0
	for range queuedCount {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return nil, nil, err
		}
	}
	rows, err := results.Query()
	if err != nil {
		_ = results.Close()
		return nil, nil, err
	}
	return rows, func() { rows.Close(); _ = results.Close() }, nil
}

// scannedRows is what a read decodes: database/sql's rows or pgx's.
type scannedRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// rowQuerier is the pool or a write transaction.
type rowQuerier interface {
	queryRows(ctx context.Context, statement string, args ...any) (scannedRows, func(), error)
}

// poolQuerier reads through the database/sql pool.
type poolQuerier struct{ pool *sql.DB }

func (querier poolQuerier) queryRows(ctx context.Context, statement string, args ...any) (scannedRows, func(), error) {
	rows, err := queryStatement(querier.pool, ctx, statement, args...)
	if err != nil {
		return nil, nil, err
	}
	return rows, func() { _ = rows.Close() }, nil
}

// errNotImplemented marks the operations this driver does not serve. Each one fails loudly instead
// of answering wrong.
func errNotImplemented(operation string) error {
	return fmt.Errorf("pgdriver: %s is not implemented on PostgreSQL", operation)
}

/* Statements */

// sqlRunner is the pool or a transaction.
type sqlRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func execStatement(runner sqlRunner, ctx context.Context, statement string, args ...any) (sql.Result, error) {
	return runner.ExecContext(ctx, bindStatement(statement), bindArguments(args)...)
}

func queryStatement(runner sqlRunner, ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	return runner.QueryContext(ctx, bindStatement(statement), bindArguments(args)...)
}

func queryRowStatement(runner sqlRunner, ctx context.Context, statement string, args ...any) *sql.Row {
	return runner.QueryRowContext(ctx, bindStatement(statement), bindArguments(args)...)
}

// bindStatement numbers the `?` placeholders of a statement: $1, $2… A `?` inside a quoted name or a
// string literal is left alone.
func bindStatement(statement string) string {
	if !strings.Contains(statement, "?") {
		return statement
	}
	rebound := strings.Builder{}
	rebound.Grow(len(statement) + 16)
	placeholderNumber := 0
	var openQuote byte
	for position := 0; position < len(statement); position++ {
		character := statement[position]
		switch {
		case openQuote != 0:
			if character == openQuote {
				openQuote = 0
			}
		case character == '"' || character == '\'':
			openQuote = character
		case character == '?':
			placeholderNumber++
			rebound.WriteByte('$')
			rebound.WriteString(strconv.Itoa(placeholderNumber))
			continue
		}
		rebound.WriteByte(character)
	}
	return rebound.String()
}

// bindArguments stores what PostgreSQL has no type for: a uint64 (or uint) as a BIGINT with its top
// bit flipped, so the stored values keep the unsigned order (Scylla's scylla/index_int_packing.go). A
// smaller unsigned value fits a BIGINT as it is.
func bindArguments(args []any) []any {
	for position, argument := range args {
		switch typed := argument.(type) {
		case uint64:
			args[position] = int64(typed ^ (1 << 63))
		case uint:
			args[position] = int64(uint64(typed) ^ (1 << 63))
		case uint32:
			args[position] = int64(typed)
		case uint16:
			args[position] = int64(typed)
		case uint8:
			args[position] = int64(typed)
		}
	}
	return args
}

// unflipUnsigned is the uint64 a BIGINT bound by bindArguments holds.
func unflipUnsigned(stored int64) uint64 { return uint64(stored) ^ (1 << 63) }
