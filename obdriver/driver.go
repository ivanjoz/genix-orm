// Package obdriver is the OceanBase (MySQL mode) implementation of db.Executor
// (docs/OCEANBASE_PLAN.md). It is its own Go module so the default Scylla build does not carry the
// MySQL client in genix-orm/go.mod.
package obdriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ConnParams is what Connect needs. User carries the tenant (`root@test`); Database is the keyspace
// tables default to. Partitions is the KEY(company_id) partition count of every table, 1 for none
// (plan O8). Engine is "" for OceanBase or EngineSeekDB.
type ConnParams struct {
	Host       string
	Port       int
	User       string
	Password   string
	Database   string
	Partitions int
	MaxConns   int
	Engine     string
}

// EngineSeekDB is seekdb (github.com/oceanbase/seekdb), the OceanBase SQL engine as one single-tenant
// server, which lacks column groups and table TTL: the deploy then creates row tables only and a
// DataFrame log that keeps its rows (fitToEngine). For benchmarks, not for a database that lives.
const EngineSeekDB = "seekdb"

// sessionQueryTimeout replaces the tenant's 10 s `ob_query_timeout` default on every connection:
// partition scans, index builds and large chunks outlive it. Microseconds, as OceanBase expects.
const sessionQueryTimeout = 600 * time.Second

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
	config.Params = map[string]string{
		"ob_query_timeout": strconv.FormatInt(sessionQueryTimeout.Microseconds(), 10),
	}
	return config
}

// sharedConnection opens the process-wide pool once (plan §6).
func sharedConnection() (*sql.DB, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if sharedPool != nil {
		return sharedPool, nil
	}
	if connParams.Host == "" || connParams.User == "" {
		return nil, errors.New("obdriver: no host/user configured (SetConnection)")
	}
	connector, err := mysql.NewConnector(connectionConfig(connParams))
	if err != nil {
		return nil, fmt.Errorf("obdriver: connector for %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	maxConns := connParams.MaxConns
	if maxConns <= 0 {
		maxConns = 64
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(maxConns)
	pool.SetMaxIdleConns(maxConns)
	sharedPool = pool
	return sharedPool, nil
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
		return fmt.Errorf("obdriver: ping %s:%d: %w", connParams.Host, connParams.Port, err)
	}
	return nil
}

// requestTimeout bounds one transaction attempt, below the session's ob_query_timeout.
const requestTimeout = 300 * time.Second

// transactionAttempts bounds the retries of a transaction that lost a deadlock or a lock wait.
const transactionAttempts = 5

// runTransaction runs body in one read-committed transaction (the tenant default, plan §6) and
// commits it. A deadlock (1213) or lock-wait timeout (1205) rolls back and runs body again, which
// re-reads whatever it locks, so the retry works on the state that won.
func runTransaction(pool *sql.DB, body func(transaction *sql.Tx, ctx context.Context) error) error {
	for attempt := 1; ; attempt++ {
		err := runTransactionOnce(pool, body)
		var serverError *mysql.MySQLError
		if err == nil || attempt >= transactionAttempts || !errors.As(err, &serverError) || (serverError.Number != 1213 && serverError.Number != 1205) {
			return err
		}
	}
}

func runTransactionOnce(pool *sql.DB, body func(transaction *sql.Tx, ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	transaction, err := pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	if err := body(transaction, ctx); err != nil {
		_ = transaction.Rollback()
		return err
	}
	return transaction.Commit()
}

// errNotImplemented marks the operations this driver does not serve yet. Each one fails loudly
// instead of answering wrong; docs/OCEANBASE_PLAN.md §16 lists the step that delivers each.
func errNotImplemented(operation string) error {
	return fmt.Errorf("obdriver: %s is not implemented on OceanBase yet", operation)
}
