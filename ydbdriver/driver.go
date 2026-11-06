// Package ydbdriver is the YDB implementation of db.Executor (docs/YDB_PLAN.md). It is its own Go
// module so the default Scylla build does not carry gRPC/protobuf in genix-orm/go.mod.
package ydbdriver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// ConnParams is what Connect needs: the gRPC endpoint (host:port), the database path and the
// keyspace directory tables default to.
type ConnParams struct {
	Endpoint string
	Database string
	Keyspace string
}

var (
	connParams      ConnParams
	connectionMutex sync.Mutex
	sharedDriver    *ydb.Driver
	connectTimeout  = 15 * time.Second
)

// SetConnection records the parameters; the driver dials on first use.
func SetConnection(params ConnParams) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	connParams = params
	if sharedDriver != nil {
		_ = sharedDriver.Close(context.Background())
		sharedDriver = nil
	}
}

// Keyspace is the default directory Connect received.
func Keyspace() string { return connParams.Keyspace }

// sharedConnection opens the process-wide *ydb.Driver once (plan §6).
func sharedConnection() (*ydb.Driver, error) {
	connectionMutex.Lock()
	defer connectionMutex.Unlock()
	if sharedDriver != nil {
		return sharedDriver, nil
	}
	if connParams.Endpoint == "" || connParams.Database == "" {
		return nil, errors.New("ydbdriver: no endpoint/database configured (SetConnection)")
	}
	openContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	openedDriver, err := ydb.Open(openContext, fmt.Sprintf("grpc://%s%s", connParams.Endpoint, connParams.Database))
	if err != nil {
		return nil, fmt.Errorf("ydbdriver: connecting to %s%s: %w", connParams.Endpoint, connParams.Database, err)
	}
	sharedDriver = openedDriver
	return sharedDriver, nil
}

// Ping opens the connection and runs a trivial query, so a wrong endpoint fails before any work.
func Ping() error {
	connection, err := sharedConnection()
	if err != nil {
		return err
	}
	pingContext, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	if err := connection.Query().Exec(pingContext, "SELECT 1"); err != nil {
		return fmt.Errorf("ydbdriver: ping: %w", err)
	}
	return nil
}

// errNotImplemented marks the db.Executor operations this driver does not serve yet. Each one fails
// loudly instead of answering wrong; docs/YDB_PLAN.md §16 lists the step that delivers each.
func errNotImplemented(operation string) error {
	return fmt.Errorf("ydbdriver: %s is not implemented on YDB yet", operation)
}
