package scylla

import (
	"errors"
	"fmt"

	"github.com/ivanjoz/genix-orm/db"
)

// Register installs this driver into the shared db layer: how ORM type IDs are
// named in CQL, and the value paths that depend on Cassandra's type system.
// It runs on import because the shared accessor engine consults both before any
// table is compiled.
func init() {
	db.DBTypeResolver = func(typeID int8) string { return cqlTypeNames[typeID] }
	db.SetCodec(scyllaValueCodec{})
	// These two take a counter key or a table name rather than table types, so they
	// cannot be generic Executor methods; the driver installs them instead.
	db.GetAutoincrementID = GetAutoincrementID
	db.QueryCachedGenericByIDs = QueryCachedGenericByIDs
	db.SetDebugLogging = SetDebugLogging
}

// Init creates the ORM internal tables required before sequence or cache-version features are used.
//
// Sonic text-search backend: callers wire the Sonic endpoint via
// text_search.Configure(host, port, password) before the first write.
// The db package can't do it here without creating a core ->
// core/types -> db -> core import cycle, so the application entry
// points (main.go, exec/init.go) call Configure themselves after
// core.PopulateVariables.
func Init() error {
	if err := CreateKeyspaceIfNotExists(); err != nil {
		return err
	}
	if err := InitSequencesTable(); err != nil {
		return err
	}
	if err := InitCacheVersionTable(); err != nil {
		return err
	}
	return nil
}

// CreateKeyspaceIfNotExists ensures the configured keyspace exists in ScyllaDB,
// creating it with SimpleStrategy / replication_factor=1 when missing.
func CreateKeyspaceIfNotExists() error {
	keyspace := connParams.Keyspace
	if keyspace == "" {
		return errors.New("CreateKeyspaceIfNotExists: no keyspace configured")
	}
	stmt := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %v WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		keyspace,
	)
	return QueryExec(stmt)
}

// InitSequencesTable ensures the shared autoincrement counter table exists before sequence-backed inserts run.
func InitSequencesTable() error {
	keyspace := connParams.Keyspace
	if keyspace == "" {
		return errors.New("Init: no keyspace configured")
	}

	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %v.sequences (
			name text, current_value counter,
			PRIMARY KEY (name)
		)
		%v;`,
		keyspace, makeStatementWith)

	return QueryExec(createTableQuery)
}
