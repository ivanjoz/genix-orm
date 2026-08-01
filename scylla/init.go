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

// Init prepares a keyspace for use: it creates the keyspace and the ORM's own internal tables.
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
	return EnsureInternalTables()
}

// EnsureInternalTables creates the tables the ORM maintains for itself: the shared sequence
// counters and the by-IDs slot versions.
//
// These are raw CQL rather than declared TableSchemas, so DeployScylla's homologation pass cannot
// discover them — it only knows about the controllers it is handed. Every entry point that prepares
// a keyspace has to call this, which is why DeployScylla does so itself rather than relying on the
// caller having run Init first. All statements are IF NOT EXISTS, so re-running costs nothing.
func EnsureInternalTables() error {
	if err := InitSequencesTable(); err != nil {
		return err
	}
	return InitCacheUpdatedVersionTable()
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
