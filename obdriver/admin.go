package obdriver

// Package-level operations the backend facade calls (docs/OCEANBASE_PLAN.md §12). Sequences live in
// sequences.go; each operation not yet implemented answers errNotImplemented until Step 7.

// WipePartition deletes one partition of a table and of its side tables (Step 7).
func WipePartition(namespace string, tableID int16, partition int64) error {
	return errNotImplemented("WipePartition")
}

// WipeDatabase drops the connected database, _sequences included, and creates it empty. For
// throwaway databases (the conformance suite resets one before each test, then deploys again).
func WipeDatabase() error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	// The counters go with the database: so do this process's key blocks of them.
	tableKeyBlocks.Clear()
	return executeDeployStatements(pool, []string{
		"DROP DATABASE IF EXISTS " + quoteName(connParams.Database),
		createDatabaseStatement(connParams.Database),
	})
}
