package pgdriver

// Package-level operations the backend facade calls. Sequences live in sequences.go.

// WipePartition deletes one partition of a table and of its side tables.
func WipePartition(namespace string, tableID int16, partition int64) error {
	return errNotImplemented("WipePartition")
}

// WipeDatabase drops the connected schema, _sequences included, and creates it empty. For throwaway
// schemas (the conformance suite resets one before each test, then deploys again).
func WipeDatabase() error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	// The counters go with the database: so do this process's key blocks of them.
	tableKeyBlocks.Clear()
	return executeDeployStatements(pool, []string{
		"DROP SCHEMA IF EXISTS " + quoteName(connParams.Database) + " CASCADE",
		createSchemaStatement(connParams.Database),
	})
}
