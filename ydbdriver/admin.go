package ydbdriver

import (
	"context"
	"fmt"
)

// WipePartition deletes one partition of a table and of its side tables (Step 7).
func WipePartition(namespace string, tableID int16, partition int64) error {
	return errNotImplemented("WipePartition")
}

// WipeKeyspace drops every table of the connected keyspace directory, _sequences included. For
// throwaway keyspaces (the conformance suite resets one before each test, then deploys again).
func WipeKeyspace() error {
	connection, err := sharedConnection()
	if err != nil {
		return err
	}
	tableNames, err := listKeyspaceTables(connection, Keyspace())
	if err != nil {
		return err
	}
	for tableName := range tableNames {
		dropContext, cancel := context.WithTimeout(context.Background(), deployStatementTimeout)
		err := connection.Query().Exec(dropContext, fmt.Sprintf("DROP TABLE `%s/%s`", Keyspace(), tableName))
		cancel()
		if err != nil {
			return fmt.Errorf("ydbdriver: wiping %s/%s: %w", Keyspace(), tableName, err)
		}
	}
	return nil
}
