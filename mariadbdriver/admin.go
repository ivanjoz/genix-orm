package mariadbdriver

import (
	"fmt"
	"slices"
)

// Package-level operations the backend facade calls (docs/OCEANBASE_PLAN.md §12). Sequences live in
// sequences.go; each operation not yet implemented answers errNotImplemented until Step 7.

// WipePartition deletes one partition of a compiled table: its rows, the rows of its side tables
// (element indexes, delta moves, cache-group marks and data) and its by-IDs cache slots. Every one of
// those tables leads its primary key with the partition column. The DataFrame aggregate tables are
// left to Controller.RebuildDataFramesPartition, which refills them from the (now empty) partition.
func WipePartition(namespace string, tableID int16, partition int64) error {
	var wipedTable *MariaTable
	mariaTableCache.Range(func(_, cacheEntryAny any) bool {
		compiledTable := cacheEntryAny.(*mariaTableCacheEntry).table
		if compiledTable != nil && compiledTable.ID == tableID && (namespace == "" || compiledTable.mainTable.database == namespace) {
			wipedTable = compiledTable
			return false
		}
		return true
	})
	if wipedTable == nil {
		return fmt.Errorf("mariadbdriver: WipePartition: no compiled table with ID %d", tableID)
	}
	if wipedTable.GetPartKey() == nil || wipedTable.GetPartKey().IsNil() {
		return fmt.Errorf("mariadbdriver: WipePartition: %s has no partition column", wipedTable.Name)
	}

	statements := []string{}
	for _, spec := range append([]tableSpec{wipedTable.mainTable}, wipedTable.sideTables...) {
		isAggregateTable := wipedTable.dataFrames != nil && slices.ContainsFunc(wipedTable.dataFrames.aggregateTables,
			func(aggregateTable tableSpec) bool { return aggregateTable.name == spec.name })
		if !isAggregateTable {
			statements = append(statements, fmt.Sprintf("DELETE FROM %s WHERE %s = ?", spec.qualifiedName(), quoteName(spec.primaryKey[0])))
		}
	}
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := pool.Exec(statement, partition); err != nil {
			return fmt.Errorf("mariadbdriver: WipePartition %s: %w", wipedTable.Name, err)
		}
	}
	if wipedTable.CacheByIDs {
		slotsTable := tableSpec{database: wipedTable.mainTable.database, name: cacheSlotsTableName}
		if _, err := pool.Exec(fmt.Sprintf("DELETE FROM %s WHERE table_id = ? AND partition_id = ?", slotsTable.qualifiedName()), tableID, partition); err != nil {
			return fmt.Errorf("mariadbdriver: WipePartition %s by-IDs slots: %w", wipedTable.Name, err)
		}
	}
	return nil
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
