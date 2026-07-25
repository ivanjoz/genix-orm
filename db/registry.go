package db

import (
	"fmt"
	"sync"
)

// The name registry exists because generics cannot resolve a table from a runtime
// string. Generated code registers one factory per table (see
// scripts/controllers/controllers_generator.go), so endpoints that receive a table
// name — the generic by-IDs read, restore, diagnostics — can reach its compiled
// metadata. Only closures are registered, so cold start stays cheap.
var (
	tableFactoriesByName = map[string]func() Table{}
	tableFactoriesMutex  sync.RWMutex
)

// RegisterTableFactory records how to compile one table by name.
func RegisterTableFactory(tableName string, makeTable func() Table) {
	tableFactoriesMutex.Lock()
	defer tableFactoriesMutex.Unlock()
	tableFactoriesByName[tableName] = makeTable
}

// ResolveTableByName compiles and returns the named table. Compilation is where
// schema validation panics surface, so callers can use this to fail early.
func ResolveTableByName(tableName string) (Table, error) {
	tableFactoriesMutex.RLock()
	makeTable, registered := tableFactoriesByName[tableName]
	tableFactoriesMutex.RUnlock()

	if !registered {
		return nil, fmt.Errorf("la tabla %q no está registrada", tableName)
	}
	return makeTable(), nil
}

// RegisteredTableNames lists every registered table, for diagnostics.
func RegisteredTableNames() []string {
	tableFactoriesMutex.RLock()
	defer tableFactoriesMutex.RUnlock()

	names := make([]string, 0, len(tableFactoriesByName))
	for tableName := range tableFactoriesByName {
		names = append(names, tableName)
	}
	return names
}

// CSVResult is a table exported as CSV, with the row count kept alongside so
// callers do not have to re-parse the content to report progress.
type CSVResult struct {
	Content   []byte
	RowsCount int32
}

// Controller is the driver-agnostic admin surface for one table: the operations
// backup, restore and maintenance tooling needs without knowing which storage
// engine is underneath.
type Controller interface {
	GetTable() Table
	GetTableName() string
	GetRecords(partValue, limit int32, lastKey any) []any
	GetRecordsGob(partValue, limit int32, lastKey any) ([]byte, error)
	RestoreCSVRecords(partValue int32, content *[]byte) error
	GetRecordsCSV(partValue int32) (CSVResult, error)
	ReloadRecords(partValue int32) error
	RecalcVirtualColumns(partValue int32) error
	RecalcGroupIndexHashes(partValue int32) error
	ResetCounter(partValue any) error
	DeleteViewsAndIndexes() error
	FlushTextSearchIndex(partValue int32) error
}
