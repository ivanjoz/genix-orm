package db

import (
	"fmt"
	"sort"
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

// Table IDs are hand-assigned and packed into the by-IDs cache key, so two tables sharing one ID
// would silently share their cached slot versions. Drivers claim the ID while compiling a table,
// which is the first moment both the ID and the name are known.
var (
	tableNamesByID   = map[int16]string{}
	tableNamesByIDMu sync.Mutex
)

// ClaimTableID validates a table's declared ID and reserves it for that table. Recompiling the
// same table is a no-op; a different table claiming the same ID panics.
func ClaimTableID(tableID int16, tableName string) {
	if tableID <= 0 || tableID > MaxTableID {
		panic(fmt.Sprintf(`Table "%v": TableSchema.ID must be between 1 and %v. Found: %v`,
			tableName, MaxTableID, tableID))
	}

	tableNamesByIDMu.Lock()
	defer tableNamesByIDMu.Unlock()

	if claimedBy, isClaimed := tableNamesByID[tableID]; isClaimed && claimedBy != tableName {
		panic(fmt.Sprintf(`TableSchema.ID %v is declared by two tables: "%v" and "%v".`,
			tableID, claimedBy, tableName))
	}
	tableNamesByID[tableID] = tableName
}

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

	// The four below are the name-addressed data surface (see generic_access.go): the read and
	// write a caller can drive knowing only the table's name. Records cross the boundary as the
	// record struct's own JSON, so a row that was read can be edited and written straight back,
	// and the write still runs through the ORM — keeping autoincrement keys, updated_version,
	// virtual columns, views and the text index.

	// DescribeTable publishes the schema in serializable form.
	DescribeTable() TableDescription
	// QueryRecordsJSON runs a read described only with strings, returning the records as a JSON
	// array and how many came back.
	QueryRecordsJSON(spec QuerySpec) (payload []byte, count int, err error)
	// InsertRecordsJSON inserts a JSON array of records, returning how many were written.
	InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error)
	// UpdateRecordsJSON updates only columnsToInclude on every record of the JSON array.
	UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error)
}

// Controllers, unlike tables, cannot be compiled from metadata alone: one carries the record type
// as a type parameter, so only the code that named that type can build it. This registry is how a
// caller holding just a name reaches the full data surface, and it is populated by whoever owns
// the table list (in genix, app/exec from MakeScyllaControllers).
var (
	controllerFactoriesByName = map[string]func() Controller{}
	controllerFactoriesMutex  sync.RWMutex
)

// RegisterControllerFactory records how to build one table's controller by name.
func RegisterControllerFactory(tableName string, makeController func() Controller) {
	controllerFactoriesMutex.Lock()
	defer controllerFactoriesMutex.Unlock()
	controllerFactoriesByName[tableName] = makeController
}

// ResolveControllerByName returns the named table's controller.
func ResolveControllerByName(tableName string) (Controller, error) {
	controllerFactoriesMutex.RLock()
	makeController, registered := controllerFactoriesByName[tableName]
	controllerFactoriesMutex.RUnlock()

	if !registered {
		return nil, fmt.Errorf("la tabla %q no tiene un controller registrado", tableName)
	}
	return makeController(), nil
}

// RegisteredControllerNames lists every table reachable by name, sorted so the listing is stable.
func RegisteredControllerNames() []string {
	controllerFactoriesMutex.RLock()
	defer controllerFactoriesMutex.RUnlock()

	names := make([]string, 0, len(controllerFactoriesByName))
	for tableName := range controllerFactoriesByName {
		names = append(names, tableName)
	}
	sort.Strings(names)
	return names
}
