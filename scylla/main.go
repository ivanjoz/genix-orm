package scylla

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
)

type ScyllaTable struct {
	// Everything a table means to any storage engine lives in TableCore; the fields
	// below are Cassandra-specific and have no counterpart in other drivers.
	db.TableCore
	// genericRecordPlan is nil unless the schema declares GenericRecord. Column resolution, the
	// SELECT projection and the per-column scan/assign closures are all precompiled here once per
	// table, so QueryCachedGenericByIDs does no reflection or type switching per row.
	genericRecordPlan   *genericRecordPlan
	indexes             map[string]*viewInfo
	views               map[string]*viewInfo
	hasTableBackedViews bool
	indexViews          []*viewInfo
	ViewsExcluded       []string
	// readConsistency overrides the cluster's LocalOne on the copy of the table one read runs with:
	// the cache reads use LocalQuorum. Zero keeps the session default.
	readConsistency   gocql.Consistency
	capabilities      []QueryCapability
	indexUpdatedTable *indexUpdatedTableInfo
	// dataFrames is nil unless the schema declares DataFrames (data_frame.go).
	dataFrames *dataFrameInfo
	// selectStatementCache is shared across copied ScyllaTable values, so it must stay behind a pointer.
	selectStatementCache *selectPlanCache
}

type indexUpdatedTableInfo struct {
	name string
}

// viewInfo is the Scylla materialization of a compiled index: the neutral db.IndexSource the
// planner matches against, plus how the index exists in CQL.
type viewInfo struct {
	*db.IndexSource
	decomposeVirtualValue func(rawValue any) []any
	getCreateScript       func() string
	// getExpectedColumns reports the columns this view must hold in the DB. Deploy compares it
	// against the live catalog: a column added to the base table does not reach a derived view on
	// its own, and the view then breaks every read or write that touches it.
	getExpectedColumns func() []viewExpectedColumn
}

// viewExpectedColumn is one column a compiled view must have in the DB, with the CQL type the view
// stores it as — a view table's fan-out key stores the slice element type, not the slice type.
type viewExpectedColumn struct {
	name   string
	dbType string
}

func MakeScyllaTable[T TableBaseInterface[E, T], E TableSchemaInterface[E]]() ScyllaTable {
	refTable := db.InitStructTable[E, T](new(E))
	return getOrCompileScyllaTable(refTable)
}

func MakeSchema[T TableBaseInterface[E, T], E TableSchemaInterface[E]]() TableSchema {
	refTable := db.InitStructTable[E, T](new(E))
	return (*refTable).GetSchema()
}

// execQuery executes a query based on TableInfo and optionally lets callers skip storing decoded rows.
func execQuery[T TableSchemaInterface[T], E any](
	schemaStruct *T,
	tableInfo *TableInfo,
	scanHandler func(record *E) bool,
) error {
	selectStartTime := time.Now()
	records := (tableInfo.RefSlice).(*[]E)
	scyllaTable := getOrCompileScyllaTable(schemaStruct)
	return selectExec(makeRecordSink(records), tableInfo, scyllaTable, erasedScanHandler(scanHandler), selectStartTime)
}

/* Increment Table */
type Increment struct {
	TableStruct[IncrementTable, Increment]
	Name         string
	CurrentValue int64
}

type IncrementTable struct {
	TableStruct[IncrementTable, Increment]
	Name         Col[*IncrementTable, string] // `db:"name,pk"`
	CurrentValue Col[*IncrementTable, int64]  // `db:"current_value,counter"`
}

func (e IncrementTable) GetSchema() TableSchema {
	return TableSchema{
		// The ORM's own tables claim IDs from the top of the range so application tables can number
		// themselves from 1 upwards without ever colliding.
		ID:             MaxTableID,
		Name:           "sequences",
		Keys:           Cols(e.Name),
		SequenceColumn: &e.CurrentValue,
	}
}

func GetCounter(keyspace string, name string, increment int) (int64, error) {
	result := []Increment{}

	if err := Query(&result).Name.Equals(name).Exec(); err != nil {
		return 0, Err("Error al obtener el counter: ", err)
	}

	storedCounterValue := int64(0)
	if len(result) > 0 {
		storedCounterValue = result[0].CurrentValue
	}

	recoveredCounter := storedCounterValue+1 <= 0
	currentValue, counterIncrement := nextCounterRange(storedCounterValue, increment)
	if recoveredCounter {
		// Counters can be moved below zero by repair/reset operations; generated IDs must stay positive.
		fmt.Printf("GetCounter recovered non-positive sequence | keyspace=%s | name=%s | stored=%d | increment=%d\n",
			keyspace, name, storedCounterValue, increment)
	}

	queryUpdateStr := fmt.Sprintf("UPDATE %v.sequences SET current_value = current_value + ? WHERE name = ?", keyspace)

	if err := QueryExec(queryUpdateStr, counterIncrement, name); err != nil {
		fmt.Println(queryUpdateStr, counterIncrement, name)
		panic(err)
	}

	return currentValue, nil
}

// ReserveCounterRange, when set, replaces GetCounter as the allocator behind every managed
// sequence: autoincrement keys and GetAutoincrementID.
//
// It exists because GetCounter cannot be made safe on its own. It reads the counter and then
// increments it, and Cassandra counters offer no way to read the result of your own increment, so
// two concurrent writers read the same value and mint the same id. An external allocator that
// serializes reservations — the fareward daemon, in this project — removes the race by being the
// only process that advances the row.
//
// Which is also the constraint: whatever is installed here takes ownership of those rows. Mixing
// it with the direct path in another process reintroduces exactly the collision it removes, so
// there is deliberately no fallback when it returns an error. A failed write is recoverable; a
// duplicate primary key is not.
//
// Left nil, nothing changes and GetCounter serves every caller as before.
var ReserveCounterRange func(keyspace, name string, increment int) (int64, error)

func reserveCounter(keyspace, name string, increment int) (int64, error) {
	if ReserveCounterRange != nil {
		return ReserveCounterRange(keyspace, name, increment)
	}
	return GetCounter(keyspace, name, increment)
}

// SetCounterValue moves a counter to an absolute value and returns what it held before. It must be
// installed by whoever installs ReserveCounterRange, and for a reason that is easy to miss: an
// external allocator reserves ranges *in advance*, so it may be serving ids derived from a value
// this reset is about to erase. Writing the row directly would leave it handing out values from a
// range that no longer means anything, and the next range it claimed would repeat them. Only the
// allocator can move the counter and drop its own reservation together.
//
// Left nil, ResetCounter reads and writes the row itself, which is correct exactly while nothing
// else owns it.
var SetCounterValue func(keyspace, name string, value int64) (int64, error)

// GetAutoincrementID reserves `recordsSize` consecutive autoincrement IDs for the
// given counter key and returns the FIRST reserved raw value (1, 2, 3, …).
// It uses the configured keyspace automatically. `key` is an arbitrary counter
// name (e.g. "images_<companyID>"), so it is reusable for any per-key sequence.
func GetAutoincrementID(key string, recordsSize int) (int64, error) {
	if recordsSize < 1 {
		recordsSize = 1
	}
	return reserveCounter(connParams.Keyspace, key, recordsSize)
}

func nextCounterRange(storedCounterValue int64, increment int) (int64, int64) {
	nextCounterValue := storedCounterValue + 1
	if nextCounterValue <= 0 {
		// Damaged counters must reserve from 1 and repair the stored value in the same update.
		nextCounterValue = 1
	}

	targetCounterValue := nextCounterValue + int64(increment) - 1
	return nextCounterValue, targetCounterValue - storedCounterValue
}

type SeqValue struct {
	ID      int64 `db:"id"`
	SeqPart int64 `db:"seq_part"`
}
