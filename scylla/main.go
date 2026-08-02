package scylla

import (
	"fmt"
	"time"

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
	keyConcatenated     []IColInfo
	keyIntPacking       []IColInfo
	// packedIndexes stores metadata for packed indexes declared in schema (local and global).
	packedIndexes []*packedIndexInfo
	// fixedValueRanges holds the schema's FixedValues per column name, which is what lets a
	// TypeDelta view size each key's digit slot without a .DecimalSize() decorator.
	fixedValueRanges map[string]columnValueRange
	// maxDeltaVersionValue is the widest "updated_version" the delta view's trailing slot can hold.
	// Zero means the table declares no delta view and nothing needs checking.
	maxDeltaVersionValue int64
	capabilities         []QueryCapability
	// Composite bucket metadata is used to materialize virtual hash sets and plan range+contains reads.
	compositeBucketIndexes []compositeBucketIndex
	indexGroups            []indexGroupInfo
	// indexGroupIDs prevents per-table collisions when logical IndexGroup names hash to the same int16.
	indexGroupIDs     map[int16]string
	indexUpdatedTable *indexUpdatedTableInfo
	textSearchIndex   *textSearchIndexInfo
	// selectStatementCache is shared across copied ScyllaTable values, so it must stay behind a pointer.
	selectStatementCache *selectPlanCache
}

// compositeBucketIndex stores the source columns and generated virtual bucket columns for one composite-bucket index.
type compositeBucketIndex struct {
	name          string
	sourceColumns []IColInfo
	bucketColumn  IColInfo
	// bucketIsWeek keeps schema-level week semantics for custom week-code arithmetic in bucketing and range planning.
	bucketIsWeek         bool
	bucketSizes          []int8
	virtualColumnsBySize map[int8]IColInfo
}

type indexGroupInfo struct {
	name                 string
	indexID              int16
	sourceColumns        []indexGroupSourceColumn
	virtualColumn        IColInfo
	usesCollectionValues bool
	// inheritFromKey routes grouped fetches through KeyIntPacking primary-key ranges instead of a secondary index.
	inheritFromKey bool
}

type indexUpdatedTableInfo struct {
	name string
}

type textSearchIndexInfo struct {
	sourceColumn IColInfo
	statusColumn IColInfo
	// The record-ID column is always the table's single key (keys[0]) and the partition is
	// always partKey, so both are read from the ScyllaTable directly rather than cached here.
}

type textSearchIndexProvider interface {
	GetTextSearchIndex() string
}

type indexGroupSourceColumn struct {
	column IColInfo
}

type viewInfo struct {
	/* 1 = Global index, 2 = Local index, 3 = Hash index, 4 = view*/
	Type          int8
	name          string
	idx           int8
	column        IColInfo
	columns       []string
	columnsNoPart []string
	columnsIdx    []int16
	// availableColumns tracks the real base-table columns that can be selected from the MV.
	// Rationale: projected views cannot satisfy default "select all columns" reads.
	availableColumns []string
	// packedSourceColumns keeps the original key columns behind packed range views so grouped scans can decompose them back.
	packedSourceColumns []IColInfo
	// packedSlotDigitsPerColumn mirrors packedSourceColumns and is reused for prefix-range planning and scan decomposition.
	packedSlotDigitsPerColumn []int64
	Operators                 []string
	// RequiresPostFilter indicates the index/view can overfetch and should be exact-filtered in memory.
	// Keep this for hash-style plans that intentionally trade exact routing for bounded overfetch.
	RequiresPostFilter    bool
	getStatementPrepared  func(statements ...ColumnStatement) []boundWhereClause
	decomposeVirtualValue func(rawValue any) []any
	getCreateScript       func() string
	fanoutColumnName      string
	tableColumns          []viewTableColumnInfo
	tableKeyColumns       []viewTableColumnInfo
	maintenanceIDColumn   IColInfo
	rebuildColumnNames    map[string]bool
}

type viewTableColumnInfo struct {
	SourceColumn     IColInfo
	UsesSliceElement bool
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
	return selectExec(records, tableInfo, scyllaTable, scanHandler, selectStartTime)
}

/* Increment Table */
type Increment struct {
	TableStruct[IncrementTable, Increment]
	Name         string
	CurrentValue int64
}

type IncrementTable struct {
	TableStruct[IncrementTable, Increment]
	Name         Col[IncrementTable, string] // `db:"name,pk"`
	CurrentValue Col[IncrementTable, int64]  // `db:"current_value,counter"`
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

// GetAutoincrementID reserves `recordsSize` consecutive autoincrement IDs for the
// given counter key and returns the FIRST reserved raw value (1, 2, 3, …).
// It uses the configured keyspace automatically. `key` is an arbitrary counter
// name (e.g. "images_<companyID>"), so it is reusable for any per-key sequence.
func GetAutoincrementID(key string, recordsSize int) (int64, error) {
	if recordsSize < 1 {
		recordsSize = 1
	}
	return GetCounter(connParams.Keyspace, key, recordsSize)
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
