package scylla

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"slices"
	"strings"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/viant/xunsafe"
	"golang.org/x/sync/errgroup"
)

const maxInsertBatchRows = 500

var getManagedUnixTime = db.ManagedUnixTime

type managedWriteValues struct {
	// stamps are the created / updated values db.StampManagedColumns set, one per record: a managed
	// column with no record field binds them.
	stamps []db.ManagedStamp
	// deltaPackedValues are the TypeDelta packed column values, by column name, one per record: an
	// update that leaves a pinned column out still binds the value it keeps (stampDeltaWrites).
	deltaPackedValues map[string][]any
}

func (e managedWriteValues) slice(start int, end int) managedWriteValues {
	slicedValues := managedWriteValues{}
	if len(e.stamps) > 0 {
		slicedValues.stamps = e.stamps[start:end]
	}
	if len(e.deltaPackedValues) > 0 {
		slicedValues.deltaPackedValues = map[string][]any{}
		for columnName, packedValues := range e.deltaPackedValues {
			slicedValues.deltaPackedValues[columnName] = packedValues[start:end]
		}
	}
	return slicedValues
}

func (e managedWriteValues) getValueForColumn(recordIndex int, column IColInfo, isInsert bool) (any, bool) {
	if packedValues, isDeltaPacked := e.deltaPackedValues[column.GetName()]; isDeltaPacked && recordIndex < len(packedValues) {
		return packedValues[recordIndex], true
	}
	if recordIndex >= len(e.stamps) {
		return nil, false
	}
	switch stamp := e.stamps[recordIndex]; column.GetName() {
	case managedCreatedColumnName:
		if isInsert && stamp.Created != 0 {
			return db.CoerceManagedInteger(column, stamp.Created), true
		}
	case managedUpdatedColumnName:
		if stamp.Updated != 0 {
			return db.CoerceManagedInteger(column, stamp.Updated), true
		}
	}
	return nil, false
}

// applyWriteManagedColumns stamps the audit columns on the records (db.StampManagedColumns) at the
// driver's managed clock, which tests can pin through getManagedUnixTime.
func applyWriteManagedColumns(records db.RecordSlice, scyllaTable ScyllaTable, isInsert bool) managedWriteValues {
	return managedWriteValues{stamps: scyllaTable.StampManagedColumns(records, isInsert, int64(getManagedUnixTime()))}
}

// PreparedStatement is the (template, bound-values) pair that gocql consumes for one query.
// Returned by MakeInsertStatement and MakeUpdateStatements so tests can inspect the exact
// statement shape and arguments that the production write path produces.
type PreparedStatement struct {
	Stmt string
	Args []any
}

// MakeInsertStatement returns the prepared INSERT statement and bound values that the
// production batch path would emit for each record. Intended for tests and debugging — the
// production path builds the same shape directly into a gocql.Batch via appendInsertQueriesToBatch.
func MakeInsertStatement[T TableBaseInterface[E, T], E TableSchemaInterface[E]](records *[]T, columnsToExclude ...Coln) []PreparedStatement {
	refTable := db.InitStructTable[E, T](new(E))
	scyllaTable := getOrCompileScyllaTable(refTable)
	managedValues := applyWriteManagedColumns(db.MakeRecordSlice(records), scyllaTable, true)

	columns := scyllaTable.InsertColumns(columnsToExclude)

	columnNames := make([]string, 0, len(columns))
	placeholders := make([]string, 0, len(columns))
	for _, col := range columns {
		columnNames = append(columnNames, col.GetName())
		placeholders = append(placeholders, "?")
	}

	stmt := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
		scyllaTable.GetFullName(), strings.Join(columnNames, ", "), strings.Join(placeholders, ", "))

	entries := make([]PreparedStatement, 0, len(*records))
	for i := range *records {
		rec := &(*records)[i]
		ptr := xunsafe.AsPointer(rec)

		args := make([]any, 0, len(columns))
		for _, col := range columns {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, true); found {
				value = managedValue
			} else if slices.Contains(scyllaTable.KeysIdx, col.GetInfo().Idx) {
				value = col.GetStatementValue(ptr)
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			args = append(args, value)
		}
		entries = append(entries, PreparedStatement{Stmt: stmt, Args: args})
	}
	return entries
}

func normalizeEmptyStringWriteValue(value any) any {
	// Rationale: Scylla should persist empty strings as NULL on write operations to keep storage semantics consistent.
	switch typedValue := value.(type) {
	case string:
		if typedValue == "" {
			return nil
		}
	case *string:
		if typedValue == nil || *typedValue == "" {
			return nil
		}
	}

	return value
}

func getNormalizedWriteValue(column IColInfo, ptr unsafe.Pointer) any {
	// Rationale: prepared statements must receive typed Go values instead of serialized CQL literals.
	writeValue := column.GetStatementValue(ptr)
	if writeValue == nil {
		writeValue = column.GetRawValue(ptr)
	}
	return normalizeEmptyStringWriteValue(writeValue)
}

func makeInsertBatch[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, scyllaTable ScyllaTable, managedValues managedWriteValues, columnsToExclude ...Coln,
) *gocql.Batch {
	columns := scyllaTable.InsertColumns(columnsToExclude)

	columnsNames := []string{}
	columnPlaceholders := []string{}
	for _, col := range columns {
		columnsNames = append(columnsNames, col.GetName())
		columnPlaceholders = append(columnPlaceholders, "?")
	}

	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)

	queryStrInsert := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
		scyllaTable.GetFullName(), strings.Join(columnsNames, ", "), strings.Join(columnPlaceholders, ", "))

	appendInsertQueriesToBatch(batch, queryStrInsert, db.MakeRecordSlice(records), columns, scyllaTable.KeysIdx, managedValues)
	return batch
}

func appendInsertQueriesToBatch(
	batch *gocql.Batch, queryStrInsert string, records db.RecordSlice, columns []IColInfo, keysIdx []int16, managedValues managedWriteValues,
) {
	for i := range records.Len() {
		ptr := records.At(i)
		values := []any{}

		for _, col := range columns {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, true); found {
				value = managedValue
			} else if slices.Contains(keysIdx, col.GetInfo().Idx) {
				// Key columns must never be coerced to null — keep empty string as-is.
				value = col.GetStatementValue(ptr)
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			values = append(values, value)
		}

		batch.Query(queryStrInsert, values...)
	}
}

func syncTableBackedViews(
	records db.RecordSlice, scyllaTable *ScyllaTable, affectedColumns []IColInfo,
) error {
	if records.Len() == 0 || !scyllaTable.hasTableBackedViews {
		return nil
	}

	session := getScyllaConnection()
	partColumn := scyllaTable.GetPartKey()
	if partColumn == nil || partColumn.IsNil() {
		return nil
	}

	for _, view := range scyllaTable.views {
		if view.Type != 9 {
			continue
		}
		if len(affectedColumns) > 0 {
			shouldSyncCurrentView := false
			for _, affectedColumn := range affectedColumns {
				if view.RebuildColumnNames[affectedColumn.GetName()] {
					shouldSyncCurrentView = true
					break
				}
			}
			if !shouldSyncCurrentView {
				continue
			}
		}

		for start := 0; start < records.Len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, records.Len())

			recordsChunk := records.Sub(start, end)
			if err := executeViewTableSyncChunk(view, recordsChunk.Len(), recordsChunk.At, session, scyllaTable); err != nil {
				return err
			}
		}
	}

	return nil
}

func Insert[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToExclude ...Coln,
) error {
	recordsForUpdate := []T{}
	return InsertUpdateBase(records, &recordsForUpdate, nil, columnsToExclude...)
}

func InsertOne[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	record T, columnsToExclude ...Coln,
) error {
	return Insert(&[]T{record}, columnsToExclude...)
}

// MakeUpdateStatements returns the prepared UPDATE statement and bound values that the
// production batch path would emit for each record. Intended for tests and debugging — the
// production path builds the same shape directly into a gocql.Batch via appendUpdateQueriesToBatch.
func MakeUpdateStatements[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToInclude ...Coln,
) []PreparedStatement {
	scyllaTable := MakeScyllaTable[T, E]()
	recordsView := db.MakeRecordSlice(records)
	managedValues := applyWriteManagedColumns(recordsView, scyllaTable, false)

	columnsToUpdate := scyllaTable.ResolveUpdateColumns(recordsView, columnsToInclude, nil)
	columnsWhere := collectUpdateWhereColumns(scyllaTable)

	setParts := make([]string, 0, len(columnsToUpdate))
	for _, col := range columnsToUpdate {
		setParts = append(setParts, fmt.Sprintf("%v = ?", col.GetName()))
	}
	whereParts := make([]string, 0, len(columnsWhere))
	for _, whereCol := range columnsWhere {
		whereParts = append(whereParts, fmt.Sprintf("%v = ?", whereCol.GetName()))
	}

	stmt := fmt.Sprintf("UPDATE %v SET %v WHERE %v",
		scyllaTable.GetFullName(),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " and "),
	)

	entries := make([]PreparedStatement, 0, len(*records))
	for i := range *records {
		rec := &(*records)[i]
		ptr := xunsafe.AsPointer(rec)

		args := make([]any, 0, len(columnsToUpdate)+len(columnsWhere))
		for _, col := range columnsToUpdate {
			var value any
			if managedValue, found := managedValues.getValueForColumn(i, col, false); found {
				value = managedValue
			} else {
				value = getNormalizedWriteValue(col, ptr)
			}
			args = append(args, value)
		}
		for _, whereCol := range columnsWhere {
			args = append(args, whereCol.GetStatementValue(ptr))
		}
		entries = append(entries, PreparedStatement{Stmt: stmt, Args: args})
	}
	return entries
}

func Update[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToInclude ...Coln,
) error {
	if len(columnsToInclude) == 0 {
		panic("No se incluyeron columnas a actualizar.")
	}

	recordsForInsert := []T{}
	return InsertUpdateBase(&recordsForInsert, records, columnsToInclude)
}

func UpdateOne[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	record T, columnsToInclude ...Coln,
) error {
	return Update(&[]T{record}, columnsToInclude...)
}

func UpdateExclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T, columnsToExclude ...Coln,
) error {
	if records == nil || len(*records) == 0 {
		return nil
	}
	// Route through the shared insert/update batch path so writes go out as prepared statements,
	// avoiding the raw-CQL string concatenation that previously broke on values containing single quotes.
	recordsForInsert := []T{}
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert: &recordsForInsert,
		recordsForUpdate: records,
		columnsToUpdate:  columnsToExclude,
	})
}

func InsertUpdateBase[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	recordsForInsert *[]T,
	recordsForUpdate *[]T,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:        recordsForInsert,
		recordsForUpdate:        recordsForUpdate,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

// erasedIsInsert adapts the caller's typed classifier to records by pointer.
func erasedIsInsert[T any](isInsert func(e *T) bool) func(unsafe.Pointer) bool {
	if isInsert == nil {
		panic("Se requiere la función isInsert para clasificar los registros.")
	}
	return func(record unsafe.Pointer) bool { return isInsert((*T)(record)) }
}

type insertUpdateBatchParams[T TableBaseInterface[E, T], E TableSchemaInterface[E]] struct {
	recordsForInsert        *[]T
	recordsForUpdate        *[]T
	useIncludeUpdateColumns bool
	columnsToUpdate         []Coln
	columnsToExcludeInsert  []Coln
	// skipTextSearchRecordsIDs lists update-record IDs whose text-search source (and status)
	// did not change, so the update-phase re-index can skip re-upserting them into GenixSearch.
	// The caller (e.g. Merge) already holds the previous record and computes this, avoiding a
	// per-record comparison inside the write path.
	skipTextSearchRecordsIDs []int64
}

// executeInsertUpdateBatch is the generic doorway to the write path. It does the three things
// that genuinely need the record and table types -- compile the table, run SelfParse through the
// record's own method set, and take the address of each slice -- and hands everything else to the
// shared body below. Splitting it this way is what stops one write engine from being compiled once
// per table; see record_slice.go.
func executeInsertUpdateBatch[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	params insertUpdateBatchParams[T, E],
) error {
	recordsForInsert := params.recordsForInsert
	recordsForUpdate := params.recordsForUpdate
	if recordsForInsert == nil {
		recordsForInsert = &[]T{}
	}
	if recordsForUpdate == nil {
		recordsForUpdate = &[]T{}
	}
	if len(*recordsForInsert) == 0 && len(*recordsForUpdate) == 0 {
		return nil
	}

	return executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		insertRecords:            db.MakeRecordSlice(recordsForInsert),
		updateRecords:            db.MakeRecordSlice(recordsForUpdate),
		compiledTable:            getOrCompileScyllaTable(db.InitStructTable[E, T](new(E))),
		useIncludeUpdateColumns:  params.useIncludeUpdateColumns,
		columnsToUpdate:          params.columnsToUpdate,
		columnsToExcludeInsert:   params.columnsToExcludeInsert,
		skipTextSearchRecordsIDs: params.skipTextSearchRecordsIDs,
	})
}

// erasedInsertUpdateParams is insertUpdateBatchParams with the record type resolved away.
type erasedInsertUpdateParams struct {
	insertRecords            db.RecordSlice
	updateRecords            db.RecordSlice
	compiledTable            ScyllaTable
	useIncludeUpdateColumns  bool
	columnsToUpdate          []Coln
	columnsToExcludeInsert   []Coln
	skipTextSearchRecordsIDs []int64
}

func executeInsertUpdateBatchErased(params erasedInsertUpdateParams) error {
	insertRecords := params.insertRecords
	updateRecords := params.updateRecords
	if insertRecords.Len() == 0 && updateRecords.Len() == 0 {
		return nil
	}
	// SelfParse runs before anything reads a column, and it mutates records in place without
	// reallocating, so the views stay valid.
	db.RunSelfParse(insertRecords)
	db.RunSelfParse(updateRecords)

	scyllaTable := params.compiledTable
	useIncludeUpdateColumns := params.useIncludeUpdateColumns
	columnsToUpdate := params.columnsToUpdate
	columnsToExcludeInsert := params.columnsToExcludeInsert

	skipTextSearchIDs := map[int64]bool(nil)
	if len(params.skipTextSearchRecordsIDs) > 0 {
		skipTextSearchIDs = make(map[int64]bool, len(params.skipTextSearchRecordsIDs))
		for _, id := range params.skipTextSearchRecordsIDs {
			skipTextSearchIDs[id] = true
		}
	}

	if useIncludeUpdateColumns && updateRecords.Len() > 0 && len(columnsToUpdate) == 0 {
		panic("No se incluyeron columnas a actualizar.")
	}

	// A view over both slices rather than slices.Concat: the marker syncs only walk the records,
	// so copying every one of them to do it was pure waste -- and the copy had to be rebuilt
	// after the write phase, because the first one held stale pre-mutation records.
	allWrittenRecords := recordSliceGroup{insertRecords, updateRecords}
	keyspace := strings.Split(scyllaTable.GetFullName(), ".")[0]
	err := scyllaTable.AssignAutoincrementKeys(insertRecords, func(counterName string, count int) (int64, error) {
		return reserveCounter(keyspace, counterName, count)
	})
	if err != nil {
		return err
	}

	managedInsertValues := applyWriteManagedColumns(insertRecords, scyllaTable, true)
	managedUpdateValues := applyWriteManagedColumns(updateRecords, scyllaTable, false)

	// The update-phase column set drives the version stamp, the view sync and the text-search sync,
	// so it is resolved once here instead of being recomputed inside each block.
	updateAffectedColumns := []IColInfo{}
	if updateRecords.Len() > 0 {
		if useIncludeUpdateColumns {
			updateAffectedColumns = scyllaTable.AffectedColumnsForInclude(columnsToUpdate)
		} else {
			updateAffectedColumns = scyllaTable.AffectedColumnsForExclude(columnsToUpdate)
		}
	}

	// The keys are final now, so the stored rows can be read: the delta indexes stamp the next
	// versions from them, the DataFrames log what changed.
	writePhases := []writePhase{
		{records: insertRecords, writtenColumns: scyllaTable.AffectedColumnsForInsert(columnsToExcludeInsert)},
		{records: updateRecords, writtenColumns: updateAffectedColumns},
	}
	storedWrittenRows, err := readStoredWrittenRows(scyllaTable, writePhases)
	if err != nil {
		return err
	}
	deltaPackedValuesByPhase, deltaMoves := stampDeltaWrites(scyllaTable, writePhases, storedWrittenRows)
	indexGroupMarks := collectIndexGroupMarks(&scyllaTable, writePhases, storedWrittenRows)
	managedInsertValues.deltaPackedValues = deltaPackedValuesByPhase[0]
	managedUpdateValues.deltaPackedValues = deltaPackedValuesByPhase[1]
	// Before the rows: a change in the table always has its log row (data_frame_log.go).
	if err := writeDataFrameLog(scyllaTable, writePhases, storedWrittenRows); err != nil {
		return err
	}
	// Before the rows: the group marks are published ahead of the data they gate (markIndexGroupsBeforeWrite).
	if err := markIndexGroupsBeforeWrite(&scyllaTable, indexGroupMarks); err != nil {
		return fmt.Errorf("marking index groups: %w", err)
	}

	session := getScyllaConnection()

	// Writes are sent in chunks of maxInsertBatchRows statements per gocql.Batch. A single
	// batch holding tens of thousands of statements overruns ScyllaDB's batch limits and
	// triggers a server-side connection reset, so inserts and updates are each chunked here.
	if insertRecords.Len() > 0 {
		insertColumns := scyllaTable.InsertColumns(columnsToExcludeInsert)
		insertColumnNames := []string{}
		insertColumnPlaceholders := []string{}
		for _, insertColumn := range insertColumns {
			insertColumnNames = append(insertColumnNames, insertColumn.GetName())
			insertColumnPlaceholders = append(insertColumnPlaceholders, "?")
		}
		insertQueryStatement := fmt.Sprintf(`INSERT INTO %v (%v) VALUES (%v)`,
			scyllaTable.GetFullName(), strings.Join(insertColumnNames, ", "), strings.Join(insertColumnPlaceholders, ", "))

		for start := 0; start < insertRecords.Len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, insertRecords.Len())
			chunk := insertRecords.Sub(start, end)
			queryBatch := session.NewBatch(gocql.UnloggedBatch)
			appendInsertQueriesToBatch(queryBatch, insertQueryStatement, chunk, insertColumns, scyllaTable.KeysIdx, managedInsertValues.slice(start, end))
			if DebugFull {
				fmt.Printf("InsertUpdate insert batch write: table=%s rows=%d statements=%d chunk=%d-%d total=%d\n",
					scyllaTable.GetFullName(), chunk.Len(), len(queryBatch.Entries), start, end, insertRecords.Len())
			}
			if err := session.ExecuteBatch(queryBatch); err != nil {
				fmt.Printf("Error executing insert batch: table=%s rows=%d chunk=%d-%d err=%v\n",
					scyllaTable.GetFullName(), chunk.Len(), start, end, err)
				return err
			}
		}
	}

	if updateRecords.Len() > 0 {
		updateColumnsToInclude := []Coln(nil)
		updateColumnsToExclude := []Coln(nil)
		if useIncludeUpdateColumns {
			updateColumnsToInclude = columnsToUpdate
		} else {
			updateColumnsToExclude = columnsToUpdate
		}

		for start := 0; start < updateRecords.Len(); start += maxInsertBatchRows {
			end := min(start+maxInsertBatchRows, updateRecords.Len())
			chunk := updateRecords.Sub(start, end)
			queryBatch := session.NewBatch(gocql.UnloggedBatch)
			appendUpdateQueriesToBatch(queryBatch, scyllaTable, chunk, managedUpdateValues.slice(start, end), updateColumnsToInclude, updateColumnsToExclude)
			if DebugFull {
				fmt.Printf("InsertUpdate update batch write: table=%s rows=%d statements=%d chunk=%d-%d total=%d\n",
					scyllaTable.GetFullName(), chunk.Len(), len(queryBatch.Entries), start, end, updateRecords.Len())
			}
			if err := session.ExecuteBatch(queryBatch); err != nil {
				fmt.Printf("Error executing update batch: table=%s rows=%d chunk=%d-%d err=%v\n",
					scyllaTable.GetFullName(), chunk.Len(), start, end, err)
				return err
			}
		}
	}

	// The post-write syncs run in two parallel phases, separated by a barrier.
	//
	// Phase 1 writes the derived DATA (view tables, search index). Phase 2 writes the FRESHNESS
	// MARKERS the caches read to decide whether to refetch (the by-IDs slot files and the delta
	// moves). The barrier is what makes the protocol safe: publishing a marker before the data it
	// gates lets a client refetch a stale row and then record the new version, permanently missing
	// the update. The index-group marks are the exception, written before the rows: their readers
	// never trust a group marked less than db.IndexGroupSettleTicks ago (markIndexGroupsBeforeWrite).
	//
	// Within a phase the syncs target distinct tables and only ever read the record slices, so
	// they are independent. Each sync KIND keeps its own insert/update phases sequential inside
	// one goroutine — the view sync is a read-modify-write cycle over a shared view table and
	// text-search upserts can land in the same bucket, so overlapping them with themselves would
	// add a hazard for no gain.
	var dataSyncGroup errgroup.Group

	if scyllaTable.hasTableBackedViews {
		dataSyncGroup.Go(func() error {
			if insertRecords.Len() > 0 {
				insertAffectedColumns := scyllaTable.AffectedColumnsForInsert(columnsToExcludeInsert)
				if err := syncTableBackedViews(insertRecords, &scyllaTable, insertAffectedColumns); err != nil {
					return fmt.Errorf("syncing view tables after insert phase: %w", err)
				}
			}
			if updateRecords.Len() > 0 {
				if err := syncTableBackedViews(updateRecords, &scyllaTable, updateAffectedColumns); err != nil {
					return fmt.Errorf("syncing view tables after update phase: %w", err)
				}
			}
			return nil
		})
	}

	if scyllaTable.TextSearchIndex != nil {
		dataSyncGroup.Go(func() error {
			// Both phases go in one call so each partition gets a single index write.
			err := syncTextSearchAfterWrite(&scyllaTable,
				textSearchWrite{
					records: insertRecords,
					columns: scyllaTable.AffectedColumnsForInsert(columnsToExcludeInsert),
				},
				textSearchWrite{records: updateRecords, columns: updateAffectedColumns, skip: skipTextSearchIDs},
			)
			if err != nil {
				return fmt.Errorf("syncing text search index: %w", err)
			}
			return nil
		})
	}

	if err := dataSyncGroup.Wait(); err != nil {
		fmt.Println("Error syncing derived data after insert-update:", err)
		return err
	}

	var markerSyncGroup errgroup.Group

	// Combined insert/update writes raise each touched slot once for the merged mutation set.
	markerSyncGroup.Go(func() error {
		if err := raiseWrittenSlots(allWrittenRecords, scyllaTable); err != nil {
			return fmt.Errorf("raising by-IDs slots: %w", err)
		}
		return nil
	})

	markerSyncGroup.Go(func() error {
		if err := writeDeltaMoves(scyllaTable, deltaMoves); err != nil {
			return fmt.Errorf("writing delta moves: %w", err)
		}
		return nil
	})

	if err := markerSyncGroup.Wait(); err != nil {
		fmt.Println("Error syncing cache markers after insert-update:", err)
		return err
	}

	return nil
}

func InsertUpdate[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	recordsForInsert *[]T,
	recordsForUpdate *[]T,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	return executeInsertUpdateBatch(insertUpdateBatchParams[T, E]{
		recordsForInsert:        recordsForInsert,
		recordsForUpdate:        recordsForUpdate,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

func InsertUpdateInclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isInsert func(e *T) bool,
	columnsToIncludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	insertRecords, updateRecords := db.SplitInsertAndUpdateRecords(db.MakeRecordSlice(records), erasedIsInsert(isInsert))
	return executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		insertRecords:           insertRecords,
		updateRecords:           updateRecords,
		compiledTable:           MakeScyllaTable[T, E](),
		useIncludeUpdateColumns: true,
		columnsToUpdate:         columnsToIncludeUpdate,
		columnsToExcludeInsert:  columnsToExcludeInsert,
	})
}

func InsertUpdateExclude[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isInsert func(e *T) bool,
	columnsToExcludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {
	insertRecords, updateRecords := db.SplitInsertAndUpdateRecords(db.MakeRecordSlice(records), erasedIsInsert(isInsert))
	return executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		insertRecords:          insertRecords,
		updateRecords:          updateRecords,
		compiledTable:          MakeScyllaTable[T, E](),
		columnsToUpdate:        columnsToExcludeUpdate,
		columnsToExcludeInsert: columnsToExcludeInsert,
	})
}

func collectUpdateWhereColumns(scyllaTable ScyllaTable) []IColInfo {
	columnsWhere := scyllaTable.Keys
	if partitionKey := scyllaTable.GetPartKey(); partitionKey != nil && !partitionKey.IsNil() {
		columnsWhere = append([]IColInfo{partitionKey}, columnsWhere...)
	}
	return columnsWhere
}

func appendUpdateQueriesToBatch(
	batch *gocql.Batch, compiledTable ScyllaTable, records db.RecordSlice, managedValues managedWriteValues,
	columnsToInclude []Coln, columnsToExclude []Coln,
) {
	scyllaTable := compiledTable
	columnsToUpdate := scyllaTable.ResolveUpdateColumns(records, columnsToInclude, columnsToExclude)
	columnsWhere := collectUpdateWhereColumns(scyllaTable)

	setStatements := []string{}
	for _, columnToUpdate := range columnsToUpdate {
		setStatements = append(setStatements, fmt.Sprintf(`%v = ?`, columnToUpdate.GetName()))
	}

	whereStatements := []string{}
	for _, whereColumn := range columnsWhere {
		whereStatements = append(whereStatements, fmt.Sprintf(`%v = ?`, whereColumn.GetName()))
	}

	queryStatement := fmt.Sprintf(
		"UPDATE %v SET %v WHERE %v",
		scyllaTable.GetFullName(), Concatx(", ", setStatements), Concatx(" and ", whereStatements),
	)

	for recordIndex := range records.Len() {
		recordPointer := records.At(recordIndex)
		values := []any{}

		for _, columnToUpdate := range columnsToUpdate {
			value := any(nil)
			if managedValue, found := managedValues.getValueForColumn(recordIndex, columnToUpdate, false); found {
				value = managedValue
			} else {
				value = getNormalizedWriteValue(columnToUpdate, recordPointer)
			}
			values = append(values, value)
		}

		for _, whereColumn := range columnsWhere {
			values = append(values, whereColumn.GetStatementValue(recordPointer))
		}

		batch.Query(queryStatement, values...)
	}
}

func InsertOrUpdate[T TableBaseInterface[E, T], E TableSchemaInterface[E]](
	records *[]T,
	isRecordForInsert func(e *T) bool,
	columnsToExcludeUpdate []Coln,
	columnsToExcludeInsert ...Coln,
) error {

	recordsToInsert := []T{}
	recordsToUpdate := []T{}

	for _, e := range *records {
		if isRecordForInsert(&e) {
			recordsToInsert = append(recordsToInsert, e)
		} else {
			recordsToUpdate = append(recordsToUpdate, e)
		}
	}

	if len(recordsToUpdate) > 0 {
		if ShouldLog() {
			fmt.Println("Registros a actualizar:", len(recordsToUpdate))
		}
		if err := UpdateExclude(&recordsToUpdate, columnsToExcludeUpdate...); err != nil {
			return err
		}
	}

	if len(recordsToInsert) > 0 {
		if ShouldLog() {
			fmt.Println("Registros a insertar:", len(recordsToInsert))
		}
		if err := Insert(&recordsToInsert, columnsToExcludeInsert...); err != nil {
			return err
		}
	}

	return nil
}
