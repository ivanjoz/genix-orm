package scylla

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"golang.org/x/sync/errgroup"
)

// ─────────────────────────────────────────────────────────────────────────────
// TypeDelta on Scylla: the delta window protocol (db/cache delta_window.go).
//
// Each TypeDelta index is a materialized view holding every stored column of the
// record, keyed so one pinned prefix is a contiguous range ordered by updated:
//
//	PRIMARY KEY ((partition), zz_dlt_<pinned>, id)   ordered DESC
//	zz_dlt_<pinned> = pinned values … | updated (31 bits), one bigint
//
// Scylla allows a single non-key base column in a view key, hence the packing (a
// keyless index packs updated alone). Every declared Key is pinned: a read names
// each value, by an equality or Delta()'s values for the last Key, or fans out one
// read per FixedValues value (resolveDeltaPrefixes). The view is also a range view
// (Type 8) of the query planner on [partition, pinned…, updated], with the same
// FixedValues fan-out, so it serves plain reads on its keys.
//
//   - Write: before the CQL, the stored version and pinned values of the written keys
//     are read (one "key IN" select per partition). Each record is stamped with the
//     next Version, and its packed column with the pinned values it ends with: a
//     partial update that leaves a pinned column out keeps the stored value. After
//     the CQL, a record whose pinned prefix changed writes one row, under its old
//     prefix, to <table>__delta_moves.
//   - Read: Delta() reads, per prefix and in parallel, the changed records straight
//     from the view, then the hashed window's (ID, version) and the moves slice of
//     the same window. The replace threshold counts across every prefix.
//
// Records of a TypeDelta table are never hard-deleted: the view would drop the row
// and no client would hear of it.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// deltaViewGCGraceSeconds is low because every update moves the view row (its key holds
	// updated): a resurrected stale row only fails a window hash, which resends the prefix.
	deltaViewGCGraceSeconds = 86400
)

/* Compile */

// materializeDeltaView gives a described TypeDelta index the materialized view that serves it.
func materializeDeltaView(dbTable *ScyllaTable, view *viewInfo) {
	source := view.IndexSource
	deltaIndex := dbTable.DeltaIndexes[slices.IndexFunc(dbTable.DeltaIndexes, func(registered db.DeltaIndexInfo) bool {
		return registered.ViewName == source.Name
	})]

	// The described value is the shared unsigned layout; Scylla compares the column as signed, so it
	// is stored with the top bit flipped (deltaPackedStoredValue).
	packedColumn := deltaIndex.PackedColumn
	describedValue := packedColumn.GetValueFn
	packedColumn.GetValueFn = func(recordPointer unsafe.Pointer) any {
		return storeVirtualPacked(describedValue(recordPointer).(uint64), false)
	}

	// The view is a range view of the query planner on [partition, pinned…, updated].
	compilePackedRangeDecoder(view, source.PackedSourceColumns, deltaIndex.SlotBits(), false)

	storedColumns := deltaIndex.StoredColumns
	view.getExpectedColumns = func() []viewExpectedColumn {
		return makeViewExpectedColumns(append(slices.Clone(storedColumns), deltaIndex.PackedColumn))
	}
	view.getCreateScript = func() string { return deltaViewCreateScript(*dbTable, deltaIndex, storedColumns) }
}

// deltaViewCreateScript is the CREATE of an index's view. The view is ordered newest first: a sync
// reads the recent end of its prefix.
func deltaViewCreateScript(scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, storedColumns []IColInfo) string {
	partitionName, keyName := scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName()
	packedName := deltaIndex.PackedColumn.GetName()
	selectNames := []string{packedName}
	for _, storedColumn := range storedColumns {
		selectNames = append(selectNames, storedColumn.GetName())
	}
	return fmt.Sprintf(`CREATE MATERIALIZED VIEW %v.%v AS
		SELECT %v FROM %v
		WHERE %v IS NOT NULL AND %v IS NOT NULL AND %v IS NOT NULL
		PRIMARY KEY ((%v), %v, %v)
		WITH CLUSTERING ORDER BY (%v DESC, %v ASC)
		AND gc_grace_seconds = %d;`,
		resolveKeyspace(scyllaTable), deltaIndex.ViewName, strings.Join(selectNames, ", "), scyllaTable.GetFullName(),
		partitionName, packedName, keyName,
		partitionName, packedName, keyName,
		packedName, keyName, deltaViewGCGraceSeconds)
}

func deltaMovesTableName(scyllaTable ScyllaTable) string { return scyllaTable.Name + "__delta_moves" }

// hasDeltaMoves reports whether any TypeDelta index pins a column, so its records can move.
func hasDeltaMoves(scyllaTable ScyllaTable) bool {
	return slices.ContainsFunc(scyllaTable.DeltaIndexes, func(deltaIndex db.DeltaIndexInfo) bool { return len(deltaIndex.Pinned) > 0 })
}

// deltaMovesCreateScript is the CREATE of the moves table: one row per record that left a pinned
// prefix, under the prefix it left (pinned_prefix is the packed column with updated 0). Its rows never
// expire: a cursor of any age reads the moves since it, so age alone never forces a replace.
func deltaMovesCreateScript(scyllaTable ScyllaTable) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v.%v (
			partition_id %v, index_tag text, pinned_prefix bigint, updated int, id %v,
			PRIMARY KEY ((partition_id, index_tag, pinned_prefix), updated, id)
		);`,
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable),
		scyllaTable.GetPartKey().GetType().DBType, scyllaTable.Keys[0].GetType().DBType)
}

/* Write */

// writePhase is one half of a write call: its records and the columns it writes. A column it does not
// write keeps its stored value, which is what the delta view and the frame log must see.
type writePhase struct {
	records        db.RecordSlice
	writtenColumns []IColInfo
}

// deltaMove is one record that left a pinned prefix.
type deltaMove struct {
	partition    int64
	recordID     int64
	updated      int64
	indexTag     string
	pinnedPrefix int64
}

// deltaPinnedColumns are the columns whose stored value a write needs: every pinned column.
func deltaPinnedColumns(scyllaTable ScyllaTable) []IColInfo {
	pinnedColumns := []IColInfo{}
	for _, deltaIndex := range scyllaTable.DeltaIndexes {
		for _, pinnedColumn := range deltaIndex.Pinned {
			if !slices.ContainsFunc(pinnedColumns, func(column IColInfo) bool { return column.GetName() == pinnedColumn.GetName() }) {
				pinnedColumns = append(pinnedColumns, pinnedColumn)
			}
		}
	}
	return pinnedColumns
}

// storedRows are the stored columns of the written records, read before the write, by (partition,
// key) and column name; a record not stored is absent and a null column has no entry.
type storedRows map[[2]int64]map[string]any

// readStoredWrittenRows reads, in one select per partition, what the delta indexes, the DataFrames
// and the index groups need of the records about to be written: the version and the pinned columns,
// the whole row for the frames (a ToFrames method may read any field), and the group columns (the
// groups a record leaves are marked too). nil when the table has none of them. The keys must be
// final (after db.AssignAutoincrementKeys).
func readStoredWrittenRows(scyllaTable ScyllaTable, phases []writePhase) (storedRows, error) {
	var columns []IColInfo
	if len(scyllaTable.DeltaIndexes) > 0 {
		columns = append(columns, scyllaTable.VersionCol)
		columns = append(columns, deltaPinnedColumns(scyllaTable)...)
	}
	if scyllaTable.dataFrames != nil {
		columns = append(columns, scyllaTable.dataFrames.storedColumns...)
	}
	if len(scyllaTable.IndexGroups) > 0 && scyllaTable.indexUpdatedTable != nil {
		columns = append(columns, indexGroupStoredColumns(&scyllaTable)...)
	}
	if len(columns) == 0 {
		return nil, nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	idsOfPartition := map[int64][]int64{}
	for _, phase := range phases {
		for i := range phase.records.Len() {
			recordPointer := phase.records.At(i)
			partition := db.ConvertToInt64(partitionColumn.GetRawValue(recordPointer))
			idsOfPartition[partition] = append(idsOfPartition[partition], db.ConvertToInt64(keyColumn.GetRawValue(recordPointer)))
		}
	}
	return readStoredRows(scyllaTable, idsOfPartition, columns)
}

// stampDeltaWrites stamps every written record with its next Version and returns, per phase, the
// packed column values its CQL binds (by column name, one per record), and the moves to write once
// the rows landed.
func stampDeltaWrites(scyllaTable ScyllaTable, phases []writePhase, stored storedRows) ([]map[string][]any, []deltaMove) {
	if len(scyllaTable.DeltaIndexes) == 0 {
		return make([]map[string][]any, len(phases)), nil
	}
	partitionColumn, keyColumn := scyllaTable.GetPartKey(), scyllaTable.Keys[0]
	pinnedColumns := deltaPinnedColumns(scyllaTable)

	packedValuesByPhase := make([]map[string][]any, len(phases))
	var moves []deltaMove
	for phaseIndex, phase := range phases {
		isWritten := map[string]bool{}
		for _, column := range phase.writtenColumns {
			isWritten[column.GetName()] = true
		}
		packedValuesByPhase[phaseIndex] = map[string][]any{}
		for i := range phase.records.Len() {
			recordPointer := phase.records.At(i)
			partition := db.ConvertToInt64(partitionColumn.GetRawValue(recordPointer))
			recordID := db.ConvertToInt64(keyColumn.GetRawValue(recordPointer))
			updated := db.ConvertToInt64(scyllaTable.UpdatedCol.GetValue(recordPointer))
			storedRow, isStored := stored[[2]int64{partition, recordID}]
			version := cache.NextVersion(storedIntegerOf(storedRow, managedVersionColumnName))
			scyllaTable.VersionCol.SetValue(recordPointer, db.CoerceManagedInteger(scyllaTable.VersionCol, version))

			// A pinned column the write leaves out keeps its stored value.
			finalValues := map[string]int64{}
			for _, column := range pinnedColumns {
				if isWritten[column.GetName()] || !isStored {
					finalValues[column.GetName()] = db.ConvertToInt64(column.GetRawValue(recordPointer))
				} else {
					finalValues[column.GetName()] = storedIntegerOf(storedRow, column.GetName())
				}
			}
			for _, deltaIndex := range scyllaTable.DeltaIndexes {
				finalPinned := deltaIndex.PinnedValuesOf(finalValues)
				columnName := deltaIndex.PackedColumn.GetName()
				packedValuesByPhase[phaseIndex][columnName] = append(packedValuesByPhase[phaseIndex][columnName],
					deltaPackedStoredValue(deltaIndex, scyllaTable.Name, finalPinned, updated))
				if !isStored {
					continue
				}
				storedPinned := make([]int64, len(deltaIndex.Pinned))
				for pinnedIndex, pinnedColumn := range deltaIndex.Pinned {
					storedPinned[pinnedIndex] = storedIntegerOf(storedRow, pinnedColumn.GetName())
				}
				if !slices.Equal(storedPinned, finalPinned) {
					moves = append(moves, deltaMove{
						partition: partition, recordID: recordID, updated: updated, indexTag: deltaIndex.Tag,
						pinnedPrefix: deltaPackedStoredValue(deltaIndex, scyllaTable.Name, storedPinned, 0),
					})
				}
			}
		}
	}
	return packedValuesByPhase, moves
}

// storedIntegerOf is an integer column of a stored row: 0 for a null column or a record not stored.
func storedIntegerOf(storedRow map[string]any, columnName string) int64 {
	if storedValue, isSet := storedRow[columnName]; isSet {
		return db.ConvertToInt64(storedValue)
	}
	return 0
}

// readStoredRows reads columns of the given keys, by partition and key, at LocalQuorum so a write
// right after another sees it.
func readStoredRows(scyllaTable ScyllaTable, idsOfPartition map[int64][]int64, columns []IColInfo) (storedRows, error) {
	columnNames := []string{scyllaTable.Keys[0].GetName()}
	for _, column := range columns {
		if !slices.Contains(columnNames, column.GetName()) {
			columnNames = append(columnNames, column.GetName())
		}
	}
	for partition := range idsOfPartition {
		slices.Sort(idsOfPartition[partition])
		idsOfPartition[partition] = slices.Compact(idsOfPartition[partition])
	}

	stored := storedRows{}
	err := forEachIDsBatch(scyllaTable, idsOfPartition, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, partition int64) error {
			iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
			for {
				row := map[string]any{}
				if !iterator.MapScan(row) {
					break
				}
				storedRow := map[string]any{}
				for _, columnName := range columnNames {
					if storedValue := row[columnName]; storedValue != nil {
						storedRow[columnName] = storedValue
					}
				}
				stored[[2]int64{partition, db.ConvertToInt64(row[columnNames[0]])}] = storedRow
			}
			return iterator.Close()
		})
	if err != nil {
		return nil, fmt.Errorf("table %q: reading the stored rows: %w", scyllaTable.Name, err)
	}
	return stored, nil
}

// writeDeltaMoves writes the moves of a write. It runs after the rows landed: a move published first
// would remove, from its old prefix's clients, a record whose write then failed.
func writeDeltaMoves(scyllaTable ScyllaTable, moves []deltaMove) error {
	if len(moves) == 0 {
		return nil
	}
	statement := fmt.Sprintf("INSERT INTO %v.%v (partition_id, index_tag, pinned_prefix, updated, id) VALUES (?, ?, ?, ?, ?)",
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable))
	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)
	for _, move := range moves {
		batch.Query(statement, db.CoerceManagedInteger(scyllaTable.GetPartKey(), move.partition), move.indexTag,
			move.pinnedPrefix, int32(move.updated), db.CoerceManagedInteger(scyllaTable.Keys[0], move.recordID))
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("table %q: writing %d delta move(s): %w", scyllaTable.Name, len(moves), err)
	}
	return nil
}

/* Read */

// deltaViewRow is the (ID, updated, version) of one row of a delta view.
type deltaViewRow struct {
	recordID int64
	updated  int64
	version  int16
}

// deltaViewUpdatedMax is the highest updated a view slot holds: an upper bound meaning "up to now".
var deltaViewUpdatedMax = int64(db.SlotMaxValue(db.DeltaUpdatedSlotBits))

// deltaViewRange is the WHERE of a prefix's view rows with updated in [fromUpdated, toUpdated]: one
// span of the packed column, read newest first.
func deltaViewRange(scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated, toUpdated int64) (string, []any) {
	packedName := deltaIndex.PackedColumn.GetName()
	return fmt.Sprintf("%v = ? AND %v >= ? AND %v <= ?", scyllaTable.GetPartKey().GetName(), packedName, packedName),
		[]any{
			db.CoerceManagedInteger(scyllaTable.GetPartKey(), partition),
			deltaPackedStoredValue(deltaIndex, scyllaTable.Name, pinnedValues, fromUpdated),
			deltaPackedStoredValue(deltaIndex, scyllaTable.Name, pinnedValues, toUpdated),
		}
}

// readDeltaViewRecords reads whole records of a prefix straight from the view into sink: updated in
// [fromUpdated, toUpdated], newest first. rowLimit > 0 stops the read there. columnNames projects
// the read; nil reads every column the view copies.
func readDeltaViewRecords(
	sink recordSink, scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64, rowLimit int, columnNames []string,
) error {
	if toUpdated < fromUpdated {
		return nil
	}
	if columnNames == nil {
		for _, column := range deltaIndex.StoredColumns {
			columnNames = append(columnNames, column.GetName())
		}
	}
	whereClause, queryValues := deltaViewRange(scyllaTable, deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	queryString := fmt.Sprintf("SELECT %v FROM %v.%v WHERE %v", strings.Join(columnNames, ", "),
		resolveKeyspace(scyllaTable), deltaIndex.ViewName, whereClause)
	if rowLimit > 0 {
		queryString += fmt.Sprintf(" LIMIT %d", rowLimit)
	}
	scyllaTable.readConsistency = gocql.LocalQuorum
	return scanSelectQueryRows(queryString, queryValues, buildDefaultScanColumns(columnNames), scyllaTable, sink, nil, nil, time.Now())
}

// readDeltaViewRows reads the (ID, updated, version) of a prefix's view rows with updated in
// [fromUpdated, toUpdated], newest first: what the window hash needs, not the records.
func readDeltaViewRows(
	scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64,
) ([]deltaViewRow, error) {
	if toUpdated < fromUpdated {
		return nil, nil
	}
	keyName := scyllaTable.Keys[0].GetName()
	whereClause, queryValues := deltaViewRange(scyllaTable, deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	queryString := fmt.Sprintf("SELECT %v, %v, %v FROM %v.%v WHERE %v", keyName, managedUpdatedColumnName, managedVersionColumnName,
		resolveKeyspace(scyllaTable), deltaIndex.ViewName, whereClause)

	var viewRows []deltaViewRow
	iterator := getScyllaConnection().Query(queryString, queryValues...).Consistency(gocql.LocalQuorum).Iter()
	for {
		row := map[string]any{}
		if !iterator.MapScan(row) {
			break
		}
		viewRows = append(viewRows, deltaViewRow{
			recordID: db.ConvertToInt64(row[keyName]),
			updated:  db.ConvertToInt64(row[managedUpdatedColumnName]),
			version:  int16(db.ConvertToInt64(row[managedVersionColumnName])),
		})
	}
	if err := iterator.Close(); err != nil {
		return nil, fmt.Errorf("table %q: reading the delta view %v: %w", scyllaTable.Name, deltaIndex.ViewName, err)
	}
	return viewRows, nil
}

// readDeltaMovedIDs reads the IDs that left a prefix with updated ≥ fromUpdated, up to rowLimit.
func readDeltaMovedIDs(scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated int64, rowLimit int) ([]int64, error) {
	queryString := fmt.Sprintf("SELECT id FROM %v.%v WHERE partition_id = ? AND index_tag = ? AND pinned_prefix = ? AND updated >= ? LIMIT %d",
		resolveKeyspace(scyllaTable), deltaMovesTableName(scyllaTable), rowLimit)
	iterator := getScyllaConnection().Query(queryString,
		db.CoerceManagedInteger(scyllaTable.GetPartKey(), partition), deltaIndex.Tag,
		deltaPackedStoredValue(deltaIndex, scyllaTable.Name, pinnedValues, 0), int32(fromUpdated)).Consistency(gocql.LocalQuorum).Iter()
	var movedIDs []int64
	var movedID int64
	for iterator.Scan(&movedID) {
		movedIDs = append(movedIDs, movedID)
	}
	if err := iterator.Close(); err != nil {
		return nil, fmt.Errorf("table %q: reading the delta moves: %w", scyllaTable.Name, err)
	}
	return movedIDs, nil
}

// deltaPrefixRead is what one prefix of a Delta() read returned. Each prefix is read by its own
// goroutine into its own fields.
type deltaPrefixRead struct {
	records    []unsafe.Pointer
	movedIDs   []int64
	windowRows []deltaViewRow
}

// readDeltaPrefixes runs readPrefix for every prefix in parallel, under the same concurrency cap as
// a fanned-out select.
func readDeltaPrefixes(prefixes [][]int64, reads []deltaPrefixRead, readPrefix func(prefix []int64, read *deltaPrefixRead) error) error {
	group := errgroup.Group{}
	group.SetLimit(db.MaxFixedValueFanoutQueries)
	for prefixIndex, prefix := range prefixes {
		group.Go(func() error { return readPrefix(prefix, &reads[prefixIndex]) })
	}
	return group.Wait()
}

// execDelta is Exec for a Delta() read (db/cache delta_window.go describes the protocol). Every
// step reads each pinned prefix on its own, in parallel: one contiguous span of the view.
func execDelta(sink recordSink, tableInfo *TableInfo, scyllaTable ScyllaTable) error {
	deltaRead := tableInfo.Delta
	deltaPlan, err := scyllaTable.PlanDeltaRead(tableInfo)
	if err != nil {
		return err
	}
	deltaIndex, prefixes, partition := deltaPlan.Index, deltaPlan.Prefixes, deltaPlan.Partition

	// The client hashes the records it holds whose last pinned value is one the read covered.
	metadata := cache.DeltaWindowMetadata{}
	if len(deltaIndex.Pinned) > 0 {
		lastPinned := deltaIndex.Pinned[len(deltaIndex.Pinned)-1]
		metadata.FilterField = deltaFieldJSONName(sink, lastPinned)
		for _, prefix := range prefixes {
			if lastValue := prefix[len(prefix)-1]; !slices.Contains(metadata.FilterValues, lastValue) {
				metadata.FilterValues = append(metadata.FilterValues, lastValue)
			}
		}
	}

	// V for the client's next sync, on the clock that stamps updated: taken before any read, so
	// every write stamped before V − DeltaWriteLagTicks was visible to the reads below.
	metadata.ReadTime = getManagedUnixTime()
	cursor := deltaRead.Cursor
	plan := cache.PlanDeltaSync(cursor)
	// A first sync is a replace; any other cursor, however old, is decided by how much changed.
	metadata.IsReplace = cursor.Updated == 0
	projection := deltaProjection(tableInfo, scyllaTable, deltaIndex)
	// A view that declared Cols copies fewer columns than the table: a Select() naming one it lacks
	// would fail in CQL, so the read fails first, naming the column.
	for _, projectedName := range projection {
		if !slices.ContainsFunc(deltaIndex.StoredColumns, func(column IColInfo) bool { return column.GetName() == projectedName }) {
			return fmt.Errorf(`table %q: Delta() projects column %q, which the delta view %v does not copy: add it to the index Cols`,
				scyllaTable.Name, projectedName, deltaIndex.ViewName)
		}
	}
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
	}
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer))
	}

	// The changed records and the moves of each prefix. A sync needs at most threshold + 1 of each
	// per prefix to know it is a replace: the view lists newest first, so an old cursor costs no more
	// than a recent one.
	reads := make([]deltaPrefixRead, len(prefixes))
	decisionLimit := deltaIndex.ReplaceThreshold + 1
	if !metadata.IsReplace {
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			if err := readDeltaViewRecords(sink.collectInto(&read.records), scyllaTable, deltaIndex, partition, prefix,
				int64(plan.ChangedFrom), deltaViewUpdatedMax, decisionLimit, projection); err != nil {
				return err
			}
			var err error
			if len(deltaIndex.Pinned) > 0 {
				read.movedIDs, err = readDeltaMovedIDs(scyllaTable, deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit)
			}
			return err
		})
		if err != nil {
			return err
		}
		changedCount, movedCount := 0, 0
		for _, read := range reads {
			changedCount += len(read.records)
			movedCount += len(read.movedIDs)
		}
		metadata.IsReplace = changedCount > deltaIndex.ReplaceThreshold || movedCount > deltaIndex.ReplaceThreshold
	}

	if metadata.IsReplace {
		// Every record of the prefixes, newest first across them, so a Limit keeps the most recently
		// written ones. Each prefix comes from its snapshot when that one still holds (delta_snapshot.go).
		reads = make([]deltaPrefixRead, len(prefixes))
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			return readDeltaPrefixReplace(sink, scyllaTable, deltaIndex, partition, prefix, tableInfo.Limit, projection,
				metadata.ReadTime, read)
		})
		if err != nil {
			return err
		}
		var replaceRecords []unsafe.Pointer
		for _, read := range reads {
			replaceRecords = append(replaceRecords, read.records...)
		}
		slices.SortStableFunc(replaceRecords, func(left, right unsafe.Pointer) int {
			return cmp.Compare(updatedOf(right), updatedOf(left))
		})
		if tableInfo.Limit > 0 && len(replaceRecords) > int(tableInfo.Limit) {
			replaceRecords = replaceRecords[:tableInfo.Limit]
		}
		// One copy into an array of the final size, not log2(n) doublings: on a 50k first sync the
		// doublings allocated more than the records themselves. +1 is the metadata record.
		if sink.reserve != nil {
			sink.reserve(len(replaceRecords) + 1)
		}
		for _, recordPointer := range replaceRecords {
			metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
			sink.appendRecord(recordPointer)
		}
	} else {
		// The unsettled slice of every prefix: its rows are not resent, only hashed.
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			var err error
			read.windowRows, err = readDeltaViewRows(scyllaTable, deltaIndex, partition, prefix,
				int64(plan.WindowFrom), int64(plan.WindowTo)-1)
			return err
		})
		if err != nil {
			return err
		}
		metadata.WindowFrom, metadata.WindowTo = plan.WindowFrom, plan.WindowTo
		metadata.NextCursor = cursor.Updated
		if sink.reserve != nil {
			changedRecordCount := 1 // the metadata record
			for _, read := range reads {
				changedRecordCount += len(read.records)
			}
			sink.reserve(changedRecordCount)
		}
		isInView := map[int64]bool{}
		for _, read := range reads {
			for _, viewRow := range read.windowRows {
				isInView[viewRow.recordID] = true
				metadata.WindowHash += cache.DeltaRecordHash(viewRow.recordID, viewRow.version)
			}
			for _, recordPointer := range read.records {
				isInView[recordIDOf(recordPointer)] = true
				metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
				sink.appendRecord(recordPointer)
			}
		}
		// A record that moved to another prefix the read covers is in the view again, with a newer
		// updated: it is not removed.
		for _, read := range reads {
			for _, movedID := range read.movedIDs {
				if !isInView[movedID] && !slices.Contains(metadata.RemovedIDs, movedID) {
					metadata.RemovedIDs = append(metadata.RemovedIDs, movedID)
				}
			}
		}
	}

	metadataRecord, err := deltaMetadataRecord(sink, scyllaTable, cache.EncodeDeltaWindowMetadata(metadata))
	if err != nil {
		return err
	}
	sink.appendRecord(metadataRecord)
	return nil
}

// deltaProjection is the caller's Select() projection plus what the client's window hash reads: the
// key, updated, version and the pinned columns. nil (every column) without a projection.
func deltaProjection(tableInfo *TableInfo, scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo) []string {
	if len(tableInfo.ColumnsInclude) == 0 && len(tableInfo.ColumnsExclude) == 0 {
		return nil
	}
	columnNames, _, _ := buildSelectProjection(tableInfo, scyllaTable)
	requiredNames := []string{scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName(), managedUpdatedColumnName, managedVersionColumnName}
	for _, pinnedColumn := range deltaIndex.Pinned {
		requiredNames = append(requiredNames, pinnedColumn.GetName())
	}
	for _, requiredName := range requiredNames {
		if !slices.Contains(columnNames, requiredName) {
			columnNames = append(columnNames, requiredName)
		}
	}
	return columnNames
}

// deltaFieldJSONName is the JSON name the client reads a column's field by.
func deltaFieldJSONName(sink recordSink, column IColInfo) string {
	recordType := reflect.TypeOf(sink.asRecordPointer(sink.newRecord())).Elem()
	fieldName := column.GetInfo().FieldName
	field, isFound := recordType.FieldByName(fieldName)
	if !isFound {
		return fieldName
	}
	if jsonName, _, _ := strings.Cut(field.Tag.Get("json"), ","); jsonName != "" && jsonName != "-" {
		return jsonName
	}
	return fieldName
}

// deltaMetadataRecord is the Metadata record of a Delta() read: −1 as its key, the Metadata text in
// its `Metadata *string json:"mdt,omitempty"` field.
func deltaMetadataRecord(sink recordSink, scyllaTable ScyllaTable, metadataText string) (unsafe.Pointer, error) {
	record := sink.newRecord()
	metadataField := reflect.ValueOf(sink.asRecordPointer(record)).Elem().FieldByName("Metadata")
	if !metadataField.IsValid() || metadataField.Type() != reflect.TypeFor[*string]() {
		return nil, fmt.Errorf(`table %q: Delta() returns a Metadata record and the record has no field "Metadata *string" (json "mdt,omitempty")`, scyllaTable.Name)
	}
	metadataField.Set(reflect.ValueOf(&metadataText))
	keyColumn := scyllaTable.Keys[0]
	keyColumn.SetValue(record, db.CoerceManagedInteger(keyColumn, -1))
	return record, nil
}

// deltaPackedStoredValue is the value stored in a delta view's packed column. The layout is the
// shared unsigned one (db.DeltaIndexInfo.PackedKey); Scylla compares the column as signed, so the
// top bit is flipped.
func deltaPackedStoredValue(deltaIndex db.DeltaIndexInfo, tableName string, pinnedValues []int64, updated int64) int64 {
	return storeVirtualPacked(deltaIndex.PackedKey(tableName, pinnedValues, updated), false).(int64)
}
