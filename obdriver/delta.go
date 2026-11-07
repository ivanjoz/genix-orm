package obdriver

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"golang.org/x/sync/errgroup"
)

// TypeDelta on OceanBase (docs/OCEANBASE_PLAN.md §11): the delta window protocol (db/cache
// delta_window.go) of Scylla's execDelta, step for step. A delta index is a native index on
// (partition, packed delta column), packed = the pinned values then updated in the shared unsigned
// layout, so one pinned prefix is one contiguous span of the index ordered by updated. The rows,
// their index entries and their moves commit together (write.go), so there is no view lag; the
// protocol keeps its settle margin anyway.

// deltaWindowRow is the (ID, version) of one record of a prefix's unsettled slice.
type deltaWindowRow struct {
	recordID int64
	version  int16
}

// deltaPrefixRead is what one prefix of a Delta() read returned; each prefix has its own goroutine.
type deltaPrefixRead struct {
	records    []unsafe.Pointer
	movedIDs   []int64
	windowRows []deltaWindowRow
}

var deltaUpdatedMax = int64(db.SlotMaxValue(db.DeltaUpdatedSlotBits))

// deltaRangeCondition is the WHERE of a prefix's rows with updated in [fromUpdated, toUpdated], and
// the hint naming the delta index that serves it.
func (table *ObTable) deltaRangeCondition(deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated, toUpdated int64) (string, string, []any) {
	sourcePosition := slices.IndexFunc(table.IndexSources, func(source *db.IndexSource) bool {
		return source.IsDeltaView && source.Column.GetName() == deltaIndex.PackedColumn.GetName()
	})
	hint := fmt.Sprintf("/*+ INDEX(%s %s) */ ", table.Name, table.indexOfSource(table.IndexSources[sourcePosition]).name)
	packedName := quoteName(deltaIndex.PackedColumn.GetName())
	condition := fmt.Sprintf("%s = ? AND %s >= ? AND %s <= ?", quoteName(table.GetPartKey().GetName()), packedName, packedName)
	return hint, condition, []any{
		partition, deltaIndex.PackedKey(table.Name, pinnedValues, fromUpdated), deltaIndex.PackedKey(table.Name, pinnedValues, toUpdated),
	}
}

// readDeltaRecords reads the records of one prefix with updated in [fromUpdated, toUpdated], newest
// first, into sink. rowLimit > 0 stops there. projection nil reads every column the index copies.
func (table *ObTable) readDeltaRecords(
	sink recordSink, deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64, rowLimit int, projection []string,
) error {
	if toUpdated < fromUpdated {
		return nil
	}
	if projection == nil {
		for _, column := range deltaIndex.StoredColumns {
			projection = append(projection, column.GetName())
		}
	}
	selectedColumns := []storedColumn{}
	for _, stored := range table.storedColumns {
		if stored.hasField() && slices.Contains(projection, stored.name) {
			selectedColumns = append(selectedColumns, stored)
		}
	}
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	hint, condition, args := table.deltaRangeCondition(deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	statement := fmt.Sprintf("SELECT %s%s FROM %s WHERE %s ORDER BY %s DESC", hint, quotedNames(selectedNames), table.qualifiedName(),
		condition, quoteName(deltaIndex.PackedColumn.GetName()))
	if rowLimit > 0 {
		statement += fmt.Sprintf(" LIMIT %d", rowLimit)
	}
	return table.queryInto(statement, args, selectedColumns, sink)
}

// readDeltaWindowRows reads the (ID, version) of a prefix's rows with updated in [fromUpdated,
// toUpdated]: what the window hash needs, not the records.
func (table *ObTable) readDeltaWindowRows(deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated, toUpdated int64) ([]deltaWindowRow, error) {
	if toUpdated < fromUpdated {
		return nil, nil
	}
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	hint, condition, args := table.deltaRangeCondition(deltaIndex, partition, pinnedValues, fromUpdated, toUpdated)
	rows, err := pool.Query(fmt.Sprintf("SELECT %s%s, %s FROM %s WHERE %s", hint, quoteName(table.Keys[0].GetName()),
		quoteName(table.VersionCol.GetName()), table.qualifiedName(), condition), args...)
	if err != nil {
		return nil, fmt.Errorf("obdriver: reading the delta index of %q: %w", table.Name, err)
	}
	defer rows.Close()
	var windowRows []deltaWindowRow
	for rows.Next() {
		var windowRow deltaWindowRow
		if err := rows.Scan(&windowRow.recordID, &windowRow.version); err != nil {
			return nil, err
		}
		windowRows = append(windowRows, windowRow)
	}
	return windowRows, rows.Err()
}

// readDeltaMovedIDs reads the IDs that left a prefix with updated ≥ fromUpdated, up to rowLimit.
func (table *ObTable) readDeltaMovedIDs(deltaIndex db.DeltaIndexInfo, partition int64, pinnedValues []int64, fromUpdated int64, rowLimit int) ([]int64, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	movesTable := deltaMovesTableSpec(table.Namespace, table.Name, "", 0)
	rows, err := pool.Query(fmt.Sprintf("SELECT `id` FROM %s WHERE `partition_id` = ? AND `index_tag` = ? AND `pinned_prefix` = ? AND `updated` >= ? LIMIT %d",
		movesTable.qualifiedName(), rowLimit), partition, deltaIndex.Tag, deltaIndex.PackedKey(table.Name, pinnedValues, 0), fromUpdated)
	if err != nil {
		return nil, fmt.Errorf("obdriver: reading the delta moves of %q: %w", table.Name, err)
	}
	defer rows.Close()
	var movedIDs []int64
	for rows.Next() {
		var movedID int64
		if err := rows.Scan(&movedID); err != nil {
			return nil, err
		}
		movedIDs = append(movedIDs, movedID)
	}
	return movedIDs, rows.Err()
}

func readDeltaPrefixes(prefixes [][]int64, reads []deltaPrefixRead, readPrefix func(prefix []int64, read *deltaPrefixRead) error) error {
	group := errgroup.Group{}
	group.SetLimit(db.MaxFixedValueFanoutQueries)
	for prefixIndex, prefix := range prefixes {
		group.Go(func() error { return readPrefix(prefix, &reads[prefixIndex]) })
	}
	return group.Wait()
}

// selectDelta is Select for a Delta() read: Scylla's execDelta (scylla/delta_view.go), step for step.
func selectDelta(table *ObTable, tableInfo *db.TableInfo, sink recordSink) error {
	deltaPlan, err := table.PlanDeltaRead(tableInfo)
	if err != nil {
		return err
	}
	deltaIndex, prefixes, partition := deltaPlan.Index, deltaPlan.Prefixes, deltaPlan.Partition

	// The client hashes the records it holds whose last pinned value is one the read covered.
	metadata := cache.DeltaWindowMetadata{}
	if len(deltaIndex.Pinned) > 0 {
		metadata.FilterField = deltaFieldJSONName(sink.recordType, deltaIndex.Pinned[len(deltaIndex.Pinned)-1])
		for _, prefix := range prefixes {
			if lastValue := prefix[len(prefix)-1]; !slices.Contains(metadata.FilterValues, lastValue) {
				metadata.FilterValues = append(metadata.FilterValues, lastValue)
			}
		}
	}

	// V for the client's next sync, taken before any read.
	metadata.ReadTime = db.ManagedUnixTime()
	cursor := tableInfo.Delta.Cursor
	plan := cache.PlanDeltaSync(cursor)
	metadata.IsReplace = cursor.Updated == 0
	projection := table.deltaProjection(tableInfo, deltaIndex)
	for _, projectedName := range projection {
		if !slices.ContainsFunc(deltaIndex.StoredColumns, func(column db.IColInfo) bool { return column.GetName() == projectedName }) {
			return fmt.Errorf(`table %q: Delta() projects column %q, which the delta view %v does not copy: add it to the index Cols`,
				table.Name, projectedName, deltaIndex.ViewName)
		}
	}
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))
	}
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer))
	}

	// A sync needs at most threshold + 1 changes or moves per prefix to know it is a replace.
	reads := make([]deltaPrefixRead, len(prefixes))
	decisionLimit := deltaIndex.ReplaceThreshold + 1
	if !metadata.IsReplace {
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			if err := table.readDeltaRecords(sink.collectInto(&read.records), deltaIndex, partition, prefix,
				int64(plan.ChangedFrom), deltaUpdatedMax, decisionLimit, projection); err != nil {
				return err
			}
			var err error
			if len(deltaIndex.Pinned) > 0 {
				read.movedIDs, err = table.readDeltaMovedIDs(deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit)
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
		// Every record of the prefixes, newest first across them; each prefix from its snapshot when
		// that one still holds (delta_snapshot.go).
		reads = make([]deltaPrefixRead, len(prefixes))
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			return table.readDeltaPrefixReplace(sink, deltaIndex, partition, prefix, tableInfo.Limit, projection, metadata.ReadTime, read)
		})
		if err != nil {
			return err
		}
		var replaceRecords []unsafe.Pointer
		for _, read := range reads {
			replaceRecords = append(replaceRecords, read.records...)
		}
		slices.SortStableFunc(replaceRecords, func(left, right unsafe.Pointer) int { return cmp.Compare(updatedOf(right), updatedOf(left)) })
		if tableInfo.Limit > 0 && len(replaceRecords) > int(tableInfo.Limit) {
			replaceRecords = replaceRecords[:tableInfo.Limit]
		}
		for _, recordPointer := range replaceRecords {
			metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
			sink.appendRecord(recordPointer)
		}
	} else {
		// The unsettled slice of every prefix: hashed, not resent.
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			var err error
			read.windowRows, err = table.readDeltaWindowRows(deltaIndex, partition, prefix, int64(plan.WindowFrom), int64(plan.WindowTo)-1)
			return err
		})
		if err != nil {
			return err
		}
		metadata.WindowFrom, metadata.WindowTo = plan.WindowFrom, plan.WindowTo
		metadata.NextCursor = cursor.Updated
		isInView := map[int64]bool{}
		for _, read := range reads {
			for _, windowRow := range read.windowRows {
				isInView[windowRow.recordID] = true
				metadata.WindowHash += cache.DeltaRecordHash(windowRow.recordID, windowRow.version)
			}
			for _, recordPointer := range read.records {
				isInView[recordIDOf(recordPointer)] = true
				metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
				sink.appendRecord(recordPointer)
			}
		}
		// A record that moved to another prefix the read covers is in the view again: not removed.
		for _, read := range reads {
			for _, movedID := range read.movedIDs {
				if !isInView[movedID] && !slices.Contains(metadata.RemovedIDs, movedID) {
					metadata.RemovedIDs = append(metadata.RemovedIDs, movedID)
				}
			}
		}
	}

	metadataRecord, err := table.deltaMetadataRecord(sink, cache.EncodeDeltaWindowMetadata(metadata))
	if err != nil {
		return err
	}
	sink.appendRecord(metadataRecord)
	return nil
}

// deltaProjection is the caller's Select() projection plus what the client's window hash reads: the
// key, updated, version and the pinned columns. nil (every column the index copies) without one.
func (table *ObTable) deltaProjection(tableInfo *db.TableInfo, deltaIndex db.DeltaIndexInfo) []string {
	if len(tableInfo.ColumnsInclude) == 0 && len(tableInfo.ColumnsExclude) == 0 {
		return nil
	}
	columnNames := []string{}
	for _, stored := range table.projectedColumns(tableInfo) {
		columnNames = append(columnNames, stored.name)
	}
	requiredNames := []string{table.GetPartKey().GetName(), table.Keys[0].GetName(), table.UpdatedCol.GetName(), table.VersionCol.GetName()}
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
func deltaFieldJSONName(recordType reflect.Type, column db.IColInfo) string {
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
func (table *ObTable) deltaMetadataRecord(sink recordSink, metadataText string) (unsafe.Pointer, error) {
	record := sink.newRecord()
	metadataField := reflect.NewAt(sink.recordType, record).Elem().FieldByName("Metadata")
	if !metadataField.IsValid() || metadataField.Type() != reflect.TypeFor[*string]() {
		return nil, fmt.Errorf(`table %q: Delta() returns a Metadata record and the record has no field "Metadata *string" (json "mdt,omitempty")`, table.Name)
	}
	metadataField.Set(reflect.ValueOf(&metadataText))
	table.Keys[0].SetValue(record, db.CoerceManagedInteger(table.Keys[0], -1))
	return record, nil
}
