package fdbdriver

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/fdb"
	"golang.org/x/sync/errgroup"
)

// TypeDelta on FoundationDB: the delta window protocol (db/cache delta_window.go) over entries.
//
// Each record has one entry per TypeDelta index, keyed (partition, 'd', deltaID, packed, pk) with
// packed = the pinned values and updated in the shared unsigned layout (db.DeltaIndexInfo.PackedKey),
// so one pinned prefix is one contiguous span ordered by updated. A sync reads each prefix's span
// newest first with one range read: each entry's value is a copy of the record's row, written in the
// same commit as the row itself. A record leaving a prefix writes a move row (partition, 'm', deltaID, oldPrefix,
// updated, pk) in the same commit as its row, so there is no view or replica lag; the protocol keeps
// its settle margin (DeltaWriteLagTicks) anyway.
//
// Prefix resolution and index choice are shared with every driver (db.TableCore.PlanDeltaRead).

// maxDeltaPrefixReads is how many prefixes are read concurrently.
const maxDeltaPrefixReads = 16

// deltaPrefixRead is what one prefix of a Delta() read returned; each prefix has its own goroutine.
type deltaPrefixRead struct {
	records    []unsafe.Pointer
	movedIDs   []int64
	windowRows []deltaWindowRow
}

type deltaWindowRow struct {
	recordID int64
	version  int16
}

var deltaUpdatedMax = int64(db.SlotMaxValue(db.DeltaUpdatedSlotBits))

// readDeltaRecords reads the records of one prefix with updated in [fromUpdated, toUpdated], newest
// first, decoding the filter's columns. rowLimit > 0 stops there.
func (table *FdbTable) readDeltaRecords(
	gate *fdb.Client, sink recordSink, deltaIndex fdbDeltaIndex, partition int64, pinnedValues []int64,
	fromUpdated, toUpdated int64, rowLimit int, filter columnFilter,
) ([]unsafe.Pointer, error) {
	if toUpdated < fromUpdated {
		return nil, nil
	}
	entryPrefix := table.deltaEntryPrefix(partition, deltaIndex.id)
	readRange := fdb.Range{
		Begin:   binary.BigEndian.AppendUint64(slices.Clone(entryPrefix), deltaIndex.PackedKey(table.Name, pinnedValues, fromUpdated)),
		End:     binary.BigEndian.AppendUint64(slices.Clone(entryPrefix), deltaIndex.PackedKey(table.Name, pinnedValues, toUpdated)+1),
		Reverse: true,
	}
	var records []unsafe.Pointer
	for {
		pageLimit := readPageRecords
		if rowLimit > 0 {
			pageLimit = min(pageLimit, rowLimit-len(records))
		}
		readRange.Limit = uint32(pageLimit)
		readRange.ByteLimit = readPageBytes
		ctx, cancel := requestContext()
		results, err := gate.GetRanges(ctx, []fdb.Range{readRange})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("table %q: reading delta index %v: %w", table.Name, deltaIndex.Tag, err)
		}
		// Each entry's value is a copy of the record's row (write.go appendDeltaMutations).
		for _, row := range results[0].Values.Slices() {
			recordPointer := sink.newRecord()
			if err := table.decodeRow(row, recordPointer, filter); err != nil {
				return nil, err
			}
			records = append(records, recordPointer)
		}
		if !results[0].More || (rowLimit > 0 && len(records) >= rowLimit) {
			return records, nil
		}
		readRange.End = results[0].NextBegin
	}
}

// readDeltaMovedIDs reads the IDs that left a prefix with updated ≥ fromUpdated, up to rowLimit.
func (table *FdbTable) readDeltaMovedIDs(gate *fdb.Client, deltaIndex fdbDeltaIndex, partition int64, pinnedValues []int64, fromUpdated int64, rowLimit int) ([]int64, error) {
	movePrefix := table.deltaMovePrefix(partition, deltaIndex.id, deltaIndex.PackedKey(table.Name, pinnedValues, 0))
	readRange := fdb.Range{
		Begin: appendOrderedInt64(slices.Clone(movePrefix), fromUpdated),
		End:   prefixEnd(movePrefix),
		Limit: uint32(rowLimit),
	}
	ctx, cancel := requestContext()
	results, err := gate.GetRanges(ctx, []fdb.Range{readRange})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("table %q: reading delta moves: %w", table.Name, err)
	}
	var movedIDs []int64
	for _, moveKey := range results[0].Keys.Slices() {
		// The key ends with the record ID, an ordered int64.
		movedIDs = append(movedIDs, int64(binary.BigEndian.Uint64(moveKey[len(moveKey)-8:])^(1<<63)))
	}
	return movedIDs, nil
}

func readDeltaPrefixes(prefixes [][]int64, reads []deltaPrefixRead, readPrefix func(prefix []int64, read *deltaPrefixRead) error) error {
	group := errgroup.Group{}
	group.SetLimit(maxDeltaPrefixReads)
	for prefixIndex, prefix := range prefixes {
		group.Go(func() error { return readPrefix(prefix, &reads[prefixIndex]) })
	}
	return group.Wait()
}

// selectDelta is Select for a Delta() read: the Scylla driver's execDelta, step for step.
func selectDelta(table *FdbTable, tableInfo *db.TableInfo, sink recordSink) error {
	gate, err := gateClient()
	if err != nil {
		return err
	}
	deltaRead := tableInfo.Delta
	deltaPlan, err := table.PlanDeltaRead(tableInfo)
	if err != nil {
		return err
	}
	// Index positions in deltaIndexes mirror TableCore.DeltaIndexes, which the plan indexes.
	deltaIndex, prefixes, partition := table.deltaIndexes[deltaPlan.IndexPosition], deltaPlan.Prefixes, deltaPlan.Partition

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
	cursor := deltaRead.Cursor
	plan := cache.PlanDeltaSync(cursor)
	metadata.IsReplace = cursor.Updated == 0
	required := []string{table.GetPartKey().GetName(), table.Keys[0].GetName(), managedUpdatedColumnName, managedVersionColumnName}
	for _, pinnedColumn := range deltaIndex.Pinned {
		required = append(required, pinnedColumn.GetName())
	}
	filter := table.projectionFilter(tableInfo, nil, required)
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer))
	}
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))
	}

	reads := make([]deltaPrefixRead, len(prefixes))
	decisionLimit := deltaIndex.ReplaceThreshold + 1
	if !metadata.IsReplace {
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			var err error
			if read.records, err = table.readDeltaRecords(gate, sink, deltaIndex, partition, prefix,
				int64(plan.ChangedFrom), deltaUpdatedMax, decisionLimit, filter); err != nil {
				return err
			}
			if len(deltaIndex.Pinned) > 0 {
				read.movedIDs, err = table.readDeltaMovedIDs(gate, deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit)
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
		reads = make([]deltaPrefixRead, len(prefixes))
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			var err error
			read.records, err = table.readDeltaRecords(gate, sink, deltaIndex, partition, prefix,
				0, deltaUpdatedMax, int(tableInfo.Limit), filter)
			return err
		})
		if err != nil {
			return err
		}
		var replaceRecords []unsafe.Pointer
		for _, read := range reads {
			replaceRecords = append(replaceRecords, read.records...)
		}
		if len(reads) > 1 {
			slices.SortStableFunc(replaceRecords, func(left, right unsafe.Pointer) int { return cmp.Compare(updatedOf(right), updatedOf(left)) })
		}
		if tableInfo.Limit > 0 && len(replaceRecords) > int(tableInfo.Limit) {
			replaceRecords = replaceRecords[:tableInfo.Limit]
		}
		sink.reserve(len(replaceRecords) + 1)
		for _, recordPointer := range replaceRecords {
			metadata.NextCursor = max(metadata.NextCursor, int32(updatedOf(recordPointer)))
			sink.appendRecord(recordPointer)
		}
	} else {
		// The unsettled slice of every prefix: hashed, not resent.
		versionFilter := table.projectionFilter(&db.TableInfo{ColumnsInclude: []db.ColumnInfo{{ColInfo: db.ColInfo{Name: table.Keys[0].GetName()}}}}, nil,
			[]string{managedUpdatedColumnName, managedVersionColumnName})
		err := readDeltaPrefixes(prefixes, reads, func(prefix []int64, read *deltaPrefixRead) error {
			windowRecords, err := table.readDeltaRecords(gate, sink, deltaIndex, partition, prefix,
				int64(plan.WindowFrom), int64(plan.WindowTo)-1, 0, versionFilter)
			for _, recordPointer := range windowRecords {
				read.windowRows = append(read.windowRows, deltaWindowRow{
					recordID: recordIDOf(recordPointer),
					version:  int16(db.ConvertToInt64(table.VersionCol.GetRawValue(recordPointer))),
				})
			}
			return err
		})
		if err != nil {
			return err
		}
		metadata.WindowFrom, metadata.WindowTo = plan.WindowFrom, plan.WindowTo
		metadata.NextCursor = cursor.Updated
		changedRecordCount := 1
		for _, read := range reads {
			changedRecordCount += len(read.records)
		}
		sink.reserve(changedRecordCount)
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
		for _, read := range reads {
			for _, movedID := range read.movedIDs {
				if !isInView[movedID] && !slices.Contains(metadata.RemovedIDs, movedID) {
					metadata.RemovedIDs = append(metadata.RemovedIDs, movedID)
				}
			}
		}
	}

	metadataRecord, err := deltaMetadataRecord(sink, table, cache.EncodeDeltaWindowMetadata(metadata))
	if err != nil {
		return err
	}
	sink.appendRecord(metadataRecord)
	return nil
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
func deltaMetadataRecord(sink recordSink, table *FdbTable, metadataText string) (unsafe.Pointer, error) {
	record := sink.newRecord()
	metadataField := reflect.NewAt(sink.recordType, record).Elem().FieldByName("Metadata")
	if !metadataField.IsValid() || metadataField.Type() != reflect.TypeFor[*string]() {
		return nil, fmt.Errorf(`table %q: Delta() returns a Metadata record and the record has no field "Metadata *string"`, table.Name)
	}
	metadataField.Set(reflect.ValueOf(&metadataText))
	table.Keys[0].SetValue(record, int64(-1))
	return record, nil
}
