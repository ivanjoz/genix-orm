package obdriver

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"sync"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// Record snapshots (db/cache record_snapshot.go and delta_snapshot.go), as Scylla's driver keeps them
// (scylla/record_snapshot.go, scylla/delta_snapshot.go): the same files, keys and checks, so a
// snapshot answers a replace exactly as the index read would.

// recordSnapshotCodec is what a record type needs to be kept in a snapshot.
type recordSnapshotCodec struct {
	// fingerprint names the record type's colbin schema: a file written by another schema is a miss.
	fingerprint uint64
	// isEligible is false when a stored column is a field colbin drops (cb:"-").
	isEligible bool
}

var recordSnapshotCodecs sync.Map

func (table *ObTable) recordSnapshotCodecFor(recordType reflect.Type) recordSnapshotCodec {
	if cachedCodec, isCached := recordSnapshotCodecs.Load(recordType); isCached {
		return cachedCodec.(recordSnapshotCodec)
	}
	codec := recordSnapshotCodec{}
	schema, schemaErr := colbin.SchemaOf(reflect.New(recordType).Interface())
	fieldIDs, fieldsErr := colbin.FieldIDs(reflect.New(recordType).Interface())
	if schemaErr == nil && fieldsErr == nil {
		fingerprintHash := fnv.New64a()
		fingerprintHash.Write(schema.Bytes())
		codec.fingerprint = fingerprintHash.Sum64()
		codec.isEligible = true
		for _, stored := range table.storedColumns {
			if !stored.hasField() {
				continue
			}
			if _, isEncoded := fieldIDs[stored.column.GetInfo().FieldName]; !isEncoded {
				codec.isEligible = false
				break
			}
		}
	} else {
		fmt.Println("Record snapshot: table", table.Name, "is not colbin-encodable, snapshots disabled:", errors.Join(schemaErr, fieldsErr))
	}
	recordSnapshotCodecs.Store(recordType, codec)
	return codec
}

// decodeRecordSnapshotFile opens a snapshot file for a read at readTime. isUsable is false when the
// file can't answer: unreadable, written by another schema, or expired.
func decodeRecordSnapshotFile(
	content []byte, codec recordSnapshotCodec, recordType reflect.Type, readTime int32, snapshotKey string,
) (snapshot cache.RecordSnapshot, records []unsafe.Pointer, isUsable bool) {
	snapshot, err := cache.DecodeRecordSnapshot(content)
	if err != nil || snapshot.SchemaFingerprint != codec.fingerprint || cache.IsRecordSnapshotExpired(snapshot, readTime) {
		return snapshot, nil, false
	}
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	if err := colbin.Unmarshal(snapshot.Records, recordsPointer.Interface()); err != nil {
		fmt.Println("Record snapshot: decoding", snapshotKey, "failed:", err)
		return snapshot, nil, false
	}
	return snapshot, db.MakeReflectRecordSlice(recordsPointer).Pointers(), true
}

// writeRecordSnapshotFile stores records as a snapshot labelled cursor, read at readTime. etag is the
// file the read replaced ("" = none): when another request wrote it first, this write is skipped.
// A failed write only logs: the response already has its records.
func writeRecordSnapshotFile(
	fileStore utils.FileStore, snapshotKey, etag string, codec recordSnapshotCodec, recordType reflect.Type,
	cursor, readTime int32, records []unsafe.Pointer,
) {
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	recordList := recordsPointer.Elem()
	recordList.Set(reflect.MakeSlice(recordList.Type(), len(records), len(records)))
	for index, recordPointer := range records {
		recordList.Index(index).Set(reflect.NewAt(recordType, recordPointer).Elem())
	}
	encodedRecords, err := colbin.Marshal(recordsPointer.Interface())
	if err != nil {
		fmt.Println("Record snapshot: encoding", snapshotKey, "failed:", err)
		return
	}
	content := cache.EncodeRecordSnapshot(cache.RecordSnapshot{
		SchemaFingerprint: codec.fingerprint, Cursor: cursor, ReadTime: readTime, Records: encodedRecords,
	})
	if _, err := fileStore.PutFileIfMatch(context.Background(), snapshotKey, content, etag); err != nil &&
		!errors.Is(err, utils.ErrFilePreconditionFailed) {
		fmt.Println("Record snapshot: writing", snapshotKey, "failed:", err)
	}
}

// readDeltaPrefixReplace fills read.records with every record of one prefix, newest first up to
// limit: from the prefix's snapshot when it is still good, else from the index, which then rewrites
// the snapshot. readTime is the clock taken before any read of this sync.
func (table *ObTable) readDeltaPrefixReplace(
	sink recordSink, deltaIndex db.DeltaIndexInfo, partition int64, prefix []int64,
	limit int32, projection []string, readTime int32, read *deltaPrefixRead,
) error {
	readFromIndex := func() error {
		return table.readDeltaRecords(sink.collectInto(&read.records), deltaIndex, partition, prefix, 0, deltaUpdatedMax, int(limit), projection)
	}
	fileStore := cache.FileStore()
	if fileStore == nil || !cache.RecordSnapshotsEnabled() {
		return readFromIndex()
	}
	codec := table.recordSnapshotCodecFor(sink.recordType)
	if !codec.isEligible {
		return readFromIndex()
	}

	snapshotKey := cache.DeltaSnapshotKey(table.Name, partition, deltaIndex.Tag, prefix, limit, projection)
	content, etag, err := fileStore.GetFile(snapshotKey)
	switch {
	case errors.Is(err, utils.ErrFileNotFound):
		etag = ""
	case err != nil:
		fmt.Println("Delta snapshot: reading", snapshotKey, "failed, reading the index:", err)
		return readFromIndex()
	default:
		mergedRecords, isServed, err := table.serveDeltaSnapshot(sink, deltaIndex, partition, prefix, limit, projection, codec, readTime, content, snapshotKey)
		if err != nil {
			return err
		}
		if isServed {
			read.records = mergedRecords
			return nil
		}
	}

	if err := readFromIndex(); err != nil {
		return err
	}
	// An empty prefix is not stored: it has no cursor to sync from, and reading it again costs nothing.
	if len(read.records) > 0 {
		cursor := int64(0)
		for _, recordPointer := range read.records {
			cursor = max(cursor, db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer)))
		}
		writeRecordSnapshotFile(fileStore, snapshotKey, etag, codec, sink.recordType, int32(cursor), readTime, read.records)
	}
	return nil
}

// serveDeltaSnapshot answers a prefix from its snapshot file: the snapshot's records with the changes
// since its cursor applied, checked by the window protocol as a client's records are. isServed is
// false when the file can't answer; the caller reads the index instead.
func (table *ObTable) serveDeltaSnapshot(
	sink recordSink, deltaIndex db.DeltaIndexInfo, partition int64, prefix []int64,
	limit int32, projection []string, codec recordSnapshotCodec, readTime int32, content []byte, snapshotKey string,
) (mergedRecords []unsafe.Pointer, isServed bool, err error) {
	snapshot, snapshotRecords, isUsable := decodeRecordSnapshotFile(content, codec, sink.recordType, readTime, snapshotKey)
	if !isUsable {
		return nil, false, nil
	}
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))
	}
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer))
	}

	// The sync a client holding the snapshot would run: changed records, moves, the unsettled slice.
	plan := cache.PlanDeltaSync(cache.DeltaWindowCursor{Updated: snapshot.Cursor, ReadTime: snapshot.ReadTime})
	decisionLimit := deltaIndex.ReplaceThreshold + 1
	var changedRecords []unsafe.Pointer
	if err := table.readDeltaRecords(sink.collectInto(&changedRecords), deltaIndex, partition, prefix,
		int64(plan.ChangedFrom), deltaUpdatedMax, decisionLimit, projection); err != nil {
		return nil, false, err
	}
	if len(changedRecords) > deltaIndex.ReplaceThreshold {
		return nil, false, nil
	}
	var movedIDs []int64
	if len(deltaIndex.Pinned) > 0 {
		if movedIDs, err = table.readDeltaMovedIDs(deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit); err != nil {
			return nil, false, err
		}
		if len(movedIDs) > deltaIndex.ReplaceThreshold {
			return nil, false, nil
		}
	}
	windowRows, err := table.readDeltaWindowRows(deltaIndex, partition, prefix, int64(plan.WindowFrom), int64(plan.WindowTo)-1)
	if err != nil {
		return nil, false, err
	}

	// Removed: moved and not in the index again. The held hash leaves out what the sync brought or
	// removed, as genix-ui does.
	isInView := map[int64]bool{}
	excludedIDs := map[int64]bool{}
	var serverHash uint32
	for _, windowRow := range windowRows {
		isInView[windowRow.recordID] = true
		serverHash += cache.DeltaRecordHash(windowRow.recordID, windowRow.version)
	}
	for _, recordPointer := range changedRecords {
		isInView[recordIDOf(recordPointer)] = true
		excludedIDs[recordIDOf(recordPointer)] = true
	}
	var removedIDs []int64
	for _, movedID := range movedIDs {
		if !isInView[movedID] && !excludedIDs[movedID] {
			removedIDs = append(removedIDs, movedID)
			excludedIDs[movedID] = true
		}
	}
	heldRows := make([]cache.DeltaHashRow, len(snapshotRecords))
	for index, recordPointer := range snapshotRecords {
		heldRows[index] = cache.DeltaHashRow{
			ID: recordIDOf(recordPointer), Updated: int32(updatedOf(recordPointer)),
			Version: int16(db.ConvertToInt64(table.VersionCol.GetRawValue(recordPointer))),
		}
	}
	if cache.DeltaHeldWindowHash(heldRows, plan.WindowFrom, plan.WindowTo, excludedIDs) != serverHash {
		fmt.Println("Delta snapshot: window hash mismatch on", table.Name, "prefix", prefix, ", reading the index")
		return nil, false, nil
	}

	// The snapshot, minus the removed, with the changed records in place of their old copies.
	recordByID := make(map[int64]unsafe.Pointer, len(snapshotRecords)+len(changedRecords))
	for _, recordPointer := range snapshotRecords {
		recordByID[recordIDOf(recordPointer)] = recordPointer
	}
	for _, removedID := range removedIDs {
		delete(recordByID, removedID)
	}
	for _, recordPointer := range changedRecords {
		recordByID[recordIDOf(recordPointer)] = recordPointer
	}
	// A snapshot cut at limit that lost records no longer holds the newest limit.
	if limit > 0 && len(snapshotRecords) >= int(limit) && len(recordByID) < int(limit) {
		return nil, false, nil
	}
	mergedRecords = make([]unsafe.Pointer, 0, len(recordByID))
	for _, recordPointer := range recordByID {
		mergedRecords = append(mergedRecords, recordPointer)
	}
	slices.SortFunc(mergedRecords, func(left, right unsafe.Pointer) int {
		return cmp.Or(cmp.Compare(updatedOf(right), updatedOf(left)), cmp.Compare(recordIDOf(right), recordIDOf(left)))
	})
	if limit > 0 && len(mergedRecords) > int(limit) {
		mergedRecords = mergedRecords[:limit]
	}
	return mergedRecords, true, nil
}
