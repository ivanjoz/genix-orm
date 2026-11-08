package scylla

import (
	"cmp"
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// Delta snapshots (db/cache delta_snapshot.go): each prefix of a replace is answered from its
// snapshot file when the window protocol, run against the file, says it is still good; otherwise
// from the view, and that full read writes the file. A merged snapshot is served, never stored.

// readDeltaPrefixReplace fills read.records with every record of one prefix, newest first up to
// limit: from the prefix's snapshot when it is still good, else from the view, which then rewrites
// the snapshot. A projected read (projection != nil) has its own file, and merges the changes read
// with the same projection, so it never serves a column it excluded.
// readTime is the server clock taken before any read of this sync: the V a written snapshot keeps.
func readDeltaPrefixReplace(
	sink recordSink, scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, prefix []int64,
	limit int32, projection []string, readTime int32, read *deltaPrefixRead,
) error {
	readFromView := func() error {
		return readDeltaViewRecords(sink.collectInto(&read.records), scyllaTable, deltaIndex, partition, prefix,
			0, deltaViewUpdatedMax, int(limit), projection)
	}
	fileStore := cache.FileStore()
	if fileStore == nil || !cache.RecordSnapshotsEnabled() {
		return readFromView()
	}
	recordType := sinkRecordType(sink)
	codec := recordSnapshotCodecFor(scyllaTable, recordType)
	if !codec.isEligible {
		return readFromView()
	}

	snapshotKey := cache.DeltaSnapshotKey(scyllaTable.Name, partition, deltaIndex.Tag, prefix, limit, projection)
	content, etag, err := fileStore.GetFile(snapshotKey)
	switch {
	case errors.Is(err, utils.ErrFileNotFound):
		etag = ""
	case err != nil:
		// The store didn't answer: the view does, and nothing is written over a file we could not see.
		fmt.Println("Delta snapshot: reading", snapshotKey, "failed, reading the view:", err)
		return readFromView()
	default:
		mergedRecords, isServed, err := serveDeltaSnapshot(sink, scyllaTable, deltaIndex, partition, prefix, limit, projection, codec, recordType, readTime, content, snapshotKey)
		if err != nil {
			return err
		}
		if isServed {
			read.records = mergedRecords
			return nil
		}
	}

	if err := readFromView(); err != nil {
		return err
	}
	writeDeltaSnapshot(fileStore, snapshotKey, etag, codec, scyllaTable, recordType, readTime, read.records)
	return nil
}

// serveDeltaSnapshot answers a prefix from its snapshot file: the snapshot's records with the
// changes since its cursor applied, the window protocol checking it as a client's records are
// checked. isServed is false when the file can't answer (another schema, older than
// cache.RecordSnapshotMaxAgeTicks at readTime, more than the threshold of changes or moves, a hash
// mismatch, a cut at limit that a removal opened): the caller reads the view instead.
func serveDeltaSnapshot(
	sink recordSink, scyllaTable ScyllaTable, deltaIndex db.DeltaIndexInfo, partition int64, prefix []int64,
	limit int32, projection []string, codec recordSnapshotCodec, recordType reflect.Type, readTime int32, content []byte, snapshotKey string,
) (mergedRecords []unsafe.Pointer, isServed bool, err error) {
	snapshot, snapshotRecords, isUsable := decodeRecordSnapshotFile(content, codec, recordType, readTime, snapshotKey)
	if !isUsable {
		return nil, false, nil
	}
	recordIDOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
	}
	updatedOf := func(recordPointer unsafe.Pointer) int64 {
		return db.ConvertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer))
	}

	// The sync a client holding the snapshot would run: changed records, moves, the unsettled slice's rows.
	plan := cache.PlanDeltaSync(cache.DeltaWindowCursor{Updated: snapshot.Cursor, ReadTime: snapshot.ReadTime})
	decisionLimit := deltaIndex.ReplaceThreshold + 1
	// Read with the snapshot's projection: a changed record replaces its old copy whole, so a wider
	// read would put columns the caller excluded into the response.
	var changedRecords []unsafe.Pointer
	if err := readDeltaViewRecords(sink.collectInto(&changedRecords), scyllaTable, deltaIndex, partition, prefix,
		int64(plan.ChangedFrom), deltaViewUpdatedMax, decisionLimit, projection); err != nil {
		return nil, false, err
	}
	if len(changedRecords) > deltaIndex.ReplaceThreshold {
		return nil, false, nil
	}
	var movedIDs []int64
	if len(deltaIndex.Pinned) > 0 {
		if movedIDs, err = readDeltaMovedIDs(scyllaTable, deltaIndex, partition, prefix, int64(plan.MovesFrom), decisionLimit); err != nil {
			return nil, false, err
		}
		if len(movedIDs) > deltaIndex.ReplaceThreshold {
			return nil, false, nil
		}
	}
	windowRows, err := readDeltaViewRows(scyllaTable, deltaIndex, partition, prefix, int64(plan.WindowFrom), int64(plan.WindowTo)-1)
	if err != nil {
		return nil, false, err
	}

	// Removed: moved and not in the view again. The held hash leaves out what the sync brought or
	// removed, as genix-ui does.
	isInView := map[int64]bool{}
	excludedIDs := map[int64]bool{}
	var serverHash uint32
	for _, viewRow := range windowRows {
		isInView[viewRow.recordID] = true
		serverHash += cache.DeltaRecordHash(viewRow.recordID, viewRow.version)
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
			ID:      recordIDOf(recordPointer),
			Updated: int32(updatedOf(recordPointer)),
			Version: int16(db.ConvertToInt64(scyllaTable.VersionCol.GetRawValue(recordPointer))),
		}
	}
	if cache.DeltaHeldWindowHash(heldRows, plan.WindowFrom, plan.WindowTo, excludedIDs) != serverHash {
		fmt.Println("Delta snapshot: window hash mismatch on", scyllaTable.Name, "prefix", prefix, ", reading the view")
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
	// A snapshot cut at limit that lost records no longer holds the newest limit: the older records
	// the view would bring to fill the cut are not in the file.
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

// writeDeltaSnapshot stores the records a full view read returned as the prefix's snapshot. etag is
// the file the read replaced ("" = none): when another request wrote it first, this write is
// skipped, its snapshot being as good. An empty prefix is not stored: it has no cursor to sync from,
// and reading it again costs nothing. A failed write only logs: the response already has its records.
func writeDeltaSnapshot(
	fileStore utils.FileStore, snapshotKey, etag string, codec recordSnapshotCodec,
	scyllaTable ScyllaTable, recordType reflect.Type, readTime int32, records []unsafe.Pointer,
) {
	if len(records) == 0 {
		return
	}
	cursor := int64(0)
	for _, recordPointer := range records {
		cursor = max(cursor, db.ConvertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer)))
	}
	writeRecordSnapshotFile(fileStore, snapshotKey, etag, codec, recordType, int32(cursor), readTime, records)
}
