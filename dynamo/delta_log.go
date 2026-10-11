package dynamo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta logs: the change-log files of the TypeDelta indexes (delta.go), in the
// FileStore, and the Delta() read over them.
//
//	key = delta-log/<base pk>/<index tag>/<pinned value>/…/<bucket>
//	bucket = floor(SUnixTime / 300): the 10 minutes the append landed in
//
// Write: once the records landed, every write appends one entry per index to the
// file of each pinned folder the record is in (delta_log_codec.go), and a deleted
// entry to each folder it left: a hard delete, or a pinned value that changed. The
// appends run in parallel, under appendDeltaLogTimeout, and an append that fails
// fails the write: the caller retries it.
//
// Read: a client's cursor holds, per file still open, how many of its bytes it
// applied. Append order is landing order, so reading every file past those offsets
// misses nothing, whatever clock stamped the writes. A bucket is sealed once its
// last append can no longer land: deltaBucketSealDelay after its end (the append
// timeout plus clock skew). Then the client drops it from its cursor.
//
//   - First sync (no cursor), or a replace: read the current bucket's file (and the
//     previous one while it isn't sealed), then list the view. The files are read
//     first, so a write landing in between comes again on the next sync, where
//     the client skips it by version.
//   - Later sync: read the held files, the current one, and, when the cursor is
//     more than one bucket behind, the files a listing finds in between. Per log ID
//     the last entry past the cursor wins; the IDs not deleted are read with one
//     consistent BatchGetItem. A cursor older than deltaLogReplaceAfterBuckets, or
//     more than deltaLogMaxFilesRead files behind, gets a replace: a first sync whose
//     Metadata records tell the client to drop what the response doesn't bring.
//
// The response ends with one Metadata record per file that moved for the client:
// its log ID's last column is −bucket, and Metadata is the Safe85 of the file's
// whole content behind a header (deltaMetadataBytes): flags, bucket, log ID widths.
//
// Retention: a lifecycle rule on the bucket deletes the files after 8 days, the 7
// days a cursor is answered from the log plus a margin.
// ─────────────────────────────────────────────────────────────────────────────

const (
	deltaLogFolder = "delta-log/"
	// deltaBucketTicks is a bucket's length in SUnixTime ticks: 10 minutes.
	deltaBucketTicks = 300
	// appendDeltaLogTimeout bounds the appends of one write.
	appendDeltaLogTimeout = 20 * time.Second
	// deltaBucketSealDelay is how long after its end a bucket may still take an append.
	deltaBucketSealDelay = 30 * time.Second
	// deltaLogReplaceAfterBuckets: a cursor older than 7 days is answered with the whole view.
	deltaLogReplaceAfterBuckets = 7 * 144
	// deltaLogMaxFilesRead: past one day of files, listing the view is cheaper than reading them.
	deltaLogMaxFilesRead = 144
)

// deltaBucketOf is the bucket a moment falls in.
func deltaBucketOf(moment time.Time) int64 { return sUnixTimeOf(moment) / deltaBucketTicks }

// isDeltaBucketSealed reports whether bucket can no longer take an append at now.
func isDeltaBucketSealed(bucket int64, now time.Time) bool {
	bucketEnd := sUnixTimeToTime((bucket + 1) * deltaBucketTicks)
	return !now.Before(bucketEnd.Add(deltaBucketSealDelay))
}

// deltaLogWrite is one landed write: the record as stored before it (nil when new, or not read: a
// table without hidden rows can't move between folders), as written (nil for a delete), and the
// Version the write gave it.
type deltaLogWrite struct {
	storedPtr, writtenPtr unsafe.Pointer
	version               int64
}

// appendDeltaLogs appends the entries of the landed writes to their delta logs.
func (m *tableMeta) appendDeltaLogs(writes []deltaLogWrite) error {
	if len(m.deltaIndexes) == 0 || len(writes) == 0 {
		return nil
	}
	if fileStore == nil {
		return fmt.Errorf("db: %s has a TypeDelta index and no file store holds its log: call SetFileStore at boot", m.recordType.Name())
	}
	bucketName := strconv.FormatInt(deltaBucketOf(Now()), 10)
	// Per file, one entry per log ID: the last write of a record in this call wins.
	type fileAppend struct {
		is64BitScale bool
		logIDs       []uint64
		entryByLogID map[uint64]DeltaLogEntry
	}
	appendsByFile := map[string]*fileAppend{}
	addEntry := func(fileKey string, deltaIndex *deltaIndexMeta, entry DeltaLogEntry) {
		pending := appendsByFile[fileKey]
		if pending == nil {
			pending = &fileAppend{is64BitScale: deltaIndex.is64BitScale, entryByLogID: map[uint64]DeltaLogEntry{}}
			appendsByFile[fileKey] = pending
		}
		if _, isListed := pending.entryByLogID[entry.LogID]; !isListed {
			pending.logIDs = append(pending.logIDs, entry.LogID)
		}
		pending.entryByLogID[entry.LogID] = entry
	}
	for _, write := range writes {
		keyPtr := write.writtenPtr
		if keyPtr == nil {
			keyPtr = write.storedPtr
		}
		basePK := m.pkValue(keyPtr)
		// Status 0 leaves every view: physical deletes are not part of the app's flow, so the deleted
		// entry is the soft delete's. Whether a screen keeps deactivated records is its own decision:
		// such a route lists its view instead of reading the log.
		isSoftDeleted := write.writtenPtr != nil && m.status != nil && m.status.getI64(write.writtenPtr) == 0
		for i := range m.deltaIndexes {
			deltaIndex := &m.deltaIndexes[i]
			indexFolder := deltaLogFolder + basePK + "/" + deltaIndex.tag + "/"
			logID, version := deltaIndex.logIDOf(keyPtr), wireVersion(int16(write.version))
			writtenFolders := m.pinnedFolders(deltaIndex, write.writtenPtr)
			leftFolders := m.pinnedFolders(deltaIndex, write.storedPtr)
			if isSoftDeleted {
				leftFolders, writtenFolders = append(leftFolders, writtenFolders...), nil
			}
			for _, folder := range writtenFolders {
				addEntry(indexFolder+folder+bucketName, deltaIndex, DeltaLogEntry{LogID: logID, Version: version})
			}
			for _, folder := range leftFolders {
				if !slices.Contains(writtenFolders, folder) {
					addEntry(indexFolder+folder+bucketName, deltaIndex, DeltaLogEntry{LogID: logID, Version: version, IsDeleted: true})
				}
			}
		}
	}

	fileKeys := make([]string, 0, len(appendsByFile))
	for fileKey := range appendsByFile {
		fileKeys = append(fileKeys, fileKey)
	}
	ctx, cancel := context.WithTimeout(context.Background(), appendDeltaLogTimeout)
	defer cancel()
	return parallel.Run(len(fileKeys), func(fileIndex int) error {
		pending := appendsByFile[fileKeys[fileIndex]]
		entries := make([]DeltaLogEntry, len(pending.logIDs))
		for i, logID := range pending.logIDs {
			entries[i] = pending.entryByLogID[logID]
		}
		if err := fileStore.AppendBytesToFile(ctx, fileKeys[fileIndex], encodeDeltaLogEntries(entries, pending.is64BitScale)); err != nil {
			return fmt.Errorf("db: %s appending to the delta log %s: %w", m.recordType.Name(), fileKeys[fileIndex], err)
		}
		return nil
	})
}

// execDelta is Exec for a Delta() read (see the top of this file).
func (q *QueryBuilder[E]) execDelta(client *dynamodb.Client, dst *[]E) error {
	if q.planErr != nil {
		return q.planErr
	}
	if fileStore == nil {
		return fmt.Errorf("db: %s Delta() reads a delta log and no file store holds it: call SetFileStore at boot", q.meta.recordType.Name())
	}
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}
	partitionValues, isPartitionPinned := pinPartition(byField, q.meta.partition, map[string]bool{})
	if !isPartitionPinned {
		return fmt.Errorf("db: %s Delta() needs an equality on every Partition column: each partition has its own log", q.meta.recordType.Name())
	}
	folder := deltaLogFolder + q.meta.numericKey(partitionValues, q.meta.partition) + "/" + q.deltaIndex.tag + "/"
	for _, column := range q.deltaIndex.pinned {
		pinnedValue := byField[column.fieldName].v1
		if byField[column.fieldName].op == opContains {
			pinnedValue = pinnedValue.([]any)[0]
		}
		folder += pinnedValueText(pinnedValue) + "/"
	}

	now := Now()
	currentBuckets := []int64{deltaBucketOf(now)}
	if !isDeltaBucketSealed(currentBuckets[0]-1, now) {
		currentBuckets = append(currentBuckets, currentBuckets[0]-1)
	}
	buckets, isReplace := currentBuckets, len(q.deltaCursor) == 0
	if !isReplace {
		oldestHeldBucket := currentBuckets[0]
		for heldBucket := range q.deltaCursor {
			buckets = append(buckets, heldBucket)
			oldestHeldBucket = min(oldestHeldBucket, heldBucket)
		}
		isReplace = oldestHeldBucket < currentBuckets[0]-deltaLogReplaceAfterBuckets
		if !isReplace && currentBuckets[0]-oldestHeldBucket > 1 {
			listedBuckets, err := listDeltaLogBuckets(folder, oldestHeldBucket)
			if err != nil {
				return err
			}
			buckets = append(buckets, listedBuckets...)
		}
		slices.Sort(buckets)
		buckets = slices.Compact(buckets)
		if isReplace || len(buckets) > deltaLogMaxFilesRead {
			buckets, isReplace = currentBuckets, true
		}
	}
	slices.Sort(buckets)

	// The files go first: a write landing after them comes again on the next sync.
	contents, err := readDeltaLogFiles(folder, buckets)
	if err != nil {
		return err
	}
	if isReplace {
		err = q.execPlans(client, dst)
	} else {
		err = q.appendLoggedRecords(client, partitionValues, byField, buckets, contents, dst)
	}
	if err != nil {
		return err
	}

	flags := byte(0)
	if q.deltaIndex.is64BitScale {
		flags |= deltaMetadataFlag64BitScale
	}
	if isReplace {
		flags |= deltaMetadataFlagReplace
	}
	for i, bucket := range buckets {
		heldBytes, isHeld := q.deltaCursor[bucket]
		isSealed := isDeltaBucketSealed(bucket, now)
		if !isReplace && isHeld && heldBytes == len(contents[i]) && !isSealed {
			continue // nothing new, and the client keeps it
		}
		bucketFlags := flags
		if isSealed {
			bucketFlags |= deltaMetadataFlagSealed
		}
		*dst = append(*dst, q.deltaMetadataRecord(bucket, bucketFlags, contents[i]))
	}
	return nil
}

// appendLoggedRecords reads the records the log entries past the cursor name, the last entry per log
// ID winning, and appends those not deleted to dst.
func (q *QueryBuilder[E]) appendLoggedRecords(client *dynamodb.Client, partitionValues []uint64, byField map[string]predicate, buckets []int64, contents [][]byte, dst *[]E) error {
	var logIDs []uint64
	lastEntryByLogID := map[uint64]DeltaLogEntry{}
	for i, bucket := range buckets {
		unreadContent := contents[i]
		// A file shorter than what the client holds is not the one it read: read it whole.
		if heldBytes := q.deltaCursor[bucket]; heldBytes <= len(unreadContent) {
			unreadContent = unreadContent[heldBytes:]
		}
		entries, err := decodeDeltaLogEntries(unreadContent, q.deltaIndex.is64BitScale)
		if err != nil {
			return fmt.Errorf("db: %s delta log bucket %d: %w", q.meta.recordType.Name(), bucket, err)
		}
		for _, entry := range entries {
			if _, isListed := lastEntryByLogID[entry.LogID]; !isListed {
				logIDs = append(logIDs, entry.LogID)
			}
			lastEntryByLogID[entry.LogID] = entry
		}
	}

	// Every key shares the partition and the pinned Keys columns; the log ID fills in the rest.
	var keyTemplate E
	keyTemplateValue := reflect.ValueOf(&keyTemplate).Elem()
	for i, column := range q.meta.partition {
		setKeyField(keyTemplateValue, column.fieldName, partitionValues[i])
	}
	for _, column := range q.deltaIndex.pinned {
		if pinnedPredicate, isKeysColumn := byField[column.fieldName]; isKeysColumn && pinnedPredicate.op == opEq && q.meta.isKeysColumn(column.fieldName) {
			setKeyField(keyTemplateValue, column.fieldName, pinnedPredicate.v1)
		}
	}
	var itemKeys []map[string]types.AttributeValue
	for _, logID := range logIDs {
		if lastEntryByLogID[logID].IsDeleted {
			continue
		}
		recordKey := keyTemplate
		recordKeyValue := reflect.ValueOf(&recordKey).Elem()
		logIDValues := []uint64{logID}
		if len(q.deltaIndex.logIDColumns) > 1 {
			logIDValues = unpackDeltaLogID(logID, q.deltaIndex.logIDColumnBits)
		}
		for j, column := range q.deltaIndex.logIDColumns {
			setKeyField(recordKeyValue, column.fieldName, logIDValues[j])
		}
		itemKeys = append(itemKeys, q.meta.keyOnly(unsafe.Pointer(&recordKey)))
	}
	items, _, err := batchGet(client, itemKeys, true)
	if err != nil {
		return err
	}
	for _, item := range items {
		var record E
		if err := q.meta.unmarshalItem(item, &record); err != nil {
			return err
		}
		*dst = append(*dst, record)
	}
	return nil
}

// deltaMetadataRecord is the Metadata record of one log file: −bucket in its log ID's last column.
func (q *QueryBuilder[E]) deltaMetadataRecord(bucket int64, flags byte, content []byte) E {
	var record E
	recordValue := reflect.ValueOf(&record).Elem()
	setKeyField(recordValue, q.deltaIndex.logIDColumns[len(q.deltaIndex.logIDColumns)-1].fieldName, -bucket)
	metadataText := EncodeSafe85(deltaMetadataBytes(flags, bucket, q.deltaIndex.logIDColumnBits, content))
	recordValue.FieldByName(metadataFieldName).Set(reflect.ValueOf(&metadataText))
	return record
}

// setKeyField sets a key field of a record from any integer or string value.
func setKeyField(record reflect.Value, fieldName string, value any) {
	field := record.FieldByName(fieldName)
	field.Set(reflect.ValueOf(value).Convert(field.Type()))
}

// isKeysColumn reports whether fieldName is one of the table's Keys.
func (m *tableMeta) isKeysColumn(fieldName string) bool {
	return slices.ContainsFunc(m.keys, func(column keyCol) bool { return column.fieldName == fieldName })
}

// listDeltaLogBuckets lists the buckets of the files in folder from fromBucket on.
func listDeltaLogBuckets(folder string, fromBucket int64) ([]int64, error) {
	fileKeys, err := fileStore.ListFiles(folder)
	if err != nil {
		return nil, fmt.Errorf("db: listing the delta log %s: %w", folder, err)
	}
	var buckets []int64
	for _, fileKey := range fileKeys {
		bucket, err := strconv.ParseInt(strings.TrimPrefix(fileKey, folder), 10, 64)
		if err == nil && bucket >= fromBucket {
			buckets = append(buckets, bucket)
		}
	}
	return buckets, nil
}

// readDeltaLogFiles reads the files of buckets, in order; a missing one is empty.
func readDeltaLogFiles(folder string, buckets []int64) ([][]byte, error) {
	contents := make([][]byte, len(buckets))
	err := parallel.Run(len(buckets), func(bucketIndex int) error {
		fileKey := folder + strconv.FormatInt(buckets[bucketIndex], 10)
		content, _, err := fileStore.GetFile(fileKey)
		if errors.Is(err, ErrFileNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("db: reading the delta log %s: %w", fileKey, err)
		}
		contents[bucketIndex] = content
		return nil
	})
	return contents, err
}

// DeltaReplaceMetadata is the Metadata of a route not backed by a delta log that answers its whole
// view every time: one record carrying it (any negative ID) tells the client to drop every held
// record the response doesn't bring. It is sealed, so the client keeps no cursor and sends none.
func DeltaReplaceMetadata() string {
	return EncodeSafe85(deltaMetadataBytes(deltaMetadataFlagReplace|deltaMetadataFlagSealed, 0, nil, nil))
}
