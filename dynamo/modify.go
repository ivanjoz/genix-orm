package dynamo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/colbin"

	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// Optimistic concurrency: PutManyIfVersion and Modify, read-modify-writes that
// never lose a concurrent write.
//
// A record is one opaque blob, so a write always replaces the whole record and
// two read-modify-writes of the same record lose one of them. On a versioned
// table (VersionedWrites, SaveUpdatedVersion or a TypeDelta index) every write
// stamps the managed UpdatedVersion (delta.go), and the base item also carries it
// as the number attribute "upv", the one value DynamoDB can compare.
//
// PutManyIfVersion writes each record with a conditional PutItem: "upv" must
// still hold the UpdatedVersion the record was read with (no item at all, for 0).
// Records that lost to a concurrent write come back unwritten, for the caller to
// read again, re-apply its edit and retry. Modify is that loop for one record.
//
// Derived data: a record computed from other records is read first, then its
// inputs (with Consistent()), and every writer of those inputs recomputes the
// derived record after its own write. Then the last write to land is always
// computed from the newest inputs: a recompute that read stale inputs is either
// overwritten by the later one, or loses its conditional write and runs again.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// versionColumn is the item attribute holding UpdatedVersion on a versioned table.
	versionColumn     = "upv"
	modifyMaxAttempts = 8
)

// ErrWriteConflict is what Modify returns when other writes kept landing between
// its read and its write for modifyMaxAttempts attempts.
var ErrWriteConflict = errors.New("db: the record kept changing under Modify")

// ConflictBackoff is the pause before retry attempt n (1-based) of a lost
// conditional write: 10ms, 40ms, 90ms… about 2s over 8 attempts.
func ConflictBackoff(attempt int) time.Duration {
	return time.Duration(attempt*attempt) * 10 * time.Millisecond
}

// PutManyIfVersion writes each record only while its stored item still carries
// the UpdatedVersion the record holds (0: no item may exist yet). The records
// come from a consistent read (GetMany, Query().Consistent()) and were edited
// since. As in PutMany, one version is reserved per base pk for the whole call,
// the hidden rows are diffed against the stored versions, and the slot versions
// are bumped once. BatchWriteItem takes no condition, so the base items are
// conditional PutItems, run in parallel. It returns the records that lost to a
// concurrent write, unwritten: read them again, re-apply the edit and retry.
func (r *Repo[T, E]) PutManyIfVersion(records []E) ([]E, error) {
	recordName := r.meta.recordType.Name()
	if r.meta.writeVersion == nil {
		return nil, fmt.Errorf("db: %s PutManyIfVersion needs a versioned table: set VersionedWrites", recordName)
	}
	if len(records) == 0 {
		return nil, nil
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}
	ptrs := make([]unsafe.Pointer, len(records))
	expectedVersions := make([]int64, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
		expectedVersions[i] = r.meta.writeVersion.acc.getI64(ptrs[i])
	}
	if err := r.meta.checkFrameValues(ptrs); err != nil {
		return nil, err
	}
	// The caller read the records before this reservation, so the version is above the one each
	// write replaces, as a frame's log entry needs: no need to read before reserving here.
	windowStart := Now()
	if _, err := r.meta.prepareWrite(ptrs); err != nil {
		return nil, err
	}
	storedByKey, oldestCachedAt, err := r.storedVersionsForWrite(client, ptrs, expectedVersions)
	if err != nil {
		return nil, err
	}
	// The write deadline counts from the oldest stored read the write diffs against: a cached one, or
	// its own, which comes after windowStart.
	if !oldestCachedAt.IsZero() && oldestCachedAt.Before(windowStart) {
		windowStart = oldestCachedAt
	}
	window := r.meta.frameWriteWindowFrom(windowStart)
	r.meta.stampCreatedVersions(ptrs, storedByKey)

	items := make([]map[string]types.AttributeValue, len(records))
	rowDeletesByRecord := make([][]types.WriteRequest, len(records))
	// Per record, so only the records that win their conditional write move the counters.
	groupDeltasByRecord := make([]map[string]*groupCounterDelta, len(records))
	var rowPuts []types.WriteRequest
	for i := range records {
		if items[i], err = r.meta.marshalItem(ptrs[i], &records[i]); err != nil {
			return nil, err
		}
		storedPtr := storedByKey[r.meta.recordKey(ptrs[i])]
		groupDeltasByRecord[i] = map[string]*groupCounterDelta{}
		if err := r.meta.addGroupCounterDeltas(groupDeltasByRecord[i], storedPtr, ptrs[i]); err != nil {
			return nil, err
		}
		if len(r.meta.arrayIndexes) == 0 {
			continue
		}
		blob := items[i][dataColumn].(*types.AttributeValueMemberB).Value
		puts, deletes := r.meta.arrayIndexWrites(storedPtr, ptrs[i], blob)
		rowPuts = append(rowPuts, puts...)
		rowDeletesByRecord[i] = deletes
	}
	// The log entries, the new hidden rows and the base items land within the window. New rows first,
	// as in PutMany: a record that then loses its conditional write deletes its delta rows below, and
	// leaves its fan-out rows as extra rows, which every read re-checks and drops.
	ctx, cancel := window.landingContext()
	defer cancel()
	isSent := make([]bool, len(records))
	isWritten := make([]bool, len(records))
	isUncertain := make([]bool, len(records))
	err = r.meta.appendFrameLogEntries(ctx, r.meta.frameWritesOf(ptrs, storedByKey), false)
	if err == nil {
		_, err = r.batchWriteAll(ctx, client, rowPuts)
	}
	if err == nil {
		err = parallel.Run(len(records), func(recordIndex int) (putErr error) {
			if putErr = ctx.Err(); putErr != nil {
				return putErr
			}
			isSent[recordIndex] = true
			isWritten[recordIndex], putErr = r.putItemIfVersion(ctx, client, items[recordIndex], expectedVersions[recordIndex])
			isUncertain[recordIndex] = putErr != nil
			return putErr
		})
	}

	var lostRecords []E
	var writtenPtrs, cancelledPtrs []unsafe.Pointer
	var staleRowDeletes []types.WriteRequest
	groupDeltas := map[string]*groupCounterDelta{}
	for i := range records {
		switch {
		case isWritten[i]:
			writtenPtrs = append(writtenPtrs, ptrs[i])
			staleRowDeletes = append(staleRowDeletes, rowDeletesByRecord[i]...)
			mergeGroupCounterDeltas(groupDeltas, groupDeltasByRecord[i])
			refreshStoredItem(items[i], r.meta.writeVersion.acc.getI64(ptrs[i]))
		case !isSent[i]:
			cancelledPtrs = append(cancelledPtrs, ptrs[i])
		case !isUncertain[i]:
			lostRecords = append(lostRecords, records[i])
			cancelledPtrs = append(cancelledPtrs, ptrs[i])
			staleRowDeletes = append(staleRowDeletes, r.meta.deltaRowDeletes(ptrs[i])...)
		}
	}
	// A record that lost its condition or was never sent did not land, and its version may be above
	// a write that did: its log entry, left in the log, would read as the record's values after that
	// write. Its cancel marker voids it. A record whose put failed may have landed: it keeps its entry.
	cancelErr := r.meta.cancelFrameWrites(window, r.meta.frameWritesOf(cancelledPtrs, storedByKey))
	if err != nil || cancelErr != nil {
		return nil, errors.Join(frameLandingError(err), cancelErr)
	}
	if _, err := r.batchWriteAll(context.Background(), client, staleRowDeletes); err != nil {
		return nil, err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return nil, err
	}
	return lostRecords, r.meta.bumpSlotVersions(client, writtenPtrs)
}

// deltaRowDeletes deletes the delta rows a record that lost its conditional write
// had put. Their sk holds the write version this call reserved, which no other
// write ever holds, so no live record can need them. Fan-out rows carry no version
// and may be the winner's too: those stay, as extra rows reads re-check.
func (m *tableMeta) deltaRowDeletes(lostPtr unsafe.Pointer) []types.WriteRequest {
	var deletes []types.WriteRequest
	basePK := m.pkValue(lostPtr)
	for i := range m.arrayIndexes {
		deltaIndex := &m.arrayIndexes[i]
		if !deltaIndex.isDelta {
			continue
		}
		for _, rowSK := range m.arrayRowSKs(deltaIndex, lostPtr) {
			deletes = append(deletes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: itemKey(basePK+deltaIndex.columnID, rowSK)}})
		}
	}
	return deletes
}

// storedVersionsForWrite is storedVersions for a version-checked write, keyed by
// recordKey. A record expected new (version 0) has no stored version to read, and
// one whose blob the write cache holds at its expected version is decoded from
// there: in both cases the write's condition proves the shortcut, since it only
// lands if the stored item is exactly that. Only the rest is read. oldestCachedAt
// is when the oldest cached blob used was read (zero: none). Nil without hidden
// rows or GroupBy.
func (r *Repo[T, E]) storedVersionsForWrite(client *dynamodb.Client, ptrs []unsafe.Pointer, expectedVersions []int64) (
	storedByKey map[string]unsafe.Pointer, oldestCachedAt time.Time, err error) {
	if !r.meta.readsStoredVersion() {
		return nil, oldestCachedAt, nil
	}
	storedByKey = map[string]unsafe.Pointer{}
	var uncachedPtrs []unsafe.Pointer
	for i, ptr := range ptrs {
		if expectedVersions[i] == 0 {
			continue
		}
		recordKey := r.meta.recordKey(ptr)
		storedBlob, cachedAt, isCached := cachedStoredBlob(recordKey, expectedVersions[i])
		if !isCached {
			uncachedPtrs = append(uncachedPtrs, ptr)
			continue
		}
		if oldestCachedAt.IsZero() || cachedAt.Before(oldestCachedAt) {
			oldestCachedAt = cachedAt
		}
		storedRecord := new(E)
		if err := colbin.Unmarshal(storedBlob, storedRecord); err != nil {
			return nil, oldestCachedAt, fmt.Errorf("db: colbin unmarshaling %s: %w", r.meta.recordType.Name(), err)
		}
		storedByKey[recordKey] = unsafe.Pointer(storedRecord)
	}
	if len(uncachedPtrs) == 0 {
		return storedByKey, oldestCachedAt, nil
	}
	readByKey, err := r.storedVersions(client, uncachedPtrs)
	if err != nil {
		return nil, oldestCachedAt, err
	}
	for recordKey, storedPtr := range readByKey {
		storedByKey[recordKey] = storedPtr
	}
	return storedByKey, oldestCachedAt, nil
}

// putItemIfVersion is one conditional PutItem, false when the stored item moved
// on. A stored item with no "upv" can never match: that is an error, not a race.
func (r *Repo[T, E]) putItemIfVersion(ctx context.Context, client *dynamodb.Client, item map[string]types.AttributeValue, expectedVersion int64) (bool, error) {
	input := &dynamodb.PutItemInput{
		TableName:                           aws.String(tableName()),
		Item:                                item,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	if expectedVersion == 0 {
		input.ConditionExpression = aws.String("attribute_not_exists(pk)")
	} else {
		input.ConditionExpression = aws.String("#upv = :upv")
		input.ExpressionAttributeNames = map[string]string{"#upv": versionColumn}
		input.ExpressionAttributeValues = map[string]types.AttributeValue{":upv": &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion, 10)}}
	}
	_, err := client.PutItem(ctx, input)
	var lostRaceErr *types.ConditionalCheckFailedException
	if errors.As(err, &lostRaceErr) {
		if _, hasVersion := lostRaceErr.Item[versionColumn]; len(lostRaceErr.Item) > 0 && !hasVersion {
			return false, fmt.Errorf("db: %s pk %s sk %s was stored before its table was versioned and has no %q attribute: Put it once",
				r.meta.recordType.Name(), item["pk"].(*types.AttributeValueMemberN).Value, item["sk"].(*types.AttributeValueMemberS).Value, versionColumn)
		}
		return false, nil
	}
	return err == nil, err
}

// Modify reads the record with key's Keys (consistently), lets change edit it and
// writes it back with PutManyIfVersion, reading and re-running the change when
// another write landed in between (up to modifyMaxAttempts, then
// ErrWriteConflict), so change must be safe to run more than once. exists is
// false when nothing is stored: change then gets key itself and may create the
// record (left as the bare key, nothing is written). A change that leaves the
// record byte-identical writes nothing and moves no version. It returns the
// record as stored when Modify ends (nil when nothing is). change must not edit
// the Keys; what it does to UpdatedVersion is ignored.
func (r *Repo[T, E]) Modify(key E, change func(record *E, exists bool) error) (*E, error) {
	recordName := r.meta.recordType.Name()
	if r.meta.writeVersion == nil {
		return nil, fmt.Errorf("db: %s Modify needs a versioned table: set VersionedWrites", recordName)
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}
	targetKey := r.meta.recordKey(unsafe.Pointer(&key))

	for attempt := 1; attempt <= modifyMaxAttempts; attempt++ {
		out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
			TableName:      aws.String(tableName()),
			Key:            r.meta.keyOnly(unsafe.Pointer(&key)),
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			return nil, err
		}
		record, exists := key, len(out.Item) > 0
		// The write below diffs its hidden rows against this read, not a second one.
		if exists && r.meta.readsStoredVersion() {
			rememberStoredItems([]map[string]types.AttributeValue{out.Item})
		}
		storedVersion := int64(0)
		unchangedBlob, err := colbin.Marshal(&key)
		if err != nil {
			return nil, fmt.Errorf("db: colbin marshaling %s: %w", recordName, err)
		}
		if exists {
			if err := r.meta.unmarshalItem(out.Item, &record); err != nil {
				return nil, err
			}
			storedVersion = r.meta.writeVersion.acc.getI64(unsafe.Pointer(&record))
			unchangedBlob = out.Item[dataColumn].(*types.AttributeValueMemberB).Value
		}

		if err := change(&record, exists); err != nil {
			return nil, err
		}
		if r.meta.recordKey(unsafe.Pointer(&record)) != targetKey {
			return nil, fmt.Errorf("db: %s Modify of %s changed the record's Keys", recordName, targetKey)
		}
		changedBlob, err := colbin.Marshal(&record)
		if err != nil {
			return nil, fmt.Errorf("db: colbin marshaling %s: %w", recordName, err)
		}
		if bytes.Equal(changedBlob, unchangedBlob) {
			if !exists {
				return nil, nil
			}
			return &record, nil
		}

		// The write is conditioned on the version read, whatever change did to the field.
		r.meta.writeVersion.acc.setI64(unsafe.Pointer(&record), storedVersion)
		modifiedRecords := []E{record}
		lostRecords, err := r.PutManyIfVersion(modifiedRecords)
		if err != nil {
			return nil, err
		}
		if len(lostRecords) == 0 {
			return &modifiedRecords[0], nil
		}
		time.Sleep(ConflictBackoff(attempt))
	}
	return nil, fmt.Errorf("%w: %s %s, %d attempts", ErrWriteConflict, recordName, targetKey, modifyMaxAttempts)
}
