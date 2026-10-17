package dynamo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/colbin"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Optimistic concurrency: PutManyIfVersion and Modify, read-modify-writes that
// never lose a concurrent write.
//
// A record is one opaque blob, so a write always replaces the whole record and
// two read-modify-writes of the same record lose one of them. Every base item
// carries the managed Version (delta.go) as the number attribute "ver", the one
// value DynamoDB compares, and every write moves it.
//
// PutManyIfVersion writes each record with a conditional PutItem: "ver" must
// still hold the Version the record was read with (no item at all, for 0).
// Records that lost to a concurrent write come back unwritten, for the caller to
// read again, re-apply its edit and retry. Modify is that loop for one record.
//
// Derived data: a record computed from other records is read first, then its
// inputs (with Consistent()), and every writer of those inputs recomputes the
// derived record after its own write. Then the last write to land is always
// computed from the newest inputs: a recompute that read stale inputs is either
// overwritten by the later one, or loses its conditional write and runs again.
// ─────────────────────────────────────────────────────────────────────────────

const modifyMaxAttempts = 8

// ErrWriteConflict is what Modify returns when other writes kept landing between
// its read and its write for modifyMaxAttempts attempts.
var ErrWriteConflict = errors.New("db: the record kept changing under Modify")

// ConflictBackoff is the pause before retry attempt n (1-based) of a lost
// conditional write: 10ms, 40ms, 90ms… about 2s over 8 attempts.
func ConflictBackoff(attempt int) time.Duration {
	return time.Duration(attempt*attempt) * 10 * time.Millisecond
}

// PutManyIfVersion writes each record only while its stored item still carries
// the Version the record holds (0: no item may exist yet). The records come from a
// consistent read (GetMany, Query().Consistent()) and were edited since. As in
// PutMany, Updated is stamped, the hidden rows are diffed against the stored
// versions, and the logs and by-IDs slots are written once. BatchWriteItem takes no
// condition, so the base items are conditional PutItems, run in parallel. It
// returns the records that lost to a concurrent write, unwritten: read them again,
// re-apply the edit and retry.
func (r *Repo[T, E]) PutManyIfVersion(records []E) ([]E, error) {
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
		expectedVersions[i] = r.meta.version.getI64(ptrs[i])
	}
	if _, err := r.meta.assignAutoIDs(ptrs); err != nil {
		return nil, err
	}
	r.meta.stampUpdated(ptrs)
	storedByKey, err := r.storedVersionsForWrite(client, ptrs, expectedVersions)
	if err != nil {
		return nil, err
	}
	lostIndexes, err := r.putIfVersion(client, records, storedByKey)
	if err != nil {
		return nil, err
	}
	lostRecords := make([]E, len(lostIndexes))
	for i, lostIndex := range lostIndexes {
		lostRecords[i] = records[lostIndex]
	}
	return lostRecords, nil
}

// putIfVersion writes each record over the Version it holds, which storedByKey (keyed by recordKey)
// holds as stored, and returns the indexes of the records that lost to a concurrent write: those keep
// the Version they held. The new hidden rows go first, as in PutMany; a record that then loses leaves
// them as extra rows, which every read re-checks and drops.
func (r *Repo[T, E]) putIfVersion(client *dynamodb.Client, records []E, storedByKey map[string]storedRecord) ([]int, error) {
	ptrs := make([]unsafe.Pointer, len(records))
	expectedVersions := make([]int64, len(records))
	items := make([]map[string]types.AttributeValue, len(records))
	rowDeletesByRecord := make([][]types.WriteRequest, len(records))
	// Per record, so only the records that win their conditional write move the counters.
	groupDeltasByRecord := make([]map[string]*groupCounterDelta, len(records))
	var rowPuts []types.WriteRequest
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
		expectedVersions[i] = r.meta.version.getI64(ptrs[i])
		r.meta.version.setI64(ptrs[i], nextVersion(expectedVersions[i]))
		var err error
		if items[i], err = r.meta.marshalItem(ptrs[i], &records[i]); err != nil {
			return nil, err
		}
		stored := storedByKey[r.meta.recordKey(ptrs[i])]
		storedPtr := stored.ptr
		if err := r.meta.putChangelog(items[i], stored, ptrs[i]); err != nil {
			return nil, err
		}
		groupDeltasByRecord[i] = map[string]*groupCounterDelta{}
		if err := r.meta.addGroupCounterDeltas(groupDeltasByRecord[i], storedPtr, ptrs[i]); err != nil {
			return nil, err
		}
		puts, deletes := r.meta.arrayIndexWrites(storedPtr, ptrs[i], items[i])
		rowPuts = append(rowPuts, puts...)
		rowDeletesByRecord[i] = deletes
	}
	ctx := context.Background()
	if err := r.batchWriteAll(ctx, client, rowPuts); err != nil {
		return nil, err
	}
	isWritten := make([]bool, len(records))
	err := utils.RunParallel(len(records), func(recordIndex int) (putErr error) {
		isWritten[recordIndex], putErr = r.putItemIfVersion(ctx, client, items[recordIndex], expectedVersions[recordIndex])
		return putErr
	})
	if err != nil {
		return nil, err
	}

	var lostIndexes []int
	var writtenPtrs []unsafe.Pointer
	var staleRowDeletes []types.WriteRequest
	var logWrites []deltaLogWrite
	groupDeltas := map[string]*groupCounterDelta{}
	for i := range records {
		if !isWritten[i] {
			lostIndexes = append(lostIndexes, i)
			r.meta.version.setI64(ptrs[i], expectedVersions[i])
			continue
		}
		writtenVersion := r.meta.version.getI64(ptrs[i])
		writtenPtrs = append(writtenPtrs, ptrs[i])
		staleRowDeletes = append(staleRowDeletes, rowDeletesByRecord[i]...)
		mergeGroupCounterDeltas(groupDeltas, groupDeltasByRecord[i])
		refreshStoredItem(items[i], writtenVersion)
		logWrites = append(logWrites, deltaLogWrite{storedPtr: storedByKey[r.meta.recordKey(ptrs[i])].ptr, writtenPtr: ptrs[i], version: writtenVersion})
	}
	if err := r.batchWriteAll(ctx, client, staleRowDeletes); err != nil {
		return nil, err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return nil, err
	}
	if err := r.meta.appendDeltaLogs(logWrites); err != nil {
		return nil, err
	}
	return lostIndexes, r.meta.setSlotsUpdated(writtenPtrs)
}

// storedVersionsForWrite is storedVersions for a version-checked write, keyed by
// recordKey. A record expected new (version 0) has no stored version to read, and
// one whose blob the write cache holds at its expected version is decoded from
// there: in both cases the write's condition proves the shortcut, since it only
// lands if the stored item is exactly that. Only the rest is read. Nil without
// hidden rows or GroupBy.
func (r *Repo[T, E]) storedVersionsForWrite(client *dynamodb.Client, ptrs []unsafe.Pointer, expectedVersions []int64) (map[string]storedRecord, error) {
	if !r.meta.readsStoredVersion() {
		return nil, nil
	}
	storedByKey := map[string]storedRecord{}
	var uncachedPtrs []unsafe.Pointer
	for i, ptr := range ptrs {
		if expectedVersions[i] == 0 {
			continue
		}
		recordKey := r.meta.recordKey(ptr)
		storedBlob, storedChangelog, isCached := cachedStoredBlob(recordKey, expectedVersions[i])
		if !isCached {
			uncachedPtrs = append(uncachedPtrs, ptr)
			continue
		}
		record := new(E)
		if err := colbin.Unmarshal(storedBlob, record); err != nil {
			return nil, fmt.Errorf("db: colbin unmarshaling %s: %w", r.meta.recordType.Name(), err)
		}
		r.meta.version.setI64(unsafe.Pointer(record), expectedVersions[i])
		storedByKey[recordKey] = storedRecord{ptr: unsafe.Pointer(record), changelog: storedChangelog}
	}
	if len(uncachedPtrs) == 0 {
		return storedByKey, nil
	}
	readByKey, err := r.storedVersions(client, uncachedPtrs)
	if err != nil {
		return nil, err
	}
	maps.Copy(storedByKey, readByKey)
	return storedByKey, nil
}

// putItemIfVersion is one conditional UpdateItem that SETs every attribute of item (crt only when
// missing), false when the stored item moved on. A stored item with no "ver" can never match: that is
// an error, not a race. Every attribute a record writes is in item, but cgl, which is REMOVEd when
// the record has none left.
func (r *Repo[T, E]) putItemIfVersion(ctx context.Context, client *dynamodb.Client, item map[string]types.AttributeValue, expectedVersion int64) (bool, error) {
	names := map[string]string{"#ver": versionColumn}
	values := map[string]types.AttributeValue{":ver": item[versionColumn]}
	updateExpression := "SET " + strings.Join(append(setClausesOf(item, names, values), "#ver = :ver"), ", ")
	if _, hasChangelog := item[changelogColumn]; r.meta.frames != nil && !hasChangelog {
		names["#cgl"] = changelogColumn
		updateExpression += " REMOVE #cgl"
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                           aws.String(tableName()),
		Key:                                 itemKeyOf(item),
		UpdateExpression:                    aws.String(updateExpression),
		ExpressionAttributeNames:            names,
		ExpressionAttributeValues:           values,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	}
	if expectedVersion == 0 {
		input.ConditionExpression = aws.String("attribute_not_exists(pk)")
	} else {
		input.ConditionExpression = aws.String("#ver = :expected")
		values[":expected"] = numberAttr(expectedVersion)
	}
	_, err := client.UpdateItem(ctx, input)
	var lostRaceErr *types.ConditionalCheckFailedException
	if errors.As(err, &lostRaceErr) {
		if _, hasVersion := lostRaceErr.Item[versionColumn]; len(lostRaceErr.Item) > 0 && !hasVersion {
			return false, fmt.Errorf("db: %s pk %s sk %q has no %q attribute: it was stored before Version, migrate it",
				r.meta.recordType.Name(), item["pk"].(*types.AttributeValueMemberN).Value, binaryKeyText(item["sk"]), versionColumn)
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
// the Keys; what it does to Version is ignored.
func (r *Repo[T, E]) Modify(key E, change func(record *E, exists bool) error) (*E, error) {
	recordName := r.meta.recordType.Name()
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
			storedVersion = r.meta.version.getI64(unsafe.Pointer(&record))
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
		r.meta.version.setI64(unsafe.Pointer(&record), storedVersion)
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
