package dynamo

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta sync: the managed "Updated" field, the TypeDelta index and its Delta() read.
//
// Updated, stamped on every Put/PutMany/PutIfAbsent before the write, is the
// write time in milliseconds since UpdatedEpoch (SetUpdatedEpoch), an int64 field
// (json "upd"). It costs no round trip, and it is the one watermark every cache
// syncs on: the delta cache, the by-IDs cache (cache_by_ids.go), GroupDelta and
// the DataFrame checkpoints. Each record gets at least its stored Updated + 1, and
// the process clock never hands the same value twice, so a rewrite always moves.
//
// A timestamp, unlike a sequence, is not unique across processes and a write
// stamped at t can land after a client synced past t (in flight, or stamped by a
// Lambda whose clock lags). So Delta() re-reads the last DeltaOverlap before the
// client's watermark, the window, and fingerprints it (DeltaFingerprint): when the
// client's fingerprint of the same window matches, it holds every write there and
// the window is not sent again; when it differs, the whole window is resent and
// the client upserts it by ID. Only the delta rows are read to decide, never the
// records.
//
// A delta index, {Type: TypeDelta, Keys: Cols(...)}, is a hidden-rows index
// (array_index.go) whose row sk is <pinned Keys>#<Updated>#<base sk>:
//
//   - The last Key, unless it is a ColSlice, is the sync filter column. It is not
//     part of the row sk: Delta()'s values filter it in memory, on a first sync
//     only. Cols(Status) is the usual shape: active records on a first sync,
//     every status afterwards, so soft-deleted ones reach the clients caching them.
//   - The other Keys are pinned: Delta() needs an Eq on each (a Contains on a
//     ColSlice, which fans the rows out per element as in a fan-out index).
//
// Updated changes on every write, so every write moves the record's rows (a put
// and a delete per row), and their sync is the fan-out one: crash-safe extra
// rows, re-checked on read.
// ─────────────────────────────────────────────────────────────────────────────

const (
	updatedFieldName = "Updated"
	// updatedBits is the sk width of Updated: 7 base64 digits, about 139 years of milliseconds.
	updatedBits = 42
	// DeltaOverlap is how far below the client's watermark Delta() reads again: it covers a write
	// still in flight and the clock skew between Lambdas. genix-ui's delta cache keeps the Updated
	// values of the same window to fingerprint it, so both sides must use the same value.
	DeltaOverlap = 4 * time.Second
)

// Now is the clock of the managed Updated. An app with its own clock assigns it once at boot.
var Now = time.Now

// updatedEpochMillis is UpdatedEpoch in milliseconds (SetUpdatedEpoch).
var updatedEpochMillis int64

// lastStampedUpdated is the last Updated this process handed out (nextUpdated).
var lastStampedUpdated atomic.Int64

// SetUpdatedEpoch sets the moment Updated counts from, in unix seconds. Call it once at boot, before
// the first write. It is permanent: every stored Updated, delta row and client watermark counts from
// it, so changing it later corrupts them all.
func SetUpdatedEpoch(unixSeconds int64) { updatedEpochMillis = unixSeconds * 1000 }

// UpdatedNow is the clock in Updated units: milliseconds since the UpdatedEpoch.
func UpdatedNow() int64 { return UpdatedOfTime(Now()) }

// UpdatedOfTime is a moment in Updated units, to range a query over Updated.
func UpdatedOfTime(moment time.Time) int64 { return moment.UnixMilli() - updatedEpochMillis }

// UpdatedToTime turns an Updated value back into the time it stamps.
func UpdatedToTime(updated int64) time.Time { return time.UnixMilli(updated + updatedEpochMillis) }

// nextUpdated is UpdatedNow, but above every value this process handed out before: two write calls
// of one process never share an Updated.
func nextUpdated() int64 {
	for {
		lastStamped := lastStampedUpdated.Load()
		stamp := max(UpdatedNow(), lastStamped+1)
		if lastStampedUpdated.CompareAndSwap(lastStamped, stamp) {
			return stamp
		}
	}
}

// resolveUpdated validates the record's managed Updated field. Every integer "Updated" must be an
// int64, the unit every cache compares; isRequired is a schema whose indexes or writes consume it.
// It returns the field as a key column, nil for a record without one.
func resolveUpdated(recordType reflect.Type, accessors map[string]*colAccessor, isRequired bool) *keyCol {
	field, hasField := recordType.FieldByName(updatedFieldName)
	isInteger := hasField && accessors[updatedFieldName] != nil && accessors[updatedFieldName].kind.isInteger()
	if isInteger && field.Type.Kind() != reflect.Int64 {
		panic(fmt.Sprintf("db: %s.%s must be an int64: milliseconds since the UpdatedEpoch", recordType.Name(), updatedFieldName))
	}
	if !isInteger {
		if isRequired {
			panic(fmt.Sprintf("db: %s declares a TypeDelta index, GroupDelta, CacheByIDs, VersionedWrites or DataFrames and needs an int64 field %q (json \"upd\")",
				recordType.Name(), updatedFieldName))
		}
		return nil
	}
	return &keyCol{fieldName: updatedFieldName, kind: kindInt, bits: updatedBits, acc: accessors[updatedFieldName]}
}

// compileDeltaIndex resolves a TypeDelta Index: its pinned Keys, then the managed
// Updated, with the last scalar Key split off as the sync filter column.
func compileDeltaIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index, updatedColumn keyCol) arrayIndexMeta {
	rowIndex := index
	syncFilterField := ""
	if lastKey := len(index.Keys) - 1; lastKey >= 0 {
		if _, isSlice := index.Keys[lastKey].(SliceColn); !isSlice {
			syncFilterField = index.Keys[lastKey].col().fieldName
			if accessors[syncFilterField] == nil {
				panic(fmt.Sprintf("db: delta index column %q is not an exported field of %s", syncFilterField, recordType.Name()))
			}
			rowIndex.Keys = index.Keys[:lastKey]
		}
	}

	deltaIndex := resolveArrayIndex(recordType, accessors, rowIndex)
	deltaIndex.keys = append(deltaIndex.keys, updatedColumn)
	deltaIndex.isDelta, deltaIndex.syncFilterField = true, syncFilterField
	if deltaIndex.elementPosition < 0 {
		firstFieldName := updatedColumn.fieldName
		if len(rowIndex.Keys) > 0 {
			firstFieldName = rowIndex.Keys[0].col().fieldName
		}
		deltaIndex.columnID = cbColumnID(recordType, firstFieldName)
	}
	return deltaIndex
}

// updatedOfDeltaRow reads the Updated a delta row sk holds: its last index Key.
func (deltaIndex *arrayIndexMeta) updatedOfDeltaRow(rowSK string) int64 {
	keyParts := strings.SplitN(rowSK, keySeparator, len(deltaIndex.keys)+1)
	updated, _ := DecodeOrderedUint(keyParts[len(deltaIndex.keys)-1])
	return int64(updated)
}

// stampManagedColumns sets Updated on every record about to be written: one nextUpdated for the call,
// raised per record above minimumUpdated (its stored Updated + 1, 0 for none), so a rewrite always
// moves the record past the version it replaces even when the stored one came from a clock ahead.
func (m *tableMeta) stampManagedColumns(ptrs []unsafe.Pointer, minimumUpdated func(ptr unsafe.Pointer) int64) {
	if m.updated == nil {
		return
	}
	writeUpdated := nextUpdated()
	for _, ptr := range ptrs {
		recordUpdated := writeUpdated
		if minimumUpdated != nil {
			recordUpdated = max(recordUpdated, minimumUpdated(ptr))
		}
		m.updated.acc.setI64(ptr, recordUpdated)
	}
}

// aboveStored is the minimumUpdated of a write that read storedByKey: one above each stored record.
func (m *tableMeta) aboveStored(storedByKey map[string]unsafe.Pointer) func(ptr unsafe.Pointer) int64 {
	return func(ptr unsafe.Pointer) int64 {
		if storedPtr := storedByKey[m.recordKey(ptr)]; storedPtr != nil {
			return m.updated.acc.getI64(storedPtr) + 1
		}
		return 0
	}
}

// DeltaSince is what a client holds of a delta read: Updated, the highest Updated it received (0 on
// a first sync), and Fingerprint, the DeltaFingerprint of the Updated values it received inside the
// window [Updated - DeltaOverlap, Updated].
type DeltaSince struct {
	Updated     int64
	Fingerprint uint32
}

// DeltaFingerprint folds Updated values into an order-independent 32-bit sum of their mixes: a
// missing, extra or rewritten record changes it. genix-ui's delta cache computes the same; the shared
// test vector in delta_test.go pins the two together.
func DeltaFingerprint(updatedValues []int64) uint32 {
	fingerprint := uint32(0)
	for _, updated := range updatedValues {
		fingerprint += mixUpdated(updated)
	}
	return fingerprint
}

// mixUpdated hashes one Updated value: its two 32-bit halves folded, then murmur3's finalizer.
func mixUpdated(updated int64) uint32 {
	mixed := uint32(updated) ^ uint32(uint64(updated)>>32)*0x9E3779B1
	mixed ^= mixed >> 16
	mixed *= 0x85EBCA6B
	mixed ^= mixed >> 13
	mixed *= 0xC2B2AE35
	mixed ^= mixed >> 16
	return mixed
}

// Delta turns the query into a delta-cache read: the records written after since.Updated, and the
// window below it when since.Fingerprint shows the client is missing something there (see the top of
// this file). A first sync (since.Updated 0) reads every record. Call it last: it picks the TypeDelta
// index from the predicates already set, the one whose pinned Keys all have an Eq (or the Contains),
// the most specific when several fit. syncFilterValues filter its sync filter column on a first sync;
// a later sync returns every value, so records that left them (soft-deleted) reach the clients still
// caching them.
func (q *QueryBuilder[E]) Delta(since DeltaSince, syncFilterValues ...any) *QueryBuilder[E] {
	readFrom := int64(0)
	if since.Updated > 0 {
		readFrom = max(since.Updated-DeltaOverlap.Milliseconds(), 0)
		q.deltaSince = since
	} else if len(syncFilterValues) > 0 {
		q.syncFilter = &predicate{op: opIn, v1: syncFilterValues}
	}
	return q.deltaFrom(readFrom, len(syncFilterValues) > 0)
}

// deltaFrom reads the delta rows whose Updated is at least fromUpdated, with no window: the plain
// range a DataFrame run reads, and the one Delta() narrows.
func (q *QueryBuilder[E]) deltaFrom(fromUpdated int64, needsSyncFilter bool) *QueryBuilder[E] {
	deltaIndex, err := q.meta.selectDeltaIndex(q.preds, needsSyncFilter)
	if err != nil {
		q.planErr = err
		return q
	}
	q.deltaIndex = deltaIndex
	if q.syncFilter != nil {
		q.syncFilter.field = deltaIndex.syncFilterField
	}
	q.preds = append(q.preds, predicate{field: updatedFieldName, op: opGte, v1: fromUpdated})
	return q
}

// execDeltaSince is Exec for a Delta() past a first sync. It reads every delta row from the window on
// (keys only, cheap), drops the window's rows when the client already holds them (dropHeldWindow),
// and only then reads the records the remaining rows point to: a sync with nothing new costs that
// one query.
func (q *QueryBuilder[E]) execDeltaSince(client *dynamodb.Client, plans []*queryPlan, dst *[]E) error {
	rowsByPlan := make([][]map[string]types.AttributeValue, len(plans))
	for planIndex, plan := range plans {
		input, err := q.queryInput(plan)
		if err != nil {
			return err
		}
		for {
			out, err := client.Query(context.Background(), input)
			if err != nil {
				return err
			}
			rowsByPlan[planIndex] = append(rowsByPlan[planIndex], out.Items...)
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}

	rowsByPlan = q.deltaIndex.dropHeldWindow(rowsByPlan, q.deltaSince)
	returnedSKs := map[string]bool{}
	for planIndex, plan := range plans {
		rows := rowsByPlan[planIndex]
		if len(rows) == 0 {
			continue
		}
		items, rowSKs, _, err := recordItemsOfArrayRows(client, plan.basePK, plan.arrayIndex, rows, q.consistentRead)
		if err != nil {
			return err
		}
		isLimitReached, err := q.appendRecords(plan, items, rowSKs, returnedSKs, dst)
		if err != nil || isLimitReached {
			return err
		}
	}
	return nil
}

// dropHeldWindow fingerprints the window's rows (Updated at most since.Updated) across every plan,
// each record version once: a record in several rows of a Contains counts once, and an extra row a
// crash left counts apart, so it reads as a mismatch. When the fingerprint equals the client's, the
// client holds the window and its rows are dropped; otherwise every row is kept, the window resent.
func (deltaIndex *arrayIndexMeta) dropHeldWindow(rowsByPlan [][]map[string]types.AttributeValue, since DeltaSince) [][]map[string]types.AttributeValue {
	rowUpdated := func(row map[string]types.AttributeValue) int64 {
		return deltaIndex.updatedOfDeltaRow(row["sk"].(*types.AttributeValueMemberS).Value)
	}
	isFingerprinted := map[string]bool{}
	var windowUpdatedValues []int64
	for _, rows := range rowsByPlan {
		for _, row := range rows {
			updated := rowUpdated(row)
			recordVersion := baseSKOfArrayRow(row["sk"].(*types.AttributeValueMemberS).Value, len(deltaIndex.keys)) + keySeparator + strconv.FormatInt(updated, 10)
			if updated <= since.Updated && !isFingerprinted[recordVersion] {
				isFingerprinted[recordVersion] = true
				windowUpdatedValues = append(windowUpdatedValues, updated)
			}
		}
	}
	if DeltaFingerprint(windowUpdatedValues) != since.Fingerprint {
		return rowsByPlan
	}
	for planIndex := range rowsByPlan {
		rowsByPlan[planIndex] = slices.DeleteFunc(rowsByPlan[planIndex], func(row map[string]types.AttributeValue) bool {
			return rowUpdated(row) <= since.Updated
		})
	}
	return rowsByPlan
}

// selectDeltaIndex picks the TypeDelta index whose pinned Keys are all pinned by
// an Eq or a Contains, the one pinning the most when several fit.
func (m *tableMeta) selectDeltaIndex(preds []predicate, needsSyncFilter bool) (*arrayIndexMeta, error) {
	isPinned := map[string]bool{}
	for _, p := range preds {
		if p.op == opEq || p.op == opContains {
			isPinned[p.field] = true
		}
	}
	var selectedIndex *arrayIndexMeta
	selectedPinnedCount, isAmbiguous := -1, false
	for i := range m.arrayIndexes {
		deltaIndex := &m.arrayIndexes[i]
		if !deltaIndex.isDelta || (needsSyncFilter && deltaIndex.syncFilterField == "") {
			continue
		}
		pinnedKeys := deltaIndex.keys[:len(deltaIndex.keys)-1]
		allPinned := true
		for _, pinnedKey := range pinnedKeys {
			allPinned = allPinned && isPinned[pinnedKey.fieldName]
		}
		if !allPinned {
			continue
		}
		switch {
		case len(pinnedKeys) > selectedPinnedCount:
			selectedIndex, selectedPinnedCount, isAmbiguous = deltaIndex, len(pinnedKeys), false
		case len(pinnedKeys) == selectedPinnedCount:
			isAmbiguous = true
		}
	}
	if selectedIndex == nil {
		return nil, fmt.Errorf("db: %s Delta() found no TypeDelta index whose pinned Keys all have an Eq or Contains (and a sync filter column when values are given)",
			m.recordType.Name())
	}
	if isAmbiguous {
		return nil, fmt.Errorf("db: %s Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index", m.recordType.Name())
	}
	return selectedIndex, nil
}
