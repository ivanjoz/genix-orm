package dynamo

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta sync: the port of genix-orm's managed "updated" / "updated_version"
// columns, its TypeDelta index and its Delta() read.
//
// Managed fields, stamped on every Put/PutMany/PutIfAbsent before the write:
//
//   - Updated: a record integer field named "Updated" gets the write time as a
//     SUnixTime, (unix - 1e9) / 2, read from Now.
//   - UpdatedVersion: on a table declaring a TypeDelta index, SaveUpdatedVersion
//     or VersionedWrites, the int32 field "UpdatedVersion" (json "upv") gets the
//     write sequence number: one value per write call per base pk, reserved from
//     a sequence item (sequence.go, sk "<base pk>#upv") with the same atomic ADD
//     as autoincrement. Unlike a timestamp it never repeats, so a client asking
//     for "> my watermark" misses nothing and is resent nothing.
//
// A delta index, {Type: TypeDelta, Keys: Keys(...)}, is a hidden-rows index
// (array_index.go) whose row sk is <pinned Keys>#<UpdatedVersion>#<base sk>:
//
//   - The last Key, unless it is a ColSlice, is the sync filter column. It is not
//     part of the row sk: Delta()'s values filter it in memory, on a first sync
//     only. Keys(Status) is the usual shape: active records on a first sync,
//     every status afterwards, so soft-deleted ones reach the clients caching them.
//   - The other Keys are pinned: Delta() needs an Eq on each (a Contains on a
//     ColSlice, which fans the rows out per element as in a fan-out index).
//
// UpdatedVersion changes on every write, so every write moves the record's rows
// (a put and a delete per row), and their sync is the fan-out one: crash-safe
// extra rows, re-checked on read.
// ─────────────────────────────────────────────────────────────────────────────

const (
	updatedFieldName = "Updated"
	// updatedVersionBits is the sk width of UpdatedVersion: every positive int32.
	updatedVersionBits      = 31
	updatedVersionSeqSuffix = "#upv"
)

// Now is the clock of the managed Updated. An app with its own clock assigns it once at boot.
var Now = time.Now

// resolveWriteVersion validates the managed UpdatedVersion field and returns it as a key column.
func resolveWriteVersion(recordType reflect.Type, accessors map[string]*colAccessor) *keyCol {
	field, ok := recordType.FieldByName(updatedVersionFieldName)
	if !ok || field.Type.Kind() != reflect.Int32 {
		panic(fmt.Sprintf("db: %s declares a TypeDelta index, SaveUpdatedVersion or VersionedWrites and needs an int32 field %q (json \"upv\")",
			recordType.Name(), updatedVersionFieldName))
	}
	return &keyCol{fieldName: updatedVersionFieldName, kind: kindInt, bits: updatedVersionBits, acc: accessors[updatedVersionFieldName]}
}

// compileDeltaIndex resolves a TypeDelta Index: its pinned Keys, then the managed
// UpdatedVersion, with the last scalar Key split off as the sync filter column.
func compileDeltaIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index, versionColumn keyCol) arrayIndexMeta {
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
	deltaIndex.keys = append(deltaIndex.keys, versionColumn)
	deltaIndex.isDelta, deltaIndex.syncFilterField = true, syncFilterField
	if deltaIndex.elementPosition < 0 {
		firstFieldName := versionColumn.fieldName
		if len(rowIndex.Keys) > 0 {
			firstFieldName = rowIndex.Keys[0].col().fieldName
		}
		deltaIndex.columnID = cbColumnID(recordType, firstFieldName)
	}
	return deltaIndex
}

// stampManagedColumns sets Updated and UpdatedVersion on every record about to be
// written. It must run after assignAutoIDs: a partition column may be the ID.
func (m *tableMeta) stampManagedColumns(ptrs []unsafe.Pointer) error {
	if m.updated != nil {
		writeTime := (Now().Unix() - 1e9) / 2
		for _, ptr := range ptrs {
			m.updated.setI64(ptr, writeTime)
		}
	}
	if m.writeVersion == nil {
		return nil
	}
	versionByPK := map[string]int64{}
	for _, ptr := range ptrs {
		basePK := m.pkValue(ptr)
		version, isReserved := versionByPK[basePK]
		if !isReserved {
			var err error
			if version, err = reserveSequence(basePK+updatedVersionSeqSuffix, 1); err != nil {
				return err
			}
			if uint64(version) > maxValueForBits(updatedVersionBits) {
				return fmt.Errorf("db: %s UpdatedVersion %d of pk %s overflows int32", m.recordType.Name(), version, basePK)
			}
			versionByPK[basePK] = version
		}
		m.writeVersion.acc.setI64(ptr, version)
	}
	return nil
}

// Delta turns the query into a delta-cache read: the records written after
// updatedSince, the highest UpdatedVersion the client holds (0 on a first sync).
// Call it last: it picks the TypeDelta index from the predicates already set, the
// one whose pinned Keys all have an Eq (or the Contains), the most specific when
// several fit. syncFilterValues filter its sync filter column on a first sync;
// a later sync returns every value, so records that left them (soft-deleted)
// reach the clients still caching them.
func (q *QueryBuilder[E]) Delta(updatedSince int32, syncFilterValues ...any) *QueryBuilder[E] {
	deltaIndex, err := q.meta.selectDeltaIndex(q.preds, len(syncFilterValues) > 0)
	if err != nil {
		q.planErr = err
		return q
	}
	q.deltaIndex = deltaIndex
	// ">= W+1" rather than "> W": versions start at 1, so a first sync reads every row.
	versionColumn := deltaIndex.keys[len(deltaIndex.keys)-1]
	q.preds = append(q.preds, predicate{field: versionColumn.fieldName, op: opGte, v1: int64(max(updatedSince, 0)) + 1})
	if updatedSince <= 0 && len(syncFilterValues) > 0 {
		q.syncFilter = &predicate{field: deltaIndex.syncFilterField, op: opIn, v1: syncFilterValues}
	}
	return q
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
