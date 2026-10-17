package dynamo

import (
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Delta sync: the managed Version and Updated, the TypeDelta index and Delta().
//
// Version (an int16 field "Version", json "ver") counts the writes of a record: 1
// on insert, one more on every write, 1 again after 32767. It lives in the item
// attribute "ver", out of the blob, so a write can raise it inside DynamoDB with
// no read (repo.go), and it is what PutManyIfVersion and Modify condition on.
//
// Updated (an optional int32 field "Updated", json "upd") is the SUnixTime of the
// write, (unix seconds − 1e9) / 2: a time, not a version, so two writes may share
// it. The by-IDs cache counts its slot ticks in it.
//
// A delta index, {Type: TypeDelta, Keys: Cols(...)}, is a change log (delta_log.go):
// every write appends (log ID, Version, deleted) to the index's file of the
// record's partition, pinned values and 10-minute bucket, in the utils.FileStore.
//
//   - The last Key, unless it is a ColSlice, is the sync filter column: Delta()'s
//     values filter it on a first sync only. Cols(Status) is the usual shape:
//     active records on a first sync, every status afterwards, so soft-deleted ones
//     reach the clients caching them.
//   - The other Keys are pinned: Delta() needs an Eq on each (a Contains with one
//     value on a ColSlice). Each pinned value has its own log.
//   - A pinned column that is neither a Partition column nor part of a leading run
//     of the Keys also gets hidden rows <pinned Keys>#<base sk> (array_index.go),
//     which list the view on a first sync. Otherwise the base table lists it.
//   - The Keys that are not pinned make the log ID: one integer column, or several
//     packed into one integer of at most 53 bits (a JS number), the first column in
//     the high bits. With every Key pinned, the Keys themselves are the log ID.
// ─────────────────────────────────────────────────────────────────────────────

const (
	updatedFieldName  = "Updated"
	versionFieldName  = "Version"
	metadataFieldName = "Metadata"
	// versionColumn is the item attribute holding Version.
	versionColumn = "ver"
	// maxVersion is the highest Version: the write after it stores 1 again.
	maxVersion = 32767
	// maxPackedLogIDBits keeps a packed log ID an exact JS number.
	maxPackedLogIDBits = 53
)

// Now is the ORM's clock. An app with its own clock assigns it once at boot.
var Now = time.Now

// sUnixTimeToTime turns an SUnixTime back into the start of its 2-second tick.
func sUnixTimeToTime(sUnixTime int64) time.Time { return time.Unix(sUnixTime*2+1e9, 0) }

// nextVersion is the Version of the write after the one stored at version (0: nothing stored).
func nextVersion(version int64) int64 {
	if version >= maxVersion {
		return 1
	}
	return version + 1
}

// resolveVersion validates the record's managed Version field: every table needs an int16 one.
func resolveVersion(recordType reflect.Type, accessors map[string]*colAccessor) *colAccessor {
	field, hasField := recordType.FieldByName(versionFieldName)
	if !hasField || field.Type.Kind() != reflect.Int16 {
		panic(fmt.Sprintf("db: %s needs the managed field `%s int16 `json:\"ver,omitempty\" cb:\"-\"``", recordType.Name(), versionFieldName))
	}
	if field.Tag.Get("cb") != "-" {
		panic(fmt.Sprintf("db: %s.%s must be `cb:\"-\"`: it lives in the item attribute %q, and a copy in the blob would lag one write behind",
			recordType.Name(), versionFieldName, versionColumn))
	}
	return accessors[versionFieldName]
}

// resolveUpdated validates the record's managed Updated field, an int32 SUnixTime. isRequired is a
// schema that consumes it (CacheByIDs). It returns the field as a key column, nil without one.
func resolveUpdated(recordType reflect.Type, accessors map[string]*colAccessor, isRequired bool) *keyCol {
	field, hasField := recordType.FieldByName(updatedFieldName)
	isInteger := hasField && accessors[updatedFieldName] != nil && accessors[updatedFieldName].kind.isInteger()
	if isInteger && field.Type.Kind() != reflect.Int32 {
		panic(fmt.Sprintf("db: %s.%s must be an int32: the SUnixTime of the write", recordType.Name(), updatedFieldName))
	}
	if !isInteger {
		if isRequired {
			panic(fmt.Sprintf("db: %s uses CacheByIDs and needs an int32 field %q (json \"upd\")", recordType.Name(), updatedFieldName))
		}
		return nil
	}
	return &keyCol{fieldName: updatedFieldName, kind: kindInt, bits: 32, acc: accessors[updatedFieldName]}
}

// stampUpdated sets Updated, the SUnixTime of the write, on every record about to be written.
func (m *tableMeta) stampUpdated(ptrs []unsafe.Pointer) {
	if m.updated == nil {
		return
	}
	writeTime := utils.SUnixTimeOf(Now())
	for _, ptr := range ptrs {
		m.updated.acc.setI64(ptr, writeTime)
	}
}

// writeTimeOf is the SUnixTime of a record's write: its stamped Updated, the clock without one.
func (m *tableMeta) writeTimeOf(ptr unsafe.Pointer) int64 {
	if m.updated == nil {
		return utils.SUnixTimeOf(Now())
	}
	return m.updated.acc.getI64(ptr)
}

// deltaIndexMeta is one resolved TypeDelta index (see the top of this file).
type deltaIndexMeta struct {
	// tag names the index's folder in the log: the cb ids of its declared Keys, dash-joined.
	tag string
	// pinned are the pinned Keys in declared order; a ColSlice one holds the slice's field name.
	pinned          []keyCol
	syncFilterField string
	// rowsIndex is the position in tableMeta.arrayIndexes of the hidden rows that list the view,
	// -1 when the base table lists it.
	rowsIndex       int
	logIDColumns    []keyCol
	logIDColumnBits []int8 // the packed columns' Sizes; empty when the log ID is one column
	is64BitScale    bool
}

// compileDeltaIndex resolves a TypeDelta Index against the compiled table, and the hidden rows it
// needs (nil without them).
func compileDeltaIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index, meta *tableMeta) (deltaIndexMeta, *arrayIndexMeta) {
	if len(index.Keys) == 0 {
		panic(fmt.Sprintf("db: %s declares a TypeDelta index with no Keys: Cols(Status) is the usual one", recordType.Name()))
	}
	if len(index.GroupBy) > 0 {
		panic(fmt.Sprintf("db: %s declares GroupBy on a TypeDelta index: declare it on a GSI, a fan-out or a slot-less Index", recordType.Name()))
	}
	columnIDs := make([]string, len(index.Keys))
	for i, column := range index.Keys {
		columnIDs[i] = cbColumnID(recordType, column.col().fieldName)
	}
	deltaIndex := deltaIndexMeta{tag: strings.Join(columnIDs, "-"), rowsIndex: -1}

	pinnedKeys := index.Keys
	if lastKey := index.Keys[len(index.Keys)-1]; !isSliceColn(lastKey) {
		deltaIndex.syncFilterField = lastKey.col().fieldName
		if accessors[deltaIndex.syncFilterField] == nil {
			panic(fmt.Sprintf("db: delta index column %q is not an exported field of %s", deltaIndex.syncFilterField, recordType.Name()))
		}
		pinnedKeys = index.Keys[:len(index.Keys)-1]
	}
	isPinned := map[string]bool{}
	for _, column := range pinnedKeys {
		fieldName := column.col().fieldName
		isPinned[fieldName] = true
		if isSliceColn(column) {
			deltaIndex.pinned = append(deltaIndex.pinned, keyCol{fieldName: fieldName, kind: kindString})
			continue
		}
		deltaIndex.pinned = append(deltaIndex.pinned, resolveKeyCols(recordType, accessors, []Coln{column})...)
	}

	// Pinned base key columns (the partition, a leading run of the Keys) list the view from the base
	// table; any other pinned column needs hidden rows.
	pinnedOutsideKeys := 0
	for _, column := range deltaIndex.pinned {
		if !slices.ContainsFunc(meta.partition, func(partitionColumn keyCol) bool { return partitionColumn.fieldName == column.fieldName }) {
			pinnedOutsideKeys++
		}
	}
	isKeysPrefix := pinnedOutsideKeys <= len(meta.keys)
	for _, keyColumn := range meta.keys[:min(pinnedOutsideKeys, len(meta.keys))] {
		isKeysPrefix = isKeysPrefix && isPinned[keyColumn.fieldName]
	}
	var rows *arrayIndexMeta
	if pinnedOutsideKeys > 0 && !isKeysPrefix {
		resolvedRows := resolveArrayIndex(recordType, accessors, Index{Keys: pinnedKeys})
		if resolvedRows.elementPosition < 0 {
			resolvedRows.columnID = columnIDs[0]
		}
		rows = &resolvedRows
	}

	// The log ID: the Keys that are not pinned, or all of them when every one is.
	for _, keyColumn := range meta.keys {
		if !isPinned[keyColumn.fieldName] {
			deltaIndex.logIDColumns = append(deltaIndex.logIDColumns, keyColumn)
		}
	}
	if len(deltaIndex.logIDColumns) == 0 {
		deltaIndex.logIDColumns = meta.keys
	}
	logIDBits := 0
	for _, column := range deltaIndex.logIDColumns {
		if column.kind != kindInt {
			panic(fmt.Sprintf("db: %s delta index log ID column %q must be a signed integer: an unpinned Keys column identifies the record in the log",
				recordType.Name(), column.fieldName))
		}
		logIDBits += int(column.bits)
	}
	if len(deltaIndex.logIDColumns) > 1 {
		if logIDBits > maxPackedLogIDBits {
			panic(fmt.Sprintf("db: %s delta index log ID packs %d bits, over %d: shrink the Keys Sizes", recordType.Name(), logIDBits, maxPackedLogIDBits))
		}
		for _, column := range deltaIndex.logIDColumns {
			deltaIndex.logIDColumnBits = append(deltaIndex.logIDColumnBits, column.bits)
		}
	}
	deltaIndex.is64BitScale = logIDBits > 32
	return deltaIndex, rows
}

func isSliceColn(column Coln) bool {
	_, isSlice := column.(SliceColn)
	return isSlice
}

// logIDOf is the record's log ID in deltaIndex.
func (deltaIndex *deltaIndexMeta) logIDOf(ptr unsafe.Pointer) uint64 {
	values := make([]uint64, len(deltaIndex.logIDColumns))
	for i, column := range deltaIndex.logIDColumns {
		values[i] = column.acc.getU64(ptr)
	}
	if len(values) == 1 {
		return values[0]
	}
	return packDeltaLogID(values, deltaIndex.logIDColumnBits)
}

// pinnedValueText is one pinned value as a log folder name.
func pinnedValueText(value any) string {
	reflected := reflect.ValueOf(value)
	switch {
	case reflected.CanInt():
		return strconv.FormatInt(reflected.Int(), 10)
	case reflected.CanUint():
		return strconv.FormatUint(reflected.Uint(), 10)
	default:
		return url.PathEscape(fmt.Sprint(value))
	}
}

// pinnedFolders lists the log folders, under the index's, the record at ptr is in: one per value
// of its pinned columns, one per element for a ColSlice. Nil for a nil ptr.
func (m *tableMeta) pinnedFolders(deltaIndex *deltaIndexMeta, ptr unsafe.Pointer) []string {
	if ptr == nil {
		return nil
	}
	folders := []string{""}
	record := reflect.NewAt(m.recordType, ptr).Elem()
	for _, column := range deltaIndex.pinned {
		field := record.FieldByName(column.fieldName)
		values := []reflect.Value{field}
		if field.Kind() == reflect.Slice {
			values = make([]reflect.Value, field.Len())
			for i := range values {
				values[i] = field.Index(i)
			}
		}
		var nextFolders []string
		for _, folder := range folders {
			for _, value := range values {
				nextFolders = append(nextFolders, folder+pinnedValueText(value.Interface())+"/")
			}
		}
		folders = nextFolders
	}
	slices.Sort(folders)
	return slices.Compact(folders)
}

// DeltaCursor is what a client holds of a delta index's log: per log file still open (its 10-minute
// bucket), how many of its bytes it applied. Empty on a first sync.
type DeltaCursor map[int64]int

// Delta turns the query into a delta-cache read (delta_log.go). A first sync (an empty cursor) lists
// the view, with syncFilterValues filtering its sync filter column; a later one reads the log past
// the cursor and returns the records written since, in every status, so records that left the
// filter (soft-deleted) reach the clients still caching them. Either way the result ends with one
// Metadata record per log file read, which the client keeps as its cursor. Call it last: it picks
// the TypeDelta index from the predicates already set, the one whose pinned Keys all have an Eq (or
// the Contains), the most specific when several fit.
func (q *QueryBuilder[E]) Delta(cursor DeltaCursor, syncFilterValues ...any) *QueryBuilder[E] {
	q.deltaCursor, q.readsDeltaLog = cursor, true
	if len(cursor) > 0 {
		return q.selectDelta(false)
	}
	return q.DeltaList(syncFilterValues...)
}

// DeltaList lists a delta index's view as a first sync does, with no Metadata records: the read of
// a handler that needs the records, not a client's cache.
func (q *QueryBuilder[E]) DeltaList(syncFilterValues ...any) *QueryBuilder[E] {
	if len(syncFilterValues) > 0 {
		q.syncFilter = &predicate{op: opIn, v1: syncFilterValues}
	}
	q.selectDelta(len(syncFilterValues) > 0)
	if q.syncFilter != nil && q.deltaIndex != nil {
		q.syncFilter.field = q.deltaIndex.syncFilterField
	}
	return q
}

func (q *QueryBuilder[E]) selectDelta(needsSyncFilter bool) *QueryBuilder[E] {
	deltaIndex, err := q.meta.selectDeltaIndex(q.preds, needsSyncFilter)
	if err != nil {
		q.planErr = err
		return q
	}
	q.deltaIndex = deltaIndex
	return q
}

// deltaRows is the hidden-rows index that lists the delta read's view, nil when the base table does.
func (q *QueryBuilder[E]) deltaRows() *arrayIndexMeta {
	if q.deltaIndex == nil || q.deltaIndex.rowsIndex < 0 {
		return nil
	}
	return &q.meta.arrayIndexes[q.deltaIndex.rowsIndex]
}

// selectDeltaIndex picks the TypeDelta index whose pinned Keys are all pinned by an Eq or a
// one-value Contains, the one pinning the most when several fit.
func (m *tableMeta) selectDeltaIndex(preds []predicate, needsSyncFilter bool) (*deltaIndexMeta, error) {
	isPinned := map[string]bool{}
	for _, p := range preds {
		if p.op == opEq || (p.op == opContains && len(p.v1.([]any)) == 1) {
			isPinned[p.field] = true
		}
	}
	var selectedIndex *deltaIndexMeta
	selectedPinnedCount, isAmbiguous := -1, false
	for i := range m.deltaIndexes {
		deltaIndex := &m.deltaIndexes[i]
		if needsSyncFilter && deltaIndex.syncFilterField == "" {
			continue
		}
		allPinned := true
		for _, pinnedKey := range deltaIndex.pinned {
			allPinned = allPinned && isPinned[pinnedKey.fieldName]
		}
		if !allPinned {
			continue
		}
		switch {
		case len(deltaIndex.pinned) > selectedPinnedCount:
			selectedIndex, selectedPinnedCount, isAmbiguous = deltaIndex, len(deltaIndex.pinned), false
		case len(deltaIndex.pinned) == selectedPinnedCount:
			isAmbiguous = true
		}
	}
	if selectedIndex == nil {
		return nil, fmt.Errorf("db: %s Delta() found no TypeDelta index whose pinned Keys all have an Eq or a one-value Contains (and a sync filter column when values are given)",
			m.recordType.Name())
	}
	if isAmbiguous {
		return nil, fmt.Errorf("db: %s Delta() fits several TypeDelta indexes equally: pin one more Keys column, or drop an index", m.recordType.Name())
	}
	return selectedIndex, nil
}
