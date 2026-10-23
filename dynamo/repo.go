package dynamo

import (
	"context"
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db/cache"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Repo is the typed entry point for one entity, analogous to how genix hangs
// operations off the table struct. T is the schema (column handles), E is the
// record. Create one (usually a package-level var) and reuse it:
//
//	var Products = db.NewRepo[ProductTable, Product]()
//
//	Products.Put(&p)
//	Products.Query().Eq(Products.T.Category, "coffee").Exec(&out)
//
// Products.T is the compiled table struct: its column handles carry their names,
// so you reference them in queries with full static typing.
//
// Go compiles a generic method once per record type, so Repo's own methods are
// thin typed shells: they turn records into pointers and call repoCore, the
// non-generic engine every table shares. Its methods that take no record (the
// Controller's maintenance operations) are promoted to Repo as they are.
// ─────────────────────────────────────────────────────────────────────────────

type Repo[T any, E any] struct {
	repoCore
	// T exposes the named column handles for use in query predicates.
	T T
}

// repoCore is one entity's reads and writes over records held by pointer (each a *E).
type repoCore struct {
	meta *tableMeta
}

// NewRepo compiles (and caches) the schema and returns a ready repo.
func NewRepo[T any, E any]() *Repo[T, E] {
	meta, table := getOrCompile[T, E]()
	return &Repo[T, E]{repoCore: repoCore{meta: meta}, T: table}
}

// ── Records by pointer ───────────────────────────────────────────────────────

// recordSink appends a record (recordPtr is a *E) to the caller's typed slice and
// returns the slice's new length. It is the only typed step of a read.
type recordSink func(recordPtr unsafe.Pointer) int

// appendTo is the recordSink of a typed slice.
func appendTo[E any](dst *[]E) recordSink {
	return func(recordPtr unsafe.Pointer) int {
		*dst = append(*dst, *(*E)(recordPtr))
		return len(*dst)
	}
}

// pointersOf points at each record of records, in place.
func pointersOf[E any](records []E) []unsafe.Pointer {
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	return ptrs
}

// newRecord allocates a zero record of the entity.
func (m *tableMeta) newRecord() unsafe.Pointer { return reflect.New(m.recordType).UnsafePointer() }

// recordPointer is the record at ptr as a typed *E behind an any, for colbin and encoding/json.
func (m *tableMeta) recordPointer(ptr unsafe.Pointer) any {
	return reflect.NewAt(m.recordType, ptr).Interface()
}

// recordValue is a copy of the record at ptr as an E behind an any, for the type-erased Controller.
func (m *tableMeta) recordValue(ptr unsafe.Pointer) any {
	return reflect.NewAt(m.recordType, ptr).Elem().Interface()
}

// copyRecord copies the record at sourcePtr over the one at targetPtr.
func (m *tableMeta) copyRecord(targetPtr, sourcePtr unsafe.Pointer) {
	reflect.NewAt(m.recordType, targetPtr).Elem().Set(reflect.NewAt(m.recordType, sourcePtr).Elem())
}

// decodeRecord decodes an item into a new record of the entity and returns a pointer to it.
func (m *tableMeta) decodeRecord(item map[string]types.AttributeValue) (unsafe.Pointer, error) {
	ptr := m.newRecord()
	if err := m.unmarshalItem(item, m.recordPointer(ptr)); err != nil {
		return nil, err
	}
	return ptr, nil
}

// ── Writes ───────────────────────────────────────────────────────────────────

// Put upserts a single record. When the entity uses autoincrement and the
// record's ID is still zero, a fresh ID is reserved and written into *record
// before it is stored. It is PutMany of one, so array index rows sync the same way.
func (r *Repo[T, E]) Put(record *E) error {
	return r.putMany([]unsafe.Pointer{unsafe.Pointer(record)}, false)
}

// PutIfAbsent writes the record, at Version 1, only when no item with its key
// exists yet, and reports false (without an error) when the key was already taken.
// The check and the write are one conditional PutItem, so of two concurrent writers
// of the same key exactly one wins — the primitive for "enqueue once" and "claim
// once". Autoincrement IDs are assigned as in Put.
func (r *Repo[T, E]) PutIfAbsent(record *E) (bool, error) {
	return r.putIfAbsent(unsafe.Pointer(record))
}

func (r *repoCore) putIfAbsent(ptr unsafe.Pointer) (bool, error) {
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return false, err
	}
	if _, err := r.meta.assignAutoIDs([]unsafe.Pointer{ptr}); err != nil {
		return false, err
	}
	r.meta.stampUpdated([]unsafe.Pointer{ptr})
	r.meta.version.setI64(ptr, 1)
	item, err := r.meta.marshalItem(ptr, r.meta.recordPointer(ptr))
	if err != nil {
		return false, err
	}
	// A record put only when absent has no stored version: it joins its groups.
	groupDeltas := map[string]*groupCounterDelta{}
	if err := r.meta.addGroupCounterDeltas(groupDeltas, nil, ptr); err != nil {
		return false, err
	}
	client, err := Client()
	if err != nil {
		return false, err
	}
	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName:           aws.String(tableName()),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	var keyTakenErr *types.ConditionalCheckFailedException
	if errors.As(err, &keyTakenErr) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Unlike PutMany, the array index rows go after the base item: written first,
	// they would index a record that then loses the race for its key.
	arrayRowPuts, _ := r.meta.arrayIndexWrites(nil, ptr, item)
	if err := r.batchWriteAll(context.Background(), client, arrayRowPuts); err != nil {
		return true, err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return true, err
	}
	landedWrites := []deltaLogWrite{{writtenPtr: ptr, version: 1}}
	if err := r.meta.appendDeltaLogs(landedWrites); err != nil {
		return true, err
	}
	if err := r.meta.syncTextSearch(landedWrites); err != nil {
		return true, err
	}
	return true, r.meta.setSlotsUpdated([]unsafe.Pointer{ptr})
}

// PutMany upserts records. When the entity uses autoincrement, every record whose
// ID is still zero is assigned one in a single sequence reservation, and the
// managed Updated is stamped (delta.go). Each record's Version is raised:
//
//   - A table without hidden rows or GroupBy reads nothing: each record is one
//     UpdateItem raising "ver" inside DynamoDB, run in parallel.
//   - Otherwise the stored versions are read to diff the hidden rows and counters,
//     and the records are written as PutManyIfVersion over the versions read, a
//     record that lost to a concurrent write being read and written again (a plain
//     Put is last-writer-wins). A record whose ID this call assigned is new by
//     construction, so it is not read.
//
// Then the delta logs are appended (delta_log.go), and with CacheByIDs the touched
// slots are set last.
func (r *Repo[T, E]) PutMany(records []E) error { return r.putMany(pointersOf(records), false) }

// InsertMany is PutMany for records the caller knows are not stored yet, such as
// rows keyed by an ID reserved in the same operation (AssignIDs): it writes them at
// Version 1 with BatchWriteItem, reading nothing. A record that was in fact stored
// is overwritten at Version 1, keeps its old hidden rows and counts twice in its
// GroupBy groups until Reprocess.
func (r *Repo[T, E]) InsertMany(records []E) error { return r.putMany(pointersOf(records), true) }

// AssignIDs reserves and sets the autoincrement ID of every record whose ID is
// still zero, as Put would, so related records can be keyed by it before anything
// is written. Write the records with InsertMany: their IDs are new.
func (r *Repo[T, E]) AssignIDs(records []E) error {
	_, err := r.meta.assignAutoIDs(pointersOf(records))
	return err
}

func (r *repoCore) putMany(ptrs []unsafe.Pointer, areAllNew bool) error {
	if len(ptrs) == 0 {
		return nil
	}
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}
	assignedPtrs, err := r.meta.assignAutoIDs(ptrs)
	if err != nil {
		return err
	}
	r.meta.stampUpdated(ptrs)
	switch {
	case areAllNew:
		return r.insertNew(client, ptrs)
	case !r.meta.readsStoredVersion():
		return r.putRaisingVersions(client, ptrs)
	default:
		return r.putOverStoredVersions(client, ptrs, assignedPtrs)
	}
}

// insertNew writes records known to be new at Version 1, with BatchWriteItem: the new hidden rows
// first, then the base items (array_index.go), then the GroupBy counters, the logs and the slots.
func (r *repoCore) insertNew(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	baseWrites := make([]types.WriteRequest, 0, len(ptrs))
	var arrayRowPuts []types.WriteRequest
	logWrites := make([]deltaLogWrite, len(ptrs))
	groupDeltas := map[string]*groupCounterDelta{}
	for i, ptr := range ptrs {
		r.meta.version.setI64(ptr, 1)
		item, err := r.meta.marshalItem(ptr, r.meta.recordPointer(ptr))
		if err != nil {
			return err
		}
		baseWrites = append(baseWrites, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		rowPuts, _ := r.meta.arrayIndexWrites(nil, ptr, item)
		arrayRowPuts = append(arrayRowPuts, rowPuts...)
		if err := r.meta.addGroupCounterDeltas(groupDeltas, nil, ptr); err != nil {
			return err
		}
		logWrites[i] = deltaLogWrite{writtenPtr: ptr, version: 1}
	}
	ctx := context.Background()
	if err := r.batchWriteAll(ctx, client, arrayRowPuts); err != nil {
		return err
	}
	if err := r.batchWriteAll(ctx, client, baseWrites); err != nil {
		return err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return err
	}
	if err := r.meta.appendDeltaLogs(logWrites); err != nil {
		return err
	}
	if err := r.meta.syncTextSearch(logWrites); err != nil {
		return err
	}
	return r.meta.setSlotsUpdated(ptrs)
}

// putRaisingVersions writes records of a table that diffs nothing (no hidden rows, no GroupBy): one
// UpdateItem each, in parallel, raising its Version with no read.
func (r *repoCore) putRaisingVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	items := make([]map[string]types.AttributeValue, len(ptrs))
	for i, ptr := range ptrs {
		var err error
		if items[i], err = r.meta.marshalItem(ptr, r.meta.recordPointer(ptr)); err != nil {
			return err
		}
	}
	logWrites := make([]deltaLogWrite, len(ptrs))
	err := utils.RunParallel(len(ptrs), func(recordIndex int) error {
		writtenVersion, err := putItemRaisingVersion(context.Background(), client, items[recordIndex])
		r.meta.version.setI64(ptrs[recordIndex], writtenVersion)
		logWrites[recordIndex] = deltaLogWrite{writtenPtr: ptrs[recordIndex], version: writtenVersion}
		return err
	})
	if err != nil {
		return err
	}
	if err := r.meta.appendDeltaLogs(logWrites); err != nil {
		return err
	}
	if err := r.meta.syncTextSearch(logWrites); err != nil {
		return err
	}
	return r.meta.setSlotsUpdated(ptrs)
}

// putItemRaisingVersion writes item with one UpdateItem that SETs every attribute and raises "ver"
// inside DynamoDB: 1 for a new item, one above the stored one, and 1 again after cache.MaxVersion, where the
// conditional raise fails and the write is issued again starting over. It returns the Version written.
func putItemRaisingVersion(ctx context.Context, client *dynamodb.Client, item map[string]types.AttributeValue) (int64, error) {
	names := map[string]string{"#ver": versionColumn}
	values := map[string]types.AttributeValue{":one": numberAttr(1), ":zero": numberAttr(0), ":max": numberAttr(cache.MaxVersion)}
	setClauses := setClausesOf(item, names, values)
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       map[string]types.AttributeValue{"pk": item["pk"], "sk": item["sk"]},
		UpdateExpression:          aws.String("SET " + strings.Join(append(setClauses, "#ver = if_not_exists(#ver, :zero) + :one"), ", ")),
		ConditionExpression:       aws.String("attribute_not_exists(#ver) OR #ver < :max"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ReturnValues:              types.ReturnValueUpdatedNew,
	}
	out, err := client.UpdateItem(ctx, input)
	var atMaxVersionErr *types.ConditionalCheckFailedException
	if errors.As(err, &atMaxVersionErr) {
		delete(values, ":zero")
		delete(values, ":max")
		input.UpdateExpression = aws.String("SET " + strings.Join(append(setClauses, "#ver = :one"), ", "))
		input.ConditionExpression = nil
		out, err = client.UpdateItem(ctx, input)
	}
	if err != nil {
		return 0, err
	}
	return numberAttrValue(out.Attributes, versionColumn), nil
}

// setClausesOf is the UpdateExpression SET of every attribute of item but its key and "ver", their
// names and values added to names and values. crt only fills a missing one: a stored record keeps
// when it was created.
func setClausesOf(item map[string]types.AttributeValue, names map[string]string, values map[string]types.AttributeValue) []string {
	var setClauses []string
	for i, attrName := range slices.Sorted(maps.Keys(item)) {
		if attrName == "pk" || attrName == "sk" || attrName == versionColumn {
			continue
		}
		placeholder := strconv.Itoa(i)
		names["#a"+placeholder], values[":a"+placeholder] = attrName, item[attrName]
		if attrName == createdColumn {
			setClauses = append(setClauses, "#a"+placeholder+" = if_not_exists(#a"+placeholder+", :a"+placeholder+")")
			continue
		}
		setClauses = append(setClauses, "#a"+placeholder+" = :a"+placeholder)
	}
	return setClauses
}

// putOverStoredVersions writes records of a table that diffs its hidden rows or counters: each one is
// conditioned on the version just read, and the ones that lost to a concurrent write are read and
// written again, up to modifyMaxAttempts rounds. A record whose ID this call assigned is not read.
// Each round writes copies: only a written record goes back to the caller's, with its new Version. One
// still losing after the last round keeps no new Version, but its ID and Updated, set by putMany
// before the rounds, stay assigned.
func (r *repoCore) putOverStoredVersions(client *dynamodb.Client, ptrs []unsafe.Pointer, assignedPtrs []unsafe.Pointer) error {
	isAssignedNow := map[unsafe.Pointer]bool{}
	for _, assignedPtr := range assignedPtrs {
		isAssignedNow[assignedPtr] = true
	}
	pendingIndexes := make([]int, len(ptrs))
	for i := range pendingIndexes {
		pendingIndexes[i] = i
	}
	for attempt := 1; ; attempt++ {
		roundPtrs := make([]unsafe.Pointer, len(pendingIndexes))
		var possiblyStoredPtrs []unsafe.Pointer
		for i, recordIndex := range pendingIndexes {
			roundPtrs[i] = r.meta.newRecord()
			r.meta.copyRecord(roundPtrs[i], ptrs[recordIndex])
			if !isAssignedNow[ptrs[recordIndex]] {
				possiblyStoredPtrs = append(possiblyStoredPtrs, roundPtrs[i])
			}
		}
		storedByKey, err := r.storedVersions(client, possiblyStoredPtrs)
		if err != nil {
			return err
		}
		for _, ptr := range roundPtrs {
			storedVersion := int64(0)
			if stored := storedByKey[r.meta.recordKey(ptr)]; stored.ptr != nil {
				storedVersion = r.meta.version.getI64(stored.ptr)
			}
			r.meta.version.setI64(ptr, storedVersion)
		}
		lostIndexes, err := r.putIfVersion(client, roundPtrs, storedByKey)
		if err != nil {
			return err
		}
		var nextPendingIndexes []int
		for i, recordIndex := range pendingIndexes {
			if slices.Contains(lostIndexes, i) {
				nextPendingIndexes = append(nextPendingIndexes, recordIndex)
				continue
			}
			r.meta.copyRecord(ptrs[recordIndex], roundPtrs[i])
		}
		if len(nextPendingIndexes) == 0 {
			return nil
		}
		if attempt == modifyMaxAttempts {
			return fmt.Errorf("%w: %s, %d records, %d attempts", ErrWriteConflict, r.meta.recordType.Name(), len(nextPendingIndexes), modifyMaxAttempts)
		}
		pendingIndexes = nextPendingIndexes
		time.Sleep(ConflictBackoff(attempt))
	}
}

// storedRecord is a record as stored, decoded, with its item's changelog (cgl).
type storedRecord struct {
	ptr       unsafe.Pointer
	changelog []byte
}

// storedVersions reads (consistently) the stored version of each record, keyed by recordKey, so its
// hidden rows, GroupBy counters and DataFrame changelog can be diffed. Nil without them.
func (r *repoCore) storedVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) (map[string]storedRecord, error) {
	if !r.meta.readsStoredVersion() {
		return nil, nil
	}
	keys := make([]map[string]types.AttributeValue, len(ptrs))
	for i, ptr := range ptrs {
		keys[i] = r.meta.keyOnly(ptr)
	}
	items, _, err := batchGet(client, keys, true)
	if err != nil {
		return nil, err
	}
	storedByKey := make(map[string]storedRecord, len(items))
	for _, item := range items {
		recordPtr, err := r.meta.decodeRecord(item)
		if err != nil {
			return nil, err
		}
		storedByKey[r.meta.recordKey(recordPtr)] = storedRecord{ptr: recordPtr, changelog: changelogOf(item)}
	}
	return storedByKey, nil
}

// recordKey identifies a record by its pk and sk (pk is all digits, so the join is unambiguous).
func (m *tableMeta) recordKey(ptr unsafe.Pointer) string {
	return m.pkValue(ptr) + keySeparator + m.skValue(ptr)
}

// batchWriteAll writes any number of requests in BatchWriteItem chunks of 25, in order.
func (r *repoCore) batchWriteAll(ctx context.Context, client *dynamodb.Client, writes []types.WriteRequest) error {
	for start := 0; start < len(writes); start += 25 {
		if err := r.batchWrite(ctx, client, writes[start:min(start+25, len(writes))]); err != nil {
			return err
		}
	}
	return nil
}

// batchWrite issues one BatchWriteItem and retries any unprocessed items.
func (r *repoCore) batchWrite(ctx context.Context, client *dynamodb.Client, writes []types.WriteRequest) error {
	table := tableName()
	req := map[string][]types.WriteRequest{table: writes}
	for attempt := 0; attempt < 8; attempt++ {
		out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: req,
		})
		if err != nil {
			return err
		}
		if len(out.UnprocessedItems) == 0 {
			return nil
		}
		req = out.UnprocessedItems
	}
	return fmt.Errorf("db: BatchWriteItem left items unprocessed after retries")
}

// Delete removes the item identified by the record's partition + sort fields.
// Only the key fields of `record` need to be populated. The item comes back from
// the DeleteItem (ALL_OLD), with no read before it: its hidden rows are deleted
// after it, its GroupBy counters move, and its logs get a deleted entry one
// Version above it. The record's Updated is set to the delete's.
func (r *Repo[T, E]) Delete(record *E) error { return r.delete(unsafe.Pointer(record)) }

func (r *repoCore) delete(ptr unsafe.Pointer) error {
	// A record that vanishes takes its changelog with it, so the frames could never move it out.
	if r.meta.frames != nil {
		return fmt.Errorf("db: %s has DataFrames and is never deleted: set its Status to 0", r.meta.recordType.Name())
	}
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}
	out, err := client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName:    aws.String(tableName()),
		Key:          r.meta.keyOnly(ptr),
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		return err
	}
	// The delete's Updated: the value of its by-IDs slot.
	r.meta.stampUpdated([]unsafe.Pointer{ptr})
	if len(out.Attributes) > 0 {
		storedPtr, err := r.meta.decodeRecord(out.Attributes)
		if err != nil {
			return err
		}
		_, arrayRowDeletes := r.meta.arrayIndexWrites(storedPtr, nil, nil)
		if err := r.batchWriteAll(context.Background(), client, arrayRowDeletes); err != nil {
			return err
		}
		groupDeltas := map[string]*groupCounterDelta{}
		if err := r.meta.addGroupCounterDeltas(groupDeltas, storedPtr, nil); err != nil {
			return err
		}
		if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
			return err
		}
		landedWrites := []deltaLogWrite{{storedPtr: storedPtr, version: cache.NextVersion(r.meta.version.getI64(storedPtr))}}
		if err := r.meta.appendDeltaLogs(landedWrites); err != nil {
			return err
		}
		if err := r.meta.syncTextSearch(landedWrites); err != nil {
			return err
		}
	}
	return r.meta.setSlotsUpdated([]unsafe.Pointer{ptr})
}

// ── Reads ────────────────────────────────────────────────────────────────────

// Get fetches one item by its full key. `key` only needs its partition and sort
// fields set. It returns (nil, nil) when the item does not exist.
func (r *Repo[T, E]) Get(key E) (*E, error) {
	recordPtr, err := r.get(unsafe.Pointer(&key))
	return (*E)(recordPtr), err
}

func (r *repoCore) get(keyPtr unsafe.Pointer) (unsafe.Pointer, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName()),
		Key:       r.meta.keyOnly(keyPtr),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	return r.meta.decodeRecord(out.Item)
}

// GetMany reads records by key with a consistent BatchGetItem (100 keys per call;
// each item is billed rounded up to 4 KB): the read before a PutManyIfVersion.
// keys only need their partition and sort fields set. Missing records are left
// out, and the order is not the keys' order.
func (r *Repo[T, E]) GetMany(keys []E) ([]E, error) {
	return getManyAs(&r.repoCore, keys, false)
}

// GetManyForUpdate is GetMany for records about to go through PutManyIfVersion:
// it also keeps their stored blobs in the process write cache (write_cache.go),
// so that write diffs their hidden rows without reading them again. On a table
// without fan-out or delta indexes it is GetMany.
func (r *Repo[T, E]) GetManyForUpdate(keys []E) ([]E, error) {
	return getManyAs(&r.repoCore, keys, true)
}

// getManyAs is getMany into a typed slice: nil for no keys, never nil otherwise.
func getManyAs[E any](r *repoCore, keys []E, rememberForUpdate bool) ([]E, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	records := make([]E, 0, len(keys))
	if err := r.getMany(pointersOf(keys), rememberForUpdate, appendTo(&records)); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *repoCore) getMany(keyPtrs []unsafe.Pointer, rememberForUpdate bool, appendRecord recordSink) error {
	client, err := Client()
	if err != nil {
		return err
	}
	itemKeys := make([]map[string]types.AttributeValue, len(keyPtrs))
	for i, keyPtr := range keyPtrs {
		itemKeys[i] = r.meta.keyOnly(keyPtr)
	}
	items, _, err := batchGet(client, itemKeys, true)
	if err != nil {
		return err
	}
	// Without hidden rows or GroupBy a write diffs nothing, so there is nothing worth keeping.
	if rememberForUpdate && r.meta.readsStoredVersion() {
		rememberStoredItems(items)
	}
	for _, item := range items {
		recordPtr, err := r.meta.decodeRecord(item)
		if err != nil {
			return err
		}
		appendRecord(recordPtr)
	}
	return nil
}

// Query starts a new statically-typed query for this entity. It is strict:
// every predicate must be served by the key condition, and Exec fails on one
// that would have to be filtered in memory (use QueryScan for that).
func (r *Repo[T, E]) Query() *QueryBuilder[E] {
	return &QueryBuilder[E]{queryState{meta: r.meta}}
}

// QueryScan is Query plus an in-memory filter: one index still has to serve it
// (partition equality, a full GSI key or a Contains), and the predicates no key
// can serve are evaluated on each decoded record. It pays for every row the index
// range holds, not only the ones it returns, so it fails once it has read 5 MB
// and still has more to read (see queryScanMaxReadUnits).
func (r *Repo[T, E]) QueryScan() *QueryBuilder[E] {
	return &QueryBuilder[E]{queryState{meta: r.meta, allowsMemoryFilter: true}}
}

// TopN returns up to n records from a single partition, in ascending sort-key
// order (chain a raw Query().Desc() if you want newest-first). Pass one value
// per Partition column, in schema order; pass none for an entity that declares
// no Partition column (the whole entity lives in one partition).
func (r *Repo[T, E]) TopN(n int32, partitionValues ...any) ([]E, error) {
	var out []E
	if err := r.topN(n, partitionValues, appendTo(&out)); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *repoCore) topN(n int32, partitionValues []any, appendRecord recordSink) error {
	if len(partitionValues) != len(r.meta.partition) {
		return fmt.Errorf("db: %s has %d partition column(s), got %d value(s)",
			r.meta.recordType.Name(), len(r.meta.partition), len(partitionValues))
	}
	q := &queryState{meta: r.meta, limit: n}
	for i, kc := range r.meta.partition {
		q.preds = append(q.preds, predicate{field: kc.fieldName, op: opEq, v1: partitionValues[i]})
	}
	return q.exec(appendRecord)
}

// Scan returns up to limit records of this entity from the base table,
// regardless of partition (scanItems) — handy for admin/debug listings, not for
// hot paths. Order is unspecified (physical). limit <= 0 means no cap.
func (r *Repo[T, E]) Scan(limit int32) ([]E, error) {
	var out []E
	err := r.scan(limit, appendTo(&out))
	return out, err
}

func (r *repoCore) scan(limit int32, appendRecord recordSink) error {
	client, err := Client()
	if err != nil {
		return err
	}
	return scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange()}, nil, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			recordPtr, err := r.meta.decodeRecord(item)
			if err != nil {
				return err
			}
			if recordCount := appendRecord(recordPtr); limit > 0 && int32(recordCount) >= limit {
				return errStopScan
			}
		}
		return nil
	})
}

// pkRange is a span of pks one read covers: an entity's base rows, or its hidden rows (the fan-out,
// delta and local index rows and the GroupBy counters).
type pkRange struct{ lowest, highest string }

// baseRowsRange spans the pks of the entity's records: the single pk TableID without partition columns.
func (m *tableMeta) baseRowsRange() pkRange {
	lowestPK, highestPK := m.partitionRange(0)
	return pkRange{lowestPK, highestPK}
}

// hiddenRowsRange spans the pks of the entity's hidden rows: a base pk ‖ 3 digits.
func (m *tableMeta) hiddenRowsRange() pkRange {
	lowestPK, highestPK := m.partitionRange(arrayIndexColumnIDDigits)
	return pkRange{lowestPK, highestPK}
}

// errStopScan, returned by an onPage of scanItems, ends the read early without an error.
var errStopScan = errors.New("db: scan stopped")

// scanItems reads every item whose pk falls in one of the ranges and hands them to onPage a page at
// a time, with only the projected attributes (all of them when projection is empty). A single range
// of one pk (an entity without partition columns) is a Query; anything else is a table Scan filtered
// on the ranges, which DynamoDB bills for the whole shared table, filtered or not.
func scanItems(client *dynamodb.Client, table string, ranges []pkRange, projection []string, onPage func(items []map[string]types.AttributeValue) error) error {
	names := map[string]string{"#pk": "pk"}
	values := map[string]types.AttributeValue{}
	var projectionExpression *string
	if len(projection) > 0 {
		placeholders := make([]string, len(projection))
		for i, attrName := range projection {
			placeholders[i] = "#p" + strconv.Itoa(i)
			names[placeholders[i]] = attrName
		}
		projectionExpression = aws.String(strings.Join(placeholders, ", "))
	}

	var readPage func(startKey map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error)
	if len(ranges) == 1 && ranges[0].lowest == ranges[0].highest {
		values[":pk"] = &types.AttributeValueMemberN{Value: ranges[0].lowest}
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String("#pk = :pk"),
			ProjectionExpression:      projectionExpression,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		}
		readPage = func(startKey map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
			input.ExclusiveStartKey = startKey
			out, err := client.Query(context.Background(), input)
			if err != nil {
				return nil, nil, err
			}
			return out.Items, out.LastEvaluatedKey, nil
		}
	} else {
		rangeConditions := make([]string, len(ranges))
		for i, span := range ranges {
			placeholder := strconv.Itoa(i)
			values[":lo"+placeholder] = &types.AttributeValueMemberN{Value: span.lowest}
			values[":hi"+placeholder] = &types.AttributeValueMemberN{Value: span.highest}
			rangeConditions[i] = "#pk BETWEEN :lo" + placeholder + " AND :hi" + placeholder
		}
		input := &dynamodb.ScanInput{
			TableName:                 aws.String(table),
			FilterExpression:          aws.String(strings.Join(rangeConditions, " OR ")),
			ProjectionExpression:      projectionExpression,
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		}
		readPage = func(startKey map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
			input.ExclusiveStartKey = startKey
			out, err := client.Scan(context.Background(), input)
			if err != nil {
				return nil, nil, err
			}
			return out.Items, out.LastEvaluatedKey, nil
		}
	}

	var startKey map[string]types.AttributeValue
	for {
		items, lastKey, err := readPage(startKey)
		if err != nil {
			return err
		}
		if err := onPage(items); err != nil {
			if errors.Is(err, errStopScan) {
				return nil
			}
			return err
		}
		if len(lastKey) == 0 {
			return nil
		}
		startKey = lastKey
	}
}
