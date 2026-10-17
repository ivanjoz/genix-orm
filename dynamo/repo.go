package dynamo

import (
	"context"
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db/cache"
	"maps"
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
// ─────────────────────────────────────────────────────────────────────────────

type Repo[T any, E any] struct {
	meta *tableMeta
	// T exposes the named column handles for use in query predicates.
	T T
}

// NewRepo compiles (and caches) the schema and returns a ready repo.
func NewRepo[T any, E any]() *Repo[T, E] {
	meta, table := getOrCompile[T, E]()
	return &Repo[T, E]{meta: meta, T: table}
}

// Put upserts a single record. When the entity uses autoincrement and the
// record's ID is still zero, a fresh ID is reserved and written into *record
// before it is stored. It is PutMany of one, so array index rows sync the same way.
func (r *Repo[T, E]) Put(record *E) error {
	records := []E{*record}
	err := r.PutMany(records)
	*record = records[0]
	return err
}

// PutIfAbsent writes the record, at Version 1, only when no item with its key
// exists yet, and reports false (without an error) when the key was already taken.
// The check and the write are one conditional PutItem, so of two concurrent writers
// of the same key exactly one wins — the primitive for "enqueue once" and "claim
// once". Autoincrement IDs are assigned as in Put.
func (r *Repo[T, E]) PutIfAbsent(record *E) (bool, error) {
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return false, err
	}
	ptr := unsafe.Pointer(record)
	if _, err := r.meta.assignAutoIDs([]unsafe.Pointer{ptr}); err != nil {
		return false, err
	}
	r.meta.stampUpdated([]unsafe.Pointer{ptr})
	r.meta.version.setI64(ptr, 1)
	item, err := r.meta.marshalItem(ptr, record)
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
func (r *Repo[T, E]) PutMany(records []E) error { return r.putMany(records, false) }

// InsertMany is PutMany for records the caller knows are not stored yet, such as
// rows keyed by an ID reserved in the same operation (AssignIDs): it writes them at
// Version 1 with BatchWriteItem, reading nothing. A record that was in fact stored
// is overwritten at Version 1, keeps its old hidden rows and counts twice in its
// GroupBy groups until Reprocess.
func (r *Repo[T, E]) InsertMany(records []E) error { return r.putMany(records, true) }

// AssignIDs reserves and sets the autoincrement ID of every record whose ID is
// still zero, as Put would, so related records can be keyed by it before anything
// is written. Write the records with InsertMany: their IDs are new.
func (r *Repo[T, E]) AssignIDs(records []E) error {
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	_, err := r.meta.assignAutoIDs(ptrs)
	return err
}

func (r *Repo[T, E]) putMany(records []E, areAllNew bool) error {
	if len(records) == 0 {
		return nil
	}
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
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
		return r.putOverStoredVersions(client, records, assignedPtrs)
	}
}

// insertNew writes records known to be new at Version 1, with BatchWriteItem: the new hidden rows
// first, then the base items (array_index.go), then the GroupBy counters, the logs and the slots.
func (r *Repo[T, E]) insertNew(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	baseWrites := make([]types.WriteRequest, 0, len(ptrs))
	var arrayRowPuts []types.WriteRequest
	logWrites := make([]deltaLogWrite, len(ptrs))
	groupDeltas := map[string]*groupCounterDelta{}
	for i, ptr := range ptrs {
		r.meta.version.setI64(ptr, 1)
		item, err := r.meta.marshalItem(ptr, (*E)(ptr))
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
func (r *Repo[T, E]) putRaisingVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	items := make([]map[string]types.AttributeValue, len(ptrs))
	for i, ptr := range ptrs {
		var err error
		if items[i], err = r.meta.marshalItem(ptr, (*E)(ptr)); err != nil {
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
func (r *Repo[T, E]) putOverStoredVersions(client *dynamodb.Client, records []E, assignedPtrs []unsafe.Pointer) error {
	isAssignedNow := map[unsafe.Pointer]bool{}
	for _, assignedPtr := range assignedPtrs {
		isAssignedNow[assignedPtr] = true
	}
	pendingIndexes := make([]int, len(records))
	for i := range pendingIndexes {
		pendingIndexes[i] = i
	}
	for attempt := 1; ; attempt++ {
		round := make([]E, len(pendingIndexes))
		roundPtrs := make([]unsafe.Pointer, len(pendingIndexes))
		var possiblyStoredPtrs []unsafe.Pointer
		for i, recordIndex := range pendingIndexes {
			round[i] = records[recordIndex]
			roundPtrs[i] = unsafe.Pointer(&round[i])
			if !isAssignedNow[unsafe.Pointer(&records[recordIndex])] {
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
		lostIndexes, err := r.putIfVersion(client, round, storedByKey)
		if err != nil {
			return err
		}
		var nextPendingIndexes []int
		for i, recordIndex := range pendingIndexes {
			if slices.Contains(lostIndexes, i) {
				nextPendingIndexes = append(nextPendingIndexes, recordIndex)
				continue
			}
			records[recordIndex] = round[i]
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
func (r *Repo[T, E]) storedVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) (map[string]storedRecord, error) {
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
		record := new(E)
		if err := r.meta.unmarshalItem(item, record); err != nil {
			return nil, err
		}
		storedByKey[r.meta.recordKey(unsafe.Pointer(record))] = storedRecord{ptr: unsafe.Pointer(record), changelog: changelogOf(item)}
	}
	return storedByKey, nil
}

// recordKey identifies a record by its pk and sk (pk is all digits, so the join is unambiguous).
func (m *tableMeta) recordKey(ptr unsafe.Pointer) string {
	return m.pkValue(ptr) + keySeparator + m.skValue(ptr)
}

// batchWriteAll writes any number of requests in BatchWriteItem chunks of 25, in order.
func (r *Repo[T, E]) batchWriteAll(ctx context.Context, client *dynamodb.Client, writes []types.WriteRequest) error {
	for start := 0; start < len(writes); start += 25 {
		if err := r.batchWrite(ctx, client, writes[start:min(start+25, len(writes))]); err != nil {
			return err
		}
	}
	return nil
}

// batchWrite issues one BatchWriteItem and retries any unprocessed items.
func (r *Repo[T, E]) batchWrite(ctx context.Context, client *dynamodb.Client, writes []types.WriteRequest) error {
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
func (r *Repo[T, E]) Delete(record *E) error {
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
	ptr := unsafe.Pointer(record)
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
		storedRecord := new(E)
		if err := r.meta.unmarshalItem(out.Attributes, storedRecord); err != nil {
			return err
		}
		storedPtr := unsafe.Pointer(storedRecord)
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

// Get fetches one item by its full key. `key` only needs its partition and sort
// fields set. It returns (nil, nil) when the item does not exist.
func (r *Repo[T, E]) Get(key E) (*E, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName()),
		Key:       r.meta.keyOnly(unsafe.Pointer(&key)),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	var record E
	if err := r.meta.unmarshalItem(out.Item, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

// GetMany reads records by key with a consistent BatchGetItem (100 keys per call;
// each item is billed rounded up to 4 KB): the read before a PutManyIfVersion.
// keys only need their partition and sort fields set. Missing records are left
// out, and the order is not the keys' order.
func (r *Repo[T, E]) GetMany(keys []E) ([]E, error) {
	return r.getMany(keys, false)
}

// GetManyForUpdate is GetMany for records about to go through PutManyIfVersion:
// it also keeps their stored blobs in the process write cache (write_cache.go),
// so that write diffs their hidden rows without reading them again. On a table
// without fan-out or delta indexes it is GetMany.
func (r *Repo[T, E]) GetManyForUpdate(keys []E) ([]E, error) {
	return r.getMany(keys, true)
}

func (r *Repo[T, E]) getMany(keys []E, rememberForUpdate bool) ([]E, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}
	itemKeys := make([]map[string]types.AttributeValue, len(keys))
	for i := range keys {
		itemKeys[i] = r.meta.keyOnly(unsafe.Pointer(&keys[i]))
	}
	items, _, err := batchGet(client, itemKeys, true)
	if err != nil {
		return nil, err
	}
	// Without hidden rows or GroupBy a write diffs nothing, so there is nothing worth keeping.
	if rememberForUpdate && r.meta.readsStoredVersion() {
		rememberStoredItems(items)
	}
	records := make([]E, len(items))
	for i, item := range items {
		if err := r.meta.unmarshalItem(item, &records[i]); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// Query starts a new statically-typed query for this entity. It is strict:
// every predicate must be served by the key condition, and Exec fails on one
// that would have to be filtered in memory (use QueryScan for that).
func (r *Repo[T, E]) Query() *QueryBuilder[E] {
	return &QueryBuilder[E]{meta: r.meta}
}

// QueryScan is Query plus an in-memory filter: one index still has to serve it
// (partition equality, a full GSI key or a Contains), and the predicates no key
// can serve are evaluated on each decoded record. It pays for every row the index
// range holds, not only the ones it returns, so it fails once it has read 5 MB
// and still has more to read (see queryScanMaxReadUnits).
func (r *Repo[T, E]) QueryScan() *QueryBuilder[E] {
	return &QueryBuilder[E]{meta: r.meta, allowsMemoryFilter: true}
}

// TopN returns up to n records from a single partition, in ascending sort-key
// order (chain a raw Query().Desc() if you want newest-first). Pass one value
// per Partition column, in schema order; pass none for an entity that declares
// no Partition column (the whole entity lives in one partition).
func (r *Repo[T, E]) TopN(n int32, partitionValues ...any) ([]E, error) {
	if len(partitionValues) != len(r.meta.partition) {
		return nil, fmt.Errorf("db: %s has %d partition column(s), got %d value(s)",
			r.meta.recordType.Name(), len(r.meta.partition), len(partitionValues))
	}
	q := r.Query()
	for i, kc := range r.meta.partition {
		q.preds = append(q.preds, predicate{field: kc.fieldName, op: opEq, v1: partitionValues[i]})
	}
	q.Limit(n)
	var out []E
	if err := q.Exec(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// Scan returns up to limit records of this entity from the base table,
// regardless of partition (scanItems) — handy for admin/debug listings, not for
// hot paths. Order is unspecified (physical). limit <= 0 means no cap.
func (r *Repo[T, E]) Scan(limit int32) ([]E, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	var out []E
	err = scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange()}, nil, func(items []map[string]types.AttributeValue) error {
		for _, item := range items {
			var record E
			if err := r.meta.unmarshalItem(item, &record); err != nil {
				return err
			}
			out = append(out, record)
			if limit > 0 && int32(len(out)) >= limit {
				return errStopScan
			}
		}
		return nil
	})
	return out, err
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
