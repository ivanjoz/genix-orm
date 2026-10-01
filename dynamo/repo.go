package dynamo

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

// PutIfAbsent writes the record only when no item with its key exists yet, and
// reports false (without an error) when the key was already taken. The check and
// the write are one conditional PutItem, so of two concurrent writers of the same
// key exactly one wins — the primitive for "enqueue once" and "claim once".
// Autoincrement IDs are assigned as in Put.
func (r *Repo[T, E]) PutIfAbsent(record *E) (bool, error) {
	ptr := unsafe.Pointer(record)
	if _, err := r.meta.prepareWrite([]unsafe.Pointer{ptr}); err != nil {
		return false, err
	}
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
	arrayRowPuts, _ := r.meta.arrayIndexWrites(nil, ptr, item[dataColumn].(*types.AttributeValueMemberB).Value)
	if err := r.batchWriteAll(client, arrayRowPuts); err != nil {
		return true, err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return true, err
	}
	return true, r.meta.bumpSlotVersions(client, []unsafe.Pointer{ptr})
}

// PutMany upserts records in batches of 25 (the BatchWriteItem limit). When the
// entity uses autoincrement, every record whose ID is still zero is assigned one
// in a single sequence reservation before the batch is written, and the managed
// Updated / UpdatedVersion are stamped (delta.go). With array or delta indexes it
// first reads the stored versions and writes in three passes: new rows, then the
// base items, then stale rows (see array_index.go). GroupBy counters are ADDed
// after that (group_by.go), and with SaveUpdatedVersion the touched slot versions
// are bumped last. A record whose ID this call assigned is new by construction, so
// its stored version is not read.
func (r *Repo[T, E]) PutMany(records []E) error { return r.putMany(records, false) }

// InsertMany is PutMany for records the caller knows are not stored yet, such as
// rows keyed by an ID reserved in the same operation (AssignIDs): it skips the read
// of the stored versions, one round trip. A record that was in fact stored keeps
// its old hidden rows and counts twice in its GroupBy groups until RebuildGroups.
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
	client, err := Client()
	if err != nil {
		return err
	}
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	isAssignedNow, err := r.meta.prepareWrite(ptrs)
	if err != nil {
		return err
	}
	var possiblyStoredPtrs []unsafe.Pointer
	for _, ptr := range ptrs {
		if !areAllNew && !isAssignedNow[ptr] {
			possiblyStoredPtrs = append(possiblyStoredPtrs, ptr)
		}
	}
	storedByKey, err := r.storedVersions(client, possiblyStoredPtrs)
	if err != nil {
		return err
	}

	baseWrites := make([]types.WriteRequest, 0, len(records))
	var arrayRowPuts, arrayRowDeletes []types.WriteRequest
	groupDeltas := map[string]*groupCounterDelta{}
	for i := range records {
		item, err := r.meta.marshalItem(ptrs[i], &records[i])
		if err != nil {
			return err
		}
		baseWrites = append(baseWrites, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
		storedPtr := storedByKey[r.meta.recordKey(ptrs[i])]
		if err := r.meta.addGroupCounterDeltas(groupDeltas, storedPtr, ptrs[i]); err != nil {
			return err
		}
		if len(r.meta.arrayIndexes) == 0 {
			continue
		}
		blob := item[dataColumn].(*types.AttributeValueMemberB).Value
		puts, deletes := r.meta.arrayIndexWrites(storedPtr, ptrs[i], blob)
		arrayRowPuts = append(arrayRowPuts, puts...)
		arrayRowDeletes = append(arrayRowDeletes, deletes...)
	}

	for _, writes := range [][]types.WriteRequest{arrayRowPuts, baseWrites, arrayRowDeletes} {
		if err := r.batchWriteAll(client, writes); err != nil {
			return err
		}
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return err
	}
	return r.meta.bumpSlotVersions(client, ptrs)
}

// storedVersions reads (consistently) the stored version of each record, keyed
// by recordKey, so its hidden rows and GroupBy counters can be diffed. Nil without them.
func (r *Repo[T, E]) storedVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) (map[string]unsafe.Pointer, error) {
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
	storedByKey := make(map[string]unsafe.Pointer, len(items))
	for _, item := range items {
		storedRecord := new(E)
		if err := r.meta.unmarshalItem(item, storedRecord); err != nil {
			return nil, err
		}
		storedByKey[r.meta.recordKey(unsafe.Pointer(storedRecord))] = unsafe.Pointer(storedRecord)
	}
	return storedByKey, nil
}

// recordKey identifies a record by its pk and sk (pk is all digits, so the join is unambiguous).
func (m *tableMeta) recordKey(ptr unsafe.Pointer) string {
	return m.pkValue(ptr) + keySeparator + m.skValue(ptr)
}

// batchWriteAll writes any number of requests in BatchWriteItem chunks of 25.
func (r *Repo[T, E]) batchWriteAll(client *dynamodb.Client, writes []types.WriteRequest) error {
	for start := 0; start < len(writes); start += 25 {
		if err := r.batchWrite(client, writes[start:min(start+25, len(writes))]); err != nil {
			return err
		}
	}
	return nil
}

// batchWrite issues one BatchWriteItem and retries any unprocessed items.
func (r *Repo[T, E]) batchWrite(client *dynamodb.Client, writes []types.WriteRequest) error {
	table := tableName()
	req := map[string][]types.WriteRequest{table: writes}
	for attempt := 0; attempt < 8; attempt++ {
		out, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
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
// Only the key fields of `record` need to be populated: with array indexes the
// stored version is read to find its rows, which are deleted after the item.
func (r *Repo[T, E]) Delete(record *E) error {
	client, err := Client()
	if err != nil {
		return err
	}
	ptr := unsafe.Pointer(record)
	storedByKey, err := r.storedVersions(client, []unsafe.Pointer{ptr})
	if err != nil {
		return err
	}
	var arrayRowDeletes []types.WriteRequest
	groupDeltas := map[string]*groupCounterDelta{}
	if storedPtr := storedByKey[r.meta.recordKey(ptr)]; storedPtr != nil {
		_, arrayRowDeletes = r.meta.arrayIndexWrites(storedPtr, nil, nil)
		if err := r.meta.addGroupCounterDeltas(groupDeltas, storedPtr, nil); err != nil {
			return err
		}
	}
	_, err = client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName()),
		Key:       r.meta.keyOnly(ptr),
	})
	if err != nil {
		return err
	}
	if err := r.batchWriteAll(client, arrayRowDeletes); err != nil {
		return err
	}
	if err := r.meta.applyGroupCounterDeltas(client, groupDeltas); err != nil {
		return err
	}
	return r.meta.bumpSlotVersions(client, []unsafe.Pointer{ptr})
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
// regardless of partition. It is a table Scan filtered to the entity's pk
// range — handy for admin/debug listings, not for hot paths. Order is
// unspecified (physical). limit <= 0 means no cap.
func (r *Repo[T, E]) Scan(limit int32) ([]E, error) {
	// Without partition columns the whole entity is the single pk TableID: a Query.
	if len(r.meta.partition) == 0 {
		var out []E
		err := r.Query().Limit(limit).Exec(&out)
		return out, err
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}

	lowestPK, highestPK := r.meta.partitionRange(0)
	input := &dynamodb.ScanInput{
		TableName:                aws.String(tableName()),
		FilterExpression:         aws.String("#pk BETWEEN :lo AND :hi"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lo": &types.AttributeValueMemberN{Value: lowestPK},
			":hi": &types.AttributeValueMemberN{Value: highestPK},
		},
	}

	var out []E
	for {
		res, err := client.Scan(context.Background(), input)
		if err != nil {
			return nil, err
		}
		for _, item := range res.Items {
			var record E
			if err := r.meta.unmarshalItem(item, &record); err != nil {
				return nil, err
			}
			out = append(out, record)
			if limit > 0 && int32(len(out)) >= limit {
				return out, nil
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		input.ExclusiveStartKey = res.LastEvaluatedKey
	}
}
