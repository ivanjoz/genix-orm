package dynamo

import (
	"context"
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
// before it is stored.
func (r *Repo[T, E]) Put(record *E) error {
	if err := r.meta.assignAutoIDs([]unsafe.Pointer{unsafe.Pointer(record)}); err != nil {
		return err
	}
	item, err := r.meta.marshalItem(unsafe.Pointer(record), record)
	if err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}
	_, err = client.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(tableName()),
		Item:      item,
	})
	return err
}

// PutMany upserts records in batches of 25 (the BatchWriteItem limit). When the
// entity uses autoincrement, every record whose ID is still zero is assigned one
// in a single sequence reservation before the batch is written.
func (r *Repo[T, E]) PutMany(records []E) error {
	client, err := Client()
	if err != nil {
		return err
	}
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	if err := r.meta.assignAutoIDs(ptrs); err != nil {
		return err
	}
	const batchSize = 25
	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		writes := make([]types.WriteRequest, 0, end-start)
		for i := start; i < end; i++ {
			item, err := r.meta.marshalItem(unsafe.Pointer(&records[i]), &records[i])
			if err != nil {
				return err
			}
			writes = append(writes, types.WriteRequest{
				PutRequest: &types.PutRequest{Item: item},
			})
		}
		if err := r.batchWrite(client, writes); err != nil {
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
// Only the key fields of `record` need to be populated.
func (r *Repo[T, E]) Delete(record *E) error {
	client, err := Client()
	if err != nil {
		return err
	}
	_, err = client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName()),
		Key:       r.meta.keyOnly(unsafe.Pointer(record)),
	})
	return err
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

// Query starts a new statically-typed query for this entity.
func (r *Repo[T, E]) Query() *QueryBuilder[E] {
	return &QueryBuilder[E]{meta: r.meta}
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
// namespace — handy for admin/debug listings, not for hot paths. Order is
// unspecified (physical). limit <= 0 means no cap.
func (r *Repo[T, E]) Scan(limit int32) ([]E, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}

	filter := "begins_with(#pk, :p)"
	prefix := r.meta.entity + keySeparator
	if len(r.meta.partition) == 0 {
		filter, prefix = "#pk = :p", r.meta.entity
	}

	input := &dynamodb.ScanInput{
		TableName:                 aws.String(tableName()),
		FilterExpression:          aws.String(filter),
		ExpressionAttributeNames:  map[string]string{"#pk": "pk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": &types.AttributeValueMemberS{Value: prefix}},
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
