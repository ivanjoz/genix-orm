package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Controller: type-erased entity operations
//
// Mirrors genix's ScyllaController / ScyllaControllerInterface. A Controller is
// a non-generic handle to one entity's admin/maintenance operations, so a
// heterogeneous set of entities can be driven by a single command — e.g. wiping
// every entity in a maintenance task:
//
//	controllers := []db.Controller{ models.Products, models.Orders }
//	for _, c := range controllers {
//	    n, err := c.DeleteRecordsAll()
//	    ...
//	}
//
// *Repo[T,E] implements Controller, so the same value you use for reads/writes is
// also its controller. NewController builds one for a table type when you don't
// already hold a Repo (the analogue of genix's makeDBController[T,E]()).
// ─────────────────────────────────────────────────────────────────────────────

// Controller exposes entity-level operations behind a non-generic interface.
type Controller interface {
	// Entity is the entity's name (its keys are namespaced by the TableID).
	Entity() string
	// TableName is the physical DynamoDB table (shared by every entity).
	TableName() string
	// Schema is the serializable schema description (see introspect.go).
	Schema() TableSchema
	// DeleteRecordsAll removes every record of this entity and returns the number
	// deleted. See the method on *Repo for details.
	DeleteRecordsAll() (int, error)
	// QueryRecords runs a dynamic, type-erased query and returns the matching
	// records as JSON-serializable values, newest first when desc is set. See the
	// method on *Repo for the strict key rules it enforces.
	QueryRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error)
	// QueryScanRecords is QueryRecords with QueryScan's in-memory filter for the
	// predicates no key serves.
	QueryScanRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error)
	// DecodeRecords decodes a JSON array into this entity's records (E values) and
	// checks that each one builds its keys, so a bad payload fails before a write.
	DecodeRecords(recordsJSON []byte) ([]any, error)
	// PutRecords upserts E values (from DecodeRecords or QueryRecords) through
	// PutMany and returns them as written, autoincrement IDs assigned.
	PutRecords(records []any) ([]any, error)
	// RebuildGroupsAll recomputes every GroupBy counter from the records and returns
	// how many it rewrote; an error when the entity declares no GroupBy (group_by.go).
	RebuildGroupsAll() (int, error)
}

// NewController compiles the schema (like NewRepo) and returns it as a
// Controller. Use it to build a heterogeneous []Controller for admin commands.
func NewController[T any, E any]() Controller { return NewRepo[T, E]() }

// Entity returns the entity's name.
func (r *Repo[T, E]) Entity() string { return r.meta.entity }

// TableName returns the physical DynamoDB table name.
func (r *Repo[T, E]) TableName() string { return tableName() }

// DeleteRecordsAll scans this entity's key namespace and deletes every record,
// returning the count removed. It projects only the pk/sk key attributes (never
// decoding the "d" blob) and deletes in BatchWriteItem batches of 25 with the
// same unprocessed-item retry as PutMany.
//
// It is scoped to this entity's pk ranges (its base rows and its array index
// rows), so sibling entities (and the internal sequence counters) in the shared
// table are untouched, and it keeps the by-IDs slot-versions items that share the
// array range. The returned count includes array index rows and GroupBy counters.
// This is a destructive maintenance operation — there is no undo.
func (r *Repo[T, E]) DeleteRecordsAll() (int, error) {
	client, err := Client()
	if err != nil {
		return 0, err
	}

	lowestPK, highestPK := r.meta.partitionRange(0)
	lowestArrayPK, highestArrayPK := r.meta.partitionRange(arrayIndexColumnIDDigits)
	input := &dynamodb.ScanInput{
		TableName:                aws.String(tableName()),
		FilterExpression:         aws.String("#pk BETWEEN :lo AND :hi OR #pk BETWEEN :arrayLo AND :arrayHi"),
		ProjectionExpression:     aws.String("#pk, #sk"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lo":      &types.AttributeValueMemberN{Value: lowestPK},
			":hi":      &types.AttributeValueMemberN{Value: highestPK},
			":arrayLo": &types.AttributeValueMemberN{Value: lowestArrayPK},
			":arrayHi": &types.AttributeValueMemberN{Value: highestArrayPK},
		},
	}

	deleted := 0
	var batch []types.WriteRequest
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.batchWrite(client, batch); err != nil {
			return err
		}
		deleted += len(batch)
		batch = batch[:0]
		return nil
	}

	for {
		res, err := client.Scan(context.Background(), input)
		if err != nil {
			return deleted, err
		}
		for _, item := range res.Items {
			// Slot versions survive a wipe: counting again from 0 could hand a
			// recreated record a version a client still holds for the old one. The
			// GroupBy counters sharing their pk go with the records they count.
			if isSlotVersionsPK(item["pk"].(*types.AttributeValueMemberN).Value) && item["sk"].(*types.AttributeValueMemberS).Value == slotVersionsSK {
				continue
			}
			batch = append(batch, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{"pk": item["pk"], "sk": item["sk"]},
				},
			})
			if len(batch) == 25 {
				if err := flush(); err != nil {
					return deleted, err
				}
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = res.LastEvaluatedKey
	}
	if err := flush(); err != nil {
		return deleted, err
	}
	return deleted, nil
}

// DecodeRecords decodes a JSON array of records (keyed by the record's json
// tags) and builds every key of each one: the item and its fan-out rows. A key
// value that overflows its Size(bits) or is negative panics on the write path;
// here it comes back as an error, so a type-erased caller can validate a payload
// without writing it.
func (r *Repo[T, E]) DecodeRecords(recordsJSON []byte) (records []any, err error) {
	var decodedRecords []E
	if err := json.Unmarshal(recordsJSON, &decodedRecords); err != nil {
		return nil, fmt.Errorf("db: %s records are not a valid JSON array: %w", r.meta.recordType.Name(), err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			records, err = nil, fmt.Errorf("%v", recovered)
		}
	}()
	records = make([]any, len(decodedRecords))
	for i := range decodedRecords {
		ptr := unsafe.Pointer(&decodedRecords[i])
		if _, err := r.meta.marshalItem(ptr, &decodedRecords[i]); err != nil {
			return nil, fmt.Errorf("db: %s record %d: %w", r.meta.recordType.Name(), i, err)
		}
		for arrayIndex := range r.meta.arrayIndexes {
			r.meta.arrayRowSKs(&r.meta.arrayIndexes[arrayIndex], ptr)
		}
		// Builds the group keys and checks every float GroupBy value fits its sum.
		if err := r.meta.addGroupCounterDeltas(map[string]*groupCounterDelta{}, nil, ptr); err != nil {
			return nil, fmt.Errorf("db: %s record %d: %w", r.meta.recordType.Name(), i, err)
		}
		records[i] = decodedRecords[i]
	}
	return records, nil
}

// PutRecords is PutMany for type-erased records: each must be an E value.
func (r *Repo[T, E]) PutRecords(records []any) ([]any, error) {
	typedRecords := make([]E, len(records))
	for i, record := range records {
		typedRecord, isRecord := record.(E)
		if !isRecord {
			return nil, fmt.Errorf("db: %s PutRecords got a %T at %d", r.meta.recordType.Name(), record, i)
		}
		typedRecords[i] = typedRecord
	}
	if err := r.PutMany(typedRecords); err != nil {
		return nil, err
	}
	writtenRecords := make([]any, len(typedRecords))
	for i := range typedRecords {
		writtenRecords[i] = typedRecords[i]
	}
	return writtenRecords, nil
}
