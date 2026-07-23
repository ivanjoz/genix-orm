package dynamo

import (
	"context"

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
	// Entity is the entity's namespace/discriminator within the shared table.
	Entity() string
	// TableName is the physical DynamoDB table (shared by every entity).
	TableName() string
	// Schema is the serializable schema description (see introspect.go).
	Schema() TableSchema
	// DeleteRecordsAll removes every record of this entity and returns the number
	// deleted. See the method on *Repo for details.
	DeleteRecordsAll() (int, error)
	// QueryRecords runs a dynamic, type-erased query and returns the matching
	// records as JSON-serializable values. See the method on *Repo for the strict
	// key rules it enforces.
	QueryRecords(preds []QueryPredicate, limit int32) ([]any, error)
}

// NewController compiles the schema (like NewRepo) and returns it as a
// Controller. Use it to build a heterogeneous []Controller for admin commands.
func NewController[T any, E any]() Controller { return NewRepo[T, E]() }

// Entity returns the entity's namespace within the shared table.
func (r *Repo[T, E]) Entity() string { return r.meta.entity }

// TableName returns the physical DynamoDB table name.
func (r *Repo[T, E]) TableName() string { return tableName() }

// DeleteRecordsAll scans this entity's key namespace and deletes every record,
// returning the count removed. It projects only the pk/sk key attributes (never
// decoding the "d" blob) and deletes in BatchWriteItem batches of 25 with the
// same unprocessed-item retry as PutMany.
//
// It is scoped to this entity's pk prefix, so sibling entities (and the internal
// sequence counters) in the shared table are untouched. This is a destructive
// maintenance operation — there is no undo.
func (r *Repo[T, E]) DeleteRecordsAll() (int, error) {
	client, err := Client()
	if err != nil {
		return 0, err
	}

	// Same entity-namespace scoping as Repo.Scan.
	filter := "begins_with(#pk, :p)"
	prefix := r.meta.entity + keySeparator
	if len(r.meta.partition) == 0 {
		filter, prefix = "#pk = :p", r.meta.entity
	}

	input := &dynamodb.ScanInput{
		TableName:                 aws.String(tableName()),
		FilterExpression:          aws.String(filter),
		ProjectionExpression:      aws.String("#pk, #sk"),
		ExpressionAttributeNames:  map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":p": &types.AttributeValueMemberS{Value: prefix}},
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
