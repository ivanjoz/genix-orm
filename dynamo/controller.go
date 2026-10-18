package dynamo

import (
	"context"
	"encoding/json"
	"fmt"
	"unsafe"

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
	// Reprocess rebuilds the hidden rows, GroupBy counters and by-IDs slots from the
	// records, writing only what differs; write false is a dry run (reprocess_derived.go).
	Reprocess(write bool) (DerivedReport, error)
	// MaterializeDataFrames builds or brings up to date every DataFrame (data_frame_backend.go).
	MaterializeDataFrames() error
	// RebuildDataFrames computes a DataFrame ("" every one) again from the records.
	RebuildDataFrames(frameName string) error
	// DropDataFrame deletes the state item and files of a frame the schema no longer declares.
	DropDataFrame(frameName string) (int, error)
	// The four below maintain the text index of an entity that declares TextSearch
	// (textsearch.go); on any other entity they do nothing. A partition is given by
	// one value per Partition column, as in TopN.

	// ReindexTextSearch rebuilds one partition's index from its active records.
	ReindexTextSearch(partitionValues ...any) error
	// ReindexTextSearchAll runs ReindexTextSearch on every partition that has records.
	ReindexTextSearchAll() error
	// CompactTextSearch folds the partition's pending index writes, for backends with a write log.
	CompactTextSearch(partitionValues ...any) error
	// DropTextSearch empties the partition's index.
	DropTextSearch(partitionValues ...any) error
	// BackfillCreated gives crt to every stored record without one; one-off (backfill_created.go).
	BackfillCreated(write bool) (int, []string, error)
	// ReprocessRecordsAny runs transform (record is the *E) over every record and writes
	// back the ones it changed, silently; write false is a dry run (reprocess_records.go).
	ReprocessRecordsAny(transform func(record any) (bool, error), write bool) (RecordsReport, error)
	// CopyRecordsFrom writes every record stored in another table into the current one, at the key
	// the schema builds now, skipping the keys already copied; write false is a dry run (copy_table.go).
	CopyRecordsFrom(sourceTable string, write bool) (RecordsReport, error)
	// ReprocessBlobs re-encodes in the current colbin every stored blob written by the previous
	// one, which decodeLegacy reads; write false is a dry run (reprocess.go).
	ReprocessBlobs(decodeLegacy func(data []byte, dst any) error, write bool) (ReprocessReport, error)
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
// table are untouched. The by-IDs slots go too: a recreated record is stamped a
// later Updated, so it can't match a value a client holds. The returned count
// includes array index rows and GroupBy counters. With TextSearch synced on write, the
// index of every partition it emptied is dropped too.
// This is a destructive maintenance operation — there is no undo.
func (r *Repo[T, E]) DeleteRecordsAll() (int, error) {
	if err := r.meta.checkTextSearchBackend(); err != nil {
		return 0, err
	}
	var emptiedPartitionsValues [][]uint64
	if r.meta.syncsTextSearch() {
		var err error
		if emptiedPartitionsValues, err = r.storedPartitionsValues(); err != nil {
			return 0, err
		}
	}
	deleted, err := r.deleteItemsAll()
	if err != nil {
		return deleted, err
	}
	for _, values := range emptiedPartitionsValues {
		partitionValues := make([]any, len(values))
		for i, value := range values {
			partitionValues[i] = value
		}
		if err := r.DropTextSearch(partitionValues...); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// deleteItemsAll deletes every base and hidden row of the entity.
func (r *Repo[T, E]) deleteItemsAll() (int, error) {
	client, err := Client()
	if err != nil {
		return 0, err
	}
	deleted := 0
	err = scanItems(client, tableName(), []pkRange{r.meta.baseRowsRange(), r.meta.hiddenRowsRange()}, []string{"pk", "sk"}, func(items []map[string]types.AttributeValue) error {
		deletes := make([]types.WriteRequest, len(items))
		for i, item := range items {
			deletes[i] = types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: itemKeyOf(item)}}
		}
		if err := r.batchWriteAll(context.Background(), client, deletes); err != nil {
			return err
		}
		deleted += len(deletes)
		return nil
	})
	return deleted, err
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
