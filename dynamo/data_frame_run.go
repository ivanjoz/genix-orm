package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// dataframe.Table on DynamoDB: the frame's state item and the record reads of the
// runs, rebuilds and fresh reads, whose logic is the dataframe module's
// (materialize.go there). A frame's versions are Updated values (delta.go). The
// state item sits beside the GroupBy counters:
//
//	pk = base pk ‖ 000   (beside the GroupBy counters and the by-IDs slots)   sk = "f" + frame name
//	w        the snapshot of the files; absent: not built (a new frame, or a changed shape)
//	nx, nxt  the newest checkpoint: an Updated value, and when it was read (unix seconds)
//	px       the previous checkpoint, settled
//	ix       the days whose _ixt holds blocks to merge into their _idx, a number set
//	sh       the shape the files were built with
//
// Every read is consistent: the state item, the delta index (a hidden base-table
// row) and the records by key.
// ─────────────────────────────────────────────────────────────────────────────

// frameTable is dataframe.Table over the Repo's entity.
type frameTable[E any] struct{ meta *tableMeta }

func (table frameTable[E]) Now() time.Time { return Now() }

func (m *tableMeta) frameStateKey(frame *dataframe.Frame) map[string]types.AttributeValue {
	// Frames need an entity without Partition, so the base pk is the TableID.
	return itemKey(groupCountersPK(m.tableID), frameStateSKPrefix+frame.Name)
}

func frameStateOf(item map[string]types.AttributeValue) dataframe.State {
	state := dataframe.State{
		Snapshot:       numberAttrValue(item, "w"),
		Target:         numberAttrValue(item, "nx"),
		TargetTime:     numberAttrValue(item, "nxt"),
		PreviousTarget: numberAttrValue(item, "px"),
		Shape:          uint32(numberAttrValue(item, "sh")),
	}
	_, state.HasSnapshot = item["w"]
	_, state.HasTarget = item["nx"]
	_, state.HasPreviousTarget = item["px"]
	_, state.HasShape = item["sh"]
	if days, isSet := item["ix"].(*types.AttributeValueMemberNS); isSet {
		for _, day := range days.Value {
			parsedDay, _ := strconv.ParseInt(day, 10, 64)
			state.ExtendedDays = append(state.ExtendedDays, parsedDay)
		}
	}
	return state
}

func numberAttr(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func numberSetAttr(values []int64) types.AttributeValue {
	numbers := make([]string, len(values))
	for i, value := range values {
		numbers[i] = strconv.FormatInt(value, 10)
	}
	return &types.AttributeValueMemberNS{Value: numbers}
}

func (table frameTable[E]) ReadState(frame *dataframe.Frame) (dataframe.State, error) {
	client, err := Client()
	if err != nil {
		return dataframe.State{}, err
	}
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName()),
		Key:            table.meta.frameStateKey(frame),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return dataframe.State{}, err
	}
	return frameStateOf(out.Item), nil
}

func (table frameTable[E]) PushCheckpoint(frame *dataframe.Frame, seen dataframe.State, checkpoint, checkpointTime int64) (dataframe.State, bool, error) {
	client, err := Client()
	if err != nil {
		return seen, false, err
	}
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       table.meta.frameStateKey(frame),
		UpdateExpression:          aws.String("SET #nx = :nx, #nxt = :nxt"),
		ConditionExpression:       aws.String("attribute_not_exists(#nxt)"),
		ExpressionAttributeNames:  map[string]string{"#nx": "nx", "#nxt": "nxt"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":nx": numberAttr(checkpoint), ":nxt": numberAttr(checkpointTime)},
		ReturnValues:              types.ReturnValueAllNew,
	}
	if seen.HasTarget {
		input.UpdateExpression = aws.String("SET #px = #nx, #nx = :nx, #nxt = :nxt")
		input.ConditionExpression = aws.String("#nxt = :seenNxt")
		input.ExpressionAttributeNames["#px"] = "px"
		input.ExpressionAttributeValues[":seenNxt"] = numberAttr(seen.TargetTime)
	}
	out, err := client.UpdateItem(context.Background(), input)
	var pushedMeanwhileErr *types.ConditionalCheckFailedException
	if errors.As(err, &pushedMeanwhileErr) {
		return seen, false, nil
	}
	if err != nil {
		return seen, false, err
	}
	return frameStateOf(out.Attributes), true, nil
}

func (table frameTable[E]) ResetState(ctx context.Context, frame *dataframe.Frame, checkpoint, checkpointTime int64) error {
	return table.updateState(ctx, frame, "SET #sh = :sh, #nx = :nx, #nxt = :nxt REMOVE #w, #px, #ix",
		map[string]string{"#sh": "sh", "#nx": "nx", "#nxt": "nxt", "#w": "w", "#px": "px", "#ix": "ix"},
		map[string]types.AttributeValue{":sh": numberAttr(int64(frame.Shape)), ":nx": numberAttr(checkpoint), ":nxt": numberAttr(checkpointTime)})
}

func (table frameTable[E]) CommitSnapshot(ctx context.Context, frame *dataframe.Frame, snapshot int64, addedDays, mergedDays []int64) error {
	update := "SET #w = :w"
	names := map[string]string{"#w": "w"}
	values := map[string]types.AttributeValue{":w": numberAttr(snapshot)}
	// One of the two at a time: DynamoDB refuses an ADD and a DELETE of the same attribute.
	if len(addedDays) > 0 {
		update += " ADD #ix :ix"
		names["#ix"], values[":ix"] = "ix", numberSetAttr(addedDays)
	}
	if len(mergedDays) > 0 {
		update += " DELETE #ix :ix"
		names["#ix"], values[":ix"] = "ix", numberSetAttr(mergedDays)
	}
	return table.updateState(ctx, frame, update, names, values)
}

func (table frameTable[E]) updateState(ctx context.Context, frame *dataframe.Frame, update string, names map[string]string, values map[string]types.AttributeValue) error {
	client, err := Client()
	if err != nil {
		return err
	}
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       table.meta.frameStateKey(frame),
		UpdateExpression:          aws.String(update),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	return err
}

// CurrentWriteVersion is the Updated clock now. A write stamped up to it by a Lambda whose clock
// lags lands within its deadline, which the settle time (and its skew margin) waits out.
func (table frameTable[E]) CurrentWriteVersion() (int64, error) { return UpdatedNow(), nil }

func (m *tableMeta) frameRecordStates(frame *dataframe.Frame, ptrs []unsafe.Pointer) []dataframe.RecordState {
	states := make([]dataframe.RecordState, len(ptrs))
	for i, ptr := range ptrs {
		states[i] = dataframe.RecordState{SK: m.skValue(ptr), CreatedVersion: m.createdVersion.getI64(ptr), Values: m.frameValuesOf(frame, ptr)}
	}
	return states
}

func (table frameTable[E]) ReadAllRecords(frame *dataframe.Frame) ([]dataframe.RecordState, error) {
	var records []E
	if err := (&QueryBuilder[E]{meta: table.meta}).Consistent().Exec(&records); err != nil {
		return nil, err
	}
	return table.meta.frameRecordStates(frame, recordPointers(records)), nil
}

// ReadRecordsInRange queries the range of Keys[0], consistently when it leads the base Keys; through a
// GSI it can't be.
func (table frameTable[E]) ReadRecordsInRange(frame *dataframe.Frame, fromKey, toKey int64) ([]dataframe.RecordState, error) {
	var records []E
	query := &QueryBuilder[E]{meta: table.meta}
	query.preds = append(query.preds, predicate{field: frame.Keys[0].FieldName, op: opBetween, v1: fromKey, v2: toKey})
	if table.meta.frameKeyLeadsBaseKeys(frame) {
		query.Consistent()
	}
	if err := query.Exec(&records); err != nil {
		return nil, err
	}
	return table.meta.frameRecordStates(frame, recordPointers(records)), nil
}

// ReadRecordsWrittenAfter reads, consistently, every record whose Updated is above snapshot,
// through the whole-entity delta index. Unlike Query().Delta() it keeps a record whose row moved while
// the read ran (a later write landed): it is still written after snapshot, and a run must not miss it.
func (table frameTable[E]) ReadRecordsWrittenAfter(frame *dataframe.Frame, snapshot int64) ([]dataframe.RecordState, error) {
	meta := table.meta
	client, err := Client()
	if err != nil {
		return nil, err
	}
	plans, err := (&QueryBuilder[E]{meta: meta}).deltaFrom(snapshot+1, false).Consistent().plans()
	if err != nil {
		return nil, err
	}
	plan := plans[0]
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName()),
		KeyConditionExpression:    aws.String(plan.keyCond),
		ExpressionAttributeNames:  plan.names,
		ExpressionAttributeValues: plan.values,
		ConsistentRead:            aws.Bool(true),
	}
	var records []E
	isReturned := map[string]bool{}
	for {
		out, err := client.Query(context.Background(), input)
		if err != nil {
			return nil, err
		}
		items, _, _, err := recordItemsOfArrayRows(client, plan.basePK, plan.arrayIndex, out.Items, true)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			var record E
			if err := meta.unmarshalItem(item, &record); err != nil {
				return nil, err
			}
			ptr := unsafe.Pointer(&record)
			if sk := meta.skValue(ptr); !isReturned[sk] && meta.updated.acc.getI64(ptr) > snapshot {
				isReturned[sk] = true
				records = append(records, record)
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return meta.frameRecordStates(frame, recordPointers(records)), nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

func (table frameTable[E]) ReadRecordsBySK(frame *dataframe.Frame, sks []string) ([]dataframe.RecordState, error) {
	client, err := Client()
	if err != nil {
		return nil, err
	}
	keys := make([]map[string]types.AttributeValue, len(sks))
	for i, sk := range sks {
		keys[i] = itemKey(table.meta.tableID, sk)
	}
	items, _, err := batchGet(client, keys, true)
	if err != nil {
		return nil, err
	}
	records := make([]E, len(items))
	for i, item := range items {
		if err := table.meta.unmarshalItem(item, &records[i]); err != nil {
			return nil, err
		}
	}
	return table.meta.frameRecordStates(frame, recordPointers(records)), nil
}

func recordPointers[E any](records []E) []unsafe.Pointer {
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	return ptrs
}

// ─────────────────────────────────────────────────────────────────────────────
// MaterializeDataFrames and the rebuilds
// ─────────────────────────────────────────────────────────────────────────────

// MaterializeDataFrames brings every frame of the table up to date (dataframe.Materialize): each one
// takes its lock and compacts from its snapshot to its newest settled checkpoint, then pushes a new
// checkpoint for the next run. A frame whose lock another compaction holds is skipped. Without frames
// it does nothing.
func (r *Repo[T, E]) MaterializeDataFrames() error {
	var runErrors []error
	for i := range r.meta.dataFrames {
		if err := dataframe.Materialize(frameTable[E]{r.meta}, &r.meta.dataFrames[i]); err != nil {
			runErrors = append(runErrors, fmt.Errorf("db: %s DataFrame %q: %w", r.meta.entity, r.meta.dataFrames[i].Name, err))
		}
	}
	return errors.Join(runErrors...)
}

// RebuildDataFrames recomputes from the records, at the frame's snapshot, the files of a frame (""
// for every frame) whose Keys[0] is in [fromKey, toKey] (at most 400 values), and writes only those
// that differ (dataframe.RebuildRange). It is the fix for drift: a write from a Lambda still on older
// code, an InsertMany of a stored record, files edited by hand.
func (r *Repo[T, E]) RebuildDataFrames(frameName string, fromKey, toKey int64) error {
	return r.rebuildDataFrames(frameName, func(frame *dataframe.Frame) error {
		return dataframe.RebuildRange(frameTable[E]{r.meta}, frame, fromKey, toKey)
	})
}

// RebuildDataFramesAll recomputes every file of a frame ("" for every frame) from all the records, at
// the frame's snapshot, and deletes the files nothing produces. It reads the whole entity and rewrites
// every file.
func (r *Repo[T, E]) RebuildDataFramesAll(frameName string) error {
	return r.rebuildDataFrames(frameName, func(frame *dataframe.Frame) error {
		return dataframe.RebuildAll(frameTable[E]{r.meta}, frame)
	})
}

// rebuildDataFrames runs rebuild on each selected frame.
func (r *Repo[T, E]) rebuildDataFrames(frameName string, rebuild func(frame *dataframe.Frame) error) error {
	isRebuilt := false
	for i := range r.meta.dataFrames {
		frame := &r.meta.dataFrames[i]
		if frameName != "" && frame.Name != frameName {
			continue
		}
		isRebuilt = true
		if err := rebuild(frame); err != nil {
			return fmt.Errorf("db: %s DataFrame %q: %w", r.meta.entity, frame.Name, err)
		}
	}
	if !isRebuilt {
		return fmt.Errorf("db: %s has no DataFrame %q", r.meta.entity, frameName)
	}
	return nil
}
