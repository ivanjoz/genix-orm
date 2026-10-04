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
	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrame runs (DATA_FRAMES_PLAN.md, "The snapshot rule" and after). Every file
// holds its frame at a snapshot: the sum of what each record held at that
// UpdatedVersion. The frame's state item records the snapshot the files have
// reached and the checkpoints compactions target:
//
//	pk = base pk ‖ 000   (beside the GroupBy counters and slot versions)   sk = "f" + frame name
//	w        the snapshot of the files; absent: not built (a new frame, or a changed shape)
//	nx, nxt  the newest checkpoint: an upv sequence value, and when it was read (unix seconds)
//	px       the previous checkpoint, settled
//	ix       the days whose _ixt holds blocks to merge into their _idx, a number set
//	sh       the shape the files were built with
//
// A version is reserved before its write lands, so a compaction targets a checkpoint
// only once it is frameSettle old: every write that took a version up to it has
// landed or given up (the write deadline, data_frame.go). Every compaction (the
// scheduled run, a rebuild, the express compaction of a fresh read) holds the
// frame's lock, an object of its S3 folder, while it writes the files and w, so one
// writes at a time (dataframe/lock.go). The file work is the dataframe package's.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// A rebuild waits up to a minute for the compaction holding the lock: it takes seconds, and a
	// crashed one's lock expires within dataframe.LockDuration.
	frameLockWaitAttempts = 12
	frameLockWaitInterval = 5 * time.Second
)

func (m *tableMeta) frameRecordStates(frame *dataFrameMeta, ptrs []unsafe.Pointer) []dataframe.RecordState {
	states := make([]dataframe.RecordState, len(ptrs))
	for i, ptr := range ptrs {
		states[i] = dataframe.RecordState{SK: m.skValue(ptr), CreatedVersion: m.createdVersion.getI64(ptr), Values: m.frameValuesOf(frame, ptr)}
	}
	return states
}

// ─────────────────────────────────────────────────────────────────────────────
// The state item
// ─────────────────────────────────────────────────────────────────────────────

type frameState struct {
	readAt            int64 // unix seconds, taken before the read
	hasSnapshot       bool
	snapshot          int64 // w
	hasTarget         bool
	target            int64 // nx
	targetTime        int64 // nxt
	hasPreviousTarget bool
	previousTarget    int64   // px
	extendedDays      []int64 // ix
	hasShape          bool
	shape             uint32 // sh
}

func frameStateOf(item map[string]types.AttributeValue, readAt int64) frameState {
	state := frameState{
		readAt:         readAt,
		snapshot:       numberAttrValue(item, "w"),
		target:         numberAttrValue(item, "nx"),
		targetTime:     numberAttrValue(item, "nxt"),
		previousTarget: numberAttrValue(item, "px"),
		shape:          uint32(numberAttrValue(item, "sh")),
	}
	_, state.hasSnapshot = item["w"]
	_, state.hasTarget = item["nx"]
	_, state.hasPreviousTarget = item["px"]
	_, state.hasShape = item["sh"]
	if days, isSet := item["ix"].(*types.AttributeValueMemberNS); isSet {
		for _, day := range days.Value {
			parsedDay, _ := strconv.ParseInt(day, 10, 64)
			state.extendedDays = append(state.extendedDays, parsedDay)
		}
	}
	return state
}

// isBuiltAs reports whether the frame's files are built, in its current shape.
func (state frameState) isBuiltAs(frame *dataFrameMeta) bool {
	return state.hasSnapshot && state.hasShape && state.shape == frame.Shape
}

// frameSettleSeconds is frameSettle rounded up: checkpoint times are whole seconds.
func frameSettleSeconds() int64 { return int64((frameSettle + time.Second - 1) / time.Second) }

// settledTarget is the newest checkpoint settled at now: nx once it is more than frameSettle old
// (nxt was rounded down), else px, which a push replaces only once nx has settled. false when there
// is neither.
func (state frameState) settledTarget(now int64) (int64, bool) {
	if state.hasTarget && now-state.targetTime > frameSettleSeconds() {
		return state.target, true
	}
	return state.previousTarget, state.hasPreviousTarget
}

func (m *tableMeta) frameStateKey(frame *dataFrameMeta) map[string]types.AttributeValue {
	// Frames need an entity without Partition, so the base pk is the TableID.
	return itemKey(groupCountersPK(m.tableID), frameStateSKPrefix+frame.Name)
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

// readFrameState reads the frame's state item, consistently.
func (m *tableMeta) readFrameState(client *dynamodb.Client, frame *dataFrameMeta) (frameState, error) {
	readAt := Now().Unix()
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName()),
		Key:            m.frameStateKey(frame),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return frameState{}, err
	}
	return frameStateOf(out.Item, readAt), nil
}

// pushFrameCheckpoint pushes a checkpoint once the newest has settled, or when there is none: the upv
// sequence value now becomes the newest, and the newest the previous one. The sequence is read before
// the clock, so every version up to the value was reserved by the time recorded. The push is
// conditioned on the newest checkpoint read: of two concurrent pushers one goes through, and the
// other keeps the state it read, whose checkpoints are as valid. It returns the state after.
func (m *tableMeta) pushFrameCheckpoint(client *dynamodb.Client, frame *dataFrameMeta, state frameState) (frameState, error) {
	if state.hasTarget && Now().Unix()-state.targetTime <= frameSettleSeconds() {
		return state, nil
	}
	checkpoint, err := m.currentWriteVersion(client)
	if err != nil {
		return state, err
	}
	now := Now().Unix()
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       m.frameStateKey(frame),
		UpdateExpression:          aws.String("SET #nx = :nx, #nxt = :nxt"),
		ConditionExpression:       aws.String("attribute_not_exists(#nxt)"),
		ExpressionAttributeNames:  map[string]string{"#nx": "nx", "#nxt": "nxt"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":nx": numberAttr(checkpoint), ":nxt": numberAttr(now)},
		ReturnValues:              types.ReturnValueAllNew,
	}
	if state.hasTarget {
		input.UpdateExpression = aws.String("SET #px = #nx, #nx = :nx, #nxt = :nxt")
		input.ConditionExpression = aws.String("#nxt = :seenNxt")
		input.ExpressionAttributeNames["#px"] = "px"
		input.ExpressionAttributeValues[":seenNxt"] = numberAttr(state.targetTime)
	}
	out, err := client.UpdateItem(context.Background(), input)
	var pushedMeanwhileErr *types.ConditionalCheckFailedException
	if errors.As(err, &pushedMeanwhileErr) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	return frameStateOf(out.Attributes, now), nil
}

// commitFrameState applies update to the state item as lock's holder. The write is bounded as the
// holder's file writes are (Lock.WriteContext), so it lands before another compaction can take the
// lock over.
func (m *tableMeta) commitFrameState(client *dynamodb.Client, frame *dataFrameMeta, lock *dataframe.Lock, update string, names map[string]string, values map[string]types.AttributeValue) error {
	ctx, cancel, err := lock.WriteContext()
	if err != nil {
		return err
	}
	defer cancel()
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       m.frameStateKey(frame),
		UpdateExpression:          aws.String(update),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	return err
}

// currentWriteVersion reads the base pk's upv sequence: the last version reserved.
func (m *tableMeta) currentWriteVersion(client *dynamodb.Client) (int64, error) {
	sequenceName := m.tableID + updatedVersionSeqSuffix
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName()),
		Key:            sequenceKey(sequenceName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, err
	}
	return sequenceCounterValue(sequenceName, out.Item)
}

// ─────────────────────────────────────────────────────────────────────────────
// MaterializeDataFrames and the rebuilds
// ─────────────────────────────────────────────────────────────────────────────

// MaterializeDataFrames brings every frame of the table up to date: each one takes
// its lock and compacts from its snapshot to its newest settled checkpoint, then
// pushes a new checkpoint for the next run. A frame whose lock another compaction
// holds is skipped. Without frames it does nothing.
func (r *Repo[T, E]) MaterializeDataFrames() error {
	var runErrors []error
	for i := range r.meta.dataFrames {
		if err := r.materializeDataFrame(&r.meta.dataFrames[i]); err != nil {
			runErrors = append(runErrors, fmt.Errorf("db: %s DataFrame %q: %w", r.meta.entity, r.meta.dataFrames[i].Name, err))
		}
	}
	return errors.Join(runErrors...)
}

func (r *Repo[T, E]) materializeDataFrame(frame *dataFrameMeta) error {
	if frameStore == nil {
		return fmt.Errorf("no frame store is set: call SetDataFrames at boot")
	}
	client, err := Client()
	if err != nil {
		return err
	}
	lock, err := dataframe.TakeLock(frameStore, &frame.Frame, Now)
	if err != nil || lock == nil {
		return err
	}
	// Released after a failure too, so the next tick retries; a lock not released expires on its own.
	defer lock.Release()
	// Read once the lock is held: until it is released, no other compaction moves w.
	state, err := r.meta.readFrameState(client, frame)
	if err != nil {
		return err
	}
	return r.runDataFrame(client, frame, lock, state)
}

// runDataFrame is one run of a frame, as lock's holder.
func (r *Repo[T, E]) runDataFrame(client *dynamodb.Client, frame *dataFrameMeta, lock *dataframe.Lock, state frameState) error {
	// A new frame, or one whose shape changed: the old shape's log goes, w is removed with the
	// checkpoints and ix, and the first run after the checkpoint read now settles rebuilds every file
	// at it.
	if !state.hasShape || state.shape != frame.Shape {
		if state.hasShape {
			if err := lock.Delete(dataframe.ShapeLogKey(frame.Folder, state.shape)); err != nil {
				return err
			}
		}
		checkpoint, err := r.meta.currentWriteVersion(client)
		if err != nil {
			return err
		}
		return r.meta.commitFrameState(client, frame, lock, "SET #sh = :sh, #nx = :nx, #nxt = :nxt REMOVE #w, #px, #ix",
			map[string]string{"#sh": "sh", "#nx": "nx", "#nxt": "nxt", "#w": "w", "#px": "px", "#ix": "ix"},
			map[string]types.AttributeValue{":sh": numberAttr(int64(frame.Shape)), ":nx": numberAttr(checkpoint), ":nxt": numberAttr(Now().Unix())})
	}
	state, err := r.meta.pushFrameCheckpoint(client, frame, state)
	if err != nil {
		return err
	}
	target, hasTarget := state.settledTarget(Now().Unix())

	snapshot := state.snapshot
	switch {
	case !state.hasSnapshot && !hasTarget:
		return nil
	case !state.hasSnapshot:
		var records []dataframe.RecordState
		if records, err = r.frameRecordsOfAll(frame); err == nil {
			err = dataframe.RebuildAllFiles(lock, &frame.Frame, records, target)
		}
		snapshot = target
	default:
		// An express compaction may already have moved w past the target: then only the days in ix
		// are merged.
		if hasTarget && target > snapshot {
			snapshot = target
		}
		readWrittenAfter, readBySK := frameRecordReaders[E](r.meta, client, frame)
		err = dataframe.CompactFrame(lock, &frame.Frame, state.snapshot, snapshot, state.extendedDays, readWrittenAfter, readBySK)
	}
	if err != nil {
		return err
	}
	update := "SET #w = :w"
	names := map[string]string{"#w": "w"}
	values := map[string]types.AttributeValue{":w": numberAttr(snapshot)}
	if len(state.extendedDays) > 0 {
		update += " DELETE #ix :ix"
		names["#ix"], values[":ix"] = "ix", numberSetAttr(state.extendedDays)
	}
	if err := r.meta.commitFrameState(client, frame, lock, update, names, values); err != nil {
		return err
	}
	// After the commit: a crash here leaves entries at or below w, which every reader ignores.
	return dataframe.TruncateLog(frameStore, &frame.Frame, snapshot)
}

// ─────────────────────────────────────────────────────────────────────────────
// Express compactions (DATA_FRAMES_PLAN.md, D6)
// ─────────────────────────────────────────────────────────────────────────────

// frameExpressMinChanges is how many records must have changed between w and the target for a fresh
// read to write them into the files.
const frameExpressMinChanges = 100

// expressCompactFrame runs after a fresh read, verified by reading startState before it and endState
// after. It pushes a checkpoint when the newest has settled. Then, when more than
// frameExpressMinChanges records changed between w and M, the newest checkpoint settled when the read
// began, it writes those changes into the files: every write up to M had landed by then, so the read
// already holds what each record held at M. It takes the frame's lock, and skips while another
// compaction holds it or once one moved w since startState: the read's changes start at that w. One
// that loses its lock is not an error: the files it wrote are ahead of w, as a crashed one's.
func (m *tableMeta) expressCompactFrame(client *dynamodb.Client, frame *dataFrameMeta, startState, endState frameState, fresh *dataframe.FreshRead) error {
	if _, err := m.pushFrameCheckpoint(client, frame, endState); err != nil {
		return err
	}
	target, hasTarget := startState.settledTarget(startState.readAt)
	if !hasTarget || target <= startState.snapshot || fresh.ChangedRecords(target) <= frameExpressMinChanges {
		return nil
	}
	lock, err := dataframe.TakeLock(frameStore, &frame.Frame, Now)
	if err != nil || lock == nil {
		return err
	}
	defer lock.Release()
	state, err := m.readFrameState(client, frame)
	if err != nil || !state.isBuiltAs(frame) || state.snapshot != startState.snapshot {
		return err
	}
	extendedDays, err := fresh.CompactTo(lock, &frame.Frame, target)
	if err == nil {
		update := "SET #w = :w"
		names := map[string]string{"#w": "w"}
		values := map[string]types.AttributeValue{":w": numberAttr(target)}
		if len(extendedDays) > 0 {
			update += " ADD #ix :ix"
			names["#ix"], values[":ix"] = "ix", numberSetAttr(extendedDays)
		}
		err = m.commitFrameState(client, frame, lock, update, names, values)
	}
	if errors.Is(err, dataframe.ErrLockLost) {
		return nil
	}
	return err
}

// frameRecordsOfAll reads every record of the entity, consistently.
func (r *Repo[T, E]) frameRecordsOfAll(frame *dataFrameMeta) ([]dataframe.RecordState, error) {
	var records []E
	if err := r.Query().Consistent().Exec(&records); err != nil {
		return nil, err
	}
	return r.meta.frameRecordStates(frame, recordPointers(records)), nil
}

// frameRecordReaders are the record reads dataframe.CompactFrame and dataframe.ReadFreshFiles take.
func frameRecordReaders[E any](meta *tableMeta, client *dynamodb.Client, frame *dataFrameMeta) (
	readWrittenAfter func(snapshot int64) ([]dataframe.RecordState, error), readBySK func(sks []string) ([]dataframe.RecordState, error)) {
	readWrittenAfter = func(snapshot int64) ([]dataframe.RecordState, error) {
		return frameRecordsWrittenAfter[E](meta, client, frame, snapshot)
	}
	readBySK = func(sks []string) ([]dataframe.RecordState, error) {
		return frameRecordsBySK[E](meta, client, frame, sks)
	}
	return readWrittenAfter, readBySK
}

// frameRecordsWrittenAfter reads, consistently, every record whose UpdatedVersion is
// above snapshot, through the whole-entity delta index. Unlike Query().Delta() it
// keeps a record whose row moved while the read ran (a later write landed): it is
// still written after snapshot, and a run must not miss it.
func frameRecordsWrittenAfter[E any](meta *tableMeta, client *dynamodb.Client, frame *dataFrameMeta, snapshot int64) ([]dataframe.RecordState, error) {
	plans, err := (&QueryBuilder[E]{meta: meta}).Delta(int32(snapshot)).Consistent().plans()
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
			if sk := meta.skValue(ptr); !isReturned[sk] && meta.writeVersion.acc.getI64(ptr) > snapshot {
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

// frameRecordsBySK reads records by their sk, consistently; missing ones are left out.
func frameRecordsBySK[E any](meta *tableMeta, client *dynamodb.Client, frame *dataFrameMeta, sks []string) ([]dataframe.RecordState, error) {
	keys := make([]map[string]types.AttributeValue, len(sks))
	for i, sk := range sks {
		keys[i] = itemKey(meta.tableID, sk)
	}
	items, _, err := batchGet(client, keys, true)
	if err != nil {
		return nil, err
	}
	records := make([]E, len(items))
	for i, item := range items {
		if err := meta.unmarshalItem(item, &records[i]); err != nil {
			return nil, err
		}
	}
	return meta.frameRecordStates(frame, recordPointers(records)), nil
}

func recordPointers[E any](records []E) []unsafe.Pointer {
	ptrs := make([]unsafe.Pointer, len(records))
	for i := range records {
		ptrs[i] = unsafe.Pointer(&records[i])
	}
	return ptrs
}

// RebuildDataFrames recomputes from the records, at the frame's snapshot, the files of
// a frame ("" for every frame) whose Keys[0] is in [fromKey, toKey] (at most 400
// values), and writes only those that differ. It is the fix for drift: a write
// from a Lambda still on older code, an InsertMany of a stored record, files edited
// by hand. It takes the frame's lock, waiting up to a minute for the compaction holding it.
func (r *Repo[T, E]) RebuildDataFrames(frameName string, fromKey, toKey int64) error {
	if fromKey < 0 || toKey < fromKey || toKey-fromKey >= dataframe.MaxFirstKeys {
		return fmt.Errorf("db: %s RebuildDataFrames takes 0 <= fromKey <= toKey, at most %d values", r.meta.entity, dataframe.MaxFirstKeys)
	}
	return r.rebuildDataFrames(frameName, func(frame *dataFrameMeta, lock *dataframe.Lock, snapshot int64) error {
		var records []E
		query := r.Query()
		query.preds = append(query.preds, predicate{field: frame.keys[0].fieldName, op: opBetween, v1: fromKey, v2: toKey})
		if frame.firstKeyLeadsBaseKeys {
			query.Consistent()
		}
		if err := query.Exec(&records); err != nil {
			return err
		}
		return dataframe.RebuildFilesInRange(lock, &frame.Frame, r.meta.frameRecordStates(frame, recordPointers(records)), snapshot, fromKey, toKey)
	})
}

// RebuildDataFramesAll recomputes every file of a frame ("" for every frame) from all
// the records, at the frame's snapshot, and deletes the files nothing produces. It
// reads the whole entity and rewrites every file.
func (r *Repo[T, E]) RebuildDataFramesAll(frameName string) error {
	return r.rebuildDataFrames(frameName, func(frame *dataFrameMeta, lock *dataframe.Lock, snapshot int64) error {
		records, err := r.frameRecordsOfAll(frame)
		if err != nil {
			return err
		}
		return dataframe.RebuildAllFiles(lock, &frame.Frame, records, snapshot)
	})
}

// rebuildDataFrames runs rebuild on each selected frame under its lock, at the frame's snapshot. The
// snapshot does not move, so the next run goes on from where the files now are.
func (r *Repo[T, E]) rebuildDataFrames(frameName string, rebuild func(frame *dataFrameMeta, lock *dataframe.Lock, snapshot int64) error) error {
	if frameStore == nil {
		return fmt.Errorf("db: no frame store is set: call SetDataFrames at boot")
	}
	client, err := Client()
	if err != nil {
		return err
	}
	isRebuilt := false
	for i := range r.meta.dataFrames {
		frame := &r.meta.dataFrames[i]
		if frameName != "" && frame.Name != frameName {
			continue
		}
		isRebuilt = true
		var lock *dataframe.Lock
		for attempt := 1; lock == nil; attempt++ {
			if lock, err = dataframe.TakeLock(frameStore, &frame.Frame, Now); err != nil {
				return err
			}
			if lock == nil && attempt == frameLockWaitAttempts {
				return fmt.Errorf("db: %s DataFrame %q is held by a compaction: retry in a few minutes", r.meta.entity, frame.Name)
			}
			if lock == nil {
				time.Sleep(frameLockWaitInterval)
			}
		}
		state, rebuildErr := r.meta.readFrameState(client, frame)
		if rebuildErr == nil && !state.isBuiltAs(frame) {
			rebuildErr = dataframe.ErrNotBuilt
		}
		if rebuildErr == nil {
			rebuildErr = rebuild(frame, lock, state.snapshot)
		}
		if err := errors.Join(rebuildErr, lock.Release()); err != nil {
			return fmt.Errorf("db: %s DataFrame %q: %w", r.meta.entity, frame.Name, err)
		}
	}
	if !isRebuilt {
		return fmt.Errorf("db: %s has no DataFrame %q", r.meta.entity, frameName)
	}
	return nil
}
