package dynamo

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
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
// UpdatedVersion. The frame's state item records the snapshot every file has
// reached and the next run's target:
//
//	pk = base pk ‖ 000   (beside the GroupBy counters and slot versions)   sk = "f" + frame name
//	w        the snapshot of the files; absent: not built (a new frame, or a changed shape)
//	nx, nxt  the upv sequence value a run read and when (unix seconds): the next run's target
//	sh       the shape the files were built with
//	lo, le   the lease: its owner, and when it expires (unix seconds)
//
// A version is reserved before its write lands, so a run targets nx only once it is
// frameSettle old: every write that took a version up to it has landed and logged.
// nx only moves at the commit, so a run that crashed halfway is retried to the same
// target. The file work from W to the target is dataframe.CompactFrame.
// ─────────────────────────────────────────────────────────────────────────────

const (
	frameLeaseSeconds = 600
	// A rebuild waits up to a minute for a run to release the lease: the run of every slot may hold
	// it, and it takes seconds.
	frameLeaseWaitAttempts = 12
	frameLeaseWaitInterval = 5 * time.Second
)

func (m *tableMeta) frameRecordStates(frame *dataFrameMeta, ptrs []unsafe.Pointer) []dataframe.RecordState {
	states := make([]dataframe.RecordState, len(ptrs))
	for i, ptr := range ptrs {
		states[i] = dataframe.RecordState{SK: m.skValue(ptr), CreatedVersion: m.createdVersion.getI64(ptr), Values: m.frameValuesOf(frame, ptr)}
	}
	return states
}

// ─────────────────────────────────────────────────────────────────────────────
// The state item and the lease
// ─────────────────────────────────────────────────────────────────────────────

type frameState struct {
	hasSnapshot bool
	snapshot    int64 // w
	target      int64 // nx
	targetTime  int64 // nxt
	hasShape    bool
	shape       uint32 // sh
}

func (m *tableMeta) frameStateKey(frame *dataFrameMeta) map[string]types.AttributeValue {
	// Frames need an entity without Partition, so the base pk is the TableID.
	return itemKey(groupCountersPK(m.tableID), frameStateSKPrefix+frame.Name)
}

func numberAttr(value int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

// takeFrameLease claims the frame for owner until the lease expires and returns its
// state; isTaken is false while another run holds it.
func (m *tableMeta) takeFrameLease(client *dynamodb.Client, frame *dataFrameMeta, owner string) (frameState, bool, error) {
	now := Now().Unix()
	out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName()),
		Key:                      m.frameStateKey(frame),
		UpdateExpression:         aws.String("SET #lo = :owner, #le = :expiry"),
		ConditionExpression:      aws.String("attribute_not_exists(#le) OR #le < :now"),
		ExpressionAttributeNames: map[string]string{"#lo": "lo", "#le": "le"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": &types.AttributeValueMemberS{Value: owner}, ":expiry": numberAttr(now + frameLeaseSeconds), ":now": numberAttr(now),
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	var leaseHeldErr *types.ConditionalCheckFailedException
	if errors.As(err, &leaseHeldErr) {
		return frameState{}, false, nil
	}
	if err != nil {
		return frameState{}, false, err
	}
	_, hasSnapshot := out.Attributes["w"]
	_, hasShape := out.Attributes["sh"]
	return frameState{
		hasSnapshot: hasSnapshot,
		snapshot:    numberAttrValue(out.Attributes, "w"),
		target:      numberAttrValue(out.Attributes, "nx"),
		targetTime:  numberAttrValue(out.Attributes, "nxt"),
		hasShape:    hasShape,
		shape:       uint32(numberAttrValue(out.Attributes, "sh")),
	}, true, nil
}

// updateFrameState applies update to the state item while owner still holds the
// lease. Every update ends a run, so update must REMOVE #lo, #le: the lease.
func (m *tableMeta) updateFrameState(client *dynamodb.Client, frame *dataFrameMeta, owner, update string, names map[string]string, values map[string]types.AttributeValue) error {
	names = maps.Clone(names)
	names["#lo"], names["#le"] = "lo", "le"
	values = maps.Clone(values)
	values[":owner"] = &types.AttributeValueMemberS{Value: owner}
	_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       m.frameStateKey(frame),
		UpdateExpression:          aws.String(update),
		ConditionExpression:       aws.String("#lo = :owner"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	var leaseLostErr *types.ConditionalCheckFailedException
	if errors.As(err, &leaseLostErr) {
		return fmt.Errorf("db: the lease of DataFrame %q expired during its run", frame.Name)
	}
	return err
}

func (m *tableMeta) releaseFrameLease(client *dynamodb.Client, frame *dataFrameMeta, owner string) error {
	return m.updateFrameState(client, frame, owner, "REMOVE #lo, #le", map[string]string{}, map[string]types.AttributeValue{})
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

func newFrameLeaseOwner() string { return strconv.FormatUint(rand.Uint64(), 36) }

// ─────────────────────────────────────────────────────────────────────────────
// MaterializeDataFrames and the rebuilds
// ─────────────────────────────────────────────────────────────────────────────

// MaterializeDataFrames brings every frame of the table up to date: each one takes
// its lease and runs from its snapshot to the version it read on its previous run.
// A frame whose lease another run holds is skipped. Without frames it does nothing.
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
	owner := newFrameLeaseOwner()
	state, isTaken, err := r.meta.takeFrameLease(client, frame, owner)
	if err != nil || !isTaken {
		return err
	}
	runErr := r.runDataFrame(client, frame, owner, state)
	if runErr != nil {
		// The lease would expire on its own; releasing it lets the next tick retry.
		_ = r.meta.releaseFrameLease(client, frame, owner)
	}
	return runErr
}

// runDataFrame is one run of a frame whose lease owner holds. The sequence is read
// first: it is the next run's target.
func (r *Repo[T, E]) runDataFrame(client *dynamodb.Client, frame *dataFrameMeta, owner string, state frameState) error {
	nextTarget, err := r.meta.currentWriteVersion(client)
	if err != nil {
		return err
	}
	now := Now().Unix()
	shapeValue := numberAttr(int64(frame.Shape))
	nextTargetValues := map[string]types.AttributeValue{":nx": numberAttr(nextTarget), ":nxt": numberAttr(now)}

	// A new frame, or one whose shape changed: the old shape's log goes, w is removed and the next
	// run, frameSettle later, rebuilds every file at the version read now.
	if !state.hasShape || state.shape != frame.Shape {
		if state.hasShape {
			if err := frameStore.Delete(dataframe.ShapeLogKey(frame.Folder, state.shape)); err != nil {
				return err
			}
		}
		values := maps.Clone(nextTargetValues)
		values[":sh"] = shapeValue
		return r.meta.updateFrameState(client, frame, owner, "SET #sh = :sh, #nx = :nx, #nxt = :nxt REMOVE #w, #lo, #le",
			map[string]string{"#sh": "sh", "#nx": "nx", "#nxt": "nxt", "#w": "w"}, values)
	}
	// A retried execution can come early: the target is not settled yet.
	if state.targetTime > now-int64(frameSettle/time.Second) {
		return r.meta.releaseFrameLease(client, frame, owner)
	}

	target := state.target
	switch {
	case !state.hasSnapshot:
		var records []dataframe.RecordState
		if records, err = r.frameRecordsOfAll(frame); err == nil {
			err = dataframe.RebuildAllFiles(frameStore, &frame.Frame, records, target)
		}
	case target > state.snapshot:
		readWrittenAfter, readBySK := frameRecordReaders[E](r.meta, client, frame)
		err = dataframe.CompactFrame(frameStore, &frame.Frame, state.snapshot, target, readWrittenAfter, readBySK)
	default:
		target = state.snapshot // nothing written since the last run
	}
	if err != nil {
		return err
	}
	values := maps.Clone(nextTargetValues)
	values[":w"] = numberAttr(target)
	if err := r.meta.updateFrameState(client, frame, owner, "SET #w = :w, #nx = :nx, #nxt = :nxt REMOVE #lo, #le",
		map[string]string{"#w": "w", "#nx": "nx", "#nxt": "nxt"}, values); err != nil {
		return err
	}
	// After the commit: a crash here leaves entries at or below w, which every run ignores.
	return dataframe.TruncateLog(frameStore, &frame.Frame, target)
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
// by hand. It takes the frame's lease, so it fails while a run holds it.
func (r *Repo[T, E]) RebuildDataFrames(frameName string, fromKey, toKey int64) error {
	if fromKey < 0 || toKey < fromKey || toKey-fromKey >= dataframe.MaxFirstKeys {
		return fmt.Errorf("db: %s RebuildDataFrames takes 0 <= fromKey <= toKey, at most %d values", r.meta.entity, dataframe.MaxFirstKeys)
	}
	return r.rebuildDataFrames(frameName, func(frame *dataFrameMeta, snapshot int64) error {
		var records []E
		query := r.Query()
		query.preds = append(query.preds, predicate{field: frame.keys[0].fieldName, op: opBetween, v1: fromKey, v2: toKey})
		if frame.firstKeyLeadsBaseKeys {
			query.Consistent()
		}
		if err := query.Exec(&records); err != nil {
			return err
		}
		return dataframe.RebuildFilesInRange(frameStore, &frame.Frame, r.meta.frameRecordStates(frame, recordPointers(records)), snapshot, fromKey, toKey)
	})
}

// RebuildDataFramesAll recomputes every file of a frame ("" for every frame) from all
// the records, at the frame's snapshot, and deletes the files nothing produces. It
// reads the whole entity and rewrites every file.
func (r *Repo[T, E]) RebuildDataFramesAll(frameName string) error {
	return r.rebuildDataFrames(frameName, func(frame *dataFrameMeta, snapshot int64) error {
		records, err := r.frameRecordsOfAll(frame)
		if err != nil {
			return err
		}
		return dataframe.RebuildAllFiles(frameStore, &frame.Frame, records, snapshot)
	})
}

// rebuildDataFrames runs rebuild on each selected frame under its lease. The
// snapshot does not move, so the next run goes on from where the files now are.
func (r *Repo[T, E]) rebuildDataFrames(frameName string, rebuild func(frame *dataFrameMeta, snapshot int64) error) error {
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
		owner := newFrameLeaseOwner()
		var state frameState
		isTaken := false
		for attempt := 1; !isTaken; attempt++ {
			if state, isTaken, err = r.meta.takeFrameLease(client, frame, owner); err != nil {
				return err
			}
			if !isTaken && attempt == frameLeaseWaitAttempts {
				return fmt.Errorf("db: %s DataFrame %q is held by a run: retry in a few minutes", r.meta.entity, frame.Name)
			}
			if !isTaken {
				time.Sleep(frameLeaseWaitInterval)
			}
		}
		rebuildErr := dataframe.ErrNotBuilt
		if state.hasSnapshot && state.shape == frame.Shape {
			rebuildErr = rebuild(frame, state.snapshot)
		}
		if err := errors.Join(rebuildErr, r.meta.releaseFrameLease(client, frame, owner)); err != nil {
			return fmt.Errorf("db: %s DataFrame %q: %w", r.meta.entity, frame.Name, err)
		}
	}
	if !isRebuilt {
		return fmt.Errorf("db: %s has no DataFrame %q", r.meta.entity, frameName)
	}
	return nil
}
