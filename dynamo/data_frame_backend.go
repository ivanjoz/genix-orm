package dynamo

import (
	"context"
	"errors"
	"fmt"
	"github.com/ivanjoz/genix-orm/db/cache"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// dynamo's dataframe.Backend. Each frame has one state item:
//
//	pk  = base pk ‖ 000          beside the GroupBy counters (sk g…)
//	sk  = "f" + frame name
//	gen   the generation folder the files are in; absent: not built
//	sh    the shape that generation was built with
//	w     the snapshot, a SUnixTime
//	lk    the lock's expiry (unix ms), lid its holder
//
// The records come as Histories: through the GSI on Updated for the ones written
// after a moment, through a scan of the base rows for a rebuild.
// ─────────────────────────────────────────────────────────────────────────────

const frameStateSKPrefix = "f"

// ErrFrameNotBuilt: the frame has no files in its current shape yet. The next MaterializeDataFrames builds it.
var ErrFrameNotBuilt = dataframe.ErrFrameNotBuilt

type frameBackend struct {
	meta   *tableMeta
	client *dynamodb.Client
}

func frameBackendOf(meta *tableMeta) (*frameBackend, error) {
	if cache.FileStore() == nil {
		return nil, fmt.Errorf("db: %s has DataFrames and no file store holds their files: call SetFileStore at boot", meta.recordType.Name())
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}
	return &frameBackend{meta: meta, client: client}, nil
}

// MaterializeDataFrames brings every DataFrame of the table up to date (dataframe.Materialize).
// berryapps runs it every 10 minutes.
func (r *repoCore) MaterializeDataFrames() error {
	if r.meta.frames == nil {
		return nil
	}
	backend, err := frameBackendOf(r.meta)
	if err != nil {
		return err
	}
	return dataframe.Materialize(r.meta.frames, backend, cache.FileStore(), Now())
}

// RebuildDataFrames computes the files of the frame frameName ("" every frame) again from the
// records: the fix for drift. It fails when a run holds the frame; retry it.
func (r *repoCore) RebuildDataFrames(frameName string) error {
	if r.meta.frames == nil {
		return fmt.Errorf("db: %s has no DataFrame %q", r.meta.recordType.Name(), frameName)
	}
	backend, err := frameBackendOf(r.meta)
	if err != nil {
		return err
	}
	return dataframe.Rebuild(r.meta.frames, backend, cache.FileStore(), frameName, Now())
}

// DropDataFrame deletes what a frame the schema no longer declares left behind: its state item and
// every file of its folder. It refuses a declared frame. Returns the files deleted.
func (r *repoCore) DropDataFrame(frameName string) (int, error) {
	if r.meta.frames == nil || len(r.meta.frames.Frames) == 0 {
		return 0, fmt.Errorf("db: %s has no DataFrame: its frames' folder is unknown", r.meta.recordType.Name())
	}
	if r.meta.frames.FrameNamed(frameName) != nil || frameName == "" || strings.Contains(frameName, "/") {
		return 0, fmt.Errorf("db: %s DataFrame %q is declared or not a frame name: only an undeclared frame is dropped", r.meta.recordType.Name(), frameName)
	}
	backend, err := frameBackendOf(r.meta)
	if err != nil {
		return 0, err
	}
	declared := r.meta.frames.Frames[0]
	folder := strings.TrimSuffix(declared.Folder, declared.Name+"/") + frameName + "/"
	fileKeys, err := cache.FileStore().ListFiles(folder)
	if err != nil {
		return 0, err
	}
	if err := cache.FileStore().DeleteFiles(context.Background(), fileKeys...); err != nil {
		return 0, err
	}
	_, err = backend.client.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName()), Key: itemKey(groupCountersPK(r.meta.tableID), frameStateSKPrefix+frameName),
	})
	return len(fileKeys), err
}

func (backend *frameBackend) stateKey(frame *dataframe.Frame) map[string]types.AttributeValue {
	return itemKey(groupCountersPK(backend.meta.tableID), frameStateSKPrefix+frame.Name)
}

func frameStateOf(item map[string]types.AttributeValue) dataframe.State {
	state := dataframe.State{Snapshot: numberAttrValue(item, "w")}
	if generation, isText := item["gen"].(*types.AttributeValueMemberS); isText {
		state.Generation = generation.Value
	}
	if shape, isText := item["sh"].(*types.AttributeValueMemberS); isText {
		state.Shape = shape.Value
	}
	return state
}

func (backend *frameBackend) ReadState(frame *dataframe.Frame) (dataframe.State, error) {
	out, err := backend.client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName()), Key: backend.stateKey(frame), ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return dataframe.State{}, fmt.Errorf("db: %s reading the state of DataFrame %q: %w", backend.meta.recordType.Name(), frame.Name, err)
	}
	return frameStateOf(out.Item), nil
}

func (backend *frameBackend) TakeLock(frame *dataframe.Frame, holderID string, now, lockedUntil time.Time) (dataframe.State, bool, error) {
	out, err := backend.client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       backend.stateKey(frame),
		UpdateExpression:          aws.String("SET #lk = :until, #lid = :id"),
		ConditionExpression:       aws.String("attribute_not_exists(#lk) OR #lk < :now"),
		ExpressionAttributeNames:  map[string]string{"#lk": "lk", "#lid": "lid"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":until": numberAttr(lockedUntil.UnixMilli()), ":now": numberAttr(now.UnixMilli()), ":id": &types.AttributeValueMemberS{Value: holderID}},
		ReturnValues:              types.ReturnValueAllNew,
	})
	var isHeldErr *types.ConditionalCheckFailedException
	if errors.As(err, &isHeldErr) {
		return dataframe.State{}, false, nil
	}
	if err != nil {
		return dataframe.State{}, false, fmt.Errorf("db: %s taking the lock of DataFrame %q: %w", backend.meta.recordType.Name(), frame.Name, err)
	}
	return frameStateOf(out.Attributes), true, nil
}

func (backend *frameBackend) ReleaseLock(frame *dataframe.Frame, holderID string) {
	backend.client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       backend.stateKey(frame),
		UpdateExpression:          aws.String("SET #lk = :zero"),
		ConditionExpression:       aws.String("#lid = :id"),
		ExpressionAttributeNames:  map[string]string{"#lk": "lk", "#lid": "lid"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":zero": numberAttr(0), ":id": &types.AttributeValueMemberS{Value: holderID}},
	})
}

func (backend *frameBackend) CommitSnapshot(ctx context.Context, frame *dataframe.Frame, holderID string, state dataframe.State, snapshot int64) error {
	_, err := backend.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       backend.stateKey(frame),
		UpdateExpression:          aws.String("SET #w = :target"),
		ConditionExpression:       aws.String("#lid = :id AND #gen = :gen AND #w = :w"),
		ExpressionAttributeNames:  map[string]string{"#w": "w", "#lid": "lid", "#gen": "gen"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":target": numberAttr(snapshot), ":id": &types.AttributeValueMemberS{Value: holderID}, ":gen": &types.AttributeValueMemberS{Value: state.Generation}, ":w": numberAttr(state.Snapshot)},
	})
	return err
}

func (backend *frameBackend) CommitGeneration(ctx context.Context, frame *dataframe.Frame, holderID string, generation string, snapshot int64) error {
	_, err := backend.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName()),
		Key:                       backend.stateKey(frame),
		UpdateExpression:          aws.String("SET #gen = :gen, #sh = :sh, #w = :w"),
		ConditionExpression:       aws.String("#lid = :id"),
		ExpressionAttributeNames:  map[string]string{"#gen": "gen", "#sh": "sh", "#w": "w", "#lid": "lid"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":gen": &types.AttributeValueMemberS{Value: generation}, ":sh": &types.AttributeValueMemberS{Value: frame.Shape}, ":w": numberAttr(snapshot), ":id": &types.AttributeValueMemberS{Value: holderID}},
	})
	return err
}

// HistoriesWrittenAfter reads, through the GSI on Updated, every record written after moment.
func (backend *frameBackend) HistoriesWrittenAfter(moment int64) ([]dataframe.History, error) {
	query := &queryState{meta: backend.meta, preds: []predicate{{field: updatedFieldName, op: opGt, v1: moment}}}
	plans, err := query.plans()
	if err != nil {
		return nil, err
	}
	input, err := query.queryInput(plans[0])
	if err != nil {
		return nil, err
	}
	var histories []dataframe.History
	for {
		out, err := backend.client.Query(context.Background(), input)
		if err != nil {
			return nil, err
		}
		if histories, err = backend.appendHistories(histories, out.Items); err != nil {
			return nil, err
		}
		if len(out.LastEvaluatedKey) == 0 {
			return histories, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// ScanHistories reads every record of the table, page by page.
func (backend *frameBackend) ScanHistories(onPage func(histories []dataframe.History) error) error {
	return scanItems(backend.client, tableName(), []pkRange{backend.meta.baseRowsRange()}, nil, func(items []map[string]types.AttributeValue) error {
		histories, err := backend.appendHistories(nil, items)
		if err != nil {
			return err
		}
		return onPage(histories)
	})
}

func (backend *frameBackend) appendHistories(histories []dataframe.History, items []map[string]types.AttributeValue) ([]dataframe.History, error) {
	for _, item := range items {
		recordPtr, err := backend.meta.decodeRecord(item)
		if err != nil {
			return nil, err
		}
		history, err := backend.meta.historyOf(item, recordPtr)
		if err != nil {
			return nil, err
		}
		histories = append(histories, history)
	}
	return histories, nil
}
