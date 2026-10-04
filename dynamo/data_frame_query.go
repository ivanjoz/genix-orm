package dynamo

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
)

// FrameRow is one row of a DataFrame file, as Group is one GroupBy counter.
type FrameRow[E any] struct {
	// Key is a record with only the frame's Keys and Rows set.
	Key   E
	frame *dataFrameMeta
	sums  []int64
}

// Sum returns the row's sum of a Sums column of the frame.
func (row FrameRow[E]) Sum(column Coln) int64 {
	fieldName := column.col().fieldName
	for i, sum := range row.frame.sums {
		if sum.fieldName == fieldName {
			return row.sums[i]
		}
	}
	panic(fmt.Sprintf("db: %q is not a Sums column of DataFrame %q", fieldName, row.frame.Name))
}

// FrameQuery reads the files of one DataFrame.
type FrameQuery[E any] struct {
	meta    *tableMeta
	frame   *dataFrameMeta
	preds   []predicate
	isFresh bool
	planErr error
}

// frameFreshReadAttempts bounds the fresh reads a run committing meanwhile restarts.
const frameFreshReadAttempts = 3

// QueryFrame reads a DataFrame's rows: Keys[0] takes an Eq or a Between (at most
// 400 values, one GET each), and each later Key an Eq, or nothing for itself and
// the Keys after it, which reads every file of the day through its _idx. The
// rows are at the frame's last run, 10 to 30 minutes behind the records, unless
// the query is Fresh.
func (r *Repo[T, E]) QueryFrame(frameName string) *FrameQuery[E] {
	query := &FrameQuery[E]{meta: r.meta}
	for i := range r.meta.dataFrames {
		if r.meta.dataFrames[i].Name == frameName {
			query.frame = &r.meta.dataFrames[i]
		}
	}
	if query.frame == nil {
		query.planErr = fmt.Errorf("db: %s has no DataFrame %q", r.meta.recordType.Name(), frameName)
	}
	return query
}

func (q *FrameQuery[E]) Eq(column Coln, v any) *FrameQuery[E] {
	q.preds = append(q.preds, predicate{field: column.col().fieldName, op: opEq, v1: v})
	return q
}

func (q *FrameQuery[E]) Between(column Coln, a, b any) *FrameQuery[E] {
	q.preds = append(q.preds, predicate{field: column.col().fieldName, op: opBetween, v1: a, v2: b})
	return q
}

// Fresh returns the rows as the records hold them now instead of as the frame's last run left them:
// the files plus the changes since, read from the records written after the frame's snapshot
// (consistently, through the delta index) and from the log. It costs that delta read, about 10 to
// 30 minutes of writes of the whole entity, besides the files.
func (q *FrameQuery[E]) Fresh() *FrameQuery[E] {
	q.isFresh = true
	return q
}

// Exec reads the selected files in parallel and returns their rows sorted by the
// frame Keys, then by Rows. A missing file has no rows.
func (q *FrameQuery[E]) Exec() ([]FrameRow[E], error) {
	if q.planErr != nil {
		return nil, q.planErr
	}
	frame := q.frame
	queryName := fmt.Sprintf("db: %s QueryFrame(%q)", q.meta.recordType.Name(), frame.Name)
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}

	firstKey, isPinned := byField[frame.keys[0].fieldName]
	if !isPinned || (firstKey.op != opEq && firstKey.op != opBetween) {
		return nil, fmt.Errorf("%s needs an Eq or a Between on %s", queryName, frame.keys[0].fieldName)
	}
	fromKey := valueToInt64(firstKey.v1, frame.keys[0].fieldName)
	toKey := fromKey
	if firstKey.op == opBetween {
		toKey = valueToInt64(firstKey.v2, frame.keys[0].fieldName)
	}
	if toKey < fromKey || toKey-fromKey >= dataframe.MaxFirstKeys {
		return nil, fmt.Errorf("%s ranges %s over at most %d values", queryName, frame.keys[0].fieldName, dataframe.MaxFirstKeys)
	}
	var pinnedKeys []int64
	for _, key := range frame.keys[1:] {
		p, isPinned := byField[key.fieldName]
		if !isPinned {
			break
		}
		if p.op != opEq {
			return nil, fmt.Errorf("%s takes only an Eq on %s", queryName, key.fieldName)
		}
		pinnedKeys = append(pinnedKeys, valueToInt64(p.v1, key.fieldName))
	}
	if len(byField) != 1+len(pinnedKeys) {
		return nil, fmt.Errorf("%s takes an Eq on a leading run of the Keys after %s, and no other column", queryName, frame.keys[0].fieldName)
	}
	if frameStore == nil {
		return nil, fmt.Errorf("%s: no frame store is set: call SetDataFrames at boot", queryName)
	}
	client, err := Client()
	if err != nil {
		return nil, err
	}
	snapshot, err := q.frameSnapshot(client)
	if err != nil {
		return nil, err
	}

	var fileKeys [][dataframe.MaxKeys]int64
	var files []dataframe.File
	if q.isFresh {
		fileKeys, files, err = q.readFreshFiles(client, snapshot, fromKey, toKey, pinnedKeys)
	} else if fileKeys, err = dataframe.SelectFileKeys(frameStore, &frame.Frame, fromKey, toKey, pinnedKeys); err == nil {
		files, err = dataframe.ReadFiles(frameStore, &frame.Frame, fileKeys)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", queryName, err)
	}
	// fileKeys are in key order, and a file's rows ascend.
	var rows []FrameRow[E]
	for i, file := range files {
		for j, rowID := range file.RowIDs {
			row := FrameRow[E]{frame: frame, sums: make([]int64, len(frame.sums))}
			keyPtr := unsafe.Pointer(&row.Key)
			for k, key := range frame.keys {
				key.acc.setI64(keyPtr, fileKeys[i][k])
			}
			frame.rows.acc.setI64(keyPtr, rowID)
			for s := range frame.sums {
				row.sums[s] = file.Sums[s][j]
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// frameSnapshot reads the snapshot the frame's files hold (w), consistently. It fails with
// dataframe.ErrNotBuilt while the files are not those of the frame's current shape: before its
// first run, or until a shape change is rebuilt.
func (q *FrameQuery[E]) frameSnapshot(client *dynamodb.Client) (int64, error) {
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName()),
		Key:            q.meta.frameStateKey(q.frame),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, err
	}
	if _, hasSnapshot := out.Item["w"]; !hasSnapshot || uint32(numberAttrValue(out.Item, "sh")) != q.frame.Shape {
		return 0, fmt.Errorf("%w: %s DataFrame %q", dataframe.ErrNotBuilt, q.meta.recordType.Name(), q.frame.Name)
	}
	return numberAttrValue(out.Item, "w"), nil
}

// readFreshFiles is dataframe.ReadFreshFiles from the frame's snapshot. A run that commits meanwhile
// truncates the log up to its new snapshot, maybe before the read got the entries it needed: when
// the snapshot moved, the read starts over from the new one.
func (q *FrameQuery[E]) readFreshFiles(client *dynamodb.Client, snapshot, fromKey, toKey int64, pinnedKeys []int64) ([][dataframe.MaxKeys]int64, []dataframe.File, error) {
	readWrittenAfter, readBySK := frameRecordReaders[E](q.meta, client, q.frame)
	for attempt := 1; ; attempt++ {
		fileKeys, files, err := dataframe.ReadFreshFiles(frameStore, &q.frame.Frame, snapshot, fromKey, toKey, pinnedKeys, readWrittenAfter, readBySK)
		if err != nil {
			return nil, nil, err
		}
		snapshotAfter, err := q.frameSnapshot(client)
		if err != nil || snapshotAfter == snapshot {
			return fileKeys, files, err
		}
		if attempt == frameFreshReadAttempts {
			return nil, nil, fmt.Errorf("the frame's runs kept committing during %d fresh reads: retry", attempt)
		}
		snapshot = snapshotAfter
	}
}
