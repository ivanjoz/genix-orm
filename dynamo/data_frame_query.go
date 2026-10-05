package dynamo

import (
	"fmt"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// FrameRow is one row of a DataFrame file, as Group is one GroupBy counter.
type FrameRow[E any] struct {
	// Key is a record with only the frame's Keys and Rows set.
	Key   E
	frame *dataframe.Frame
	sums  []int64
}

// Sum returns the row's sum of a Sums column of the frame.
func (row FrameRow[E]) Sum(column Coln) int64 {
	fieldName := column.col().fieldName
	for i, sum := range row.frame.Sums {
		if sum.FieldName == fieldName {
			return row.sums[i]
		}
	}
	panic(fmt.Sprintf("db: %q is not a Sums column of DataFrame %q", fieldName, row.frame.Name))
}

// Count returns how many records the row sums, on a frame declared with Count.
func (row FrameRow[E]) Count() int64 {
	if !row.frame.CountsRecords {
		panic(fmt.Sprintf("db: DataFrame %q is not declared with Count", row.frame.Name))
	}
	return row.sums[len(row.frame.Sums)]
}

// FrameQuery reads the files of one DataFrame.
type FrameQuery[E any] struct {
	meta    *tableMeta
	frame   *dataframe.Frame
	preds   []predicate
	isFresh bool
	planErr error
}

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

// Fresh returns the rows as the records hold them now instead of as the frame's last compaction left
// them: the files plus the changes since, read from the records written after the frame's snapshot
// (consistently, through the delta index) and from the log. It costs that delta read besides the
// files: the writes of the whole entity since the last compaction. When more than 100 records changed
// up to a settled checkpoint, it also writes them into the files before it returns (an express
// compaction), which keeps the next fresh reads small.
func (q *FrameQuery[E]) Fresh() *FrameQuery[E] {
	q.isFresh = true
	return q
}

// Exec reads the selected files in parallel (dataframe.Read) and returns their rows sorted by the
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

	firstKeyField := frame.Keys[0].FieldName
	firstKey, isPinned := byField[firstKeyField]
	if !isPinned || (firstKey.op != opEq && firstKey.op != opBetween) {
		return nil, fmt.Errorf("%s needs an Eq or a Between on %s", queryName, firstKeyField)
	}
	fromKey := valueToInt64(firstKey.v1, firstKeyField)
	toKey := fromKey
	if firstKey.op == opBetween {
		toKey = valueToInt64(firstKey.v2, firstKeyField)
	}
	var pinnedKeys []int64
	for _, key := range frame.Keys[1:] {
		p, isPinned := byField[key.FieldName]
		if !isPinned {
			break
		}
		if p.op != opEq {
			return nil, fmt.Errorf("%s takes only an Eq on %s", queryName, key.FieldName)
		}
		pinnedKeys = append(pinnedKeys, valueToInt64(p.v1, key.FieldName))
	}
	if len(byField) != 1+len(pinnedKeys) {
		return nil, fmt.Errorf("%s takes an Eq on a leading run of the Keys after %s, and no other column", queryName, firstKeyField)
	}

	fileKeys, files, _, err := dataframe.Read(frameTable[E]{q.meta}, frame, fromKey, toKey, pinnedKeys, q.isFresh, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", queryName, err)
	}
	// fileKeys are in key order, and a file's rows ascend.
	var rows []FrameRow[E]
	for i, file := range files {
		for j, rowID := range file.RowIDs {
			row := FrameRow[E]{frame: frame, sums: make([]int64, frame.SumsCount)}
			keyPtr := unsafe.Pointer(&row.Key)
			for k, key := range frame.Keys {
				key.Set(keyPtr, fileKeys[i][k])
			}
			frame.Rows.Set(keyPtr, rowID)
			for s := range row.sums {
				row.sums[s] = file.Sums[s][j]
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// FrameSource returns the live read of the DataFrame frameName (dataframe.FrameSource), what FrameSQL
// reads a frame through without the Repo's record type. It is declared once, at boot, so an unknown
// frame panics there.
func (r *Repo[T, E]) FrameSource(frameName string) *dataframe.FrameSource {
	query := r.QueryFrame(frameName)
	if query.planErr != nil {
		panic(query.planErr.Error())
	}
	return dataframe.NewFrameSource(frameTable[E]{r.meta}, query.frame)
}
