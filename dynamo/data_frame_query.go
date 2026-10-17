package dynamo

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db/cache"
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

// Sum returns the row's sum of an integer Sums column of the frame.
func (row FrameRow[E]) Sum(column Coln) int64 { return row.sums[row.sumPosition(column, false)] }

// SumFloat returns the row's sum of a float Sums column of the frame (kept with 6 decimals).
func (row FrameRow[E]) SumFloat(column Coln) float64 {
	return float64(row.sums[row.sumPosition(column, true)]) / dataframe.FloatScale
}

func (row FrameRow[E]) sumPosition(column Coln, isFloat bool) int {
	fieldName := column.col().fieldName
	for i, sum := range row.frame.Sums {
		if sum.FieldName == fieldName {
			if sum.IsFloat != isFloat {
				panic(fmt.Sprintf("db: DataFrame %q column %q is read with Sum when it is an integer, SumFloat when a float", row.frame.Name, fieldName))
			}
			return i
		}
	}
	panic(fmt.Sprintf("db: %q is not a Sums column of DataFrame %q", fieldName, row.frame.Name))
}

// Count returns how many records the row sums, on a frame declared with Count.
func (row FrameRow[E]) Count() int64 {
	if !row.frame.CountsRecords {
		panic(fmt.Sprintf("db: DataFrame %q is not declared with Count", row.frame.Name))
	}
	return row.sums[row.frame.SumsCount-1]
}

// FrameQuery reads the files of one DataFrame.
type FrameQuery[E any] struct {
	meta    *tableMeta
	frame   *dataframe.Frame
	preds   []predicate
	isFresh bool
	planErr error
}

// QueryFrame reads a DataFrame's rows: Keys[0] takes an Eq or a Between (at most 400 values), and
// each later Key an Eq, or nothing for itself and the Keys after it, which lists the files of each
// Keys[0] value. The rows are as of the frame's last run, 2 to 12 minutes behind the records, unless
// the query is Fresh.
func (r *Repo[T, E]) QueryFrame(frameName string) *FrameQuery[E] {
	query := &FrameQuery[E]{meta: r.meta}
	if r.meta.frames != nil {
		query.frame = r.meta.frames.FrameNamed(frameName)
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

// Fresh returns the rows as the records hold them now: the files plus the records written since the
// frame's snapshot, read through the GSI on Updated.
func (q *FrameQuery[E]) Fresh() *FrameQuery[E] {
	q.isFresh = true
	return q
}

// Exec returns the rows sorted by the frame Keys, then by Rows. A missing file has no rows.
func (q *FrameQuery[E]) Exec() ([]FrameRow[E], error) {
	if q.planErr != nil {
		return nil, q.planErr
	}
	selection, err := q.selection()
	if err != nil {
		return nil, err
	}
	backend, err := frameBackendOf[E](q.meta)
	if err != nil {
		return nil, err
	}
	rows, err := dataframe.Read(q.meta.frames, q.frame, backend, cache.FileStore(), selection, q.isFresh)
	if err != nil {
		return nil, err
	}
	frameRows := make([]FrameRow[E], len(rows))
	for i, row := range rows {
		frameRows[i] = FrameRow[E]{frame: q.frame, sums: row.Sums}
		keyPtr := unsafe.Pointer(&frameRows[i].Key)
		for k, key := range q.frame.Keys {
			q.meta.trackedAccessors[key.TrackedIndex].setI64(keyPtr, row.Keys[k])
		}
		q.meta.trackedAccessors[q.frame.Rows.TrackedIndex].setI64(keyPtr, row.RowID)
	}
	return frameRows, nil
}

// selection reads the predicates: an Eq or Between on Keys[0], then Eqs on a leading run of the rest.
func (q *FrameQuery[E]) selection() (dataframe.Selection, error) {
	queryName := fmt.Sprintf("db: %s QueryFrame(%q)", q.meta.recordType.Name(), q.frame.Name)
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}
	firstKeyField := q.frame.Keys[0].FieldName
	firstKey, isPinned := byField[firstKeyField]
	if !isPinned || (firstKey.op != opEq && firstKey.op != opBetween) {
		return dataframe.Selection{}, fmt.Errorf("%s needs an Eq or a Between on %s", queryName, firstKeyField)
	}
	selection := dataframe.Selection{FromKey: valueToInt64(firstKey.v1, firstKeyField)}
	selection.ToKey = selection.FromKey
	if firstKey.op == opBetween {
		selection.ToKey = valueToInt64(firstKey.v2, firstKeyField)
	}
	for _, key := range q.frame.Keys[1:] {
		p, isPinned := byField[key.FieldName]
		if !isPinned {
			break
		}
		if p.op != opEq {
			return dataframe.Selection{}, fmt.Errorf("%s takes only an Eq on %s", queryName, key.FieldName)
		}
		selection.PinnedKeys = append(selection.PinnedKeys, valueToInt64(p.v1, key.FieldName))
	}
	if len(byField) != 1+len(selection.PinnedKeys) {
		return dataframe.Selection{}, fmt.Errorf("%s takes an Eq on a leading run of the Keys after %s, and no other column", queryName, firstKeyField)
	}
	return selection, nil
}
