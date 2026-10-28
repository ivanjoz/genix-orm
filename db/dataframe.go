package db

import (
	"fmt"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// FrameFilter selects a Key of a DataFrame: Equals sets From = To.
type FrameFilter struct {
	Column   string
	From, To int64
}

// FrameRequest is one read of a DataFrame: one partition, the frame's name, the Key filters (Keys[0]
// takes an Equals or a Between of at most dataframe.MaxFirstKeys values, a leading run of the later
// Keys an Equals) and whether to bring the rows up to the records written since the last run.
type FrameRequest struct {
	Partition int32
	Frame     string
	Filters   []FrameFilter
	// Selection replaces Filters for a caller that already holds the frame's selection (FrameSQL).
	Selection *dataframe.Selection
	IsFresh   bool
}

// FrameRow is one row of a DataFrame file.
type FrameRow[RecordT any] struct {
	// Key is a record with only the frame's Keys and Rows columns set.
	Key RecordT
	// Sums are the frame's Sums in declaration order (floats × dataframe.FloatScale), then the record
	// count when the frame has Count.
	Sums  []int64
	Frame *dataframe.Frame
}

// Sum is the row's sum of an integer Sums column.
func (row FrameRow[RecordT]) Sum(column Coln) int64 { return row.Sums[row.sumPosition(column, false)] }

// SumFloat is the row's sum of a float Sums column (kept with 6 decimals).
func (row FrameRow[RecordT]) SumFloat(column Coln) float64 {
	return float64(row.Sums[row.sumPosition(column, true)]) / dataframe.FloatScale
}

// Count is how many records the row sums, on a frame declared with Count.
func (row FrameRow[RecordT]) Count() int64 {
	if !row.Frame.CountsRecords {
		panic(fmt.Sprintf("db: DataFrame %q is not declared with Count", row.Frame.Name))
	}
	return row.Sums[row.Frame.SumsCount-1]
}

func (row FrameRow[RecordT]) sumPosition(column Coln, isFloat bool) int {
	for i, sum := range row.Frame.Sums {
		if sum.FieldName == column.GetName() {
			if sum.IsFloat != isFloat {
				panic(fmt.Sprintf("db: DataFrame %q column %q is read with Sum when it is an integer, SumFloat when a float", row.Frame.Name, sum.FieldName))
			}
			return i
		}
	}
	panic(fmt.Sprintf("db: %q is not a Sums column of DataFrame %q", column.GetName(), row.Frame.Name))
}

// FrameQuery builds a FrameRequest and runs it on the table's driver.
type FrameQuery[TableT any, RecordT any] struct {
	executor Executor[TableT, RecordT]
	request  FrameRequest
}

// QueryFrame reads the rows of one partition of a DataFrame. The rows are as of the frame's last run
// (2 to 12 minutes behind with a 10-minute schedule) unless the query is Fresh.
func QueryFrame[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	partition int32, frameName string,
) *FrameQuery[TableT, RecordT] {
	return &FrameQuery[TableT, RecordT]{executor: executorFor[RecordT, TableT, D](), request: FrameRequest{Partition: partition, Frame: frameName}}
}

func (query *FrameQuery[TableT, RecordT]) Equals(column Coln, value int64) *FrameQuery[TableT, RecordT] {
	query.request.Filters = append(query.request.Filters, FrameFilter{Column: column.GetName(), From: value, To: value})
	return query
}

func (query *FrameQuery[TableT, RecordT]) Between(column Coln, from, to int64) *FrameQuery[TableT, RecordT] {
	query.request.Filters = append(query.request.Filters, FrameFilter{Column: column.GetName(), From: from, To: to})
	return query
}

// Fresh brings the rows up to the records as they are now: the files plus the records written since
// the frame's last run.
func (query *FrameQuery[TableT, RecordT]) Fresh() *FrameQuery[TableT, RecordT] {
	query.request.IsFresh = true
	return query
}

// Exec returns the rows sorted by the frame's Keys, then by its Rows. A missing file has no rows.
func (query *FrameQuery[TableT, RecordT]) Exec() ([]FrameRow[RecordT], error) {
	return query.executor.QueryFrame(query.request)
}

// ExecRows returns the same rows as Exec, as dataframe rows with the frame: Keys and RowValues in the
// frame's column order. A ToFrames frame's rows can only be read this way, since Exec leaves their
// Key empty.
func (query *FrameQuery[TableT, RecordT]) ExecRows() ([]dataframe.Row, *dataframe.Frame, error) {
	return query.executor.QueryFrameRows(query.request)
}

// ExecChanged answers a client (the browser) that keeps the raw files of the selection:
// heldGeneration and heldFiles ("<keys>.<snapshot>.<slot>,…") describe its copies; the answer is the
// wire body of the files it must replace, as current as the records (dataframe/changed.go).
func (query *FrameQuery[TableT, RecordT]) ExecChanged(heldGeneration, heldFiles string) ([]byte, error) {
	return query.executor.QueryFrameChanged(query.request, heldGeneration, heldFiles)
}

// FrameSelection turns a request's filters into the dataframe selection of a frame: an Equals or
// Between on Keys[0], then Equals on a leading run of the later Keys, and no other column.
func FrameSelection(frame *dataframe.Frame, filters []FrameFilter) (dataframe.Selection, error) {
	queryName := fmt.Sprintf("db: QueryFrame(%q)", frame.Name)
	filterByColumn := map[string]FrameFilter{}
	for _, filter := range filters {
		filterByColumn[filter.Column] = filter
	}
	firstKey, isFiltered := filterByColumn[frame.Keys[0].FieldName]
	if !isFiltered {
		return dataframe.Selection{}, fmt.Errorf("%s needs an Equals or a Between on %s", queryName, frame.Keys[0].FieldName)
	}
	selection := dataframe.Selection{FromKey: firstKey.From, ToKey: firstKey.To}
	for _, key := range frame.Keys[1:] {
		filter, isFiltered := filterByColumn[key.FieldName]
		if !isFiltered {
			break
		}
		if filter.From != filter.To {
			return dataframe.Selection{}, fmt.Errorf("%s takes only an Equals on %s", queryName, key.FieldName)
		}
		selection.PinnedKeys = append(selection.PinnedKeys, filter.From)
	}
	if len(filterByColumn) != 1+len(selection.PinnedKeys) {
		return dataframe.Selection{}, fmt.Errorf("%s takes Equals on a leading run of the Keys after %s, and no other column", queryName, frame.Keys[0].FieldName)
	}
	return selection, nil
}
