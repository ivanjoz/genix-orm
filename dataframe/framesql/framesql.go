// Package framesql runs FrameSQL (FRAMESQL.md): one SQL statement over one DataFrame, folded from the
// frame's rows without a record per row. It is pure: the rows come from a Source (the ORM's read),
// dates and record names from the caller's Resolvers.
//
//	parse.go    the statement's text to its syntax tree
//	plan.go     the tree bound to a frame: the read's selection, the filters, the groups, typed items
//	engine.go   the fold of the rows into one accumulator, then the items, ORDER BY and LIMIT
//
// Errors are written for an agent: they name the frame and say how to rewrite the statement.
package framesql

import (
	"github.com/ivanjoz/genix-orm/dataframe"
)

// maxKeys is the most Keys a frame has (dataframe.Compile allows 1 to 3).
const maxKeys = 3

// Kind is what a column's integers mean. The ORM stores only integers: the caller declares the rest.
type Kind uint8

const (
	KindInteger Kind = iota
	// KindDay is a UnixDay: PERIOD(), WEEK() and MONTH() take it.
	KindDay
	// KindCents is money in integer cents.
	KindCents
	// KindRef is a record ID: a name in quotes is resolved to IDs by Resolvers.Names.
	KindRef
	// KindDecimal is only a result's: a division.
	KindDecimal
	// KindQuantity is a float Sums column, kept by the frame as value × dataframe.FloatScale: a sum
	// of whole units with their fraction (1 box and 1 sixth is 1.1667). Results read it unscaled.
	KindQuantity
)

// Column is one column of a frame, as statements name it.
type Column struct {
	// Name is the snake_case name statements use ("product_id").
	Name string
	// Label is the column's name for people, passed through to the result as is.
	Label string
	Kind  Kind
	// Collection names the records a KindRef column's IDs point to. Resolvers.Names gets it back, and so
	// does the result column.
	Collection string
	// Hidden keeps a column of the files out of every statement: it is not named in errors, it can't
	// be filtered or grouped, and a hidden Rows column folds into the other Rows of its row. It is for
	// a column the frame keeps for another reader (a page's chart) and a statement would misuse.
	Hidden bool
}

// Frame is a DataFrame a statement may read: its columns, in the files' order, and its read.
type Frame struct {
	// Name is the frame's name ("day-product"). Statements may write it with '_' for '-'.
	Name string
	// Keys name the files, 1 to 3. A statement must bound Keys[0].
	Keys []Column
	// Rows are the columns that name a row of a file, 1 to dataframe.MaxRowColumns, in the frame's order.
	Rows []Column
	// Sums are the summed columns, in the order of dataframe.Row.Sums. A frame that counts records has
	// one more sum, last, which a statement never reads.
	Sums   []Column
	Source Source
}

// Source is the read of one frame (the ORM's db.FrameSource): the rows of the files whose Keys[0] is
// in [FromKey, ToKey] and whose later Keys start with PinnedKeys.
type Source func(selection dataframe.Selection) ([]dataframe.Row, error)

// Resolvers turn what a statement says in the user's terms into integers. Their errors come back
// wrapped, so the caller still recognizes its own (a question put to the user).
type Resolvers struct {
	// Period turns the text of PERIOD("…") into a range of UnixDays.
	Period func(text string) (fromDay, toDay int64, err error)
	// Names turns the names in quotes of a condition on a KindRef column into record IDs.
	Names func(column Column, names []string) ([]int64, error)
}

// Options bound a statement's size. Zero fields take the defaults. The read is bounded by the
// selection itself: at most dataframe.MaxFirstKeys values of Keys[0].
type Options struct {
	// MaxRows is the largest LIMIT, and the LIMIT of a statement without one. Default 1,000.
	MaxRows int
}

const defaultMaxRows = 1000

// Result is a statement's rows, as one typed column per SELECT item.
type Result struct {
	Columns []ResultColumn
	// RowCount is the length of every column's Values.
	RowCount int
	// FromKey and ToKey are the range of Keys[0] the read covered.
	FromKey, ToKey int64
	// Truncated reports that more groups matched than the LIMIT returned.
	Truncated bool
	// RowsRead is how many rows of the frame the read returned, before the filters.
	RowsRead int
}

// ResultColumn is one SELECT item over the result's rows. Values are float64 for every kind: integers
// are exact up to 2^53, cents are rounded to whole cents, quantities are unscaled, and NaN is no value
// (a division by zero).
type ResultColumn struct {
	// Name is the item's alias, else its column's name: product_id, amount for SUM(amount), count for
	// COUNT(*), week for WEEK(fecha). An expression without alias is named as written.
	Name string
	// Label is the source column's Label, on a group or an aggregate of one column; "" otherwise.
	Label      string
	Kind       Kind
	Collection string
	// IsGroup marks a GROUP BY value; the other columns are computed from each group's rows.
	IsGroup bool
	// Aggregate is "SUM", "AVG" or "COUNT" on an item that is only that aggregate. A group without rows
	// holds 0 of a SUM or a COUNT, and no AVG.
	Aggregate string
	// Period is "day", "week" or "month" on a group of Keys[0] when it is a day: FromKey and ToKey then
	// span every period the read covered, with rows or not.
	Period string
	Values []float64
}

// Statement is a statement bound to its frame and not read yet. Its Columns are known before any name
// is looked up: a caller checks what it will do with them first, since a lookup may stop the run to
// ask the user, and the statement must not fail once they answered.
type Statement struct {
	planner    *planner
	conditions []condition
}

// Prepare parses the statement and binds it to the frame named frameName ('_' or '-', any case): a
// statement's FROM may be left out, and must name that frame when it is not.
func Prepare(frameName, statementText string, frames []Frame, options Options) (*Statement, error) {
	parsed, err := parse(statementText)
	if err != nil {
		return nil, err
	}
	p, err := planStatement(frameName, parsed, frames, options)
	if err != nil {
		return nil, err
	}
	return &Statement{planner: p, conditions: parsed.conditions}, nil
}

// Columns describe the result's columns, without Values.
func (statement *Statement) Columns() []ResultColumn {
	columns := make([]ResultColumn, statement.planner.plan.visibleItems)
	for i := range columns {
		columns[i] = statement.planner.plan.items[i].column
	}
	return columns
}

// Run resolves WHERE (periods, names) and reads. A Statement runs once.
func (statement *Statement) Run(resolvers Resolvers) (Result, error) {
	plan, err := statement.bindConditions(resolvers)
	if err != nil {
		return Result{}, err
	}
	return execute(plan)
}

func (statement *Statement) bindConditions(resolvers Resolvers) (*queryPlan, error) {
	statement.planner.resolvers = resolvers
	if err := statement.planner.planConditions(statement.conditions); err != nil {
		return nil, err
	}
	return statement.planner.plan, nil
}

// Run prepares the statement on frameName and runs it.
func Run(frameName, statementText string, frames []Frame, resolvers Resolvers, options Options) (Result, error) {
	statement, err := Prepare(frameName, statementText, frames, options)
	if err != nil {
		return Result{}, err
	}
	return statement.Run(resolvers)
}
