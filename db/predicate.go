package db

import (
	"fmt"
	"strings"

	"github.com/ivanjoz/genix-orm/db/cache"
)

// ColumnStatement is one predicate against a column. It is a plain description
// of intent — "this column, this operator, this value" — that each driver
// compiles into its own query form (a CQL WHERE clause, a DynamoDB key
// condition). BETWEEN carries its bounds in From/To.
type ColumnStatement struct {
	Col      string
	Operator string
	Value    any
	Values   []any
	From     []ColumnStatement
	To       []ColumnStatement
}

// GetValue renders the predicate value as a literal for drivers that build
// statement text rather than binding parameters.
func (q ColumnStatement) GetValue() any {
	if len(q.Values) > 0 && q.Operator == "IN" {
		values := []string{}
		for _, v := range q.Values {
			if str, ok := v.(string); ok {
				values = append(values, `'`+str+`'`)
			} else {
				values = append(values, fmt.Sprintf("%v", v))
			}
		}
		return "(" + strings.Join(values, ", ") + ")"
	} else if str, ok := q.Value.(string); ok {
		return `'` + str + `'`
	} else {
		return q.Value
	}
}

// DeltaRead is what a Delta() call asked for: the client's cursor of the log, and the values a first
// sync (or a replace) filters the index's sync filter column with.
type DeltaRead struct {
	Cursor           cache.DeltaCursor
	SyncFilterValues []any
}

// TableInfo is the mutable per-query state collected by the fluent builder
// before Exec hands it to a driver. It is bound once per query, never shared,
// and holds no storage-engine detail — only what the caller asked for.
type TableInfo struct {
	Statements     []ColumnStatement
	ColumnsInclude []ColumnInfo
	ColumnsExclude []ColumnInfo
	GroupByColumns []ColumnInfo
	Between        ColumnStatement
	OrderBy        string
	Limit          int32
	AllowFilter    bool
	// CachedIndexGroups stores client freshness by hash for grouped-index reads.
	CachedIndexGroups map[int32]int32
	// UseIndexGroupSelect routes Exec into grouped hash fetches.
	UseIndexGroupSelect bool
	// Delta makes Exec a delta-cache read (db/cache), set by TableStruct.Delta.
	Delta *DeltaRead
	// RefSlice points at the caller's destination slice (*[]E). It is untyped here
	// so the driver boundary stays free of type parameters.
	RefSlice any
}

// The predicate setters live on TableInfo rather than only on TableStruct so a read built at
// runtime (ApplyFilters, the driver's controller) can fill one without a typed table struct.

func (t *TableInfo) SetWhere(colname string, operator string, value any) {
	t.Statements = append(t.Statements, ColumnStatement{Col: colname, Operator: operator, Value: value})
}

// SetWhereIn adds an IN predicate. It exists next to SetWhere because ColumnStatement carries IN
// operands in Values rather than in Value.
func (t *TableInfo) SetWhereIn(colname string, values []any) {
	t.Statements = append(t.Statements, ColumnStatement{Col: colname, Operator: "IN", Values: values})
}

// SetBetween adds a BETWEEN predicate from runtime values, mirroring Col.Between for callers that
// only know the column by name. Its bounds go in From/To, like every other BETWEEN.
func (t *TableInfo) SetBetween(colname string, from, to any) {
	t.Statements = append(t.Statements, ColumnStatement{
		Col:      colname,
		Operator: "BETWEEN",
		From:     []ColumnStatement{{Col: colname, Value: from}},
		To:       []ColumnStatement{{Col: colname, Value: to}},
	})
}

func (t *TableInfo) SetOrderDesc() {
	t.OrderBy = "ORDER BY %v DESC"
}
