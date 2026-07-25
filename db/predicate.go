package db

import (
	"fmt"
	"strings"
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
	// RefSlice points at the caller's destination slice (*[]E). It is untyped here
	// so the driver boundary stays free of type parameters.
	RefSlice any
}
