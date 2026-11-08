package scylla

import (
	"fmt"

	"github.com/ivanjoz/genix-orm/db"
)

// prepareWhereClauses binds the statements a view was chosen for: db plans the reads against the
// view's key (db.PlanSourceSteps) and this renders each one as CQL. nil means the view cannot serve
// these predicates.
func (view *viewInfo) prepareWhereClauses(statements ...ColumnStatement) []boundWhereClause {
	steps, filter, err := db.PlanSourceSteps(view.IndexSource, statements)
	if err != nil {
		fmt.Printf("View rejected: view=%s %v\n", view.Name, err)
		return nil
	}
	return renderAccessSteps(view, steps, filter)
}

// renderAccessPlan renders a bound AccessPlan as the WHERE clauses of its reads. A plan with no
// source reads the base table, whose values need no driver encoding.
func renderAccessPlan(scyllaTable *ScyllaTable, plan *db.AccessPlan) []boundWhereClause {
	var view *viewInfo
	if plan.Source != nil {
		view = scyllaTable.viewOfSource(plan.Source)
	}
	return renderAccessSteps(view, plan.Steps, plan.Filter)
}

// renderAccessSteps renders every step, with the Filter statements pushed into each one's WHERE.
// Each step goes through the generic batcher, so a value list on a clustering column is split into
// as many queries as the node's restriction ceiling needs.
func renderAccessSteps(view *viewInfo, steps []db.AccessStep, filter []ColumnStatement) []boundWhereClause {
	whereClauses := []boundWhereClause{}
	for _, step := range steps {
		stepStatements := append(accessStepStatements(view, step), filter...)
		whereClauses = append(whereClauses, buildRemainingWhereClauseBatches(stepStatements)...)
	}
	return whereClauses
}

// accessStepStatements turns one step back into CQL-shaped statements: = or IN per prefix column,
// a BETWEEN (inclusive both ends) or one-sided operators for the bound, CONTAINS for an element.
func accessStepStatements(view *viewInfo, step db.AccessStep) []ColumnStatement {
	if step.Element != nil {
		return append(equalPrefixStatements(view, step.Element.EqualPrefix),
			ColumnStatement{Col: step.Element.Column, Operator: "CONTAINS", Value: step.Element.Value})
	}
	keyRange := step.Range
	statements := equalPrefixStatements(view, keyRange.EqualPrefix)
	if keyRange.BoundColumn == "" {
		return statements
	}

	column := keyRange.BoundColumn
	if keyRange.From != nil && keyRange.To != nil && keyRange.FromInclusive && keyRange.ToInclusive {
		return append(statements, ColumnStatement{
			Col: column, Operator: "BETWEEN",
			From: []ColumnStatement{{Col: column, Value: encodeAccessValue(view, column, keyRange.From)}},
			To:   []ColumnStatement{{Col: column, Value: encodeAccessValue(view, column, keyRange.To)}},
		})
	}
	if keyRange.From != nil {
		operator := ">"
		if keyRange.FromInclusive {
			operator = ">="
		}
		statements = append(statements, ColumnStatement{Col: column, Operator: operator, Value: encodeAccessValue(view, column, keyRange.From)})
	}
	if keyRange.To != nil {
		operator := "<"
		if keyRange.ToInclusive {
			operator = "<="
		}
		statements = append(statements, ColumnStatement{Col: column, Operator: operator, Value: encodeAccessValue(view, column, keyRange.To)})
	}
	return statements
}

// equalPrefixStatements renders pinned key columns: = for one value, IN for several.
func equalPrefixStatements(view *viewInfo, equalPrefix []db.KeyValues) []ColumnStatement {
	statements := make([]ColumnStatement, 0, len(equalPrefix)+2)
	for _, keyValues := range equalPrefix {
		values := make([]any, 0, len(keyValues.Values))
		for _, value := range keyValues.Values {
			values = append(values, encodeAccessValue(view, keyValues.Column, value))
		}
		if len(values) == 1 {
			statements = append(statements, ColumnStatement{Col: keyValues.Column, Operator: "=", Value: values[0]})
		} else {
			statements = append(statements, ColumnStatement{Col: keyValues.Column, Operator: "IN", Values: values})
		}
	}
	return statements
}

// encodeAccessValue applies Scylla's storage encoding to a planned value: a packed column holds the
// unsigned layout with the top bit flipped, because Scylla compares integers signed.
func encodeAccessValue(view *viewInfo, column string, value any) any {
	packedValue, isPacked := value.(uint64)
	if !isPacked || view == nil {
		return value
	}
	if view.PackedIndex != nil && column == view.PackedIndex.PackedColumnName {
		return storeVirtualPacked(packedValue, view.PackedIndex.IsInt32Packed)
	}
	if view.Type == 8 && column == view.Column.GetName() {
		return storeVirtualPacked(packedValue, view.PackedIsInt32)
	}
	return value
}
