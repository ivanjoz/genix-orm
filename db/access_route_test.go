package db

import (
	"reflect"
	"testing"
)

func accessRouteTestCore() *TableCore {
	partition := accessTestColumn("company_id")
	core := &TableCore{
		Name:       "orders",
		PartKey:    partition,
		Keys:       []IColInfo{accessTestColumn("id")},
		ColumnsMap: map[string]IColInfo{},
	}
	for _, name := range []string{"company_id", "id", "status", "warehouse_id"} {
		core.ColumnsMap[name] = accessTestColumn(name)
	}
	return core
}

// A base-table read keeps the key prefix as the step and every other predicate as Filter.
func TestBindAccessPlanPrimaryKeyPrefixAndFilter(t *testing.T) {
	core := accessRouteTestCore()
	statusStatement := ColumnStatement{Col: "status", Operator: "=", Value: 2}
	tableInfo := &TableInfo{Statements: []ColumnStatement{
		statusStatement,
		{Col: "company_id", Operator: "=", Value: 1},
		{Col: "id", Operator: "IN", Values: []any{5, 6}},
	}, Limit: 10}
	tableInfo.SetOrderDesc()

	route := core.ChooseAccessRoute(tableInfo.SelectStatements(), core.ComputeCapabilities(), nil)
	if route.Kind != AccessRoutePrimaryKey {
		t.Fatalf("route kind = %d", route.Kind)
	}
	plan, err := core.BindAccessPlan(route, tableInfo)
	if err != nil {
		t.Fatal(err)
	}
	expectedRange := KeyRange{EqualPrefix: []KeyValues{
		{Column: "company_id", Values: []any{1}},
		{Column: "id", Values: []any{5, 6}},
	}}
	if len(plan.Steps) != 1 || !reflect.DeepEqual(*plan.Steps[0].Range, expectedRange) {
		t.Fatalf("steps = %+v", plan.Steps)
	}
	if len(plan.Filter) != 1 || plan.Filter[0].Col != "status" || plan.Limit != 10 || !plan.Descending {
		t.Fatalf("plan = %+v", plan)
	}
}

// A projected simple view serves its own key columns; the rest is Filter, and the read comes from
// the view's table.
func TestChooseAccessRouteProjectedView(t *testing.T) {
	core := accessRouteTestCore()
	core.IndexSources = []*IndexSource{{
		Kind: IndexKindView, Type: 6, Name: "orders__status_view",
		Column: accessTestColumn("status"), Columns: []string{"company_id", "status"},
	}}
	statements := []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: 1},
		{Col: "status", Operator: "=", Value: 2},
		{Col: "warehouse_id", Operator: "=", Value: 3},
	}
	route := core.ChooseAccessRoute(statements, core.ComputeCapabilities(), []string{"id", "status"})
	if route.Kind != AccessRouteSource || !route.ReadsSourceTable ||
		!reflect.DeepEqual(route.SelectedStatementIndexes, []int{0, 1}) || !reflect.DeepEqual(route.RemainingStatementIndexes, []int{2}) {
		t.Fatalf("route = %+v", route)
	}
	plan, err := core.BindAccessPlan(route, &TableInfo{Statements: statements})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Source != core.IndexSources[0] || len(plan.Filter) != 1 || plan.Filter[0].Col != "warehouse_id" {
		t.Fatalf("plan = %+v", plan)
	}
}
