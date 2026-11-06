package db

import "testing"

// A packed key that turns project_id into an ID range outranks the view by priority, but leaves
// status uncovered: Scylla refuses that as a filter, so the covering view must win.
func TestMatchQueryCapabilityPrefersCoverageOverPriority(t *testing.T) {
	capabilities := []QueryCapability{
		{Signature: "company_id|=|project_id|=", Priority: 400, IsKey: true},
		{
			Signature: "company_id|=|project_id|=|status|=",
			Priority:  360,
			Source:    &IndexSource{Name: "projects_epic__dlt_project_id_status_view"},
		},
	}
	statements := []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: 1},
		{Col: "project_id", Operator: "=", Value: 1},
		{Col: "status", Operator: "=", Value: 1},
	}

	bestCapability := MatchQueryCapability(statements, capabilities)
	if bestCapability == nil || bestCapability.Signature != "company_id|=|project_id|=|status|=" {
		t.Fatalf("expected the view covering status, got %+v", bestCapability)
	}
}

func TestMatchQueryCapabilityReturnsSelectedCapability(t *testing.T) {
	capabilities := []QueryCapability{
		{Signature: "empresa_id|=", Priority: 10, IsKey: true},
		{
			Signature: "empresa_id|=|status|=",
			Priority:  25,
			Source:    &IndexSource{Name: "product_supply__status_view"},
		},
	}
	statements := []ColumnStatement{
		{Col: "empresa_id", Operator: "=", Value: 1},
		{Col: "status", Operator: "=", Value: 1},
	}

	bestCapability := MatchQueryCapability(statements, capabilities)
	if bestCapability == nil {
		t.Fatal("expected a capability match")
	}
	if bestCapability.Signature != "empresa_id|=|status|=" {
		t.Fatalf("unexpected capability selected: %s", bestCapability.Signature)
	}
	if bestCapability.Source == nil || bestCapability.Source.Name != "product_supply__status_view" {
		t.Fatalf("unexpected capability source: %+v", bestCapability.Source)
	}
}

// Equality on a concatenated key's full source set is an equality on the key text; a range on its
// last pinned column is a prefix-bounded range, and other predicates are left over.
func TestKeyConcatenatedRewrite(t *testing.T) {
	core := &TableCore{
		Keys:            []IColInfo{accessTestColumn("id")},
		KeyConcatenated: []IColInfo{accessTestColumn("warehouse_id"), accessTestColumn("category_id")},
	}
	statusStatement := ColumnStatement{Col: "status", Operator: "=", Value: 1}
	rewritten, canRewrite := core.BuildKeyConcatenatedStatements([]ColumnStatement{
		{Col: "warehouse_id", Operator: "=", Value: 3},
		{Col: "category_id", Operator: "=", Value: 7},
		statusStatement,
	})
	if !canRewrite || len(rewritten) != 2 || rewritten[0].Operator != "=" || rewritten[0].Value != MakeKeyConcat(3, 7) || rewritten[1].Col != statusStatement.Col {
		t.Fatalf("full prefix rewrite = %+v", rewritten)
	}

	rewritten, canRewrite = core.BuildKeyConcatenatedStatements([]ColumnStatement{
		{Col: "warehouse_id", Operator: "=", Value: 3},
		{Col: "category_id", Operator: ">=", Value: 7},
	})
	prefix := MakeKeyConcat(3) + KeyConcatSeparator
	if !canRewrite || rewritten[0].Operator != "BETWEEN" || rewritten[0].From[0].Value != MakeKeyConcat(3, 7) ||
		rewritten[0].To[0].Value != prefix+"￿" {
		t.Fatalf("range rewrite = %+v", rewritten[0])
	}

	if _, canRewrite := core.BuildKeyConcatenatedStatements([]ColumnStatement{{Col: "id", Operator: "=", Value: "x"}}); canRewrite {
		t.Fatal("a query on the physical key must not be rewritten")
	}
}

// A partial int-packed prefix is the inclusive block of every key starting with it.
func TestKeyIntPackingRewrite(t *testing.T) {
	slotBits := []int64{20, 20, 23}
	core := &TableCore{
		Keys:                  []IColInfo{accessTestColumn("id")},
		KeyIntPacking:         []IColInfo{accessTestColumn("project_id"), accessTestColumn("epic_id"), accessTestColumn("autoincrement_placeholder")},
		KeyIntPackingSlotBits: slotBits,
	}
	rewritten, canRewrite := core.BuildKeyIntPackingStatements([]ColumnStatement{{Col: "project_id", Operator: "=", Value: int32(5)}})
	if !canRewrite || rewritten[0].Operator != "BETWEEN" ||
		rewritten[0].From[0].Value != int64(PackSlotPrefixBound([]int64{5}, slotBits, false)) ||
		rewritten[0].To[0].Value != int64(PackSlotPrefixBound([]int64{5}, slotBits, true)) {
		t.Fatalf("prefix rewrite = %+v", rewritten)
	}
}
