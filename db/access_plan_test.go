package db

import (
	"reflect"
	"testing"
)

func accessTestColumn(name string) *ColumnInfo {
	return &ColumnInfo{ColInfo: ColInfo{Name: name}, ColType: GetColTypeByName("int32")}
}

// packedRangeTestSource is a delta-like range view on [company_id | status, updated].
func packedRangeTestSource() *IndexSource {
	return &IndexSource{
		Name: "orders__status_updated_view", Type: 8,
		Column:                  accessTestColumn("zz_status_updated"),
		PartitionColumn:         accessTestColumn("company_id"),
		PackedSourceColumns:     []IColInfo{accessTestColumn("status"), accessTestColumn("updated")},
		PackedSlotBitsPerColumn: []int64{8, 16},
	}
}

func TestPackedRangeStepsPrefixAndRange(t *testing.T) {
	source := packedRangeTestSource()
	steps, filter, err := PlanSourceSteps(source, []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "status", Operator: "IN", Values: []any{int8(1), int8(2)}},
		{Col: "updated", Operator: ">=", Value: int32(500)},
	})
	if err != nil || len(filter) != 0 {
		t.Fatalf("unexpected err=%v filter=%v", err, filter)
	}
	slotBits := source.PackedSlotBitsPerColumn
	expected := []AccessStep{}
	for _, status := range []int64{1, 2} {
		expected = append(expected, AccessStep{Range: &KeyRange{
			EqualPrefix: []KeyValues{{Column: "company_id", Values: []any{int64(7)}}},
			BoundColumn: "zz_status_updated",
			From:        PackSlotValues([]int64{status, 500}, slotBits), FromInclusive: true,
			To: PackSlotPrefixBound([]int64{status}, slotBits, true), ToInclusive: true,
		}})
	}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("steps = %+v, want %+v", *steps[0].Range, *expected[0].Range)
	}
}

func TestPackedRangeStepsFullKeyIsOneValueList(t *testing.T) {
	source := packedRangeTestSource()
	steps, _, err := PlanSourceSteps(source, []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "status", Operator: "IN", Values: []any{int8(1), int8(2)}},
		{Col: "updated", Operator: "=", Value: int32(9)},
	})
	if err != nil || len(steps) != 1 {
		t.Fatalf("err=%v steps=%v", err, steps)
	}
	slotBits := source.PackedSlotBitsPerColumn
	expected := KeyRange{EqualPrefix: []KeyValues{
		{Column: "company_id", Values: []any{int64(7)}},
		{Column: "zz_status_updated", Values: []any{PackSlotValues([]int64{1, 9}, slotBits), PackSlotValues([]int64{2, 9}, slotBits)}},
	}}
	if !reflect.DeepEqual(*steps[0].Range, expected) {
		t.Fatalf("range = %+v, want %+v", *steps[0].Range, expected)
	}
}

// A filter behind an unconstrained packed slot cannot be one range: the planner refuses the source.
func TestPackedRangeStepsRefuseAFilterBehindAGap(t *testing.T) {
	_, _, err := PlanSourceSteps(packedRangeTestSource(), []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "updated", Operator: "=", Value: int32(9)},
	})
	if err == nil {
		t.Fatal("expected a refusal for a filter behind the unconstrained status slot")
	}
}

func TestKeyPrefixStepServesLeadingColumnsAndFiltersTheRest(t *testing.T) {
	source := &IndexSource{
		Name: "products__category_ids_view", Type: 9, FanoutColumnName: "category_ids",
		Columns: []string{"company_id", "category_ids", "status"},
	}
	nameStatement := ColumnStatement{Col: "name", Operator: "=", Value: "x"}
	steps, filter, err := PlanSourceSteps(source, []ColumnStatement{
		nameStatement,
		{Col: "status", Operator: ">=", Value: int8(2)},
		{Col: "category_ids", Operator: "CONTAINS", Value: int32(5)},
		{Col: "company_id", Operator: "=", Value: int32(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	expected := KeyRange{
		EqualPrefix: []KeyValues{
			{Column: "company_id", Values: []any{int32(1)}},
			{Column: "category_ids", Values: []any{int32(5)}},
		},
		BoundColumn: "status", From: int8(2), FromInclusive: true,
	}
	if len(steps) != 1 || !reflect.DeepEqual(*steps[0].Range, expected) {
		t.Fatalf("range = %+v, want %+v", *steps[0].Range, expected)
	}
	if !reflect.DeepEqual(filter, []ColumnStatement{nameStatement}) {
		t.Fatalf("filter = %+v", filter)
	}
}

func TestPackedIndexStepsOnePerPrefixAndGlobalInRule(t *testing.T) {
	packedIndex := &PackedIndexInfo{
		PackedColumnName: "zz_ixp_status_kind", SourceColumnNames: []string{"status", "kind"},
		SlotBitsPerColumn: []int64{8, 8}, PartitionColumnName: "company_id",
	}
	source := &IndexSource{Name: "t__zz_ixp_status_kind_index_1", Kind: IndexKindPacked, PackedIndex: packedIndex}
	steps, _, err := PlanSourceSteps(source, []ColumnStatement{
		{Col: "status", Operator: "IN", Values: []any{int8(1), int8(2)}},
		{Col: "kind", Operator: "BETWEEN", From: []ColumnStatement{{Col: "kind", Value: int16(3)}}, To: []ColumnStatement{{Col: "kind", Value: int16(4)}}},
	})
	if err != nil || len(steps) != 2 {
		t.Fatalf("err=%v steps=%d", err, len(steps))
	}
	if steps[1].Range.From != PackSlotValues([]int64{2, 3}, packedIndex.SlotBitsPerColumn) ||
		steps[1].Range.To != PackSlotValues([]int64{2, 4}, packedIndex.SlotBitsPerColumn) {
		t.Fatalf("second step = %+v", *steps[1].Range)
	}

	// A global packed index only takes IN on its first component.
	globalIndex := *packedIndex
	globalIndex.PartitionColumnName = ""
	globalIndex.SourceColumnNames = []string{"status", "kind", "updated"}
	globalIndex.SlotBitsPerColumn = []int64{8, 8, 8}
	source.PackedIndex = &globalIndex
	if _, _, err := PlanSourceSteps(source, []ColumnStatement{
		{Col: "status", Operator: "=", Value: int8(1)},
		{Col: "kind", Operator: "IN", Values: []any{int16(1), int16(2)}},
		{Col: "updated", Operator: "=", Value: int32(1)},
	}); err == nil {
		t.Fatal("expected a refusal for IN behind the first component of a global packed index")
	}
}

func TestGroupHashStepsDeduplicateAndReadCollectionsByElement(t *testing.T) {
	source := &IndexSource{
		Name: "t__zz_igs_tags", Kind: IndexKindGroupHash, Type: 3, Column: accessTestColumn("zz_igs_tags"),
		IndexGroup: &IndexGroupInfo{
			SourceColumns:        []IndexGroupSourceColumn{{Column: accessTestColumn("tag_ids")}},
			UsesCollectionValues: true,
		},
	}
	steps, _, err := PlanSourceSteps(source, []ColumnStatement{{Col: "tag_ids", Operator: "IN", Values: []any{int32(4), int32(4)}}})
	if err != nil {
		t.Fatal(err)
	}
	expected := []AccessStep{{Element: &ElementLookup{Column: "zz_igs_tags", Value: HashInt64(4)}}}
	if !reflect.DeepEqual(steps, expected) {
		t.Fatalf("steps = %+v", steps)
	}
}
