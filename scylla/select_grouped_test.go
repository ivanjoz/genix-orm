package scylla

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

func TestQueryIndexGroupStoresCachedGroupsInTableInfo(t *testing.T) {
	groupedRecords := []RecordGroup[indexGroupRecord]{}
	query := QueryIndexGroup[indexGroupRecord, indexGroupSchema](&groupedRecords)

	query.IncludeCachedGroup(101, 33)

	tableInfo := any(query).(interface{ GetTableInfo() *TableInfo }).GetTableInfo()
	if !tableInfo.UseIndexGroupSelect {
		t.Fatal("expected QueryIndexGroup to enable grouped execution")
	}
	if cachedCounter := tableInfo.CachedIndexGroups[101]; cachedCounter != 33 {
		t.Fatalf("expected cached group counter 33, got %d", cachedCounter)
	}
}

func TestBuildIndexGroupSelectPlanAllowsSingleColumnRawGroup(t *testing.T) {
	scyllaTable := MakeScyllaTable[indexGroupRecord, indexGroupSchema]()
	tableInfo := &TableInfo{
		Statements: []ColumnStatement{
			{Col: "empresa_id", Operator: "=", Value: int32(7)},
			{Col: "date", Operator: "BETWEEN", From: []ColumnStatement{{Col: "date", Value: int16(18754)}}, To: []ColumnStatement{{Col: "date", Value: int16(18756)}}},
		},
	}

	queryPlan, err := scyllaTable.PlanIndexGroupRead(tableInfo)
	if err != nil {
		t.Fatalf("PlanIndexGroupRead returned error: %v", err)
	}

	if queryPlan.IndexGroup.Name != "date" {
		t.Fatalf("expected single-column index group, got %q", queryPlan.IndexGroup.Name)
	}
	if queryPlan.IndexGroup.VirtualColumn != nil && !queryPlan.IndexGroup.VirtualColumn.IsNil() {
		t.Fatalf("single-column index group should not require a virtual column, got %s", queryPlan.IndexGroup.VirtualColumn.GetName())
	}
	if len(queryPlan.HashGroups) != 3 {
		t.Fatalf("expected 3 hash groups, got %+v", queryPlan.HashGroups)
	}
	expectedFechas := map[int64]bool{
		18754: false,
		18755: false,
		18756: false,
	}
	for _, hashGroup := range queryPlan.HashGroups {
		if len(hashGroup.IndexGroupValues) != 1 {
			t.Fatalf("unexpected single-column values: %+v", hashGroup)
		}
		dateValue := hashGroup.IndexGroupValues[0]
		if _, exists := expectedFechas[dateValue]; !exists {
			t.Fatalf("unexpected single-column value: %+v", hashGroup)
		}
		expectedFechas[dateValue] = true
	}
	for dateValue, wasSeen := range expectedFechas {
		if !wasSeen {
			t.Fatalf("missing expected date value %d in %+v", dateValue, queryPlan.HashGroups)
		}
	}
}

func TestBuildIndexGroupFetchQueryUsesSourceColumnForSingleColumnGroup(t *testing.T) {
	scyllaTable := MakeScyllaTable[indexGroupRecord, indexGroupSchema]()
	queryPlan := &db.IndexGroupReadPlan{
		PartitionValue: 7,
		IndexGroup:     scyllaTable.IndexGroups[0],
	}
	fetchState := db.IndexGroupFetchState{
		HashValue:        db.HashInt64(18754),
		IndexGroupValues: []int64{18754},
	}

	queryStr, queryValues := buildIndexGroupFetchQuery(scyllaTable, queryPlan, fetchState, []string{"*"})
	expectedQuery := "SELECT * FROM test_keyspace.index_group_records WHERE empresa_id = ? AND date = ?"
	if queryStr != expectedQuery {
		t.Fatalf("unexpected query: %s", queryStr)
	}
	if len(queryValues) != 2 || queryValues[0] != int32(7) || queryValues[1] != int64(18754) {
		t.Fatalf("unexpected query values: %+v", queryValues)
	}
}
