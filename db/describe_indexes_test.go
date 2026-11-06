package db

import (
	"slices"
	"strings"
	"testing"
)

func indexesTestCore() *TableCore {
	core := groupTestCore()
	for _, column := range []*ColumnInfo{
		viewTableTestColumn("created_day", ColType{FieldType: "int16"}, false),
		viewTableTestColumn("warehouse_id", ColType{FieldType: "int32"}, false),
	} {
		core.ColumnsMap[column.Name] = column
	}
	return core
}

func TestDescribeIndexesReturnsOnlyWhatADriverMustMaterialize(t *testing.T) {
	core := indexesTestCore()
	idxCount := int8(1)
	sources := core.DescribeIndexes(&idxCount, []Index{
		{Type: TypeLocalIndex, Keys: []Coln{declared(core, "day")}},
		{Type: TypeCacheGroup, Keys: []Coln{declared(core, "day")}},                   // reuses the local index above
		{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(core, "day")}},            // a key range, no index
		{Keys: []Coln{declared(core, "status")}, Cols: []Coln{declared(core, "day")}}, // Cols → a view
	})
	kinds := []IndexKind{}
	for _, source := range sources {
		kinds = append(kinds, source.Kind)
	}
	if !slices.Equal(kinds, []IndexKind{IndexKindSingleColumn, IndexKindView}) {
		t.Fatalf("kinds = %v", kinds)
	}
	if len(core.IndexGroups) != 2 {
		t.Fatalf("groups = %d", len(core.IndexGroups))
	}
	// A second call returns only the sources it added.
	more := core.DescribeIndexes(&idxCount, []Index{{Type: TypeGlobalIndex, Keys: []Coln{declared(core, "status")}}})
	if len(more) != 1 || len(core.IndexSources) != 3 {
		t.Fatalf("more = %d, total = %d", len(more), len(core.IndexSources))
	}
}

func TestDescribeIndexesRefusesBadDeclarations(t *testing.T) {
	cases := map[string]struct {
		index   func(*TableCore) Index
		message string
	}{
		"empty entry": {func(*TableCore) Index { return Index{Type: TypeLocalIndex} }, "must not be empty"},
		"partition on key": {func(c *TableCore) Index {
			return Index{Type: TypeLocalIndex, Keys: []Coln{declared(c, "day")}, Partition: declared(c, "status")}
		}, "only supported on TypeView"},
		"unknown type": {func(c *TableCore) Index { return Index{Type: 99, Keys: []Coln{declared(c, "day")}} }, "unsupported index type 99"},
	}
	for caseName, testCase := range cases {
		func() {
			defer func() {
				if message, _ := recover().(string); !strings.Contains(message, testCase.message) {
					t.Errorf("%s: panic = %q, want %q", caseName, message, testCase.message)
				}
			}()
			core := indexesTestCore()
			core.DescribeIndexes(new(int8), []Index{testCase.index(core)})
		}()
	}
}

func TestDescribeIndexesBucketsOneColumnIntoOneIndexPerSize(t *testing.T) {
	core := indexesTestCore()
	bucketed := colRef{ColumnInfo{ColInfo: ColInfo{Name: "created_day"}, CompositeBucketSizes: []int8{7, 30, 7}}}
	idxCount := int8(1)
	sources := core.DescribeIndexes(&idxCount, []Index{{Keys: []Coln{bucketed, declared(core, "warehouse_id")}}})

	if len(sources) != 2 || sources[0].Kind != IndexKindCompositeBucket {
		t.Fatalf("sources = %d (kind %v)", len(sources), sources[0].Kind)
	}
	if len(core.CompositeBucketIndexes) != 1 || !slices.Equal(core.CompositeBucketIndexes[0].BucketSizes, []int8{7, 30}) {
		t.Fatalf("composite = %+v", core.CompositeBucketIndexes)
	}
	hashColumn := sources[0].Column
	if !hashColumn.GetInfo().IsVirtual || !hashColumn.GetType().IsSlice || hashColumn.GetType().FieldType != "[]int32" {
		t.Fatalf("hash column = %v", hashColumn)
	}
}

func TestDescribeIndexesRefusesBadBucketing(t *testing.T) {
	for caseName, columns := range map[string]func(*TableCore) []Coln{
		"a single column": func(c *TableCore) []Coln {
			return []Coln{colRef{ColumnInfo{ColInfo: ColInfo{Name: "day"}, CompositeBucketSizes: []int8{7}}}}
		},
		"two bucket columns": func(c *TableCore) []Coln {
			return []Coln{
				colRef{ColumnInfo{ColInfo: ColInfo{Name: "day"}, CompositeBucketSizes: []int8{7}}},
				colRef{ColumnInfo{ColInfo: ColInfo{Name: "status"}, CompositeBucketSizes: []int8{7}}},
			}
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected a panic", caseName)
				}
			}()
			core := indexesTestCore()
			core.DescribeIndexes(new(int8), []Index{{Keys: columns(core)}})
		}()
	}
}
