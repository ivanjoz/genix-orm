package db

import (
	"strings"
	"testing"
)

// groupTestCore is keyed (company_id, id) with two scalar columns, a slice column and KeyIntPacking
// on (day, status).
func groupTestCore() *TableCore {
	companyColumn := viewTableTestColumn("company_id", ColType{FieldType: "int32"}, false)
	idColumn := viewTableTestColumn("id", ColType{FieldType: "int64"}, false)
	core := &TableCore{
		Name: "events", Keys: []IColInfo{idColumn}, PartKey: companyColumn,
		ColumnsMap: map[string]IColInfo{}, IndexGroupIDs: map[int16]string{},
	}
	for _, column := range []*ColumnInfo{
		companyColumn, idColumn,
		viewTableTestColumn("day", ColType{FieldType: "int16"}, false),
		viewTableTestColumn("status", ColType{FieldType: "int16"}, false),
		viewTableTestColumn("tag_ids", ColType{FieldType: "[]int32", IsSlice: true}, false),
	} {
		core.ColumnsMap[column.Name] = column
	}
	core.KeyIntPacking = []IColInfo{core.ColumnsMap["day"], core.ColumnsMap["status"]}
	return core
}

func TestDescribeIndexGroupOverTwoScalarsKeepsAHashColumnAndALocalIndex(t *testing.T) {
	core := groupTestCore()
	idxCount := int8(0)
	source := core.DescribeIndexGroup(&idxCount, Index{Type: TypeCacheGroup, Keys: []Coln{declared(core, "day"), declared(core, "status")}})

	if source == nil || source.Kind != IndexKindGroupHash || source.Type != 3 || source.IndexGroup == nil {
		t.Fatalf("unexpected source: %+v", source)
	}
	if !strings.HasSuffix(source.Name, "_index_1") || idxCount != 1 {
		t.Fatalf("name=%s idxCount=%d", source.Name, idxCount)
	}
	if source.IndexGroup.UsesCollectionValues || len(core.IndexGroups) != 1 {
		t.Fatalf("group = %+v / %d groups", source.IndexGroup, len(core.IndexGroups))
	}
	hashColumn := core.ColumnsMap[source.Column.GetName()]
	if hashColumn == nil || !hashColumn.GetInfo().IsVirtual || hashColumn.GetType().FieldType != "int32" {
		t.Fatalf("hash column = %v", hashColumn)
	}
}

func TestDescribeIndexGroupOverASliceIsACollectionAndGlobal(t *testing.T) {
	core := groupTestCore()
	idxCount := int8(0)
	source := core.DescribeIndexGroup(&idxCount, Index{Type: TypeCacheGroup, Keys: []Coln{declared(core, "day"), declared(core, "tag_ids")}})
	if !source.IndexGroup.UsesCollectionValues || !strings.HasSuffix(source.Name, "_index_0") {
		t.Fatalf("collection group: name=%s group=%+v", source.Name, source.IndexGroup)
	}
	if !source.Column.GetType().IsSlice || source.Column.GetType().FieldType != "[]int32" {
		t.Fatalf("hash column type = %+v", source.Column.GetType())
	}
}

func TestDescribeIndexGroupOverOneKeyReusesTheLocalIndex(t *testing.T) {
	core := groupTestCore()
	idxCount := int8(0)
	first := core.DescribeIndexGroup(&idxCount, Index{Type: TypeCacheGroup, Keys: []Coln{declared(core, "day")}})
	if first == nil || first.Kind != IndexKindSingleColumn || first.Type != 2 {
		t.Fatalf("first = %+v", first)
	}
	// A second group on the same column reuses the index that already exists.
	second := core.DescribeIndexGroup(&idxCount, Index{Type: TypeCacheGroup, Keys: []Coln{declared(core, "day")}})
	if second != nil || len(core.IndexGroups) != 2 {
		t.Fatalf("second = %+v, groups = %d", second, len(core.IndexGroups))
	}
}

// A no-index group describes nothing and reads through the widest FromKey group among its Keys,
// whichever order they were declared in.
func TestDescribeNoIndexGroupResolvesItsWidestFromKeyBase(t *testing.T) {
	core := groupTestCore()
	core.ColumnsMap["kind"] = viewTableTestColumn("kind", ColType{FieldType: "int16"}, false)
	sources := core.DescribeIndexes(new(int8), []Index{
		{Type: TypeCacheGroupNoIndex, Keys: []Coln{declared(core, "day"), declared(core, "status"), declared(core, "kind")}},
		{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(core, "day")}},
		{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(core, "day"), declared(core, "status")}},
	})
	if len(sources) != 0 {
		t.Fatalf("no group here may describe an index, got %+v", sources)
	}
	noIndexGroup := core.IndexGroups[0]
	if noIndexGroup.BaseGroup == nil || noIndexGroup.BaseGroup.Name != "day_status" {
		t.Fatalf("base = %+v, want day_status", noIndexGroup.BaseGroup)
	}
	if baseValues := noIndexGroup.BaseGroupValues([]int64{20000, 3, 8}); baseValues[0] != 20000 || baseValues[1] != 3 || len(baseValues) != 2 {
		t.Fatalf("base values = %v", baseValues)
	}
}

func TestDescribeNoIndexGroupRefusesAGroupWithoutABase(t *testing.T) {
	for caseName, keyNames := range map[string][]string{
		"no FromKey group among its keys": {"status"},
		"same keys as its only base":      {"day"},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected a panic", caseName)
				}
			}()
			core := groupTestCore()
			keys := []Coln{}
			for _, keyName := range keyNames {
				keys = append(keys, declared(core, keyName))
			}
			core.DescribeIndexes(new(int8), []Index{
				{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(core, "day")}},
				{Type: TypeCacheGroupNoIndex, Keys: keys},
			})
		}()
	}
}

func TestDescribeIndexGroupFromKeyNeedsNoIndexAndChecksThePrefix(t *testing.T) {
	core := groupTestCore()
	idxCount := int8(0)
	if source := core.DescribeIndexGroup(&idxCount, Index{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(core, "day")}}); source != nil {
		t.Fatalf("a key-range group must not describe an index, got %+v", source)
	}
	if len(core.IndexGroups) != 1 || !core.IndexGroups[0].InheritFromKey {
		t.Fatalf("groups = %+v", core.IndexGroups)
	}

	for caseName, mutate := range map[string]func(*TableCore, *Index){
		"not a prefix":   func(c *TableCore, i *Index) { i.Keys = []Coln{declared(c, "status")} },
		"no key packing": func(c *TableCore, i *Index) { c.KeyIntPacking = nil },
		"too many keys": func(c *TableCore, i *Index) {
			i.Keys = []Coln{declared(c, "day"), declared(c, "status"), declared(c, "id")}
		},
		"cols not allowed": func(c *TableCore, i *Index) { i.Cols = []Coln{declared(c, "status")} },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected a panic", caseName)
				}
			}()
			failing := groupTestCore()
			index := Index{Type: TypeCacheGroupFromKey, Keys: []Coln{declared(failing, "day")}}
			mutate(failing, &index)
			failing.DescribeIndexGroup(new(int8), index)
		}()
	}
}
