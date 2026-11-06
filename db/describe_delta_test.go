package db

import (
	"slices"
	"strings"
	"testing"
)

// deltaTestCore is a table keyed (company_id, id) with the managed updated/version columns and one
// pinnable integer column.
func deltaTestCore() *TableCore {
	companyColumn := viewTableTestColumn("company_id", ColType{FieldType: "int32"}, false)
	idColumn := viewTableTestColumn("id", ColType{FieldType: "int64"}, false)
	updatedColumn := viewTableTestColumn("updated", ColType{FieldType: "int32"}, false)
	versionColumn := viewTableTestColumn("version", ColType{FieldType: "int16"}, false)
	core := &TableCore{
		Name: "orders", Keys: []IColInfo{idColumn}, PartKey: companyColumn,
		UpdatedCol: updatedColumn, VersionCol: versionColumn, ColumnsMap: map[string]IColInfo{},
		FixedValueRanges: map[string]ColumnValueRange{"status": {MinValue: 0, MaxValue: 7}},
	}
	for _, column := range []*ColumnInfo{
		companyColumn, idColumn, updatedColumn, versionColumn,
		viewTableTestColumn("status", ColType{FieldType: "int16"}, false),
		viewTableTestColumn("note", ColType{FieldType: "string"}, false),
		viewTableTestColumn("zz_other", ColType{FieldType: "int64"}, true),
	} {
		core.ColumnsMap[column.Name] = column
	}
	return core
}

func TestDescribeDeltaPinsEveryKeyAndPacksUpdatedLast(t *testing.T) {
	core := deltaTestCore()
	source := core.DescribeDelta(Index{Type: TypeDelta, Keys: []Coln{declared(core, "status")}})

	if source.Type != 8 || !source.IsDeltaView || source.Name != "orders__dlt_status_view" {
		t.Fatalf("unexpected source: type=%d delta=%v name=%s", source.Type, source.IsDeltaView, source.Name)
	}
	if !slices.Equal(source.Columns, []string{"company_id", "status", "updated"}) {
		t.Fatalf("columns = %v", source.Columns)
	}
	if len(core.DeltaIndexes) != 1 {
		t.Fatalf("delta indexes = %d", len(core.DeltaIndexes))
	}
	deltaIndex := core.DeltaIndexes[0]
	// status is declared in FixedValues 0..7 (3 bits); updated takes the last 31 bits.
	if !slices.Equal(deltaIndex.PinnedSlotBits, []int64{3}) || !slices.Equal(source.PackedSlotBitsPerColumn, []int64{3, DeltaUpdatedSlotBits}) {
		t.Fatalf("slot bits = %v / %v", deltaIndex.PinnedSlotBits, source.PackedSlotBitsPerColumn)
	}
	packedColumn := core.ColumnsMap["zz_dlt_status"]
	if packedColumn == nil || !packedColumn.GetInfo().IsVirtual || packedColumn.GetType().FieldType != "int64" {
		t.Fatalf("packed column = %v", packedColumn)
	}
	// The planner and the cache read every stored column from a delta index; virtual ones are not stored.
	if slices.Contains(source.AvailableColumns, "zz_dlt_status") || slices.Contains(source.AvailableColumns, "zz_other") || !slices.Contains(source.AvailableColumns, "note") {
		t.Fatalf("available = %v", source.AvailableColumns)
	}
	if core.IndexSourceNamed(source.Name) != source {
		t.Fatal("the described index must be registered on the table")
	}
}

func TestDescribeDeltaKeylessPacksUpdatedAlone(t *testing.T) {
	core := deltaTestCore()
	source := core.DescribeDelta(Index{Type: TypeDelta})
	if source.Name != "orders__dlt_all_view" || core.ColumnsMap["zz_dlt_updated"] == nil {
		t.Fatalf("name=%s columns=%v", source.Name, core.ColumnsMap)
	}
	if !slices.Equal(source.Columns, []string{"company_id", "updated"}) {
		t.Fatalf("columns = %v", source.Columns)
	}
}

func TestDescribeDeltaRefusesBadTables(t *testing.T) {
	cases := map[string]struct {
		prepare func(core *TableCore) Index
		message string
	}{
		"no version column": {func(c *TableCore) Index { c.VersionCol = nil; return Index{} }, `requires the managed "version" column`},
		"string key":        {func(c *TableCore) Index { return Index{Keys: []Coln{declared(c, "note")}} }, "must be a scalar integer"},
		"the record ID":     {func(c *TableCore) Index { return Index{Keys: []Coln{declared(c, "id")}} }, "is the table's key"},
		"non-integer part": {func(c *TableCore) Index {
			c.PartKey = viewTableTestColumn("company_id", ColType{FieldType: "string"}, false)
			return Index{}
		}, "integer partition column"},
		"missing column": {func(c *TableCore) Index {
			return Index{Keys: []Coln{colRef{ColumnInfo{ColInfo: ColInfo{Name: "ghost"}}}}}
		}, "was not found"},
	}
	for caseName, testCase := range cases {
		func() {
			defer func() {
				message, _ := recover().(string)
				if !strings.Contains(message, testCase.message) {
					t.Errorf("%s: panic = %q, want it to contain %q", caseName, message, testCase.message)
				}
			}()
			core := deltaTestCore()
			core.DescribeDelta(testCase.prepare(core))
		}()
	}
}

func TestDescribeDeltaRefusesTheSameKeysTwice(t *testing.T) {
	core := deltaTestCore()
	index := Index{Type: TypeDelta, Keys: []Coln{declared(core, "status")}}
	core.DescribeDelta(index)
	defer func() {
		if message, _ := recover().(string); !strings.Contains(message, "two TypeDelta indexes declare the same Keys") {
			t.Fatalf("panic = %q", message)
		}
	}()
	core.DescribeDelta(index)
}
