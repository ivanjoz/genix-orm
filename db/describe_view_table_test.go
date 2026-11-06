package db

import (
	"slices"
	"strings"
	"testing"
)

func viewTableTestColumn(name string, columnType ColType, isVirtual bool) *ColumnInfo {
	return &ColumnInfo{ColInfo: ColInfo{Name: name, IsVirtual: isVirtual}, ColType: columnType}
}

// declared is how a schema names a column: a declaration-time reference carrying its info.
func declared(core *TableCore, columnName string) Coln {
	return colRef{*core.ColumnsMap[columnName].(*ColumnInfo)}
}

// viewTableTestCore is a table keyed (company_id, id) with a scalar, a slice and a virtual column.
func viewTableTestCore() *TableCore {
	companyColumn := viewTableTestColumn("company_id", ColType{FieldType: "int32"}, false)
	idColumn := viewTableTestColumn("id", ColType{FieldType: "int64"}, false)
	core := &TableCore{
		Name: "products", Keys: []IColInfo{idColumn}, PartKey: companyColumn,
		ColumnsMap: map[string]IColInfo{},
	}
	for _, column := range []*ColumnInfo{
		companyColumn, idColumn,
		viewTableTestColumn("name", ColType{FieldType: "string"}, false),
		viewTableTestColumn("tag_ids", ColType{FieldType: "[]int32", IsSlice: true}, false),
		viewTableTestColumn("zz_hidden", ColType{FieldType: "int64"}, true),
	} {
		core.ColumnsMap[column.Name] = column
	}
	return core
}

func TestDescribeViewTableFansOutOnTheSliceKey(t *testing.T) {
	core := viewTableTestCore()
	source := core.DescribeViewTable(Index{Type: TypeViewTable, Keys: []Coln{declared(core, "tag_ids")}})

	if source.Type != 9 || source.Name != "products__tag_ids_view" || source.FanoutColumnName != "tag_ids" {
		t.Fatalf("unexpected source: type=%d name=%s fanout=%s", source.Type, source.Name, source.FanoutColumnName)
	}
	if !slices.Equal(source.Columns, []string{"company_id", "tag_ids"}) || !slices.Equal(source.ColumnsNoPart, []string{"tag_ids"}) {
		t.Fatalf("columns = %v / %v", source.Columns, source.ColumnsNoPart)
	}
	// The fan-out column is a key, never a projected value; virtual columns are never stored.
	if !slices.Equal(source.AvailableColumns, []string{"company_id", "id", "name"}) {
		t.Fatalf("available = %v", source.AvailableColumns)
	}
	if source.MaintenanceIDColumn.GetName() != "id" || !source.RebuildColumnNames["tag_ids"] || !source.RebuildColumnNames["name"] {
		t.Fatalf("maintenance = %v / %v", source.MaintenanceIDColumn, source.RebuildColumnNames)
	}
	if !source.TableKeyColumns[0].UsesSliceElement {
		t.Fatal("the slice key must fan out one row per element")
	}
	if core.IndexSourceNamed("products__tag_ids_view") != source {
		t.Fatal("the described view must be registered on the table")
	}
}

func TestDescribeViewTableRefusesBadDeclarations(t *testing.T) {
	cases := map[string]struct {
		index   func(core *TableCore) Index
		message string
	}{
		"no keys":        {func(*TableCore) Index { return Index{Type: TypeViewTable} }, "at least one key"},
		"repeats the id": {func(c *TableCore) Index { return Index{Keys: []Coln{declared(c, "id")}} }, "must not repeat the base ID"},
		"uses hash":      {func(c *TableCore) Index { return Index{UseHash: true, Keys: []Coln{declared(c, "name")}} }, "UseHash"},
		"virtual project": {func(c *TableCore) Index {
			return Index{Keys: []Coln{declared(c, "name")}, Cols: []Coln{declared(c, "zz_hidden")}}
		}, "cannot be virtual"},
	}
	for caseName, testCase := range cases {
		func() {
			defer func() {
				message, _ := recover().(string)
				if !strings.Contains(message, testCase.message) {
					t.Errorf("%s: panic = %q, want it to contain %q", caseName, message, testCase.message)
				}
			}()
			core := viewTableTestCore()
			core.DescribeViewTable(testCase.index(core))
		}()
	}
}
