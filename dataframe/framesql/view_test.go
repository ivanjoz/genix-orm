package framesql

import (
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// testCategoryView is day-sales {product_id, divisor (hidden)} → {amount}, joined to a
// product-categories dictionary {category_id, product_id} on product_id.
func testCategoryView(t *testing.T) (Frame, *[]dataframe.Selection) {
	baseRows := []dataframe.Row{
		{Keys: []int64{1}, RowValues: []int64{10, 6}, Sums: []int64{1000}},
		{Keys: []int64{1}, RowValues: []int64{10, 12}, Sums: []int64{200}},
		{Keys: []int64{1}, RowValues: []int64{11, 6}, Sums: []int64{500}},
		{Keys: []int64{2}, RowValues: []int64{12, 6}, Sums: []int64{70}},
	}
	dictionaryReads := []dataframe.Selection{}
	base := Frame{
		Name: "day-sales",
		Keys: []Column{{Name: "fecha", Kind: KindDay}},
		Rows: []Column{{Name: "product_id", Kind: KindRef}, {Name: "divisor", Hidden: true}},
		Sums: []Column{{Name: "amount", Kind: KindCents}},
		Source: func(selection dataframe.Selection) ([]dataframe.Row, error) {
			return slices.DeleteFunc(slices.Clone(baseRows), func(row dataframe.Row) bool {
				return row.Keys[0] < selection.FromKey || row.Keys[0] > selection.ToKey
			}), nil
		},
	}
	view := View("day-category", base, Dictionary{
		Rows:      []string{"category_id", "product_id"},
		On:        "product_id",
		Column:    Column{Name: "category_id", Kind: KindRef},
		Selection: dataframe.Selection{FromKey: 0, ToKey: 0},
		Source: func(selection dataframe.Selection) ([]dataframe.Row, error) {
			dictionaryReads = append(dictionaryReads, selection)
			// Product 10 is in categories 3 and 7, product 11 in 7, product 12 in none.
			return []dataframe.Row{
				{Keys: []int64{0}, RowValues: []int64{3, 10}, Sums: []int64{1}},
				{Keys: []int64{0}, RowValues: []int64{7, 10}, Sums: []int64{1}},
				{Keys: []int64{0}, RowValues: []int64{7, 11}, Sums: []int64{1}},
			}, nil
		},
	})
	return view, &dictionaryReads
}

func TestViewGroupsByTheDictionaryColumn(t *testing.T) {
	view, dictionaryReads := testCategoryView(t)
	cases := []struct{ statement, want string }{
		{
			// Product 10 counts in 3 and in 7; product 12, without a category, under 0.
			"SELECT category_id, SUM(amount) WHERE fecha BETWEEN 1 AND 2 GROUP BY category_id",
			"category_id:3=0,3,7 | amount:2=70,1200,1700",
		},
		{
			// The base's selection is the statement's: day 2 alone reads product 12 alone.
			"SELECT category_id, SUM(amount) WHERE fecha = 2 GROUP BY category_id",
			"category_id:3=0 | amount:2=70",
		},
		{
			"SELECT product_id, SUM(amount) WHERE fecha = 1 AND category_id = 7 GROUP BY product_id",
			"product_id:3=10,11 | amount:2=1200,500",
		},
	}
	for _, testCase := range cases {
		result, err := Run("day_category", testCase.statement, []Frame{view}, Resolvers{}, Options{})
		if err != nil {
			t.Fatalf("%s: %v", testCase.statement, err)
		}
		if got := formatResult(result); got != testCase.want {
			t.Errorf("%s\n got  %s\n want %s", testCase.statement, got, testCase.want)
		}
	}
	if len(*dictionaryReads) != len(cases) || slices.ContainsFunc(*dictionaryReads, func(selection dataframe.Selection) bool {
		return selection.FromKey != 0 || selection.ToKey != 0 || len(selection.PinnedKeys) > 0
	}) {
		t.Errorf("the dictionary must be read whole with the view's selection, got %v", *dictionaryReads)
	}
}

func TestViewLeavesTheHiddenRowsOut(t *testing.T) {
	view, _ := testCategoryView(t)
	if got := columnNames(view.Rows); got != "product_id, category_id" {
		t.Errorf("view Rows = %s", got)
	}
	if _, err := Run("day_category", "SELECT divisor WHERE fecha = 1 GROUP BY divisor", []Frame{view}, Resolvers{}, Options{}); err == nil {
		t.Errorf("a hidden base column must not be named through the view")
	}
}

func TestViewRefusesADictionaryOffTheBaseRows(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("a dictionary joined on a Key must panic")
		}
	}()
	view, _ := testCategoryView(t)
	View("bad", view, Dictionary{Rows: []string{"x", "fecha"}, On: "fecha", Column: Column{Name: "x"}, Source: view.Source})
}
