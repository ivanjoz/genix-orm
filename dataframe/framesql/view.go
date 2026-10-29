package framesql

import (
	"fmt"
	"slices"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// A view is a frame over another frame's read, in a star: the base frame holds the facts and alone
// names the files a statement reads; a dictionary only adds one Rows column, looked up by a base Rows
// value. So a view has the base's Keys and Sums as they are, and a statement's selection goes to the
// base's read untouched.

// Dictionary maps a base Rows column's values to the values of the column a view adds, many to many
// ("product-categories": a product to each of its categories). It is read whole, with a selection the
// view fixes: a statement never bounds it.
type Dictionary struct {
	// Rows name the dictionary rows' RowValues, in order ("category_id", "product_id").
	Rows []string
	// On names the base's Rows column the dictionary is looked up by. It is one of Rows.
	On string
	// Column is the column the view adds. Its Name is one of Rows.
	Column    Column
	Selection dataframe.Selection
	Source    Source
}

// View declares the frame name: the base's rows, one per value of the dictionary's column for the
// row's On value (0 when it has none). A base row whose value has several is repeated under each, so
// grouped by the column every group is exact, while the groups add up to more than the base's total.
// The base's hidden Rows are left out: a statement can't name them anyway, and it keeps the view within
// dataframe.MaxRowColumns. It panics on a declaration that can't be a view, a caller's mistake.
func View(name string, base Frame, dictionary Dictionary) Frame {
	baseOnIndex := slices.IndexFunc(base.Rows, func(column Column) bool { return column.Name == dictionary.On && !column.Hidden })
	dictionaryOnIndex := slices.Index(dictionary.Rows, dictionary.On)
	dictionaryValueIndex := slices.Index(dictionary.Rows, dictionary.Column.Name)
	if baseOnIndex < 0 || dictionaryOnIndex < 0 || dictionaryValueIndex < 0 || dictionary.Source == nil {
		panic(fmt.Sprintf("framesql: view %q: the dictionary needs a Source, and On and Column.Name among its Rows, On a visible Rows column of %q",
			name, base.Name))
	}

	view := Frame{Name: name, Keys: base.Keys, Sums: base.Sums}
	keptBaseRowIndexes := []int{}
	for i, column := range base.Rows {
		if !column.Hidden {
			keptBaseRowIndexes = append(keptBaseRowIndexes, i)
			view.Rows = append(view.Rows, column)
		}
	}
	view.Rows = append(view.Rows, dictionary.Column)

	view.Source = func(selection dataframe.Selection) ([]dataframe.Row, error) {
		baseRows, err := base.Source(selection)
		if err != nil {
			return nil, err
		}
		dictionaryRows, err := dictionary.Source(dictionary.Selection)
		if err != nil {
			return nil, err
		}
		valuesByOnValue := map[int64][]int64{}
		for _, dictionaryRow := range dictionaryRows {
			onValue := dictionaryRow.RowValues[dictionaryOnIndex]
			valuesByOnValue[onValue] = append(valuesByOnValue[onValue], dictionaryRow.RowValues[dictionaryValueIndex])
		}

		viewRows := make([]dataframe.Row, 0, len(baseRows))
		for _, baseRow := range baseRows {
			values := valuesByOnValue[baseRow.RowValues[baseOnIndex]]
			if len(values) == 0 {
				values = []int64{0}
			}
			for _, value := range values {
				rowValues := make([]int64, 0, len(view.Rows))
				for _, baseRowIndex := range keptBaseRowIndexes {
					rowValues = append(rowValues, baseRow.RowValues[baseRowIndex])
				}
				viewRows = append(viewRows, dataframe.Row{Keys: baseRow.Keys, RowValues: append(rowValues, value), Sums: baseRow.Sums})
			}
		}
		return viewRows, nil
	}
	return view
}
