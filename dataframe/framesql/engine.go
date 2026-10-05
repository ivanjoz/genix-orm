package framesql

import (
	"math"
	"slices"
	"time"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// accumulator holds one slot per group: its key and, per slot, every accumulated sum and the row
// count. Source.Scan calls the fold from one goroutine at a time, so a single accumulator needs no
// lock and no merge.
type accumulator struct {
	slotByGroup map[[maxGroups]int64]int
	groupKeys   [][maxGroups]int64
	// sums[i][slot] is the sum of the plan's sumColumns[i] in the slot's group.
	sums   [][]int64
	counts []int64
}

func (acc *accumulator) slotOf(groupKey [maxGroups]int64) int {
	slot, exists := acc.slotByGroup[groupKey]
	if !exists {
		slot = len(acc.groupKeys)
		acc.slotByGroup[groupKey] = slot
		acc.groupKeys = append(acc.groupKeys, groupKey)
		for i := range acc.sums {
			acc.sums[i] = append(acc.sums[i], 0)
		}
		acc.counts = append(acc.counts, 0)
	}
	return slot
}

func execute(plan *queryPlan) (Result, error) {
	acc := &accumulator{slotByGroup: map[[maxGroups]int64]int{}, sums: make([][]int64, len(plan.sumColumns))}
	filesRead := 0
	snapshot, err := plan.frame.Source.Scan(plan.fromKey, plan.toKey, plan.pinnedKeys, plan.selectFiles,
		func(keys [dataframe.MaxKeys]int64, file dataframe.File) error {
			filesRead++
			plan.foldFile(acc, keys, file)
			return nil
		})
	// The read's errors already name the frame (the ORM's), or are the plan's own (too many files).
	if err != nil {
		return Result{}, err
	}
	// Without GROUP BY the statement has one row, even over no rows: SUM 0, COUNT 0.
	if len(plan.groups) == 0 && len(acc.groupKeys) == 0 {
		acc.slotOf([maxGroups]int64{})
	}

	groupCount := len(acc.groupKeys)
	itemValues := make([][]float64, len(plan.items))
	for i, item := range plan.items {
		itemValues[i] = make([]float64, groupCount)
		for slot := range groupCount {
			itemValues[i][slot] = roundToKind(evaluate(item.value, acc, slot), item.value.resultKind)
		}
	}

	// ORDER BY, then the groups ascending: the same statement always returns its rows in one order.
	slotOrder := make([]int, groupCount)
	for slot := range slotOrder {
		slotOrder[slot] = slot
	}
	slices.SortFunc(slotOrder, func(a, b int) int {
		for _, key := range plan.order {
			if order := compareValues(itemValues[key.item][a], itemValues[key.item][b], key.isDescending); order != 0 {
				return order
			}
		}
		return slices.Compare(acc.groupKeys[a][:], acc.groupKeys[b][:])
	})

	rowCount := min(groupCount, plan.limit)
	result := Result{RowCount: rowCount, FromKey: plan.fromKey, ToKey: plan.toKey, Snapshot: snapshot, Truncated: groupCount > plan.limit, FilesRead: filesRead}
	for i, item := range plan.items[:plan.visibleItems] {
		column := item.column
		column.Values = make([]float64, rowCount)
		for row, slot := range slotOrder[:rowCount] {
			column.Values[row] = itemValues[i][slot]
		}
		result.Columns = append(result.Columns, column)
	}
	return result, nil
}

// foldFile adds one file's rows into their groups. The Key parts of a group are the same for every
// row of the file: they are computed once, and so is the slot when no group comes from the rows.
func (plan *queryPlan) foldFile(acc *accumulator, keys [dataframe.MaxKeys]int64, file dataframe.File) {
	var groupKey [maxGroups]int64
	groupsByRow := false
	for i, group := range plan.groups {
		if group.isRow {
			groupsByRow = true
			continue
		}
		groupKey[i] = group.transform.apply(keys[group.keyIndex])
	}
	slot := -1
	for row, rowID := range file.RowIDs {
		if plan.rowFilter != nil && !plan.rowFilter.matches(rowID) {
			continue
		}
		// A group gets its slot at its first row: a file whose rows the filter drops adds no group.
		if groupsByRow || slot < 0 {
			for i, group := range plan.groups {
				if group.isRow {
					groupKey[i] = group.transform.apply(rowID)
				}
			}
			slot = acc.slotOf(groupKey)
		}
		for i, sumsColumn := range plan.sumColumns {
			acc.sums[i][slot] += file.Sums[sumsColumn][row]
		}
		acc.counts[slot]++
	}
}

// apply reduces a UnixDay to the Monday of its ISO week or the 1st of its month.
func (transform groupTransform) apply(value int64) int64 {
	switch transform {
	case transformWeek:
		// Day 0 (1970-01-01) is a Thursday, 3 days after a Monday.
		return value - ((value+3)%7+7)%7
	case transformMonth:
		day := time.Unix(value*86400, 0).UTC()
		return time.Date(day.Year(), day.Month(), 1, 0, 0, 0, 0, time.UTC).Unix() / 86400
	}
	return value
}

// PeriodStarts lists every day, week (its Monday) or month (its 1st) the days fromDay..toDay touch,
// ascending: the points of a series over a result's Period column, with rows or not.
func PeriodStarts(period string, fromDay, toDay int64) []int64 {
	transform := map[string]groupTransform{"week": transformWeek, "month": transformMonth}[period]
	var starts []int64
	for day := fromDay; day <= toDay; day++ {
		if start := transform.apply(day); len(starts) == 0 || starts[len(starts)-1] != start {
			starts = append(starts, start)
		}
	}
	return starts
}

// evaluate computes an item for one group. A division by zero is NaN: no value.
func evaluate(node *boundExpr, acc *accumulator, slot int) float64 {
	switch node.kind {
	case boundNumber:
		return node.number
	case boundGroup:
		return float64(acc.groupKeys[slot][node.index])
	case boundSum:
		return float64(acc.sums[node.index][slot])
	case boundCount:
		return float64(acc.counts[slot])
	case boundAverage:
		if acc.counts[slot] == 0 {
			return math.NaN()
		}
		return float64(acc.sums[node.index][slot]) / float64(acc.counts[slot])
	}
	left, right := evaluate(node.left, acc, slot), evaluate(node.right, acc, slot)
	switch node.op {
	case '+':
		return left + right
	case '-':
		return left - right
	case '*':
		return left * right
	}
	if right == 0 {
		return math.NaN()
	}
	return left / right
}

// roundToKind rounds money to whole cents. Integer kinds are already whole.
func roundToKind(value float64, kind Kind) float64 {
	if kind == KindCents {
		return math.Round(value)
	}
	return value
}

// compareValues orders two item values, NaN (no value) last in either direction.
func compareValues(a, b float64, isDescending bool) int {
	switch isNaNA, isNaNB := math.IsNaN(a), math.IsNaN(b); {
	case isNaNA || isNaNB:
		if isNaNA == isNaNB {
			return 0
		}
		if isNaNA {
			return 1
		}
		return -1
	case a == b:
		return 0
	case (a < b) != isDescending:
		return -1
	}
	return 1
}
