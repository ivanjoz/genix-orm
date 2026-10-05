package framesql

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// maxGroups bounds GROUP BY: every Key, the Rows column, and WEEK and MONTH of a day.
const maxGroups = dataframe.MaxKeys + 3

// queryPlan is a statement bound to its frame: what the read takes, what the fold keeps and how the
// rows come out.
type queryPlan struct {
	frame *Frame
	// fromKey, toKey and pinnedKeys are pushed to the read (Source.Scan).
	fromKey, toKey int64
	pinnedKeys     []int64
	// keyFilters drop files by their keys before any GET: Keys[0] given as a list, and the later Keys
	// that couldn't be pinned. nil: no filter on that Key.
	keyFilters [dataframe.MaxKeys]*valueFilter
	// rowFilter drops rows by their Rows value, inside the fold.
	rowFilter *valueFilter
	groups    []groupSource
	// sumColumns maps each accumulated sum to its column in dataframe.File.Sums.
	sumColumns []int
	// items are the SELECT items, then the ORDER BY terms that are not one of them (not returned).
	items        []planItem
	visibleItems int
	order        []orderKey
	limit        int
	maxFiles     int
	// unpinnedKey is the first later Key the read doesn't pin: the one a too-large read should pin.
	unpinnedKey string
}

// planItem is one item of the result: column describes it (a ResultColumn without Values).
type planItem struct {
	column ResultColumn
	value  *boundExpr
}

type orderKey struct {
	item         int
	isDescending bool
}

// valueFilter keeps the values in [from, to], or in set when set is not nil.
type valueFilter struct {
	from, to int64
	set      map[int64]bool
}

func (filter *valueFilter) matches(value int64) bool {
	if filter.set != nil {
		return filter.set[value]
	}
	return value >= filter.from && value <= filter.to
}

// single returns the filter's value when it keeps exactly one.
func (filter *valueFilter) single() (int64, bool) {
	if filter.set != nil {
		if len(filter.set) != 1 {
			return 0, false
		}
		for value := range filter.set {
			return value, true
		}
	}
	return filter.from, filter.from == filter.to
}

// bounds is the smallest range holding every value the filter keeps.
func (filter *valueFilter) bounds() (int64, int64) {
	if filter.set == nil {
		return filter.from, filter.to
	}
	values := make([]int64, 0, len(filter.set))
	for value := range filter.set {
		values = append(values, value)
	}
	return slices.Min(values), slices.Max(values)
}

type groupTransform uint8

const (
	transformNone groupTransform = iota
	transformWeek
	transformMonth
)

// groupSource is one GROUP BY value: a Key of the file, or the Rows value of the row, maybe reduced
// to its week or month.
type groupSource struct {
	text      string
	isRow     bool
	keyIndex  int
	transform groupTransform
	column    Column
	// period is "day", "week" or "month" on a group of Keys[0] when it is a day (ResultColumn.Period).
	period string
}

type boundKind uint8

const (
	boundNumber boundKind = iota
	boundGroup
	boundSum
	boundCount
	boundAverage
	boundBinary
)

// boundExpr is an item's expression bound to the fold: index is the group for boundGroup, and the
// accumulated sum for boundSum and boundAverage.
type boundExpr struct {
	kind       boundKind
	number     float64
	index      int
	op         byte
	left       *boundExpr
	right      *boundExpr
	resultKind Kind
	collection string
}

// columnRole is where a frame column lives in the files.
type columnRole uint8

const (
	roleKey columnRole = iota
	roleRows
	roleSum
)

type frameColumn struct {
	column Column
	role   columnRole
	// index is the Key's position for roleKey, the Sums position for roleSum.
	index int
}

func lookupColumn(frame *Frame, name string) (frameColumn, bool) {
	for i, key := range frame.Keys {
		if key.Name == name {
			return frameColumn{column: key, role: roleKey, index: i}, true
		}
	}
	if frame.Rows.Name == name {
		return frameColumn{column: frame.Rows, role: roleRows}, true
	}
	for i, sum := range frame.Sums {
		if sum.Name == name {
			return frameColumn{column: sum, role: roleSum, index: i}, true
		}
	}
	return frameColumn{}, false
}

// sqlName is a frame's name as statements write it.
func sqlName(frameName string) string { return strings.ReplaceAll(frameName, "-", "_") }

func columnNames(columns []Column) string {
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	return strings.Join(names, ", ")
}

func allColumns(frame *Frame) []Column {
	return slices.Concat(frame.Keys, []Column{frame.Rows}, frame.Sums)
}

// planner binds one statement to its frame.
type planner struct {
	frames    []Frame
	frame     *Frame
	resolvers Resolvers
	plan      *queryPlan
}

// planStatement binds everything but WHERE: the frame, the groups, the items and the order. WHERE
// waits for the resolvers (planConditions).
func planStatement(frameName string, parsed *statement, frames []Frame, options Options) (*planner, error) {
	normalizedName := strings.ToLower(strings.ReplaceAll(frameName, "_", "-"))
	frameIndex := slices.IndexFunc(frames, func(frame Frame) bool { return frame.Name == normalizedName })
	if frameIndex < 0 {
		names := make([]string, len(frames))
		for i, frame := range frames {
			names[i] = sqlName(frame.Name)
		}
		return nil, fmt.Errorf("there is no frame `%s`; the frames are: %s", frameName, strings.Join(names, ", "))
	}
	frame := &frames[frameIndex]
	if parsed.frameName != "" && strings.ReplaceAll(parsed.frameName, "_", "-") != frame.Name {
		return nil, fmt.Errorf("FROM `%s` is not the frame `%s` the statement runs on: leave FROM out", parsed.frameName, sqlName(frame.Name))
	}
	if err := validateFrame(frame); err != nil {
		return nil, err
	}
	p := &planner{frames: frames, frame: frame, plan: &queryPlan{
		frame: frame, limit: cmp.Or(options.MaxRows, defaultMaxRows), maxFiles: cmp.Or(options.MaxFiles, defaultMaxFiles),
	}}
	if parsed.limit > p.plan.limit {
		return nil, fmt.Errorf("LIMIT is at most %d", p.plan.limit)
	}
	if parsed.limit > 0 {
		p.plan.limit = parsed.limit
	}
	if err := p.planGroups(parsed); err != nil {
		return nil, err
	}
	itemByName := map[string]string{}
	for _, item := range parsed.items {
		value, err := p.bind(item.value)
		if err != nil {
			return nil, err
		}
		planned := p.describeItem(cmp.Or(item.alias, defaultItemName(item.value)), value)
		// Items are referred to by name (ORDER BY, and the caller's table and chart): two may not share one.
		nameKey := strings.ToLower(planned.column.Name)
		if other, isTaken := itemByName[nameKey]; isTaken {
			return nil, fmt.Errorf("`%s` and `%s` are both named `%s`: name one with AS", other, formatExpr(item.value), planned.column.Name)
		}
		itemByName[nameKey] = formatExpr(item.value)
		p.plan.items = append(p.plan.items, planned)
	}
	p.plan.visibleItems = len(p.plan.items)
	if err := p.planOrder(parsed); err != nil {
		return nil, err
	}
	return p, nil
}

// defaultItemName names an item without AS after what it reads, so statements need no aliases: a
// column is its name, SUM(amount) and AVG(amount) are amount, COUNT(*) is count, WEEK(fecha) is week.
// Anything else is named as written.
func defaultItemName(node *expr) string {
	switch {
	case node.kind == exprColumn:
		return node.name
	case node.kind != exprCall:
		return formatExpr(node)
	case node.name == "COUNT":
		return "count"
	case node.name == "WEEK" || node.name == "MONTH":
		return strings.ToLower(node.name)
	case len(node.args) == 1 && node.args[0].kind == exprColumn:
		return node.args[0].name
	}
	return formatExpr(node)
}

// describeItem tells the result how to show an item: the label of the column it reads, whether it
// is a group, which aggregate it is, and the period of a group of days.
func (p *planner) describeItem(name string, value *boundExpr) planItem {
	column := ResultColumn{Name: name, Kind: value.resultKind, Collection: value.collection}
	switch value.kind {
	case boundGroup:
		group := p.plan.groups[value.index]
		column.IsGroup, column.Label, column.Period = true, group.column.Label, group.period
	case boundSum, boundAverage:
		column.Aggregate, column.Label = "SUM", p.frame.Sums[p.plan.sumColumns[value.index]].Label
		if value.kind == boundAverage {
			column.Aggregate = "AVG"
		}
	case boundCount:
		column.Aggregate = "COUNT"
	}
	return planItem{column: column, value: value}
}

// validateFrame catches a caller's mistake in a frame declaration, before any statement uses it.
func validateFrame(frame *Frame) error {
	if len(frame.Keys) < 1 || len(frame.Keys) > dataframe.MaxKeys || len(frame.Sums) == 0 || frame.Source == nil {
		return fmt.Errorf("framesql: frame %q needs 1 to %d Keys, Sums and a Source", frame.Name, dataframe.MaxKeys)
	}
	for _, column := range allColumns(frame) {
		if column.Kind == KindDecimal {
			return fmt.Errorf("framesql: frame %q column %q: KindDecimal is only a result's", frame.Name, column.Name)
		}
	}
	for _, sum := range frame.Sums {
		if sum.Kind != KindInteger && sum.Kind != KindCents {
			return fmt.Errorf("framesql: frame %q Sums column %q must be KindInteger or KindCents", frame.Name, sum.Name)
		}
	}
	return nil
}

// unknownColumn names the frames that have the column, or this frame's columns.
func (p *planner) unknownColumn(name string) error {
	var framesWithColumn []string
	for i := range p.frames {
		if _, hasColumn := lookupColumn(&p.frames[i], name); hasColumn {
			framesWithColumn = append(framesWithColumn, "`"+sqlName(p.frames[i].Name)+"`")
		}
	}
	if len(framesWithColumn) > 0 {
		return fmt.Errorf("`%s` has no column `%s`; frames with it: %s", sqlName(p.frame.Name), name, strings.Join(framesWithColumn, ", "))
	}
	return fmt.Errorf("`%s` has no column `%s`; its columns are: %s", sqlName(p.frame.Name), name, columnNames(allColumns(p.frame)))
}

// planConditions splits WHERE into what the read takes (the range of Keys[0], a leading run of single
// values of the later Keys), filters on the file keys, and a filter on the Rows values.
func (p *planner) planConditions(conditions []condition) error {
	firstKey := p.frame.Keys[0]
	targets := make([]frameColumn, len(conditions))
	isColumnFiltered := map[string]bool{}
	for i, condition := range conditions {
		target, exists := lookupColumn(p.frame, condition.column)
		if !exists {
			return p.unknownColumn(condition.column)
		}
		if target.role == roleSum {
			return fmt.Errorf("`%s` is a summed value: WHERE filters %s", condition.column, columnNames(slices.Concat(p.frame.Keys, []Column{p.frame.Rows})))
		}
		if isColumnFiltered[condition.column] {
			return fmt.Errorf("one condition per column: `%s` has two; use IN (…) for several values", condition.column)
		}
		isColumnFiltered[condition.column], targets[i] = true, target
	}
	if !isColumnFiltered[firstKey.Name] {
		example := firstKey.Name + " BETWEEN 1 AND 10"
		if firstKey.Kind == KindDay {
			example = firstKey.Name + " IN PERIOD(\"D-13..D\")"
		}
		return fmt.Errorf("WHERE must bound `%s` on `%s`, such as %s", firstKey.Name, sqlName(p.frame.Name), example)
	}

	// The conditions with names compile last: a lookup may stop the run to ask the user, and the
	// statement must not fail on anything else once they answered.
	filterByColumn := map[string]*valueFilter{}
	for _, isNamesPass := range []bool{false, true} {
		for i, condition := range conditions {
			if slices.ContainsFunc(condition.values, func(value literal) bool { return value.isName }) != isNamesPass {
				continue
			}
			filter, err := p.compileCondition(condition, targets[i].column)
			if err != nil {
				return err
			}
			filterByColumn[condition.column] = filter
			if condition.column == firstKey.Name {
				if err := p.boundFirstKey(filter); err != nil {
					return err
				}
			}
		}
	}

	// The read pins the later Keys up to the first that doesn't hold a single value; the others filter
	// the files it lists.
	isPinning := true
	for i := 1; i < len(p.frame.Keys); i++ {
		filter := filterByColumn[p.frame.Keys[i].Name]
		if value, isSingle := filter.singleOrNone(); isPinning && isSingle {
			p.plan.pinnedKeys = append(p.plan.pinnedKeys, value)
			continue
		}
		if isPinning && filter == nil {
			p.plan.unpinnedKey = p.frame.Keys[i].Name
		}
		isPinning = false
		p.plan.keyFilters[i] = filter
	}
	p.plan.rowFilter = filterByColumn[p.frame.Rows.Name]
	return nil
}

// boundFirstKey makes the condition on Keys[0] the read's range.
func (p *planner) boundFirstKey(filter *valueFilter) error {
	p.plan.fromKey, p.plan.toKey = filter.bounds()
	if valueCount := p.plan.toKey - p.plan.fromKey + 1; valueCount > dataframe.MaxFirstKeys {
		return fmt.Errorf("the range of `%s` covers %d values; the maximum is %d", p.frame.Keys[0].Name, valueCount, dataframe.MaxFirstKeys)
	}
	// A list of days reads its range and drops the days off the list; one day is the range itself.
	if len(filter.set) > 1 {
		p.plan.keyFilters[0] = filter
	}
	return nil
}

// singleOrNone is single on a filter that may be nil.
func (filter *valueFilter) singleOrNone() (int64, bool) {
	if filter == nil {
		return 0, false
	}
	return filter.single()
}

func (p *planner) compileCondition(condition condition, column Column) (*valueFilter, error) {
	switch condition.op {
	case conditionPeriod:
		if column.Kind != KindDay {
			return nil, fmt.Errorf("PERIOD() takes a day column; `%s` is not one", column.Name)
		}
		if p.resolvers.Period == nil {
			return nil, errors.New("framesql: PERIOD() needs Resolvers.Period")
		}
		fromDay, toDay, err := p.resolvers.Period(condition.period)
		if err != nil {
			return nil, fmt.Errorf("PERIOD(\"%s\"): %w", condition.period, err)
		}
		return &valueFilter{from: fromDay, to: toDay}, nil
	case conditionBetween:
		from, to := condition.values[0], condition.values[1]
		if from.isName || to.isName {
			return nil, fmt.Errorf("BETWEEN takes whole numbers; for days write %s IN PERIOD(\"…\")", column.Name)
		}
		if from.integer > to.integer {
			return nil, fmt.Errorf("`%s` BETWEEN %d AND %d: the first bound is above the second", column.Name, from.integer, to.integer)
		}
		return &valueFilter{from: from.integer, to: to.integer}, nil
	}

	values := map[int64]bool{}
	var names []string
	for _, value := range condition.values {
		if value.isName {
			names = append(names, value.name)
		} else {
			values[value.integer] = true
		}
	}
	if len(names) > 0 {
		if column.Kind != KindRef {
			return nil, fmt.Errorf("`%s` takes numbers; names in quotes go on columns of records (%s)", column.Name, p.refColumnNames())
		}
		if p.resolvers.Names == nil {
			return nil, errors.New("framesql: names in quotes need Resolvers.Names")
		}
		ids, err := p.resolvers.Names(column, names)
		if err != nil {
			return nil, fmt.Errorf("`%s`: %w", column.Name, err)
		}
		for _, id := range ids {
			values[id] = true
		}
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("`%s`: no record matches %s", column.Name, strings.Join(names, ", "))
	}
	return &valueFilter{set: values}, nil
}

func (p *planner) refColumnNames() string {
	var names []string
	for _, column := range allColumns(p.frame) {
		if column.Kind == KindRef {
			names = append(names, column.Name)
		}
	}
	if len(names) == 0 {
		return "this frame has none"
	}
	return strings.Join(names, ", ")
}

// planGroups binds GROUP BY: Keys and Rows columns, WEEK or MONTH of a day column, or an item's position.
func (p *planner) planGroups(parsed *statement) error {
	for _, groupExpr := range parsed.groupBy {
		if groupExpr.kind == exprNumber {
			item, err := itemAtPosition(parsed, groupExpr, "GROUP BY")
			if err != nil {
				return err
			}
			groupExpr = item.value
		}
		group := groupSource{text: formatExpr(groupExpr)}
		columnExpr := groupExpr
		if groupExpr.kind == exprCall && (groupExpr.name == "WEEK" || groupExpr.name == "MONTH") && len(groupExpr.args) == 1 {
			columnExpr = groupExpr.args[0]
			group.transform = transformWeek
			if groupExpr.name == "MONTH" {
				group.transform = transformMonth
			}
		}
		if columnExpr.kind != exprColumn {
			return fmt.Errorf("GROUP BY takes columns, WEEK(day) or MONTH(day): `%s` is none of them", group.text)
		}
		target, exists := lookupColumn(p.frame, columnExpr.name)
		if !exists {
			return p.unknownColumn(columnExpr.name)
		}
		if target.role == roleSum {
			return fmt.Errorf("`%s` is a summed value, not a dimension: GROUP BY %s", target.column.Name, columnNames(slices.Concat(p.frame.Keys, []Column{p.frame.Rows})))
		}
		if group.transform != transformNone && target.column.Kind != KindDay {
			return fmt.Errorf("%s() takes a day column; `%s` is not one", groupExpr.name, target.column.Name)
		}
		group.isRow, group.keyIndex, group.column = target.role == roleRows, target.index, target.column
		if group.transform != transformNone {
			group.column = Column{Name: group.text, Label: target.column.Label, Kind: KindDay}
		}
		if target.role == roleKey && target.index == 0 && target.column.Kind == KindDay {
			group.period = [...]string{transformNone: "day", transformWeek: "week", transformMonth: "month"}[group.transform]
		}
		if slices.ContainsFunc(p.plan.groups, func(other groupSource) bool { return other.text == group.text }) {
			continue
		}
		if len(p.plan.groups) == maxGroups {
			return fmt.Errorf("GROUP BY takes at most %d columns", maxGroups)
		}
		p.plan.groups = append(p.plan.groups, group)
	}
	return nil
}

// itemAtPosition is the SELECT item a position names (GROUP BY 1, ORDER BY 2).
func itemAtPosition(parsed *statement, position *expr, clause string) (selectItem, error) {
	index := int(position.number)
	if position.isDecimal || index < 1 || index > len(parsed.items) {
		return selectItem{}, fmt.Errorf("%s %s: a position is a whole number from 1 to %d, the SELECT items", clause, position.text, len(parsed.items))
	}
	return parsed.items[index-1], nil
}

// bind types an item's expression and ties it to the fold: groups, the accumulated sums and the count.
func (p *planner) bind(node *expr) (*boundExpr, error) {
	text := formatExpr(node)
	if groupIndex := slices.IndexFunc(p.plan.groups, func(group groupSource) bool { return group.text == text }); groupIndex >= 0 {
		group := p.plan.groups[groupIndex]
		return &boundExpr{kind: boundGroup, index: groupIndex, resultKind: group.column.Kind, collection: group.column.Collection}, nil
	}
	switch node.kind {
	case exprNumber:
		resultKind := KindInteger
		if node.isDecimal {
			resultKind = KindDecimal
		}
		return &boundExpr{kind: boundNumber, number: node.number, resultKind: resultKind}, nil
	case exprColumn:
		target, exists := lookupColumn(p.frame, node.name)
		if !exists {
			return nil, p.unknownColumn(node.name)
		}
		if target.role == roleSum {
			return nil, fmt.Errorf("`%s` is a summed value: write SUM(%s) or AVG(%s)", node.name, node.name, node.name)
		}
		return nil, fmt.Errorf("`%s` is in SELECT but not in GROUP BY", node.name)
	case exprBinary:
		left, err := p.bind(node.left)
		if err != nil {
			return nil, err
		}
		right, err := p.bind(node.right)
		if err != nil {
			return nil, err
		}
		resultKind, err := binaryKind(node.op, left.resultKind, right.resultKind, text)
		if err != nil {
			return nil, err
		}
		return &boundExpr{kind: boundBinary, op: node.op, left: left, right: right, resultKind: resultKind}, nil
	}

	switch node.name {
	case "SUM", "AVG":
		target, isColumn := frameColumn{}, false
		if len(node.args) == 1 && node.args[0].kind == exprColumn {
			target, isColumn = lookupColumn(p.frame, node.args[0].name)
		}
		if !isColumn || target.role != roleSum {
			return nil, fmt.Errorf("%s() takes one summed column: %s", node.name, columnNames(p.frame.Sums))
		}
		bound := &boundExpr{kind: boundSum, index: p.sumIndex(target.index), resultKind: KindInteger}
		if node.name == "AVG" {
			bound.kind, bound.resultKind = boundAverage, KindDecimal
		}
		if target.column.Kind == KindCents {
			bound.resultKind = KindCents
		}
		return bound, nil
	case "COUNT":
		if !node.isStar {
			return nil, fmt.Errorf("write COUNT(*): it counts the frame's rows in each group")
		}
		return &boundExpr{kind: boundCount, resultKind: KindInteger}, nil
	case "WEEK", "MONTH":
		return nil, fmt.Errorf("`%s` is in SELECT but not in GROUP BY", text)
	case "MIN", "MAX", "MEDIAN", "STDDEV", "PERCENTILE", "PERCENTILE_CONT":
		return nil, fmt.Errorf("%s() is not supported: use SUM(), COUNT(*) or AVG()", node.name)
	case "PERIOD", "TODAY", "NOW", "CURRENT_DATE", "DATE":
		return nil, fmt.Errorf("%s() is not supported here: bound days in WHERE, column IN PERIOD(\"D-6..D\")", node.name)
	}
	return nil, fmt.Errorf("unknown function %s(): use SUM(), COUNT(*), AVG(), and WEEK() or MONTH() in GROUP BY", node.name)
}

// sumIndex is the accumulated sum of a Sums column, added on first use.
func (p *planner) sumIndex(sumsColumn int) int {
	if index := slices.Index(p.plan.sumColumns, sumsColumn); index >= 0 {
		return index
	}
	p.plan.sumColumns = append(p.plan.sumColumns, sumsColumn)
	return len(p.plan.sumColumns) - 1
}

// binaryKind types an arithmetic operation. Money stays money through +, - against money, and through
// * or / by a plain number; money divided by money is a ratio.
func binaryKind(op byte, left, right Kind, text string) (Kind, error) {
	isLeftCents, isRightCents := left == KindCents, right == KindCents
	switch {
	case (op == '+' || op == '-') && isLeftCents != isRightCents:
		return 0, fmt.Errorf("`%s` adds money to a value that is not money", text)
	case op == '*' && isLeftCents && isRightCents:
		return 0, fmt.Errorf("`%s` multiplies money by money", text)
	case isLeftCents && !(op == '/' && isRightCents):
		return KindCents, nil
	case isRightCents && op == '*':
		return KindCents, nil
	case op == '/' || left == KindDecimal || right == KindDecimal:
		return KindDecimal, nil
	}
	return KindInteger, nil
}

// planOrder binds ORDER BY (or SORT BY): an item's name (SORT BY amount is SUM(amount)), its
// position, the expression of an item, or any expression an item could be (bound as an item that is
// not returned).
func (p *planner) planOrder(parsed *statement) error {
	for _, term := range parsed.orderBy {
		itemIndex := -1
		switch {
		case term.value.kind == exprNumber:
			if _, err := itemAtPosition(parsed, term.value, "ORDER BY"); err != nil {
				return err
			}
			itemIndex = int(term.value.number) - 1
		case term.value.kind == exprColumn:
			itemIndex = slices.IndexFunc(p.plan.items[:p.plan.visibleItems], func(item planItem) bool { return strings.EqualFold(item.column.Name, term.value.name) })
		}
		if itemIndex < 0 {
			text := formatExpr(term.value)
			itemIndex = slices.IndexFunc(parsed.items, func(item selectItem) bool { return formatExpr(item.value) == text })
		}
		if itemIndex < 0 {
			value, err := p.bind(term.value)
			if err != nil {
				return err
			}
			p.plan.items = append(p.plan.items, planItem{column: ResultColumn{Name: formatExpr(term.value)}, value: value})
			itemIndex = len(p.plan.items) - 1
		}
		p.plan.order = append(p.plan.order, orderKey{item: itemIndex, isDescending: term.isDescending})
	}
	return nil
}

// selectFiles is the read's selectFiles: it keeps the files the key filters keep, and refuses a read
// above maxFiles before any GET.
func (plan *queryPlan) selectFiles(fileKeys [][dataframe.MaxKeys]int64) ([][dataframe.MaxKeys]int64, error) {
	selected := make([][dataframe.MaxKeys]int64, 0, len(fileKeys))
	for _, keys := range fileKeys {
		isKept := true
		for i, filter := range plan.keyFilters {
			isKept = isKept && (filter == nil || filter.matches(keys[i]))
		}
		if isKept {
			selected = append(selected, keys)
		}
	}
	if len(selected) <= plan.maxFiles {
		return selected, nil
	}
	firstKey := plan.frame.Keys[0].Name
	advice := "narrow `" + firstKey + "`"
	if plan.unpinnedKey != "" {
		advice += " or add `" + plan.unpinnedKey + " = …`"
	}
	return nil, fmt.Errorf("`%s` would read %d files; the maximum is %d: %s", sqlName(plan.frame.Name), len(selected), plan.maxFiles, advice)
}
