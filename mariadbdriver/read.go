package mariadbdriver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// The read path of a plain Select. The shared planner picks the source and binds the values (so a
// query it refuses fails as on Scylla); the plan becomes one SELECT ordered by the source's key. Every
// predicate the source leaves is a SQL condition, applied exactly
// by the server. Rows stream from the server, so a large read is not materialized there.

// recordSink is where decoded records go, seen through unsafe pointers so the read path is not
// stenciled per record type.
type recordSink struct {
	recordType reflect.Type
	newRecord  func() unsafe.Pointer
	// appendRecord stores a record, unless the caller's scan handler consumed it.
	appendRecord func(record unsafe.Pointer)
}

// collectInto is a sink that allocates as this one does and keeps the records in collected.
func (sink recordSink) collectInto(collected *[]unsafe.Pointer) recordSink {
	return recordSink{
		recordType: sink.recordType, newRecord: sink.newRecord,
		appendRecord: func(record unsafe.Pointer) { *collected = append(*collected, record) },
	}
}

func makeRecordSink[RecordT any](destination *[]RecordT, scan func(record *RecordT) bool) recordSink {
	return recordSink{
		recordType: reflect.TypeFor[RecordT](),
		newRecord:  func() unsafe.Pointer { return unsafe.Pointer(new(RecordT)) },
		appendRecord: func(record unsafe.Pointer) {
			if scan != nil && scan((*RecordT)(record)) {
				return
			}
			*destination = append(*destination, *(*RecordT)(record))
		},
	}
}

// makeReflectRecordSink appends to a reflect-built []T (records.NewRecordList()).
func makeReflectRecordSink(recordList reflect.Value) recordSink {
	recordType := recordList.Elem().Type().Elem()
	return recordSink{
		recordType: recordType,
		newRecord:  func() unsafe.Pointer { return reflect.New(recordType).UnsafePointer() },
		appendRecord: func(record unsafe.Pointer) {
			recordList.Elem().Set(reflect.Append(recordList.Elem(), reflect.NewAt(recordType, record).Elem()))
		},
	}
}

func selectRecords(table *MariaTable, tableInfo *db.TableInfo, sink recordSink) error {
	switch {
	case tableInfo.Delta != nil:
		return selectDelta(table, tableInfo, sink)
	case tableInfo.UseIndexGroupSelect:
		return errNotImplemented("cache-group reads through Select")
	case len(tableInfo.GroupByColumns) > 0:
		return selectGroupBy(table, tableInfo, sink)
	}
	selectedColumns := table.projectedColumns(tableInfo)
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	plan, err := table.BindAccessPlan(table.accessRouteOf(tableInfo, selectedNames), tableInfo)
	if err != nil {
		return fmt.Errorf("mariadbdriver: reading %q: %w", table.Name, err)
	}
	statement, args, err := table.renderAccessPlan(plan, quotedNames(selectedNames), nil)
	if err != nil {
		return err
	}
	return table.queryInto(statement, args, selectedColumns, sink)
}

// selectGroupBy is a GroupBy read as native SQL: the route the shared planner picks for the filters
// serves the WHERE, the server groups by the plain columns and sums
// (averages, maxes) the aggregated ones, each aggregate scanned back into its own column.
func selectGroupBy(table *MariaTable, tableInfo *db.TableInfo, sink recordSink) error {
	selectedColumns, selectedNames, selectExpressions, groupedNames := []storedColumn{}, []string{}, []string{}, []string{}
	for _, groupByColumn := range tableInfo.GroupByColumns {
		position := slices.IndexFunc(table.storedColumns, func(stored storedColumn) bool { return stored.name == groupByColumn.Name })
		if position < 0 {
			return fmt.Errorf(`mariadbdriver: GroupBy on %q: column %q is not stored`, table.Name, groupByColumn.Name)
		}
		stored := table.storedColumns[position]
		selectedColumns, selectedNames = append(selectedColumns, stored), append(selectedNames, stored.name)
		if groupByColumn.AggregateFn == "" {
			groupedNames = append(groupedNames, stored.name)
			selectExpressions = append(selectExpressions, quoteName(stored.name))
			continue
		}
		if groupByColumn.AggregateFn == "AVG" && groupByColumn.Type != 6 && groupByColumn.Type != 7 {
			return fmt.Errorf(`Avg() requires a float32 or float64 destination column. Column: "%v"`, stored.name)
		}
		selectExpressions = append(selectExpressions, fmt.Sprintf("%s(%s) AS %s", groupByColumn.AggregateFn, quoteName(stored.name), quoteName(stored.name)))
	}
	if len(groupedNames) == 0 {
		return fmt.Errorf(`mariadbdriver: GroupBy on %q needs at least one column to group by`, table.Name)
	}
	plan, err := table.BindAccessPlan(table.accessRouteOf(tableInfo, selectedNames), tableInfo)
	if err != nil {
		return fmt.Errorf("mariadbdriver: GroupBy on %q: %w", table.Name, err)
	}
	statement, args, err := table.renderAccessPlan(plan, strings.Join(selectExpressions, ", "), groupedNames)
	if err != nil {
		return err
	}
	return table.queryInto(statement, args, selectedColumns, sink)
}

// accessRouteOf is the shared planner's route for the read's shape (columns, operators, value
// counts, projection), cached per table: only the values change between reads of one shape.
func (table *MariaTable) accessRouteOf(tableInfo *db.TableInfo, selectedNames []string) *db.AccessRoute {
	statements := tableInfo.SelectStatements()
	shape := strings.Builder{}
	for _, statement := range statements {
		fmt.Fprintf(&shape, "%s|%s|%d|%d;", statement.Col, db.CapabilityOpForStatement(statement), len(statement.Values), len(statement.From))
	}
	shape.WriteString(strings.Join(selectedNames, ","))
	if cachedRoute, isCached := table.routeByShape.Load(shape.String()); isCached {
		return cachedRoute.(*db.AccessRoute)
	}
	route := table.ChooseAccessRoute(statements, table.capabilities, selectedNames)
	table.routeByShape.Store(shape.String(), route)
	return route
}

// renderAccessPlan renders a bound plan as one SELECT (plan §10): the steps OR-ed (a row matches a
// step at most once, so the union needs no deduplication), then the Filter and PostFilter as SQL
// conditions (both exact on the server), and the source's key order.
// A grouped read groups and orders by groupedNames instead.
func (table *MariaTable) renderAccessPlan(plan *db.AccessPlan, selectList string, groupedNames []string) (string, []any, error) {
	// No index hint: MariaDB picks the index the conditions serve. A slice source is an element side
	// table, so its rows come back in primary-key order.
	orderColumnNames := []string{}
	if plan.Source != nil && !table.isElementSource(plan.Source) {
		orderColumnNames = append(orderColumnNames, table.indexOfSource(plan.Source).columnNames()...)
	}
	for _, keyColumn := range table.primaryKeyColumns {
		if !slices.Contains(orderColumnNames, keyColumn.GetName()) {
			orderColumnNames = append(orderColumnNames, keyColumn.GetName())
		}
	}

	conditions, args := []string{}, []any{}
	stepConditions := []string{}
	for _, step := range plan.Steps {
		renderedConditions, stepArgs, err := table.renderConditions(accessStepStatements(step))
		if err != nil {
			return "", nil, err
		}
		args = append(args, stepArgs...)
		if len(renderedConditions) == 0 {
			renderedConditions = []string{"TRUE"}
		}
		stepConditions = append(stepConditions, "("+strings.Join(renderedConditions, " AND ")+")")
	}
	if len(stepConditions) > 0 {
		conditions = append(conditions, "("+strings.Join(stepConditions, " OR ")+")")
	}
	for _, statements := range [][]db.ColumnStatement{plan.Filter, plan.PostFilter} {
		renderedConditions, statementArgs, err := table.renderConditions(statements)
		if err != nil {
			return "", nil, err
		}
		conditions, args = append(conditions, renderedConditions...), append(args, statementArgs...)
	}

	statement := fmt.Sprintf("SELECT %s FROM %s", selectList, table.qualifiedName())
	if len(conditions) > 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	if len(groupedNames) > 0 {
		statement += " GROUP BY " + quotedNames(groupedNames)
		orderColumnNames = slices.Clone(groupedNames)
	}
	for position, columnName := range orderColumnNames {
		orderColumnNames[position] = quoteName(columnName)
		if plan.Descending {
			orderColumnNames[position] += " DESC"
		}
	}
	statement += " ORDER BY " + strings.Join(orderColumnNames, ", ")
	if plan.Limit > 0 {
		statement += fmt.Sprintf(" LIMIT %d", plan.Limit)
	}
	return statement, args, nil
}

// isElementSource: the source is a slice index, stored as an element side table.
func (table *MariaTable) isElementSource(source *db.IndexSource) bool {
	return slices.ContainsFunc(table.elementIndexes, func(built elementIndex) bool { return built.sourceName == source.Name })
}

// elementIndexOfColumn is the element side table of a slice column (one per slice: index_mapping.go).
func (table *MariaTable) elementIndexOfColumn(columnName string) (elementIndex, bool) {
	position := slices.IndexFunc(table.elementIndexes, func(built elementIndex) bool { return built.sliceColumn == columnName })
	if position < 0 {
		return elementIndex{}, false
	}
	return table.elementIndexes[position], true
}

// indexOfSource is the native index an IndexSource is stored as (index_mapping.go names it).
func (table *MariaTable) indexOfSource(source *db.IndexSource) indexSpec {
	indexName := boundedIdentifier("ix_" + source.Name)
	position := slices.IndexFunc(table.mainTable.indexes, func(index indexSpec) bool { return index.name == indexName })
	if position < 0 {
		panic(fmt.Sprintf(`mariadbdriver: table "%v": source "%v" has no index`, table.Name, source.Name))
	}
	return table.mainTable.indexes[position]
}

// accessStepStatements turns one step back into predicates: = or IN per pinned key column, the
// bound as a BETWEEN or one-sided operators, and an element lookup as CONTAINS.
func accessStepStatements(step db.AccessStep) []db.ColumnStatement {
	equalPrefixStatements := func(equalPrefix []db.KeyValues) []db.ColumnStatement {
		statements := []db.ColumnStatement{}
		for _, keyValues := range equalPrefix {
			if len(keyValues.Values) == 1 {
				statements = append(statements, db.ColumnStatement{Col: keyValues.Column, Operator: "=", Value: keyValues.Values[0]})
			} else {
				statements = append(statements, db.ColumnStatement{Col: keyValues.Column, Operator: "IN", Values: keyValues.Values})
			}
		}
		return statements
	}
	if step.Element != nil {
		return append(equalPrefixStatements(step.Element.EqualPrefix),
			db.ColumnStatement{Col: step.Element.Column, Operator: "CONTAINS", Value: step.Element.Value})
	}
	keyRange := step.Range
	statements := equalPrefixStatements(keyRange.EqualPrefix)
	if keyRange.BoundColumn == "" {
		return statements
	}
	if keyRange.From != nil {
		operator := map[bool]string{true: ">=", false: ">"}[keyRange.FromInclusive]
		statements = append(statements, db.ColumnStatement{Col: keyRange.BoundColumn, Operator: operator, Value: keyRange.From})
	}
	if keyRange.To != nil {
		operator := map[bool]string{true: "<=", false: "<"}[keyRange.ToInclusive]
		statements = append(statements, db.ColumnStatement{Col: keyRange.BoundColumn, Operator: operator, Value: keyRange.To})
	}
	return statements
}

// rowQuerier is the pool or a transaction.
type rowQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// queryInto runs a SELECT of selectedColumns on the pool and decodes every row into the sink.
func (table *MariaTable) queryInto(statement string, args []any, selectedColumns []storedColumn, sink recordSink) error {
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	return table.queryIntoWith(pool, context.Background(), statement, args, selectedColumns, sink)
}

// queryIntoWith is queryInto on a given querier (a transaction reads its locked rows through it).
func (table *MariaTable) queryIntoWith(querier rowQuerier, ctx context.Context, statement string, args []any, selectedColumns []storedColumn, sink recordSink) error {
	rows, err := querier.QueryContext(ctx, statement, args...)
	if err != nil {
		return fmt.Errorf("mariadbdriver: reading %q: %w", table.Name, err)
	}
	defer rows.Close()
	targets := make([]any, len(selectedColumns))
	for rows.Next() {
		for position, stored := range selectedColumns {
			targets[position] = scanTarget(stored)
		}
		if err := rows.Scan(targets...); err != nil {
			return fmt.Errorf("mariadbdriver: reading %q: %w", table.Name, err)
		}
		recordPointer := sink.newRecord()
		for position, stored := range selectedColumns {
			if err := assignScanned(stored, recordPointer, targets[position]); err != nil {
				return fmt.Errorf("mariadbdriver: reading %q column %q: %w", table.Name, stored.name, err)
			}
		}
		sink.appendRecord(recordPointer)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mariadbdriver: reading %q: %w", table.Name, err)
	}
	return nil
}

// projectedColumns are the stored columns with a record field the read decodes: the caller's
// Select/Exclude projection, every one otherwise.
func (table *MariaTable) projectedColumns(tableInfo *db.TableInfo) []storedColumn {
	isIncluded := map[string]bool{}
	for _, column := range tableInfo.ColumnsInclude {
		isIncluded[column.Name] = true
	}
	isExcluded := map[string]bool{}
	for _, column := range tableInfo.ColumnsExclude {
		isExcluded[column.Name] = true
	}
	projected := []storedColumn{}
	for _, stored := range table.storedColumns {
		if stored.hasField() && !isExcluded[stored.name] && (len(isIncluded) == 0 || isIncluded[stored.name]) {
			projected = append(projected, stored)
		}
	}
	return projected
}

// renderConditions turns predicates into SQL conditions with placeholders. A predicate on an indexed
// slice is a MEMBER OF its JSON column, the form its multi-value index serves (§18b).
func (table *MariaTable) renderConditions(statements []db.ColumnStatement) ([]string, []any, error) {
	conditions, args := []string{}, []any{}
	for _, statement := range statements {
		stored, err := table.filterColumn(statement.Col)
		if err != nil {
			return nil, nil, err
		}
		quotedColumn := quoteName(stored.name)
		switch statement.Operator {
		case "=", "!=", ">", ">=", "<", "<=":
			conditions = append(conditions, quotedColumn+" "+statement.Operator+" ?")
			args = append(args, statement.Value)
		case "IN":
			if len(statement.Values) == 0 {
				conditions = append(conditions, "FALSE")
				continue
			}
			conditions = append(conditions, fmt.Sprintf("%s IN (%s)", quotedColumn, strings.TrimSuffix(strings.Repeat("?, ", len(statement.Values)), ", ")))
			args = append(args, statement.Values...)
		case "BETWEEN":
			// A BETWEEN over several columns compares the tuples, as a clustering-key range does.
			fromColumns, fromValues := []string{}, []any{}
			for _, bound := range statement.From {
				boundColumn, err := table.filterColumn(bound.Col)
				if err != nil {
					return nil, nil, err
				}
				fromColumns = append(fromColumns, quoteName(boundColumn.name))
				fromValues = append(fromValues, bound.Value)
			}
			toValues := []any{}
			for _, bound := range statement.To {
				toValues = append(toValues, bound.Value)
			}
			if len(fromColumns) != len(toValues) {
				return nil, nil, fmt.Errorf("table %q: BETWEEN on %q has %d lower and %d upper bounds", table.Name, statement.Col, len(fromColumns), len(toValues))
			}
			tuple := "(" + strings.Join(fromColumns, ", ") + ")"
			tuplePlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(fromColumns)), ", ") + ")"
			conditions = append(conditions, tuple+" >= "+tuplePlaceholders, tuple+" <= "+tuplePlaceholders)
			args = append(append(args, fromValues...), toValues...)
		case "CONTAINS":
			built, hasElementIndex := table.elementIndexOfColumn(stored.name)
			if !hasElementIndex {
				return nil, nil, fmt.Errorf("table %q: CONTAINS on %q needs an index on that slice", table.Name, stored.name)
			}
			condition, conditionArgs := elementLookupCondition(built, table.mainTable.primaryKey, statement.Value, statements)
			conditions, args = append(conditions, condition), append(args, conditionArgs...)
		default:
			return nil, nil, fmt.Errorf("table %q: operator %q is not supported on MariaDB", table.Name, statement.Operator)
		}
	}
	return conditions, args, nil
}

// elementLookupCondition is a CONTAINS on an indexed slice: the record's primary key IN the element
// side table's rows of that element. The = and IN predicates of the same read on the side table's
// other key parts (the partition, mostly) go into the subquery too, so it is a key range there; the
// outer query keeps them as well.
func elementLookupCondition(built elementIndex, primaryKey []string, element any, siblingStatements []db.ColumnStatement) (string, []any) {
	subqueryConditions := []string{quoteName(built.sliceColumn) + " = ?"}
	args := []any{element}
	for _, sibling := range siblingStatements {
		isSideKeyPart := slices.ContainsFunc(built.keyParts, func(part indexKeyPart) bool { return part.column == sibling.Col && !part.isElementSet })
		switch {
		case !isSideKeyPart:
		case sibling.Operator == "=":
			subqueryConditions = append(subqueryConditions, quoteName(sibling.Col)+" = ?")
			args = append(args, sibling.Value)
		case sibling.Operator == "IN" && len(sibling.Values) > 0:
			subqueryConditions = append(subqueryConditions,
				fmt.Sprintf("%s IN (%s)", quoteName(sibling.Col), strings.TrimSuffix(strings.Repeat("?, ", len(sibling.Values)), ", ")))
			args = append(args, sibling.Values...)
		}
	}
	keyList := quotedNames(primaryKey)
	return fmt.Sprintf("(%s) IN (SELECT %s FROM %s WHERE %s)", keyList, keyList, built.sideTable.qualifiedName(),
		strings.Join(subqueryConditions, " AND ")), args
}

// filterColumn is the stored column a predicate filters: a declared column or the generated column
// an index keys on. A computed column (no storage) is refused.
func (table *MariaTable) filterColumn(columnName string) (storedColumn, error) {
	position := slices.IndexFunc(table.storedColumns, func(stored storedColumn) bool { return stored.name == columnName })
	if position < 0 {
		return storedColumn{}, fmt.Errorf("table %q: no stored column %q to filter on", table.Name, columnName)
	}
	return table.storedColumns[position], nil
}
