package scylla

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

type selectScanColumn struct {
	ColumnName    string
	DecomposeView *viewInfo
}

type nativeGroupByPlan struct {
	ViewTableName     string
	SelectExpressions []string
	ScanColumns       []selectScanColumn
	GroupByColumns    []string
	WhereStatements   []boundWhereClause
	OrderColumn       IColInfo
}

func buildDefaultScanColumns(columnNames []string) []selectScanColumn {
	scanColumns := make([]selectScanColumn, 0, len(columnNames))
	for _, columnName := range columnNames {
		scanColumns = append(scanColumns, selectScanColumn{ColumnName: columnName})
	}
	return scanColumns
}

func splitGroupByColumns(columns []columnInfo) ([]columnInfo, []columnInfo) {
	groupedColumns := []columnInfo{}
	aggregateColumns := []columnInfo{}
	for _, column := range columns {
		if column.AggregateFn != "" {
			aggregateColumns = append(aggregateColumns, column)
			continue
		}
		groupedColumns = append(groupedColumns, column)
	}
	return groupedColumns, aggregateColumns
}

func canUseViewForAggregateColumns(aggregateColumns []columnInfo, view *viewInfo) bool {
	if view == nil {
		return false
	}
	for _, aggregateColumn := range aggregateColumns {
		if !slices.Contains(view.AvailableColumns, aggregateColumn.GetName()) {
			if ShouldLog() {
				fmt.Printf("GroupBy aggregate view rejected: view=%s missing_column=%s available=%v\n",
					view.Name, aggregateColumn.GetName(), view.AvailableColumns)
			}
			return false
		}
	}
	return true
}

func makeGroupByAggregateProjection(column columnInfo) (string, error) {
	if column.AggregateFn == "" {
		return column.GetName(), nil
	}
	if column.AggregateFn == "AVG" && column.Type != 6 && column.Type != 7 {
		return "", fmt.Errorf(`Avg() requires a float32 or float64 destination column. Column: "%v"`, column.GetName())
	}
	return fmt.Sprintf("%s(%s) AS %s", column.AggregateFn, column.GetName(), column.GetName()), nil
}

func buildPackedPrefixRangeClauses(view *viewInfo, statements []ColumnStatement, scyllaTable ScyllaTable) ([]boundWhereClause, error) {
	if view == nil || len(view.PackedSourceColumns) == 0 || len(view.PackedSlotBitsPerColumn) == 0 {
		return nil, fmt.Errorf("packed GroupBy requires a packed view")
	}

	partitionColumn := scyllaTable.GetPartKey()
	statementByColumn := map[string]ColumnStatement{}
	allowedColumns := map[string]bool{}
	if partitionColumn != nil && !partitionColumn.IsNil() {
		allowedColumns[partitionColumn.GetName()] = true
	}
	for _, sourceColumn := range view.PackedSourceColumns {
		allowedColumns[sourceColumn.GetName()] = true
	}

	for _, statement := range statements {
		if !allowedColumns[statement.Col] {
			return nil, fmt.Errorf(`GroupBy packed view "%v" does not support filtering by column "%v"`, view.Name, statement.Col)
		}
		if _, exists := statementByColumn[statement.Col]; exists {
			return nil, fmt.Errorf(`GroupBy packed view "%v" does not support repeated filters for column "%v"`, view.Name, statement.Col)
		}
		statementByColumn[statement.Col] = statement
	}

	var partitionClause *boundWhereClause
	if partitionColumn != nil && !partitionColumn.IsNil() {
		partitionStatement, exists := statementByColumn[partitionColumn.GetName()]
		if !exists || partitionStatement.Operator != "=" {
			return nil, fmt.Errorf(`GroupBy packed view "%v" requires partition equality on "%v"`, view.Name, partitionColumn.GetName())
		}
		partitionClause = &boundWhereClause{
			Clause: fmt.Sprintf("%v = ?", partitionColumn.GetName()),
			Values: []any{partitionStatement.Value},
		}
	}

	appendPartitionClause := func(clauses ...boundWhereClause) boundWhereClause {
		combined := boundWhereClause{}
		if partitionClause != nil {
			combined.Clause = partitionClause.Clause
			combined.Values = append(combined.Values, partitionClause.Values...)
		}
		for _, clause := range clauses {
			if clause.Clause == "" {
				continue
			}
			if combined.Clause != "" {
				combined.Clause += " AND "
			}
			combined.Clause += clause.Clause
			combined.Values = append(combined.Values, clause.Values...)
		}
		return combined
	}

	prefixValueGroups := [][]int64{{}}
	rangeStatementIndex := len(view.PackedSourceColumns)
	var rangeStatement *ColumnStatement

	for sourceIndex, sourceColumn := range view.PackedSourceColumns {
		statement, exists := statementByColumn[sourceColumn.GetName()]
		if !exists {
			rangeStatementIndex = sourceIndex
			break
		}

		switch statement.Operator {
		case "=":
			nextGroups := [][]int64{}
			for _, prefixValues := range prefixValueGroups {
				nextGroups = append(nextGroups, append(slices.Clone(prefixValues), db.ConvertToInt64(statement.Value)))
			}
			prefixValueGroups = nextGroups
		case "IN":
			if len(statement.Values) == 0 {
				return nil, fmt.Errorf(`GroupBy packed view "%v" received an empty IN for column "%v"`, view.Name, sourceColumn.GetName())
			}
			nextGroups := [][]int64{}
			for _, prefixValues := range prefixValueGroups {
				for _, rawValue := range statement.Values {
					nextGroups = append(nextGroups, append(slices.Clone(prefixValues), db.ConvertToInt64(rawValue)))
				}
			}
			prefixValueGroups = nextGroups
		case "BETWEEN", ">", ">=", "<", "<=":
			rangeStatementIndex = sourceIndex
			rangeStatement = &statement
		default:
			return nil, fmt.Errorf(`GroupBy packed view "%v" does not support operator "%v" for column "%v"`, view.Name, statement.Operator, sourceColumn.GetName())
		}

		if rangeStatement != nil {
			break
		}
	}

	for trailingIndex := rangeStatementIndex + 1; trailingIndex < len(view.PackedSourceColumns); trailingIndex++ {
		trailingColumnName := view.PackedSourceColumns[trailingIndex].GetName()
		if _, exists := statementByColumn[trailingColumnName]; exists {
			return nil, fmt.Errorf(`GroupBy packed view "%v" requires filters to follow the packed key order. Column "%v" is out of place`, view.Name, trailingColumnName)
		}
	}

	packedColumnName := view.Column.GetName()
	whereStatements := []boundWhereClause{}

	// packedBound packs the prefix (plus the range value, if any) with every later slot at zero or at
	// its max, stored-encoded like the view's own values. Using the slot max for the upper side keeps
	// every bound inclusive, so no bound needs a +1 that could overflow a slot.
	packedBound := func(prefixValues []int64, fillWithMax bool, rangeValue ...int64) any {
		boundPrefix := append(slices.Clone(prefixValues), rangeValue...)
		packed := db.PackSlotPrefixBound(boundPrefix, view.PackedSlotBitsPerColumn, fillWithMax)
		return storeVirtualPacked(packed, view.PackedIsInt32)
	}
	singleBoundClause := func(operator string, value any) boundWhereClause {
		return appendPartitionClause(boundWhereClause{
			Clause: fmt.Sprintf("%v %v ?", packedColumnName, operator),
			Values: []any{value},
		})
	}
	closedRangeClause := func(fromValue, toValue any) boundWhereClause {
		return appendPartitionClause(
			boundWhereClause{Clause: fmt.Sprintf("%v >= ?", packedColumnName), Values: []any{fromValue}},
			boundWhereClause{Clause: fmt.Sprintf("%v <= ?", packedColumnName), Values: []any{toValue}},
		)
	}

	switch {
	case rangeStatement == nil && rangeStatementIndex == 0:
		whereStatements = append(whereStatements, appendPartitionClause())
	case rangeStatement == nil && rangeStatementIndex >= len(view.PackedSourceColumns):
		for _, prefixValues := range prefixValueGroups {
			whereStatements = append(whereStatements, singleBoundClause("=", packedBound(prefixValues, false)))
		}
	case rangeStatement == nil:
		for _, prefixValues := range prefixValueGroups {
			whereStatements = append(whereStatements, closedRangeClause(packedBound(prefixValues, false), packedBound(prefixValues, true)))
		}
	default:
		for _, prefixValues := range prefixValueGroups {
			switch rangeStatement.Operator {
			case "BETWEEN":
				if len(rangeStatement.From) == 0 || len(rangeStatement.To) == 0 {
					return nil, fmt.Errorf(`GroupBy packed view "%v" received an invalid BETWEEN for column "%v"`, view.Name, rangeStatement.Col)
				}
				fromValue := packedBound(prefixValues, false, db.ConvertToInt64(rangeStatement.From[0].Value))
				toValue := packedBound(prefixValues, true, db.ConvertToInt64(rangeStatement.To[0].Value))
				whereStatements = append(whereStatements, closedRangeClause(fromValue, toValue))
			case ">", "<=":
				// Everything under the value's own block sits at or below its max-filled bound.
				rangeValue := db.ConvertToInt64(rangeStatement.Value)
				whereStatements = append(whereStatements, singleBoundClause(rangeStatement.Operator, packedBound(prefixValues, true, rangeValue)))
			case ">=", "<":
				rangeValue := db.ConvertToInt64(rangeStatement.Value)
				whereStatements = append(whereStatements, singleBoundClause(rangeStatement.Operator, packedBound(prefixValues, false, rangeValue)))
			}
		}
	}

	if len(whereStatements) == 0 {
		return nil, fmt.Errorf(`GroupBy packed view "%v" could not build a packed where clause`, view.Name)
	}

	return whereStatements, nil
}

func buildNativeGroupByPlan(
	tableInfo *TableInfo,
	statements []ColumnStatement,
	scyllaTable ScyllaTable,
) (*nativeGroupByPlan, error) {
	if len(tableInfo.GroupByColumns) == 0 {
		return nil, nil
	}
	if len(tableInfo.ColumnsInclude) > 0 || len(tableInfo.ColumnsExclude) > 0 {
		return nil, fmt.Errorf("GroupBy cannot be combined with Select() or Exclude()")
	}

	groupedColumns, aggregateColumns := splitGroupByColumns(tableInfo.GroupByColumns)
	if len(groupedColumns) == 0 {
		return nil, fmt.Errorf("GroupBy requires at least one non-aggregated column")
	}

	selectExpressions := []string{}
	scanColumns := []selectScanColumn{}
	for _, groupedColumn := range groupedColumns {
		selectExpressions = append(selectExpressions, groupedColumn.GetName())
		scanColumns = append(scanColumns, selectScanColumn{ColumnName: groupedColumn.GetName()})
	}
	for _, aggregateColumn := range aggregateColumns {
		selectExpression, err := makeGroupByAggregateProjection(aggregateColumn)
		if err != nil {
			return nil, err
		}
		selectExpressions = append(selectExpressions, selectExpression)
		scanColumns = append(scanColumns, selectScanColumn{ColumnName: aggregateColumn.GetName()})
	}

	if len(groupedColumns) == 1 {
		groupedColumn := scyllaTable.ColumnsMap[groupedColumns[0].GetName()]
		if groupedColumn == nil {
			return nil, fmt.Errorf(`GroupBy column "%v" was not found`, groupedColumns[0].GetName())
		}
		if ShouldLog() {
			fmt.Printf("Native GroupBy plan selected on base table: group=%v aggregates=%d\n", groupedColumns[0].GetName(), len(aggregateColumns))
		}
		return &nativeGroupByPlan{
			SelectExpressions: selectExpressions,
			ScanColumns:       scanColumns,
			GroupByColumns:    []string{groupedColumns[0].GetName()},
			OrderColumn:       groupedColumn,
		}, nil
	}

	groupedColumnNames := make([]string, 0, len(groupedColumns))
	for _, groupedColumn := range groupedColumns {
		groupedColumnNames = append(groupedColumnNames, groupedColumn.GetName())
	}

	for _, view := range scyllaTable.indexViews {
		if view.Type != 8 || len(view.ColumnsNoPart) != len(groupedColumnNames) {
			continue
		}
		if !slices.Equal(view.ColumnsNoPart, groupedColumnNames) {
			if ShouldLog() {
				fmt.Printf("GroupBy packed view skipped: view=%s expected=%v got=%v\n",
					view.Name, groupedColumnNames, view.ColumnsNoPart)
			}
			continue
		}
		if !canUseViewForAggregateColumns(aggregateColumns, view) {
			continue
		}

		whereStatements, err := buildPackedPrefixRangeClauses(view, statements, scyllaTable)
		if err != nil {
			fmt.Printf("GroupBy packed view rejected: view=%s err=%v\n", view.Name, err)
			return nil, err
		}

		selectExpressionsPacked := []string{view.Column.GetName()}
		scanColumnsPacked := []selectScanColumn{{ColumnName: view.Column.GetName(), DecomposeView: view}}
		for _, aggregateColumn := range aggregateColumns {
			selectExpression, err := makeGroupByAggregateProjection(aggregateColumn)
			if err != nil {
				return nil, err
			}
			selectExpressionsPacked = append(selectExpressionsPacked, selectExpression)
			scanColumnsPacked = append(scanColumnsPacked, selectScanColumn{ColumnName: aggregateColumn.GetName()})
		}

		if ShouldLog() {
			fmt.Printf("Native GroupBy packed view selected: view=%s groups=%v aggregates=%d\n", view.Name, groupedColumnNames, len(aggregateColumns))
		}
		return &nativeGroupByPlan{
			ViewTableName:     view.Name,
			SelectExpressions: selectExpressionsPacked,
			ScanColumns:       scanColumnsPacked,
			GroupByColumns:    []string{view.Column.GetName()},
			WhereStatements:   whereStatements,
			OrderColumn:       view.Column,
		}, nil
	}

	return nil, fmt.Errorf(`GroupBy requires a compatible packed view for columns "%v"`, strings.Join(groupedColumnNames, ", "))
}
