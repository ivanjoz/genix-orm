package scylla

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"
)

// indexPartitionColumnName returns the name of the partition override declared on an
// index, or "" when the index keeps the base table partition.
func indexPartitionColumnName(indexCfg Index) string {
	if indexCfg.Partition == nil {
		return ""
	}
	return indexCfg.Partition.GetInfo().Name
}

// resolveIndexPartitionColumn resolves the Partition override against the base table.
// Returns nil when no override is declared, meaning the view keeps the base partition.
func resolveIndexPartitionColumn(dbTable *ScyllaTable, viewCfg Index) IColInfo {
	partitionColumnName := indexPartitionColumnName(viewCfg)
	if partitionColumnName == "" {
		return nil
	}
	column := dbTable.ColumnsMap[partitionColumnName]
	if column == nil || column.IsNil() {
		panic(fmt.Sprintf(`Table "%v": Partition column "%v" was not found`, dbTable.Name, partitionColumnName))
	}
	if column.GetInfo().IsVirtual || column.GetType().IsComplexType || column.GetType().IsSlice {
		panic(fmt.Sprintf(`Table "%v": Partition column "%v" cannot be virtual, a slice or a struct`, dbTable.Name, column.GetName()))
	}
	return column
}

func compileSchemaViewTable(dbTable *ScyllaTable, viewCfg Index) {
	if indexPartitionColumnName(viewCfg) != "" {
		panic(fmt.Sprintf(`Table "%v": ViewTables always keep the base partition; remove Partition`, dbTable.Name))
	}
	if viewCfg.UseHash {
		panic(fmt.Sprintf(`Table "%v": ViewTables does not support UseHash`, dbTable.Name))
	}
	if len(viewCfg.Keys) == 0 {
		panic(fmt.Sprintf(`Table "%v": ViewTables entry must declare at least one key column`, dbTable.Name))
	}
	if len(dbTable.Keys) != 1 {
		panic(fmt.Sprintf(`Table "%v": ViewTables currently requires exactly one base key column for ID maintenance`, dbTable.Name))
	}

	partKey := dbTable.GetPartKey()
	if partKey == nil || partKey.IsNil() {
		panic(fmt.Sprintf(`Table "%v": ViewTables requires a partition column`, dbTable.Name))
	}

	declaredColumns := []IColInfo{}
	keyColumnNames := []string{}
	physicalColumns := []viewTableColumnInfo{
		makeViewTableColumn(partKey, false),
	}
	physicalKeyColumns := []viewTableColumnInfo{}
	rebuildColumnNames := map[string]bool{}
	fanoutColumnName := ""
	sliceKeyCount := 0

	for _, declaredColumn := range viewCfg.Keys {
		column := dbTable.ColumnsMap[declaredColumn.GetInfo().Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": ViewTables column "%v" was not found`, dbTable.Name, declaredColumn.GetInfo().Name))
		}
		if column.GetType().IsComplexType {
			panic(fmt.Sprintf(`Table "%v": ViewTables column "%v" cannot be a complex type`, dbTable.Name, column.GetName()))
		}
		if column.GetInfo().Name == dbTable.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": ViewTables key "%v" must not repeat the base ID column`, dbTable.Name, column.GetName()))
		}

		useSliceElement := column.GetType().IsSlice
		if useSliceElement {
			sliceKeyCount++
			fanoutColumnName = column.GetName()
		}

		keyColumnNames = append(keyColumnNames, column.GetName())
		declaredColumns = append(declaredColumns, column)
		rebuildColumnNames[column.GetName()] = true

		physicalColumn := makeViewTableColumn(column, useSliceElement)
		physicalColumns = appendUniqueViewTableColumn(physicalColumns, physicalColumn)
		physicalKeyColumns = append(physicalKeyColumns, physicalColumn)
	}

	if sliceKeyCount > 1 {
		panic(fmt.Sprintf(`Table "%v": ViewTables currently supports only one slice-backed key column`, dbTable.Name))
	}

	idColumn := dbTable.Keys[0]
	physicalColumns = appendUniqueViewTableColumn(physicalColumns, makeViewTableColumn(idColumn, false))

	projectedColumns := []IColInfo{}
	if len(viewCfg.Cols) == 0 {
		for _, baseColumn := range dbTable.ColumnsMap {
			if baseColumn.GetInfo().IsVirtual {
				continue
			}
			if baseColumn.GetName() == fanoutColumnName {
				continue
			}
			projectedColumns = append(projectedColumns, baseColumn)
		}
	} else {
		for _, declaredProjectedColumn := range viewCfg.Cols {
			projectedColumn := dbTable.ColumnsMap[declaredProjectedColumn.GetInfo().Name]
			if projectedColumn == nil || projectedColumn.IsNil() {
				panic(fmt.Sprintf(`Table "%v": ViewTables projected column "%v" wasn't found`, dbTable.Name, declaredProjectedColumn.GetInfo().Name))
			}
			if projectedColumn.GetInfo().IsVirtual {
				panic(fmt.Sprintf(`Table "%v": ViewTables projected column "%v" cannot be virtual`, dbTable.Name, projectedColumn.GetName()))
			}
			if projectedColumn.GetName() == fanoutColumnName {
				continue
			}
			projectedColumns = append(projectedColumns, projectedColumn)
		}
	}

	for _, projectedColumn := range projectedColumns {
		physicalColumns = appendUniqueViewTableColumn(physicalColumns, makeViewTableColumn(projectedColumn, false))
		rebuildColumnNames[projectedColumn.GetName()] = true
	}

	viewColumns := append([]string{partKey.GetName()}, keyColumnNames...)
	viewName := fmt.Sprintf(`%v__%v_view`, dbTable.Name, strings.Join(keyColumnNames, "_"))
	view := &viewInfo{
		Type:                9,
		name:                viewName,
		columns:             viewColumns,
		columnsNoPart:       append([]string{}, keyColumnNames...),
		column:              declaredColumns[0],
		availableColumns:    []string{},
		Operators:           []string{"=", "IN", "CONTAINS"},
		fanoutColumnName:    fanoutColumnName,
		tableColumns:        physicalColumns,
		tableKeyColumns:     physicalKeyColumns,
		maintenanceIDColumn: idColumn,
		rebuildColumnNames:  rebuildColumnNames,
	}

	selectableColumnNames := map[string]bool{}
	selectableColumnNames[partKey.GetName()] = true
	selectableColumnNames[idColumn.GetName()] = true
	for _, declaredColumn := range declaredColumns {
		if declaredColumn.GetName() == fanoutColumnName {
			continue
		}
		selectableColumnNames[declaredColumn.GetName()] = true
	}
	for _, projectedColumn := range projectedColumns {
		if projectedColumn.GetName() == fanoutColumnName {
			continue
		}
		selectableColumnNames[projectedColumn.GetName()] = true
	}
	for selectableColumnName := range selectableColumnNames {
		view.availableColumns = append(view.availableColumns, selectableColumnName)
	}
	slices.Sort(view.availableColumns)

	viewPtr := view
	view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
		whereClauses := []boundWhereClause{}
		for _, statement := range statements {
			if len(statement.From) > 0 {
				for idx := range statement.From {
					whereClauses = append(whereClauses,
						boundWhereClause{
							Clause: fmt.Sprintf("%v >= ?", statement.From[idx].Col),
							Values: []any{statement.From[idx].Value},
						},
						boundWhereClause{
							Clause: fmt.Sprintf("%v <= ?", statement.To[idx].Col),
							Values: []any{statement.To[idx].Value},
						},
					)
				}
				continue
			}

			operator := statement.Operator
			if viewPtr.fanoutColumnName == statement.Col && operator == "CONTAINS" {
				operator = "="
			}
			if operator == "IN" {
				placeholders := make([]string, 0, len(statement.Values))
				queryValues := make([]any, 0, len(statement.Values))
				for _, value := range statement.Values {
					placeholders = append(placeholders, "?")
					queryValues = append(queryValues, value)
				}
				whereClauses = append(whereClauses, boundWhereClause{
					Clause: fmt.Sprintf("%v IN (%v)", statement.Col, strings.Join(placeholders, ", ")),
					Values: queryValues,
				})
				continue
			}
			whereClauses = append(whereClauses, boundWhereClause{
				Clause: fmt.Sprintf("%v %v ?", statement.Col, operator),
				Values: []any{statement.Value},
			})
		}

		combinedClause := boundWhereClause{}
		for _, whereClause := range whereClauses {
			if combinedClause.Clause != "" {
				combinedClause.Clause += " AND "
			}
			combinedClause.Clause += whereClause.Clause
			combinedClause.Values = append(combinedClause.Values, whereClause.Values...)
		}
		return []boundWhereClause{combinedClause}
	}
	view.getExpectedColumns = func() []viewExpectedColumn {
		expectedColumns := make([]viewExpectedColumn, 0, len(viewPtr.tableColumns))
		for _, column := range viewPtr.tableColumns {
			expectedColumns = append(expectedColumns, viewExpectedColumn{
				name:   getViewTableColumnName(column),
				dbType: getViewTableColumnType(column.SourceColumn, column.UsesSliceElement).DBType,
			})
		}
		return expectedColumns
	}
	view.getCreateScript = func() string {
		columnDefinitions := make([]string, 0, len(viewPtr.tableColumns))
		for _, column := range viewPtr.getExpectedColumns() {
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%v %v", column.name, column.dbType))
		}

		primaryKeyColumns := append([]string{}, keyColumnNames...)
		primaryKeyColumns = append(primaryKeyColumns, idColumn.GetName())
		return fmt.Sprintf(`CREATE TABLE %v.%v (
			%v,
			PRIMARY KEY ((%v), %v)
		)
		%v;`,
			dbTable.Namespace,
			viewPtr.name,
			strings.Join(columnDefinitions, ", "),
			partKey.GetName(),
			strings.Join(primaryKeyColumns, ", "),
			makeStatementWith,
		)
	}

	dbTable.views[view.name] = view
}

// compileSchemaView builds the materialized view backing one TypeView declaration. slotPlan is nil
// for a plain TypeView, whose packed digit layout is derived from .DecimalSize() hints here; a
// TypeDelta declaration resolves its layout from FixedValues up front and passes it in.
func compileSchemaView(dbTable *ScyllaTable, viewCfg Index, slotPlan *deltaSlotPlan) {
	appendUniqueColumn := func(target []IColInfo, column IColInfo) []IColInfo {
		if column == nil || column.IsNil() {
			return target
		}
		for _, existingColumn := range target {
			if existingColumn.GetName() == column.GetName() {
				return target
			}
		}
		return append(target, column)
	}
	orderColumnsBySchemaIndex := func(columns []IColInfo) []IColInfo {
		orderedColumns := slices.Clone(columns)
		slices.SortFunc(orderedColumns, func(leftColumn, rightColumn IColInfo) int {
			if idxDiff := int(leftColumn.GetInfo().Idx - rightColumn.GetInfo().Idx); idxDiff != 0 {
				return idxDiff
			}
			return strings.Compare(leftColumn.GetName(), rightColumn.GetName())
		})
		return orderedColumns
	}

	colNames := []string{}
	declaredColumns := []IColInfo{}
	columns := []IColInfo{}
	viewColumnsConfig := make([]columnInfo, 0, len(viewCfg.Keys))
	packedViewHintFound := false
	for _, declaredColumn := range viewCfg.Keys {
		columnConfig := declaredColumn.GetInfo()
		viewColumnsConfig = append(viewColumnsConfig, columnConfig)
		if columnConfig.DecimalDigits > 0 || columnConfig.UseInt32Packing {
			packedViewHintFound = true
		}
	}

	// A resolved slot plan already establishes the view as packed, so no per-column hint is needed.
	isRangeView := len(viewCfg.Keys) > 1 && (packedViewHintFound || slotPlan != nil)

	// Views keep the base table partition unless the schema declares another column.
	basePartCol := dbTable.GetPartKey()
	if basePartCol != nil && basePartCol.IsNil() {
		basePartCol = nil
	}
	viewPartCol := resolveIndexPartitionColumn(dbTable, viewCfg)
	keepsBasePart := viewPartCol == nil ||
		(basePartCol != nil && viewPartCol.GetName() == basePartCol.GetName())
	if viewPartCol == nil {
		viewPartCol = basePartCol
	}

	for _, colInfo := range viewCfg.Keys {
		column := dbTable.ColumnsMap[colInfo.GetInfo().Name]
		if column.GetType().IsComplexType {
			panic("No puede usar un struct como columna de una view.")
		}
		colNames = append(colNames, column.GetName())
		declaredColumns = append(declaredColumns, column)
		columns = append(columns, column)
	}

	colNamesNoPart := colNames
	declaredColumnCount := len(declaredColumns)
	isSingleDeclaredSimpleView := declaredColumnCount == 1 && !isRangeView

	colNamesJoined := strings.Join(colNames, "_")
	if viewPartCol != nil {
		if keepsBasePart {
			colNames = append([]string{viewPartCol.GetName()}, colNames...)
			colNamesJoined = "pk_" + colNamesJoined
		} else {
			// The override leads the view key; it must not be repeated as a clustering column.
			partColName := viewPartCol.GetName()
			if isRangeView && slices.Contains(colNames, partColName) {
				panic(fmt.Sprintf(`Table "%v": the Partition column "%v" cannot also be a packed view key`,
					dbTable.Name, partColName))
			}
			clusteringColNames := make([]string, 0, len(colNames))
			for _, colName := range colNames {
				if colName != partColName {
					clusteringColNames = append(clusteringColNames, colName)
				}
			}
			colNames = append([]string{partColName}, clusteringColNames...)
			colNamesJoined = strings.Join(colNames, "_")
		}
	}
	if isRangeView {
		colNamesJoined = colNamesJoined + "_rng"
	}

	view := &viewInfo{
		Type:          6,
		name:          fmt.Sprintf(`%v__%v_view`, dbTable.Name, colNamesJoined),
		columns:       colNames,
		columnsNoPart: colNamesNoPart,
	}

	if len(columns) > 1 {
		view.column = &columnInfo{
			ColInfo: colInfo{
				IsVirtual: true,
				Idx:       dbTable.MaxColIdx,
			},
			ColType: colType{
				FieldType: "int32", DBType: "int",
			},
		}
		view.column.GetInfo().Name = fmt.Sprintf(`zz_%v`, colNamesJoined)
		dbTable.MaxColIdx++
		dbTable.ColumnsMap[view.column.GetName()] = view.column
	}

	if isSingleDeclaredSimpleView {
		view.column = declaredColumns[0]
		view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
			// Simple MVs keep their source columns, so predicates bind without key rewriting
			// and the generic batcher applies as-is: an IN on the view's clustering key is
			// split into several queries rather than one the node would reject.
			return buildRemainingWhereClauseBatches(statements)
		}
	} else if len(columns) == 1 {
		view.column = columns[0]
	} else if isRangeView {
		view.Type = 8
		view.column.GetType().FieldType = "int64"
		view.column.GetType().DBType = "bigint"

		if len(columns) < 2 {
			panic(fmt.Sprintf(`The view "%v" in "%v" requires at least 2 columns for DecimalSize() packed range views`, view.name, dbTable.Name))
		}

		isInt32PackedView := false
		slotDigitsPerColumn := make([]int64, 0, len(viewColumnsConfig))

		if slotPlan != nil {
			// TypeDelta resolved every slot from FixedValues, so the DecimalSize() rules below do not
			// apply: the leading column carries a real width instead of absorbing a digit remainder.
			if len(slotPlan.slotDigitsPerColumn) != len(columns) {
				panic(fmt.Sprintf(`The view "%v" in "%v" got %v slot widths for %v columns`,
					view.name, dbTable.Name, len(slotPlan.slotDigitsPerColumn), len(columns)))
			}
			isInt32PackedView = slotPlan.useInt32
			slotDigitsPerColumn = append(slotDigitsPerColumn, slotPlan.slotDigitsPerColumn...)
		} else {
			if viewColumnsConfig[0].DecimalDigits > 0 {
				panic(fmt.Sprintf(`The view "%v" in "%v" cannot set DecimalSize() on the first column; it is inferred from the remaining columns`, view.name, dbTable.Name))
			}

			isInt32PackedView = viewColumnsConfig[0].UseInt32Packing

			radixSlotsByColumn := make([]int8, 0, len(viewColumnsConfig)-1)
			for columnIndex := 1; columnIndex < len(viewColumnsConfig); columnIndex++ {
				DecimalDigits := viewColumnsConfig[columnIndex].DecimalDigits
				if DecimalDigits <= 0 {
					panic(fmt.Sprintf(`The view "%v" in "%v" must set DecimalSize() on column "%v" (only the first column can be inferred)`,
						view.name, dbTable.Name, columns[columnIndex].GetName()))
				}
				radixSlotsByColumn = append(radixSlotsByColumn, DecimalDigits)
			}

			radixes := append(radixSlotsByColumn, 0)
			slices.Reverse(radixes)
			sum := int8(0)
			for i, v := range radixes {
				radixes[i] = v + sum
				sum += v
			}
			slices.Reverse(radixes)
			if radixes[0] > 17 {
				panic(fmt.Sprintf(`For view "%v" in "%v" the max radix must not be greater than 17.`, view.name, dbTable.Name))
			}

			totalDigitsForPackedView := int64(19)
			if isInt32PackedView {
				totalDigitsForPackedView = 9
			}
			sumTrailingDigits := int64(0)
			for _, DecimalDigits := range radixSlotsByColumn {
				sumTrailingDigits += int64(DecimalDigits)
			}
			slotDigitsPerColumn = append(slotDigitsPerColumn, totalDigitsForPackedView-sumTrailingDigits)
			for _, DecimalDigits := range radixSlotsByColumn {
				slotDigitsPerColumn = append(slotDigitsPerColumn, int64(DecimalDigits))
			}
		}

		if isInt32PackedView {
			view.column.GetType().FieldType = "int32"
			view.column.GetType().DBType = "int"
		}
		view.packedSourceColumns = append([]IColInfo{}, columns...)
		view.packedSlotDigitsPerColumn = append([]int64{}, slotDigitsPerColumn...)

		supportedTypes := []string{"int8", "int16", "int32", "int64", "int"}
		for _, col := range columns {
			if col.GetType().IsSlice || !slices.Contains(supportedTypes, col.GetType().FieldType) {
				panic(fmt.Sprintf(`For view "%v" in "%v" need the column %v need to be a int type for the radix value be computed.`,
					view.name, dbTable.Name, col.GetName()))
			}
		}

		makeValue := func(values []int64) int64 {
			return computePackedInt64ValueNonNegative(values, slotDigitsPerColumn)
		}

		// A scan that spans a whole slot width computes an exclusive upper bound one past the end of
		// that slot, which can land outside what the packed column physically holds — 10^10 for an
		// int, or a wrapped negative once Pow10Int64 passes 18 digits. Cap it at one past the highest
		// value this layout can actually produce; that is both in range and tighter.
		packedValueCeiling := Pow10Int64(min(sumSlotDigits(slotDigitsPerColumn, 0), 18)) - 1
		if slotPlan != nil {
			packedValueCeiling = slotPlan.maxPackedValue
		}
		clampPackedUpperBound := func(upperBound int64) int64 {
			if upperBound <= 0 || upperBound > packedValueCeiling {
				return packedValueCeiling + 1
			}
			return upperBound
		}

		slotDigitsCopy := append([]int64{}, slotDigitsPerColumn...)
		viewColsCopy := append([]IColInfo{}, columns...)
		view.decomposeVirtualValue = func(rawValue any) []any {
			packedValues := decomposePackedInt64ValueNonNegative(convertToInt64(rawValue), slotDigitsCopy)
			values := make([]any, 0, len(viewColsCopy))
			for _, packedValue := range packedValues {
				values = append(values, packedValue)
			}
			return values
		}

		viewCols := columns
		useInt32Output := isInt32PackedView
		// A slot plan sizes the packed column from declared FixedValues, so a row written outside
		// those ranges would overflow the key and silently corrupt the view. Fail loudly instead.
		maxPackedValue := int64(0)
		if slotPlan != nil {
			maxPackedValue = slotPlan.maxPackedValue
		}
		viewNameForGuard, tableNameForGuard := view.name, dbTable.Name
		view.column.(*columnInfo).GetValueFn = func(ptr unsafe.Pointer) any {
			values := []int64{}
			for _, col := range viewCols {
				values = append(values, convertToInt64(col.GetValue(ptr)))
			}
			sumValue := makeValue(values)
			if maxPackedValue > 0 && sumValue > maxPackedValue {
				panic(fmt.Sprintf(`Table "%v": view "%v" packed to %v, past the %v its declared FixedValues allow. Column values: %v`,
					tableNameForGuard, viewNameForGuard, sumValue, maxPackedValue, values))
			}
			if useInt32Output {
				return any(int32(sumValue))
			}
			return any(sumValue)
		}

		viewPtr := view
		viewPartColPtr := viewPartCol
		view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
			statementsMap := map[string]statementRangeGroup{}
			useBeetween := false

			for i := range statements {
				st := &statements[i]
				if st.Operator == "BETWEEN" {
					useBeetween = true
					for i := range st.From {
						statementsMap[st.From[i].Col] = statementRangeGroup{
							from:      &st.From[i],
							betweenTo: &st.To[i],
						}
					}
				} else {
					statementsMap[st.Col] = statementRangeGroup{from: st}
				}
			}

			var partStatement *ColumnStatement
			if viewPartColPtr != nil {
				partStatement = statementsMap[viewPartColPtr.GetName()].from
			}

			// A packed key is prefix-searchable: the leading columns pinned by equality/IN select a
			// contiguous block, and everything from the first unconstrained or range-filtered column
			// onwards spans that block's full width. Absent columns therefore count as range columns,
			// not as an equality on zero — that distinction is what lets one packed view answer
			// "Status = 1" as well as the full key.
			getValuesGroups := func() ([][]int64, []IColInfo) {
				valuesGroups := [][]int64{}
				rangeColumns := []IColInfo{}
				for _, col := range viewCols {
					if viewPartColPtr != nil && col.GetName() == viewPartColPtr.GetName() {
						continue
					}
					stRange, hasStatement := statementsMap[col.GetName()]
					isUnconstrained := !hasStatement || stRange.from == nil
					if len(rangeColumns) > 0 || isUnconstrained || slices.Contains(rangeOperators, stRange.from.Operator) {
						rangeColumns = append(rangeColumns, col)
						continue
					}
					st := stRange.from

					valuesToAdd := []int64{}
					if len(st.Values) > 0 {
						for _, value := range st.Values {
							valuesToAdd = append(valuesToAdd, convertToInt64(value))
						}
					} else {
						valuesToAdd = append(valuesToAdd, convertToInt64(st.Value))
					}

					if len(valuesGroups) > 0 {
						valuesGroupsCurrent := valuesGroups
						valuesGroups = [][]int64{}
						for _, vg := range valuesGroupsCurrent {
							for _, value := range valuesToAdd {
								valuesGroups = append(valuesGroups, append(append([]int64{}, vg...), value))
							}
						}
					} else {
						for _, value := range valuesToAdd {
							valuesGroups = append(valuesGroups, []int64{value})
						}
					}
				}
				return valuesGroups, rangeColumns
			}

			// Only the first range column can carry a bound; a filter on anything after it cannot be
			// expressed as one packed range and must not be silently dropped. Returning nil tells the
			// planner this view cannot serve the query.
			hasUnservableGap := func(rangeColumns []IColInfo) bool {
				for _, col := range rangeColumns[1:] {
					if stRange, hasStatement := statementsMap[col.GetName()]; hasStatement && stRange.from != nil {
						fmt.Printf("Packed view rejected: view=%s cannot bind a filter on \"%s\" behind an unconstrained column\n",
							viewPtr.name, col.GetName())
						return true
					}
				}
				return false
			}

			whereStatements := []boundWhereClause{}
			valuesGroups, rangeColumns := getValuesGroups()
			if len(rangeColumns) > 1 && hasUnservableGap(rangeColumns) {
				return nil
			}

			if useBeetween {
				valuesFrom, valuesTo := []int64{}, []int64{}
				for _, col := range viewCols {
					srg, hasStatement := statementsMap[col.GetName()]
					// An unconstrained column spans its whole slot, so it floors on the low bound and
					// tops out on the high one.
					if !hasStatement || srg.from == nil {
						valuesFrom = append(valuesFrom, 0)
						valuesTo = append(valuesTo, Pow10Int64(slotDigitsPerColumn[len(valuesTo)])-1)
						continue
					}
					valuesFrom = append(valuesFrom, convertToInt64(srg.from.Value))
					if srg.betweenTo != nil {
						valuesTo = append(valuesTo, convertToInt64(srg.betweenTo.Value))
					} else {
						valuesTo = append(valuesTo, convertToInt64(srg.from.Value))
					}
				}
				whereStatement := boundWhereClause{
					Clause: fmt.Sprintf("%v >= ? AND %v < ?", viewPtr.column.GetName(), viewPtr.column.GetName()),
					Values: []any{makeValue(valuesFrom), clampPackedUpperBound(makeValue(valuesTo) + 1)},
				}
				if partStatement != nil {
					whereStatement = boundWhereClause{
						Clause: fmt.Sprintf("%v = ? AND %v", viewPartColPtr.GetName(), whereStatement.Clause),
						Values: append([]any{convertToInt64(partStatement.Value)}, whereStatement.Values...),
					}
				}
				return []boundWhereClause{whereStatement}
			} else if len(rangeColumns) > 0 {
				if len(valuesGroups) == 0 {
					// Nothing is pinned by equality, so the scan starts at the packed column's floor.
					valuesGroups = [][]int64{{}}
				}
				for _, prefixValues := range valuesGroups {
					valuesFrom := slices.Clone(prefixValues)
					prefixFloorValues := slices.Clone(prefixValues)

					for _, col := range rangeColumns {
						// Only the first range column can carry a lower bound; the rest span their slot.
						rangeFrom := int64(0)
						if stRange, hasStatement := statementsMap[col.GetName()]; hasStatement && stRange.from != nil {
							rangeFrom = convertToInt64(stRange.from.Value)
						}
						valuesFrom = append(valuesFrom, rangeFrom)
						prefixFloorValues = append(prefixFloorValues, 0)
					}

					upperBound := clampPackedUpperBound(makeValue(prefixFloorValues) + Pow10Int64(sumSlotDigits(slotDigitsPerColumn, len(prefixValues))))
					whereStatements = append(whereStatements, boundWhereClause{
						Clause: fmt.Sprintf("%v >= ? AND %v < ?", viewPtr.column.GetName(), viewPtr.column.GetName()),
						Values: []any{makeValue(valuesFrom), upperBound},
					})
				}
			} else {
				hashValues := make([]any, 0, len(valuesGroups))
				for _, values := range valuesGroups {
					hashValues = append(hashValues, makeValue(values))
				}
				whereStatements = append(whereStatements,
					buildChunkedInWhereClauses(viewPtr.column.GetName(), hashValues)...)
			}

			if partStatement != nil {
				for i, ws := range whereStatements {
					whereStatements[i] = boundWhereClause{
						Clause: fmt.Sprintf("%v = ? AND %v", viewPartColPtr.GetName(), ws.Clause),
						Values: append([]any{convertToInt64(partStatement.Value)}, ws.Values...),
					}
				}
			}
			return whereStatements
		}
	} else {
		viewPtr := view
		viewPartColPtr := viewPartCol
		viewCols := columns
		view.Operators = []string{"=", "IN"}
		view.Type = 7
		view.column.(*columnInfo).GetValueFn = func(ptr unsafe.Pointer) any {
			values := []any{}
			for _, e := range viewCols {
				values = append(values, e.GetValue(ptr))
			}
			return HashInt(values...)
		}

		view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
			valuesGroups := [][]any{{}}
			for _, e := range viewCols {
				for _, st := range statements {
					if st.Col == e.GetName() {
						if len(st.Values) >= 2 {
							valuesGroupsCurrent := valuesGroups
							valuesGroups = [][]any{}
							for _, vg := range valuesGroupsCurrent {
								for _, value := range st.Values {
									valuesGroups = append(valuesGroups, append(vg, value))
								}
							}
						} else {
							if len(st.Values) == 1 {
								st.Value = st.Values[0]
							}
							for i := range valuesGroups {
								valuesGroups[i] = append(valuesGroups[i], st.Value)
							}
						}
						break
					}
				}
			}

			hashValues := make([]any, 0, len(valuesGroups))
			for _, values := range valuesGroups {
				hashValues = append(hashValues, HashInt(values...))
			}

			whereStatements := buildChunkedInWhereClauses(viewPtr.column.GetName(), hashValues)

			if viewPartColPtr != nil {
				for _, st := range statements {
					if st.Col == viewPartColPtr.GetName() {
						for i, whereStatement := range whereStatements {
							whereStatements[i] = boundWhereClause{
								Clause: fmt.Sprintf("%v = ? AND %v", st.Col, whereStatement.Clause),
								Values: append([]any{st.Value}, whereStatement.Values...),
							}
						}
						break
					}
				}
			}
			return whereStatements
		}
	}

	projectedColumnsConfig := viewCfg.Cols
	projectedColumns := []IColInfo{}
	for _, declaredProjectedColumn := range projectedColumnsConfig {
		projectedColumn := dbTable.ColumnsMap[declaredProjectedColumn.GetInfo().Name]
		if projectedColumn == nil || projectedColumn.IsNil() {
			panic(fmt.Sprintf(`The projected column "%v" for view "%v" in "%v" wasn't found.`,
				declaredProjectedColumn.GetInfo().Name, view.name, dbTable.Name))
		}
		if projectedColumn.GetInfo().IsVirtual {
			panic(fmt.Sprintf(`The projected column "%v" for view "%v" in "%v" cannot be virtual.`,
				projectedColumn.GetName(), view.name, dbTable.Name))
		}
		projectedColumns = appendUniqueColumn(projectedColumns, projectedColumn)
	}

	selectableColumns := []IColInfo{}
	if len(projectedColumns) == 0 {
		for _, baseColumn := range dbTable.ColumnsMap {
			if baseColumn.GetInfo().IsVirtual {
				continue
			}
			selectableColumns = appendUniqueColumn(selectableColumns, baseColumn)
		}
	} else {
		selectableColumns = appendUniqueColumn(selectableColumns, basePartCol)
		// A relocated partition column must always travel with the projected view.
		selectableColumns = appendUniqueColumn(selectableColumns, viewPartCol)
		if view.Type == 6 {
			for _, declaredViewColumn := range declaredColumns {
				selectableColumns = appendUniqueColumn(selectableColumns, declaredViewColumn)
			}
		}
		for _, keyColumn := range dbTable.Keys {
			selectableColumns = appendUniqueColumn(selectableColumns, keyColumn)
		}
		if view.column != nil && !view.column.IsNil() && !view.column.GetInfo().IsVirtual {
			selectableColumns = appendUniqueColumn(selectableColumns, view.column)
		}
		for _, projectedColumn := range projectedColumns {
			selectableColumns = appendUniqueColumn(selectableColumns, projectedColumn)
		}
	}
	for _, selectableColumn := range selectableColumns {
		view.availableColumns = append(view.availableColumns, selectableColumn.GetName())
	}

	viewPtr := view
	// The CREATE script and deploy's column-drift check must agree on what the view contains, so
	// both read the layout from here instead of each deriving it.
	resolveShape := func() materializedViewShape {
		whereCols := []IColInfo{}
		if viewPtr.Type == 6 && !viewPtr.column.GetInfo().IsVirtual {
			for _, declaredViewColumn := range declaredColumns {
				whereCols = appendUniqueColumn(whereCols, declaredViewColumn)
			}
		} else {
			whereCols = appendUniqueColumn(whereCols, viewPtr.column)
		}
		wherePartCol := viewPartCol
		if !keepsBasePart {
			// The base partition becomes a clustering column of the relocated view.
			whereCols = appendUniqueColumn(whereCols, basePartCol)
		}
		for _, keyColumn := range dbTable.Keys {
			whereCols = appendUniqueColumn(whereCols, keyColumn)
		}
		if wherePartCol != nil {
			whereCols = slices.DeleteFunc(whereCols, func(column IColInfo) bool {
				return column.GetName() == wherePartCol.GetName()
			})
		}

		shape := materializedViewShape{}
		if wherePartCol != nil {
			shape.primaryKeyColumns = append(shape.primaryKeyColumns, wherePartCol)
		}
		shape.primaryKeyColumns = append(shape.primaryKeyColumns, whereCols...)

		keyNames := []string{}
		for _, col := range whereCols {
			keyNames = append(keyNames, col.GetName())
		}

		shape.primaryKeyClause = strings.Join(keyNames, ",")
		if wherePartCol != nil {
			shape.primaryKeyClause = fmt.Sprintf("(%v), %v", wherePartCol.GetName(), shape.primaryKeyClause)
		}

		for _, col := range shape.primaryKeyColumns {
			shape.notNullClauses = append(shape.notNullClauses, col.GetName()+" IS NOT NULL")
		}

		if len(projectedColumns) > 0 {
			shape.selectColumns = slices.Clone(projectedColumns)
			return shape
		}
		selectColumns := slices.Clone(selectableColumns)
		for _, whereColumn := range whereCols {
			if whereColumn != nil && !whereColumn.IsNil() && whereColumn.GetInfo().IsVirtual {
				selectColumns = appendUniqueColumn(selectColumns, whereColumn)
			}
		}
		shape.selectColumns = orderColumnsBySchemaIndex(selectColumns)
		return shape
	}

	view.getExpectedColumns = func() []viewExpectedColumn {
		shape := resolveShape()
		// A materialized view stores its selected columns plus its own primary key, and a projected
		// view names only the projection in SELECT — so the key columns have to be added back here.
		return makeViewExpectedColumns(append(slices.Clone(shape.selectColumns), shape.primaryKeyColumns...))
	}

	view.getCreateScript = func() string {
		shape := resolveShape()
		selectColumnNames := make([]string, 0, len(shape.selectColumns))
		for _, selectColumn := range shape.selectColumns {
			selectColumnNames = append(selectColumnNames, selectColumn.GetName())
		}

		return fmt.Sprintf(`CREATE MATERIALIZED VIEW %v.%v AS
			SELECT %v FROM %v
			WHERE %v
			PRIMARY KEY (%v)
			%v;`,
			dbTable.Namespace, viewPtr.name, strings.Join(selectColumnNames, ", "), dbTable.GetFullName(),
			strings.Join(shape.notNullClauses, " AND "), shape.primaryKeyClause, makeStatementWith)
	}

	dbTable.views[view.name] = view
}

// materializedViewShape is the column layout of one compiled materialized view, derived once and
// consumed both by the CREATE script and by the expected-column set deploy diffs against the DB.
type materializedViewShape struct {
	selectColumns     []IColInfo
	primaryKeyColumns []IColInfo
	primaryKeyClause  string
	notNullClauses    []string
}

// makeViewExpectedColumns flattens columns to name/type pairs, dropping repeats: a projected view's
// key column appears in both halves of the union.
func makeViewExpectedColumns(columns []IColInfo) []viewExpectedColumn {
	expectedColumns := make([]viewExpectedColumn, 0, len(columns))
	namesSeen := map[string]bool{}
	for _, column := range columns {
		if column == nil || column.IsNil() || namesSeen[column.GetName()] {
			continue
		}
		namesSeen[column.GetName()] = true
		expectedColumns = append(expectedColumns, viewExpectedColumn{
			name:   column.GetName(),
			dbType: column.GetType().DBType,
		})
	}
	return expectedColumns
}
