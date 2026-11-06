package scylla

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// compileSchemaViewTable describes a TypeViewTable in db and gives it the CQL that creates and
// queries its table.
func compileSchemaViewTable(dbTable *ScyllaTable, viewCfg Index) {
	view := &viewInfo{IndexSource: dbTable.DescribeViewTable(viewCfg)}
	materializeViewTable(dbTable, view)
	dbTable.views[view.Name] = view
}

// materializeViewTable attaches the CQL of a view table: its WHERE rendering, the columns deploy
// expects and the CREATE TABLE script. Everything it needs is on the IndexSource.
func materializeViewTable(dbTable *ScyllaTable, view *viewInfo) {
	partKey := dbTable.GetPartKey()
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
			if view.FanoutColumnName == statement.Col && operator == "CONTAINS" {
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
		expectedColumns := make([]viewExpectedColumn, 0, len(view.TableColumns))
		for _, column := range view.TableColumns {
			expectedColumns = append(expectedColumns, viewExpectedColumn{
				name:   getViewTableColumnName(column),
				dbType: getViewTableColumnType(column.SourceColumn, column.UsesSliceElement).DBType,
			})
		}
		return expectedColumns
	}
	view.getCreateScript = func() string {
		columnDefinitions := make([]string, 0, len(view.TableColumns))
		for _, column := range view.getExpectedColumns() {
			columnDefinitions = append(columnDefinitions, fmt.Sprintf("%v %v", column.name, column.dbType))
		}

		primaryKeyColumns := append([]string{}, view.ColumnsNoPart...)
		primaryKeyColumns = append(primaryKeyColumns, view.MaintenanceIDColumn.GetName())
		return fmt.Sprintf(`CREATE TABLE %v.%v (
			%v,
			PRIMARY KEY ((%v), %v)
		)
		%v;`,
			dbTable.Namespace,
			view.Name,
			strings.Join(columnDefinitions, ", "),
			partKey.GetName(),
			strings.Join(primaryKeyColumns, ", "),
			makeStatementWith,
		)
	}
}

// compileSchemaView describes a TypeView in db and gives it the materialized view that serves it.
func compileSchemaView(dbTable *ScyllaTable, viewCfg Index) {
	view := &viewInfo{IndexSource: dbTable.DescribeView(viewCfg)}
	materializeView(dbTable, view)
	dbTable.views[view.Name] = view
}

// materializeView attaches the CQL of a described view: the encoding of its virtual column, its
// WHERE rendering and the CREATE MATERIALIZED VIEW script.
func materializeView(dbTable *ScyllaTable, view *viewInfo) {
	switch view.Type {
	case 6:
		// Simple MVs keep their source columns, so predicates bind without key rewriting and the
		// generic batcher applies as-is: an IN on the view's clustering key is split into several
		// queries rather than one the node would reject.
		view.getStatementPrepared = func(statements ...ColumnStatement) []boundWhereClause {
			return buildRemainingWhereClauseBatches(statements)
		}
	case 7:
		materializeHashView(view)
	case 8:
		// The described value is the unsigned layout; Scylla compares integers signed, so it is
		// stored with the top bit flipped (storeVirtualPacked).
		packedColumn := view.Column.(*columnInfo)
		describedValue, isInt32 := packedColumn.GetValueFn, view.PackedIsInt32
		packedColumn.GetValueFn = func(ptr unsafe.Pointer) any {
			return storeVirtualPacked(describedValue(ptr).(uint64), isInt32)
		}
		compilePackedRangeAccessors(view, view.PackedSourceColumns, view.PackedSlotBitsPerColumn, isInt32, view.PartitionColumn)
	}
	materializeViewCreateScript(dbTable, view)
}

// materializeHashView binds the equality / IN predicates of a hash view to its hash column.
func materializeHashView(view *viewInfo) {
	viewCols := view.DeclaredColumns
	viewPartCol := view.PartitionColumn
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
			hashValues = append(hashValues, db.HashInt(values...))
		}

		whereStatements := buildChunkedInWhereClauses(view.Column.GetName(), hashValues)

		if viewPartCol != nil {
			for _, st := range statements {
				if st.Col == viewPartCol.GetName() {
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

// materializeViewCreateScript attaches the CREATE MATERIALIZED VIEW script and the expected-column
// set. Both read the layout from resolveShape, so the script and deploy's drift check agree.
func materializeViewCreateScript(dbTable *ScyllaTable, view *viewInfo) {
	basePartCol := dbTable.GetPartKey()
	if basePartCol != nil && basePartCol.IsNil() {
		basePartCol = nil
	}
	viewPartCol, keepsBasePart := view.PartitionColumn, view.KeepsBasePartition
	declaredColumns, projectedColumns, selectableColumns := view.DeclaredColumns, view.ProjectedColumns, view.SelectableColumns

	resolveShape := func() materializedViewShape {
		whereCols := []IColInfo{}
		if view.Type == 6 && !view.Column.GetInfo().IsVirtual {
			for _, declaredViewColumn := range declaredColumns {
				whereCols = db.AppendUniqueColumn(whereCols, declaredViewColumn)
			}
		} else {
			whereCols = db.AppendUniqueColumn(whereCols, view.Column)
		}
		wherePartCol := viewPartCol
		if !keepsBasePart {
			// The base partition becomes a clustering column of the relocated view.
			whereCols = db.AppendUniqueColumn(whereCols, basePartCol)
		}
		for _, keyColumn := range dbTable.Keys {
			whereCols = db.AppendUniqueColumn(whereCols, keyColumn)
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
				selectColumns = db.AppendUniqueColumn(selectColumns, whereColumn)
			}
		}
		shape.selectColumns = db.OrderColumnsBySchemaIndex(selectColumns)
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
			dbTable.Namespace, view.Name, strings.Join(selectColumnNames, ", "), dbTable.GetFullName(),
			strings.Join(shape.notNullClauses, " AND "), shape.primaryKeyClause, makeStatementWith)
	}
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

// compilePackedRangeAccessors gives a packed range view (Type 8) its query binding and the decoding of
// its packed column: the planner reads a TypeView and a TypeDelta view through the same code.
func compilePackedRangeAccessors(view *viewInfo, viewCols []IColInfo, slotBitsPerColumn []int64, isInt32PackedView bool, viewPartCol IColInfo) {
	// Query bounds are packed in the unsigned layout and stored-encoded like any written value.
	// Upper bounds are inclusive (every trailing slot at its max), so none can overflow the layout.
	makeValue := func(values []int64) any {
		return storeVirtualPacked(db.PackSlotValues(values, slotBitsPerColumn), isInt32PackedView)
	}
	makePrefixUpperBound := func(prefixValues []int64) any {
		return storeVirtualPacked(db.PackSlotPrefixBound(prefixValues, slotBitsPerColumn, true), isInt32PackedView)
	}

	viewColsCopy := append([]IColInfo{}, viewCols...)
	view.decomposeVirtualValue = func(rawValue any) []any {
		packed := loadVirtualPacked(db.ConvertToInt64(rawValue), isInt32PackedView)
		values := make([]any, 0, len(viewColsCopy))
		for _, slotValue := range db.UnpackSlotValues(packed, slotBitsPerColumn) {
			values = append(values, slotValue)
		}
		return values
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
						valuesToAdd = append(valuesToAdd, db.ConvertToInt64(value))
					}
				} else {
					valuesToAdd = append(valuesToAdd, db.ConvertToInt64(st.Value))
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
						viewPtr.Name, col.GetName())
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
					valuesTo = append(valuesTo, int64(db.SlotMaxValue(slotBitsPerColumn[len(valuesTo)])))
					continue
				}
				valuesFrom = append(valuesFrom, db.ConvertToInt64(srg.from.Value))
				if srg.betweenTo != nil {
					valuesTo = append(valuesTo, db.ConvertToInt64(srg.betweenTo.Value))
				} else {
					valuesTo = append(valuesTo, db.ConvertToInt64(srg.from.Value))
				}
			}
			whereStatement := boundWhereClause{
				Clause: fmt.Sprintf("%v >= ? AND %v <= ?", viewPtr.Column.GetName(), viewPtr.Column.GetName()),
				Values: []any{makeValue(valuesFrom), makeValue(valuesTo)},
			}
			if partStatement != nil {
				whereStatement = boundWhereClause{
					Clause: fmt.Sprintf("%v = ? AND %v", viewPartColPtr.GetName(), whereStatement.Clause),
					Values: append([]any{db.ConvertToInt64(partStatement.Value)}, whereStatement.Values...),
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
				for _, col := range rangeColumns {
					// Only the first range column can carry a lower bound; the rest span their slot.
					rangeFrom := int64(0)
					if stRange, hasStatement := statementsMap[col.GetName()]; hasStatement && stRange.from != nil {
						rangeFrom = db.ConvertToInt64(stRange.from.Value)
					}
					valuesFrom = append(valuesFrom, rangeFrom)
				}

				whereStatements = append(whereStatements, boundWhereClause{
					Clause: fmt.Sprintf("%v >= ? AND %v <= ?", viewPtr.Column.GetName(), viewPtr.Column.GetName()),
					Values: []any{makeValue(valuesFrom), makePrefixUpperBound(prefixValues)},
				})
			}
		} else {
			hashValues := make([]any, 0, len(valuesGroups))
			for _, values := range valuesGroups {
				hashValues = append(hashValues, makeValue(values))
			}
			whereStatements = append(whereStatements,
				buildChunkedInWhereClauses(viewPtr.Column.GetName(), hashValues)...)
		}

		if partStatement != nil {
			for i, ws := range whereStatements {
				whereStatements[i] = boundWhereClause{
					Clause: fmt.Sprintf("%v = ? AND %v", viewPartColPtr.GetName(), ws.Clause),
					Values: append([]any{db.ConvertToInt64(partStatement.Value)}, ws.Values...),
				}
			}
		}
		return whereStatements
	}
}
