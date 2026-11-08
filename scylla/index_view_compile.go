package scylla

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// materializeViewTable attaches the CQL of a view table: the columns deploy expects and the CREATE
// TABLE script. Everything it needs is on the IndexSource; reads are planned by db.PlanSourceSteps.
func materializeViewTable(dbTable *ScyllaTable, view *viewInfo) {
	partKey := dbTable.GetPartKey()
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

// materializeView attaches the CQL of a described view: the encoding of its virtual column and the
// CREATE MATERIALIZED VIEW script. Reads are planned by db.PlanSourceSteps.
func materializeView(dbTable *ScyllaTable, view *viewInfo) {
	if view.Type == 8 {
		// The described value is the unsigned layout; Scylla compares integers signed, so it is
		// stored with the top bit flipped (storeVirtualPacked).
		packedColumn := view.Column.(*columnInfo)
		describedValue, isInt32 := packedColumn.GetValueFn, view.PackedIsInt32
		packedColumn.GetValueFn = func(ptr unsafe.Pointer) any {
			return storeVirtualPacked(describedValue(ptr).(uint64), isInt32)
		}
		compilePackedRangeDecoder(view, view.PackedSourceColumns, view.PackedSlotBitsPerColumn, isInt32)
	}
	materializeViewCreateScript(dbTable, view)
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

// compilePackedRangeDecoder gives a packed range view (Type 8, delta views included) the decoding
// of its scanned packed column back into the source column values.
func compilePackedRangeDecoder(view *viewInfo, viewCols []IColInfo, slotBitsPerColumn []int64, isInt32PackedView bool) {
	viewColsCopy := append([]IColInfo{}, viewCols...)
	view.decomposeVirtualValue = func(rawValue any) []any {
		packed := loadVirtualPacked(db.ConvertToInt64(rawValue), isInt32PackedView)
		values := make([]any, 0, len(viewColsCopy))
		for _, slotValue := range db.UnpackSlotValues(packed, slotBitsPerColumn) {
			values = append(values, slotValue)
		}
		return values
	}
}
