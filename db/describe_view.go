package db

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"
)

// AppendUniqueColumn adds a column unless one with the same name is already there; nil columns are skipped.
func AppendUniqueColumn(target []IColInfo, column IColInfo) []IColInfo {
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

// OrderColumnsBySchemaIndex returns the columns in declaration order, ties broken by name, so every
// driver lays a view's columns out the same way.
func OrderColumnsBySchemaIndex(columns []IColInfo) []IColInfo {
	orderedColumns := slices.Clone(columns)
	slices.SortFunc(orderedColumns, func(leftColumn, rightColumn IColInfo) int {
		if idxDiff := int(leftColumn.GetInfo().Idx - rightColumn.GetInfo().Idx); idxDiff != 0 {
			return idxDiff
		}
		return strings.Compare(leftColumn.GetName(), rightColumn.GetName())
	})
	return orderedColumns
}

// DescribeView describes a TypeView: a simple view (Type 6, one key), a hash view (Type 7, several
// keys hashed into one virtual column) or a packed range view (Type 8, several integer keys packed
// into one). A packed view takes its bit layout from .Size() hints, or from FixedValues when no key
// has a hint. A virtual column's value function returns the neutral layout (the unsigned packed
// value, the int32 hash); a driver re-encodes it for its own storage when it materializes the view.
func (core *TableCore) DescribeView(viewCfg Index) *IndexSource {
	declaredColumns := []IColInfo{}
	viewColumnsConfig := make([]ColumnInfo, 0, len(viewCfg.Keys))
	packedViewHintFound := false
	for _, declaredColumn := range viewCfg.Keys {
		columnConfig := declaredColumn.GetInfo()
		viewColumnsConfig = append(viewColumnsConfig, columnConfig)
		if columnConfig.SlotBits > 0 || columnConfig.UseInt32Packing {
			packedViewHintFound = true
		}
	}

	fixedValueSlots := core.PlanFixedValueViewSlots(viewCfg)
	isRangeView := len(viewCfg.Keys) > 1 && (packedViewHintFound || fixedValueSlots != nil)

	// Views keep the base table partition unless the schema declares another column.
	basePartCol := core.GetPartKey()
	if basePartCol != nil && basePartCol.IsNil() {
		basePartCol = nil
	}
	viewPartCol := core.ResolveIndexPartitionColumn(viewCfg)
	keepsBasePart := viewPartCol == nil ||
		(basePartCol != nil && viewPartCol.GetName() == basePartCol.GetName())
	if viewPartCol == nil {
		viewPartCol = basePartCol
	}

	colNames := []string{}
	for _, colInfo := range viewCfg.Keys {
		column := core.ColumnsMap[colInfo.GetInfo().Name]
		if column.GetType().IsComplexType {
			panic("No puede usar un struct como columna de una view.")
		}
		colNames = append(colNames, column.GetName())
		declaredColumns = append(declaredColumns, column)
	}
	columns := slices.Clone(declaredColumns)

	colNamesNoPart := colNames
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
					core.Name, partColName))
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

	source := &IndexSource{
		Kind:               IndexKindView,
		Type:               6,
		Name:               fmt.Sprintf(`%v__%v_view`, core.Name, colNamesJoined),
		Columns:            colNames,
		ColumnsNoPart:      colNamesNoPart,
		DeclaredColumns:    declaredColumns,
		PartitionColumn:    viewPartCol,
		KeepsBasePartition: keepsBasePart,
	}

	// Several keys are served through one generated column: a hash, or a packed range.
	var virtualColumn *ColumnInfo
	if len(columns) > 1 {
		virtualColumn = &ColumnInfo{
			ColInfo: ColInfo{IsVirtual: true, Idx: core.MaxColIdx, Name: fmt.Sprintf(`zz_%v`, colNamesJoined)},
			ColType: GetColTypeByName("int32"),
		}
		core.MaxColIdx++
		core.ColumnsMap[virtualColumn.GetName()] = virtualColumn
		source.Column = virtualColumn
	}

	switch {
	case len(columns) == 1:
		source.Column = declaredColumns[0]
	case isRangeView:
		describePackedRangeView(core, source, virtualColumn, columns, viewColumnsConfig, fixedValueSlots)
	default:
		source.Operators = []string{"=", "IN"}
		source.Type = 7
		virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
			values := []any{}
			for _, column := range columns {
				values = append(values, column.GetValue(ptr))
			}
			return HashInt(values...)
		}
	}

	describeViewColumns(core, source, viewCfg, basePartCol)
	core.IndexSources = append(core.IndexSources, source)
	return source
}

// describePackedRangeView settles the bit layout of a packed range view (Type 8) and gives its
// virtual column the neutral (unsigned) packed value.
func describePackedRangeView(core *TableCore, source *IndexSource, virtualColumn *ColumnInfo, columns []IColInfo, viewColumnsConfig []ColumnInfo, fixedValueSlots *FixedValueSlotPlan) {
	source.Type = 8
	virtualColumn.ColType = GetColTypeByName("int64")

	isInt32PackedView := false
	slotBitsPerColumn := make([]int64, 0, len(viewColumnsConfig))

	if fixedValueSlots != nil {
		// Every slot comes from FixedValues: the leading column carries a real width instead of
		// absorbing the bit remainder.
		isInt32PackedView = fixedValueSlots.UseInt32
		slotBitsPerColumn = append(slotBitsPerColumn, fixedValueSlots.SlotBitsPerColumn...)
	} else {
		if viewColumnsConfig[0].SlotBits > 0 {
			panic(fmt.Sprintf(`The view "%v" in "%v" cannot set Size() on the first column; it takes the bits the remaining columns leave`, source.Name, core.Name))
		}

		isInt32PackedView = viewColumnsConfig[0].UseInt32Packing
		slotBitsPerColumn = append(slotBitsPerColumn, 0) // the first slot is set once the trailing ones are summed
		for columnIndex := 1; columnIndex < len(viewColumnsConfig); columnIndex++ {
			slotBits := int64(viewColumnsConfig[columnIndex].SlotBits)
			if slotBits <= 0 {
				panic(fmt.Sprintf(`The view "%v" in "%v" must set Size() on column "%v" (only the first column can be inferred)`,
					source.Name, core.Name, columns[columnIndex].GetName()))
			}
			slotBitsPerColumn = append(slotBitsPerColumn, slotBits)
		}

		budgetBits := PackedVirtualBudgetBits(isInt32PackedView)
		slotBitsPerColumn[0] = budgetBits - SumSlotBits(slotBitsPerColumn, 1)
		if slotBitsPerColumn[0] <= 0 {
			panic(fmt.Sprintf(`The view "%v" in "%v": the trailing Size() slots take all %v bits, leaving none for "%v"`,
				source.Name, core.Name, budgetBits, columns[0].GetName()))
		}
	}

	if isInt32PackedView {
		virtualColumn.ColType = GetColTypeByName("int32")
	}
	source.PackedSourceColumns = slices.Clone(columns)
	source.PackedSlotBitsPerColumn = slices.Clone(slotBitsPerColumn)
	source.PackedIsInt32 = isInt32PackedView

	supportedTypes := []string{"int8", "int16", "int32", "int64", "int"}
	for _, column := range columns {
		if column.GetType().IsSlice || !slices.Contains(supportedTypes, column.GetType().FieldType) {
			panic(fmt.Sprintf(`For view "%v" in "%v" need the column %v need to be a int type for the radix value be computed.`,
				source.Name, core.Name, column.GetName()))
		}
	}

	viewName, tableName := source.Name, core.Name
	virtualColumn.GetValueFn = func(ptr unsafe.Pointer) any {
		values := []int64{}
		for _, column := range columns {
			values = append(values, ConvertToInt64(column.GetValue(ptr)))
		}
		return PackRowValues(tableName, viewName, columns, values, slotBitsPerColumn)
	}
}

// describeViewColumns settles which real columns a view serves: the projection, or every stored
// column when none is declared.
func describeViewColumns(core *TableCore, source *IndexSource, viewCfg Index, basePartCol IColInfo) {
	projectedColumns := []IColInfo{}
	for _, declaredProjectedColumn := range viewCfg.Cols {
		projectedColumn := core.ColumnsMap[declaredProjectedColumn.GetInfo().Name]
		if projectedColumn == nil || projectedColumn.IsNil() {
			panic(fmt.Sprintf(`The projected column "%v" for view "%v" in "%v" wasn't found.`,
				declaredProjectedColumn.GetInfo().Name, source.Name, core.Name))
		}
		if projectedColumn.GetInfo().IsVirtual {
			panic(fmt.Sprintf(`The projected column "%v" for view "%v" in "%v" cannot be virtual.`,
				projectedColumn.GetName(), source.Name, core.Name))
		}
		projectedColumns = AppendUniqueColumn(projectedColumns, projectedColumn)
	}

	selectableColumns := []IColInfo{}
	if len(projectedColumns) == 0 {
		for _, baseColumn := range core.ColumnsMap {
			if baseColumn.GetInfo().IsVirtual {
				continue
			}
			selectableColumns = AppendUniqueColumn(selectableColumns, baseColumn)
		}
	} else {
		selectableColumns = AppendUniqueColumn(selectableColumns, basePartCol)
		// A relocated partition column must always travel with the projected view.
		selectableColumns = AppendUniqueColumn(selectableColumns, source.PartitionColumn)
		if source.Type == 6 {
			for _, declaredViewColumn := range source.DeclaredColumns {
				selectableColumns = AppendUniqueColumn(selectableColumns, declaredViewColumn)
			}
		}
		for _, keyColumn := range core.Keys {
			selectableColumns = AppendUniqueColumn(selectableColumns, keyColumn)
		}
		if source.Column != nil && !source.Column.IsNil() && !source.Column.GetInfo().IsVirtual {
			selectableColumns = AppendUniqueColumn(selectableColumns, source.Column)
		}
		for _, projectedColumn := range projectedColumns {
			selectableColumns = AppendUniqueColumn(selectableColumns, projectedColumn)
		}
	}
	for _, selectableColumn := range selectableColumns {
		source.AvailableColumns = append(source.AvailableColumns, selectableColumn.GetName())
	}
	source.ProjectedColumns = projectedColumns
	source.SelectableColumns = selectableColumns
}
