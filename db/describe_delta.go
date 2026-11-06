package db

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db/cache"
)

// IsIntegerFieldType reports whether a Go field type is one of the integer widths a delta key,
// partition or ID may have.
func IsIntegerFieldType(fieldType string) bool {
	return fieldType == "int16" || fieldType == "int32" || fieldType == "int64"
}

// DeltaStoredColumns are the columns a delta index copies: every stored column of the record, in
// schema order. Delta() and the planner read whole records from the index.
func (core *TableCore) DeltaStoredColumns() []IColInfo {
	storedColumns := []IColInfo{}
	for _, column := range core.ColumnsMap {
		if !column.GetInfo().IsVirtual && !column.GetInfo().IsComputed {
			storedColumns = append(storedColumns, column)
		}
	}
	slices.SortFunc(storedColumns, func(left, right IColInfo) int { return int(left.GetInfo().Idx - right.GetInfo().Idx) })
	return storedColumns
}

// deltaIndexColumns are the columns one delta index's view copies. Without declared Cols that is
// every stored column. With Cols it is the declared ones plus what the protocol itself reads: the
// partition and key (the row's identity), version and updated (the window hash and the cursor)
// and the pinned columns (the client's filter). Every row of a write pays for each copied column,
// so a ledger whose delta consumer reads a few columns declares only those.
func (core *TableCore) deltaIndexColumns(indexCfg Index, pinned []IColInfo) []IColInfo {
	if len(indexCfg.Cols) == 0 {
		return core.DeltaStoredColumns()
	}
	wanted := map[string]bool{
		core.GetPartKey().GetName(): true, core.Keys[0].GetName(): true,
		core.VersionCol.GetName(): true, core.UpdatedCol.GetName(): true,
	}
	for _, pinnedColumn := range pinned {
		wanted[pinnedColumn.GetName()] = true
	}
	for _, declaredColumn := range indexCfg.Cols {
		column := core.ColumnsMap[declaredColumn.GetInfo().Name]
		if column == nil || column.IsNil() || column.GetInfo().IsVirtual || column.GetInfo().IsComputed {
			panic(fmt.Sprintf(`Table "%v": TypeDelta Cols column "%v" is not a stored column`, core.Name, declaredColumn.GetInfo().Name))
		}
		wanted[column.GetName()] = true
	}
	return slices.DeleteFunc(core.DeltaStoredColumns(), func(column IColInfo) bool { return !wanted[column.GetName()] })
}

// DescribeDelta describes a TypeDelta index and registers it in DeltaIndexes. Every declared Key is
// pinned, the last one being the one Delta()'s values name; the table's single integer key is the
// record ID. The packed column holds the pinned values and updated (updated alone on a keyless
// index); its value function returns the shared unsigned layout (DeltaIndexInfo.PackedKey), which a
// driver re-encodes for its own storage. The IndexSource is a range view (Type 8) on
// [partition, pinned…, updated], so an equality on the pinned values is one span of the packed
// column and an updated range narrows it.
func (core *TableCore) DescribeDelta(indexCfg Index) *IndexSource {
	if core.VersionCol == nil || core.VersionCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column; declare Version in the record and table structs`,
			core.Name, ColumnNameVersion))
	}
	if core.UpdatedCol == nil || core.UpdatedCol.IsNil() {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires the managed "%v" column`, core.Name, ColumnNameUpdated))
	}
	if partitionColumn := core.GetPartKey(); partitionColumn == nil || partitionColumn.IsNil() ||
		!IsIntegerFieldType(partitionColumn.GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires an integer partition column`, core.Name))
	}
	if len(core.Keys) != 1 || !IsIntegerFieldType(core.Keys[0].GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": TypeDelta requires exactly one integer key column, the record ID. A table keyed otherwise answers its route with DeltaReplaceMetadata() instead`,
			core.Name))
	}

	replaceThreshold := indexCfg.DeltaReplaceThreshold
	if replaceThreshold <= 0 {
		replaceThreshold = cache.DefaultDeltaReplaceThreshold
	}
	deltaIndex := DeltaIndexInfo{Tag: "all", ReplaceThreshold: replaceThreshold}

	declaredColumns := make([]IColInfo, 0, len(indexCfg.Keys))
	declaredKeyNames := make([]string, 0, len(indexCfg.Keys))
	for _, declaredKey := range indexCfg.Keys {
		column := core.ColumnsMap[declaredKey.GetInfo().Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta column "%v" was not found`, core.Name, declaredKey.GetInfo().Name))
		}
		if column.GetType().IsComplexType || column.GetType().IsSlice ||
			!IsSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" must be a scalar integer. Found: %v`,
				core.Name, column.GetName(), column.GetType().FieldType))
		}
		if column.GetName() == core.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": TypeDelta key "%v" is the table's key, the record ID`, core.Name, column.GetName()))
		}
		declaredColumns = append(declaredColumns, column)
		declaredKeyNames = append(declaredKeyNames, column.GetName())
	}
	if len(declaredColumns) > 0 {
		deltaIndex.Tag = strings.Join(declaredKeyNames, "_")
		deltaIndex.Pinned = declaredColumns
	}
	deltaIndex.ViewName = fmt.Sprintf("%v__dlt_%v_view", core.Name, deltaIndex.Tag)
	for _, registered := range core.DeltaIndexes {
		if registered.ViewName == deltaIndex.ViewName {
			panic(fmt.Sprintf(`Table "%v": two TypeDelta indexes declare the same Keys (%v)`, core.Name, deltaIndex.Tag))
		}
	}

	deltaIndex.StoredColumns = core.deltaIndexColumns(indexCfg, deltaIndex.Pinned)

	// A keyless index packs updated alone: every delta index keys on one packed column.
	deltaIndex.PinnedSlotBits = core.PlanDeltaPinnedSlots(deltaIndex.Pinned, indexCfg)
	packedColumnSuffix := ColumnNameUpdated
	if len(deltaIndex.Pinned) > 0 {
		packedColumnSuffix = deltaIndex.Tag
	}
	packedColumn := &ColumnInfo{
		ColInfo: ColInfo{IsVirtual: true, Idx: core.MaxColIdx, Name: "zz_dlt_" + packedColumnSuffix},
		ColType: GetColTypeByName("int64"),
	}
	core.MaxColIdx++
	core.ColumnsMap[packedColumn.GetName()] = packedColumn
	deltaIndex.PackedColumn = packedColumn

	// An insert carries every pinned value on the record, so this is exact there. An update binds the
	// value the write path computed from the stored pinned values instead.
	tableName, updatedColumn, compiledIndex := core.Name, core.UpdatedCol, deltaIndex
	packedColumn.GetValueFn = func(recordPointer unsafe.Pointer) any {
		pinnedValues := make([]int64, len(compiledIndex.Pinned))
		for i, pinnedColumn := range compiledIndex.Pinned {
			pinnedValues[i] = ConvertToInt64(pinnedColumn.GetValue(recordPointer))
		}
		return compiledIndex.PackedKey(tableName, pinnedValues, ConvertToInt64(updatedColumn.GetValue(recordPointer)))
	}
	core.DeltaIndexes = append(core.DeltaIndexes, deltaIndex)

	sourceColumns := append(slices.Clone(deltaIndex.Pinned), core.UpdatedCol)
	sourceNames := make([]string, len(sourceColumns))
	for i, sourceColumn := range sourceColumns {
		sourceNames[i] = sourceColumn.GetName()
	}
	source := &IndexSource{
		Kind:                    IndexKindDelta,
		Type:                    8,
		IsDeltaView:             true,
		Name:                    deltaIndex.ViewName,
		Column:                  packedColumn,
		Columns:                 append([]string{core.GetPartKey().GetName()}, sourceNames...),
		ColumnsNoPart:           sourceNames,
		PackedSourceColumns:     sourceColumns,
		PackedSlotBitsPerColumn: deltaIndex.SlotBits(),
		// A delta view is always keyed on the base partition.
		PartitionColumn:    core.GetPartKey(),
		KeepsBasePartition: true,
	}
	for _, storedColumn := range deltaIndex.StoredColumns {
		source.AvailableColumns = append(source.AvailableColumns, storedColumn.GetName())
	}
	core.IndexSources = append(core.IndexSources, source)

	if ShouldLog() {
		fmt.Printf("Delta index registered: table=%s view=%s pinnedSlotBits=%v\n", core.Name, deltaIndex.ViewName, deltaIndex.PinnedSlotBits)
	}
	return source
}
