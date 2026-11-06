package db

// Describe phase of the two-phase table compile: each function here turns one schema index
// declaration into an IndexSource (plus any generated column it needs) and appends it to
// TableCore.IndexSources. Nothing here knows how a database stores an index; a driver
// materializes every IndexSource afterwards (the Scylla driver adds CREATE scripts and WHERE
// rendering, the FDB driver writes index entries).

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"
)

// addIndexSource registers a described index and advances the shared index counter.
func (core *TableCore) addIndexSource(idxCount *int8, source *IndexSource) *IndexSource {
	source.Idx = *idxCount
	*idxCount = *idxCount + 1
	core.IndexSources = append(core.IndexSources, source)
	return source
}

// DescribeLocalIndex describes a TypeLocalIndex: one column indexed within the partition, or a
// packed index when it names several. nil when an identical single-column index already exists.
func (core *TableCore) DescribeLocalIndex(idxCount *int8, indexCfg Index) *IndexSource {
	if len(indexCfg.Keys) > 1 {
		return core.describePackedIndex(idxCount, indexCfg.Keys, true)
	}
	column := core.indexedColumn(indexCfg.Keys[0])
	// Scylla has no local index on a collection, and declarations must hold on every driver.
	if column.GetType().IsSlice {
		panic(fmt.Sprintf(`Table "%v": column "%v" is a slice and Scylla has no local index on a collection; use TypeGlobalIndex, or a TypeCacheGroup of two or more Keys`,
			core.Name, column.GetName()))
	}
	indexName := fmt.Sprintf(`%v__%v_index_1`, core.Name, column.GetName())
	if core.IndexSourceNamed(indexName) != nil {
		return nil
	}
	return core.addIndexSource(idxCount, &IndexSource{
		Kind:    IndexKindSingleColumn,
		Type:    2,
		Name:    indexName,
		Column:  column,
		Columns: []string{core.GetPartKey().GetName(), column.GetName()},
	})
}

// DescribeGlobalIndex describes a TypeGlobalIndex: one column (a slice column indexes each
// element), or a packed index when it names several. nil when it already exists.
func (core *TableCore) DescribeGlobalIndex(idxCount *int8, indexCfg Index) *IndexSource {
	if len(indexCfg.Keys) > 1 {
		return core.describePackedIndex(idxCount, indexCfg.Keys, false)
	}
	column := core.indexedColumn(indexCfg.Keys[0])
	indexName := fmt.Sprintf(`%v__%v_index_0`, core.Name, column.GetName())
	if core.IndexSourceNamed(indexName) != nil {
		return nil
	}
	return core.addIndexSource(idxCount, &IndexSource{
		Kind:    IndexKindSingleColumn,
		Type:    1,
		Name:    indexName,
		Column:  column,
		Columns: []string{column.GetName()},
	})
}

func (core *TableCore) indexedColumn(declared Coln) IColInfo {
	columnName := declared.GetInfo().Name
	column := core.ColumnsMap[columnName]
	if column == nil {
		panic(fmt.Sprintf(`Table "%v": Indexes column "%v" was not found`, core.Name, columnName))
	}
	return column
}

func IsSupportedPackedIndexNumericFieldType(fieldType string) bool {
	switch fieldType {
	case "int8", "int16", "int32", "int64", "int":
		return true
	default:
		return false
	}
}

// describePackedIndex builds a generated column packing several integer columns plus an index on
// it. Rationale: this enables composite predicates like "Status IN (...) + Updated BETWEEN/GT".
// The generated column's raw value is the UNSIGNED packed layout (uint64); a driver whose integers
// compare signed re-maps it when it materializes the index.
func (core *TableCore) describePackedIndex(idxCount *int8, indexColumns []Coln, isLocal bool) *IndexSource {
	const schemaFieldName = "Indexes"
	sourceColumns := make([]IColInfo, 0, len(indexColumns))
	sourceColumnNames := make([]string, 0, len(indexColumns))
	slotBitsPerColumn := make([]int64, 0, len(indexColumns))
	isInt32Packed := false

	for columnIndex, indexColumnConfig := range indexColumns {
		configInfo := indexColumnConfig.GetInfo()
		column := core.ColumnsMap[configInfo.Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": %v column "%v" was not found`, core.Name, schemaFieldName, configInfo.Name))
		}
		if column.GetType().IsComplexType || column.GetType().IsSlice || !IsSupportedPackedIndexNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": %v packed column "%v" must be a scalar integer. Found: %v`, core.Name, schemaFieldName, column.GetName(), column.GetType().FieldType))
		}
		if configInfo.UseInt32Packing {
			isInt32Packed = true
		}
		// Size rules: the first component takes the bits the others leave, so it must not set
		// Size(); every later component must.
		if columnIndex == 0 && configInfo.SlotBits > 0 {
			panic(fmt.Sprintf(`Table "%v": %v first column "%v" must not set Size()`, core.Name, schemaFieldName, configInfo.Name))
		}
		if columnIndex > 0 && configInfo.SlotBits <= 0 {
			panic(fmt.Sprintf(`Table "%v": %v requires Size() for column "%v" (all columns after the first must set Size)`, core.Name, schemaFieldName, configInfo.Name))
		}
		sourceColumns = append(sourceColumns, column)
		sourceColumnNames = append(sourceColumnNames, column.GetName())
		slotBitsPerColumn = append(slotBitsPerColumn, int64(configInfo.SlotBits)) // first slot set after the budget calc
	}

	budgetBits := PackedVirtualBudgetBits(isInt32Packed)
	firstSlotBits := budgetBits - SumSlotBits(slotBitsPerColumn, 1)
	if firstSlotBits <= 0 {
		panic(fmt.Sprintf(`Table "%v": %v trailing Size() slots take all %v bits, leaving none for "%v"`,
			core.Name, schemaFieldName, budgetBits, sourceColumnNames[0]))
	}
	slotBitsPerColumn[0] = firstSlotBits

	virtualColumnPrefix := "zz_gixp_"
	if isLocal {
		virtualColumnPrefix = "zz_ixp_"
	}
	virtualPackedColName := virtualColumnPrefix + strings.Join(sourceColumnNames, "_")
	if _, exists := core.ColumnsMap[virtualPackedColName]; exists {
		panic(fmt.Sprintf(`Table "%v": generated packed column already exists: %v`, core.Name, virtualPackedColName))
	}
	packedColumnTypeName := "int64"
	if isInt32Packed {
		packedColumnTypeName = "int32"
	}

	tableName := core.Name
	virtualPackedColumn := &ColumnInfo{
		ColInfo: ColInfo{
			Name:      virtualPackedColName,
			FieldName: virtualPackedColName,
			IsVirtual: true,
			Idx:       core.MaxColIdx,
		},
		ColType: GetColTypeByName(packedColumnTypeName),
	}
	virtualPackedColumn.GetRawValueFn = func(ptr unsafe.Pointer) any {
		componentValues := make([]int64, 0, len(sourceColumns))
		for _, sourceColumn := range sourceColumns {
			componentValues = append(componentValues, ConvertToInt64(sourceColumn.GetRawValue(ptr)))
		}
		return PackRowValues(tableName, virtualPackedColName, sourceColumns, componentValues, slotBitsPerColumn)
	}
	virtualPackedColumn.GetValueFn = virtualPackedColumn.GetRawValueFn
	core.MaxColIdx++
	core.ColumnsMap[virtualPackedColumn.GetName()] = virtualPackedColumn

	indexType, indexNameSuffix, partitionColumnName := int8(1), "index_0", ""
	if isLocal {
		partitionColumn := core.GetPartKey()
		if partitionColumn == nil || partitionColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": %v requires a Partition column`, core.Name, schemaFieldName))
		}
		indexType, indexNameSuffix, partitionColumnName = 2, "index_1", partitionColumn.GetName()
	}
	indexName := fmt.Sprintf(`%v__%v_%v`, core.Name, virtualPackedColName, indexNameSuffix)
	if core.IndexSourceNamed(indexName) != nil {
		panic(fmt.Sprintf(`Table "%v": %v index name already exists: %v`, core.Name, schemaFieldName, indexName))
	}

	packedIndex := &PackedIndexInfo{
		IndexName:           indexName,
		PackedColumnName:    virtualPackedColName,
		SourceColumnNames:   slices.Clone(sourceColumnNames),
		PartitionColumnName: partitionColumnName,
		SlotBitsPerColumn:   slices.Clone(slotBitsPerColumn),
		IsInt32Packed:       isInt32Packed,
	}
	core.PackedIndexes = append(core.PackedIndexes, packedIndex)

	fmt.Printf("Packed index registered: table=%s local=%v index=%s packedCol=%s isInt32=%v slotBits=%v\n",
		core.Name, isLocal, indexName, virtualPackedColName, isInt32Packed, slotBitsPerColumn)
	return core.addIndexSource(idxCount, &IndexSource{
		Kind:               IndexKindPacked,
		Type:               indexType,
		Name:               indexName,
		Column:             virtualPackedColumn,
		Columns:            slices.Clone(sourceColumnNames),
		RequiresPostFilter: true,
		PackedIndex:        packedIndex,
	})
}
