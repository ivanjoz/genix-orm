package db

import (
	"fmt"
	"slices"
	"strings"
)

// IndexPartitionColumnName returns the name of the partition override declared on an index, or ""
// when the index keeps the base table partition.
func IndexPartitionColumnName(indexCfg Index) string {
	if indexCfg.Partition == nil {
		return ""
	}
	return indexCfg.Partition.GetInfo().Name
}

// ResolveIndexPartitionColumn resolves the Partition override against the base table. Returns nil
// when no override is declared, meaning the index keeps the base partition.
func (core *TableCore) ResolveIndexPartitionColumn(indexCfg Index) IColInfo {
	partitionColumnName := IndexPartitionColumnName(indexCfg)
	if partitionColumnName == "" {
		return nil
	}
	column := core.ColumnsMap[partitionColumnName]
	if column == nil || column.IsNil() {
		panic(fmt.Sprintf(`Table "%v": Partition column "%v" was not found`, core.Name, partitionColumnName))
	}
	if column.GetInfo().IsVirtual || column.GetType().IsComplexType || column.GetType().IsSlice {
		panic(fmt.Sprintf(`Table "%v": Partition column "%v" cannot be virtual, a slice or a struct`, core.Name, column.GetName()))
	}
	return column
}

// AppendUniqueViewTableColumn adds a view-table column unless one already reads from the same source
// column: physical columns are derived from source columns, so source-name uniqueness is enough.
func AppendUniqueViewTableColumn(target []ViewTableColumnInfo, column ViewTableColumnInfo) []ViewTableColumnInfo {
	for _, existingColumn := range target {
		if existingColumn.SourceColumn.GetName() == column.SourceColumn.GetName() {
			return target
		}
	}
	return append(target, column)
}

// MakeViewTableColumn keeps only the source column reference plus the fan-out flag; the name and the
// storage type are derived on demand by each driver.
func MakeViewTableColumn(sourceColumn IColInfo, useSliceElement bool) ViewTableColumnInfo {
	return ViewTableColumnInfo{SourceColumn: sourceColumn, UsesSliceElement: useSliceElement}
}

// DescribeViewTable describes a TypeViewTable: a table of its own keyed on (partition, keys…, id),
// one row per element when a key is a slice. The driver materializes it however it stores a table.
func (core *TableCore) DescribeViewTable(viewCfg Index) *IndexSource {
	if IndexPartitionColumnName(viewCfg) != "" {
		panic(fmt.Sprintf(`Table "%v": ViewTables always keep the base partition; remove Partition`, core.Name))
	}
	if viewCfg.UseHash {
		panic(fmt.Sprintf(`Table "%v": ViewTables does not support UseHash`, core.Name))
	}
	if len(viewCfg.Keys) == 0 {
		panic(fmt.Sprintf(`Table "%v": ViewTables entry must declare at least one key column`, core.Name))
	}
	if len(core.Keys) != 1 {
		panic(fmt.Sprintf(`Table "%v": ViewTables currently requires exactly one base key column for ID maintenance`, core.Name))
	}

	partKey := core.GetPartKey()
	if partKey == nil || partKey.IsNil() {
		panic(fmt.Sprintf(`Table "%v": ViewTables requires a partition column`, core.Name))
	}

	declaredColumns := []IColInfo{}
	keyColumnNames := []string{}
	physicalColumns := []ViewTableColumnInfo{MakeViewTableColumn(partKey, false)}
	physicalKeyColumns := []ViewTableColumnInfo{}
	rebuildColumnNames := map[string]bool{}
	fanoutColumnName := ""
	sliceKeyCount := 0

	for _, declaredColumn := range viewCfg.Keys {
		column := core.ColumnsMap[declaredColumn.GetInfo().Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": ViewTables column "%v" was not found`, core.Name, declaredColumn.GetInfo().Name))
		}
		if column.GetType().IsComplexType {
			panic(fmt.Sprintf(`Table "%v": ViewTables column "%v" cannot be a complex type`, core.Name, column.GetName()))
		}
		if column.GetInfo().Name == core.Keys[0].GetName() {
			panic(fmt.Sprintf(`Table "%v": ViewTables key "%v" must not repeat the base ID column`, core.Name, column.GetName()))
		}

		useSliceElement := column.GetType().IsSlice
		if useSliceElement {
			sliceKeyCount++
			fanoutColumnName = column.GetName()
		}

		keyColumnNames = append(keyColumnNames, column.GetName())
		declaredColumns = append(declaredColumns, column)
		rebuildColumnNames[column.GetName()] = true

		physicalColumn := MakeViewTableColumn(column, useSliceElement)
		physicalColumns = AppendUniqueViewTableColumn(physicalColumns, physicalColumn)
		physicalKeyColumns = append(physicalKeyColumns, physicalColumn)
	}

	if sliceKeyCount > 1 {
		panic(fmt.Sprintf(`Table "%v": ViewTables currently supports only one slice-backed key column`, core.Name))
	}

	idColumn := core.Keys[0]
	physicalColumns = AppendUniqueViewTableColumn(physicalColumns, MakeViewTableColumn(idColumn, false))

	projectedColumns := []IColInfo{}
	if len(viewCfg.Cols) == 0 {
		for _, baseColumn := range core.ColumnsMap {
			if baseColumn.GetInfo().IsVirtual || baseColumn.GetName() == fanoutColumnName {
				continue
			}
			projectedColumns = append(projectedColumns, baseColumn)
		}
	} else {
		for _, declaredProjectedColumn := range viewCfg.Cols {
			projectedColumn := core.ColumnsMap[declaredProjectedColumn.GetInfo().Name]
			if projectedColumn == nil || projectedColumn.IsNil() {
				panic(fmt.Sprintf(`Table "%v": ViewTables projected column "%v" wasn't found`, core.Name, declaredProjectedColumn.GetInfo().Name))
			}
			if projectedColumn.GetInfo().IsVirtual {
				panic(fmt.Sprintf(`Table "%v": ViewTables projected column "%v" cannot be virtual`, core.Name, projectedColumn.GetName()))
			}
			if projectedColumn.GetName() == fanoutColumnName {
				continue
			}
			projectedColumns = append(projectedColumns, projectedColumn)
		}
	}

	for _, projectedColumn := range projectedColumns {
		physicalColumns = AppendUniqueViewTableColumn(physicalColumns, MakeViewTableColumn(projectedColumn, false))
		rebuildColumnNames[projectedColumn.GetName()] = true
	}

	source := &IndexSource{
		Type:             9,
		Name:             fmt.Sprintf(`%v__%v_view`, core.Name, strings.Join(keyColumnNames, "_")),
		Columns:          append([]string{partKey.GetName()}, keyColumnNames...),
		ColumnsNoPart:    append([]string{}, keyColumnNames...),
		Column:           declaredColumns[0],
		AvailableColumns: []string{},
		Operators:        []string{"=", "IN", "CONTAINS"},
		FanoutColumnName: fanoutColumnName,
		TableColumns:     physicalColumns,
		TableKeyColumns:  physicalKeyColumns,
		// MaintenanceIDColumn and RebuildColumnNames let a driver keep the table in step with the base row.
		MaintenanceIDColumn: idColumn,
		RebuildColumnNames:  rebuildColumnNames,
	}

	selectableColumnNames := map[string]bool{partKey.GetName(): true, idColumn.GetName(): true}
	for _, declaredColumn := range declaredColumns {
		if declaredColumn.GetName() != fanoutColumnName {
			selectableColumnNames[declaredColumn.GetName()] = true
		}
	}
	for _, projectedColumn := range projectedColumns {
		if projectedColumn.GetName() != fanoutColumnName {
			selectableColumnNames[projectedColumn.GetName()] = true
		}
	}
	for selectableColumnName := range selectableColumnNames {
		source.AvailableColumns = append(source.AvailableColumns, selectableColumnName)
	}
	slices.Sort(source.AvailableColumns)

	core.IndexSources = append(core.IndexSources, source)
	return source
}
