package db

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"
)

// DescribeIndexGroup describes a cache group (TypeCacheGroup, TypeCacheGroupS3, TypeCacheGroupFromKey)
// and registers it in IndexGroups. It returns the index a driver must materialize for the group, or
// nil when the group needs none: a TypeCacheGroupFromKey group reads primary-key ranges, and a
// one-key group reuses a local index that may already exist.
//
// Several keys, or a slice key, are served through a generated hash column. Its value functions
// return the neutral value (an int32 hash, or the []int32 hashes of a slice key); a driver encodes
// the collection for its own statements when it materializes the index.
func (core *TableCore) DescribeIndexGroup(idxCount *int8, indexCfg Index) *IndexSource {
	sourceColumnNames := make([]string, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		sourceColumnNames = append(sourceColumnNames, key.GetName())
	}
	indexID := core.AllocateIndexGroupID(sourceColumnNames)
	if len(indexCfg.Cols) > 0 || indexCfg.UseHash {
		panic(fmt.Sprintf(`Table "%v": a cache group on %v takes Keys only (no Cols, no UseHash)`, core.Name, sourceColumnNames))
	}

	if indexCfg.Type == TypeCacheGroupFromKey {
		core.describeInheritFromKeyIndexGroup(indexCfg, indexID, sourceColumnNames)
		return nil
	}

	if len(indexCfg.Keys) == 1 {
		localIndexSource := core.DescribeLocalIndex(idxCount, Index{Type: TypeLocalIndex, Keys: indexCfg.Keys})
		core.IndexGroups = append(core.IndexGroups, IndexGroupInfo{
			Name:          indexCfg.Keys[0].GetName(),
			IndexID:       indexID,
			SourceColumns: []IndexGroupSourceColumn{{Column: core.ColumnsMap[indexCfg.Keys[0].GetName()]}},
			SnapshotInS3:  indexCfg.Type == TypeCacheGroupS3,
		})
		return localIndexSource
	}

	sourceColumns := make([]IndexGroupSourceColumn, 0, len(indexCfg.Keys))
	usesCollectionValues := false
	for _, key := range indexCfg.Keys {
		baseColumn := core.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": IndexGroup column "%v" was not found`, core.Name, key.GetName()))
		}
		sourceColumns = append(sourceColumns, IndexGroupSourceColumn{Column: baseColumn})
		if baseColumn.GetType().IsSlice {
			usesCollectionValues = true
		}
	}

	virtualColumnName := MakeIndexGroupVirtualColumnName(sourceColumnNames, usesCollectionValues)
	if _, exists := core.ColumnsMap[virtualColumnName]; exists {
		panic(fmt.Sprintf(`Table "%v": generated IndexGroup column already exists: %v`, core.Name, virtualColumnName))
	}

	virtualColumn := &ColumnInfo{
		ColInfo: ColInfo{Name: virtualColumnName, FieldName: virtualColumnName, IsVirtual: true, Idx: core.MaxColIdx},
		ColType: GetColTypeByName("int32"),
	}
	if usesCollectionValues {
		virtualColumn.ColType = GetColTypeByName("[]int32")
	}

	sourceColumnsLocal := slices.Clone(sourceColumns)
	groupHashValue := func(ptr unsafe.Pointer) any {
		hashes := ComputeIndexGroupHashes(ptr, sourceColumnsLocal)
		if usesCollectionValues {
			return hashes
		}
		if len(hashes) == 0 {
			return int32(0)
		}
		return hashes[0]
	}
	virtualColumn.GetRawValueFn = groupHashValue
	virtualColumn.GetStatementValueFn = groupHashValue
	virtualColumn.GetValueFn = groupHashValue
	core.MaxColIdx++
	core.ColumnsMap[virtualColumn.GetName()] = virtualColumn

	// Every grouped read pins the partition, so a scalar hash is indexed locally. A slice key's
	// hashes are a collection: that index is global, and the suffix names the kind so a change
	// recreates it.
	indexName := fmt.Sprintf(`%v__%v_index_1`, core.Name, virtualColumnName)
	if usesCollectionValues {
		indexName = fmt.Sprintf(`%v__%v_index_0`, core.Name, virtualColumnName)
	}
	indexGroup := IndexGroupInfo{
		Name:                 strings.Join(sourceColumnNames, "_"),
		IndexID:              indexID,
		SourceColumns:        sourceColumns,
		VirtualColumn:        virtualColumn,
		UsesCollectionValues: usesCollectionValues,
		SnapshotInS3:         indexCfg.Type == TypeCacheGroupS3,
	}
	core.IndexGroups = append(core.IndexGroups, indexGroup)
	return core.addIndexSource(idxCount, &IndexSource{
		Kind:       IndexKindGroupHash,
		Type:       3,
		Name:       indexName,
		Column:     virtualColumn,
		Columns:    slices.Clone(sourceColumnNames),
		IndexGroup: &indexGroup,
	})
}

// describeInheritFromKeyIndexGroup registers a TypeCacheGroupFromKey group: its keys must be a prefix
// of KeyIntPacking, so a group is one primary-key range.
func (core *TableCore) describeInheritFromKeyIndexGroup(indexCfg Index, indexID int16, sourceColumnNames []string) {
	if len(core.KeyIntPacking) == 0 {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey requires KeyIntPacking on the table`, core.Name))
	}
	if len(indexCfg.Keys) > len(core.KeyIntPacking) {
		panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey index has %d keys but KeyIntPacking only declares %d columns`,
			core.Name, len(indexCfg.Keys), len(core.KeyIntPacking)))
	}
	sourceColumns := make([]IndexGroupSourceColumn, 0, len(indexCfg.Keys))
	for columnIndex, key := range indexCfg.Keys {
		packedColumnName := core.KeyIntPacking[columnIndex].GetName()
		if key.GetName() != packedColumnName {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey keys must be a prefix of KeyIntPacking. Position %d expected "%v" but got "%v"`,
				core.Name, columnIndex, packedColumnName, key.GetName()))
		}
		baseColumn := core.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey column "%v" was not found`, core.Name, key.GetName()))
		}
		if baseColumn.GetType().IsSlice {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupFromKey does not support slice column "%v"`, core.Name, key.GetName()))
		}
		sourceColumns = append(sourceColumns, IndexGroupSourceColumn{Column: baseColumn})
	}
	core.IndexGroups = append(core.IndexGroups, IndexGroupInfo{
		Name:           strings.Join(sourceColumnNames, "_"),
		IndexID:        indexID,
		SourceColumns:  sourceColumns,
		InheritFromKey: true,
	})
}
