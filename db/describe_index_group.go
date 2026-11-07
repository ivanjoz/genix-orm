package db

import (
	"fmt"
	"math/bits"
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
	if indexCfg.Type == TypeCacheGroupNoIndex {
		core.describeNoIndexGroup(indexCfg, indexID, sourceColumnNames)
		return nil
	}

	if len(indexCfg.Keys) == 1 {
		localIndexSource := core.DescribeLocalIndex(idxCount, Index{Type: TypeLocalIndex, Keys: indexCfg.Keys})
		core.IndexGroups = append(core.IndexGroups, IndexGroupInfo{
			Name:          indexCfg.Keys[0].GetName(),
			IndexID:       indexID,
			SourceColumns: []IndexGroupSourceColumn{{Column: core.ColumnsMap[indexCfg.Keys[0].GetName()]}},
			SnapshotInS3:  indexCfg.Type == TypeCacheGroupS3,
			PackBits:      core.indexGroupPackBits(indexCfg.Keys),
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
		PackBits:             core.indexGroupPackBits(indexCfg.Keys),
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

// describeNoIndexGroup registers a TypeCacheGroupNoIndex group. Its BaseGroup is resolved once every
// index is described (resolveNoIndexBaseGroups), so the FromKey group it reads may be declared after it.
func (core *TableCore) describeNoIndexGroup(indexCfg Index, indexID int16, sourceColumnNames []string) {
	sourceColumns := make([]IndexGroupSourceColumn, 0, len(indexCfg.Keys))
	for _, key := range indexCfg.Keys {
		baseColumn := core.ColumnsMap[key.GetName()]
		if baseColumn == nil || baseColumn.IsNil() {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupNoIndex column "%v" was not found`, core.Name, key.GetName()))
		}
		if baseColumn.GetType().IsSlice {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupNoIndex does not support slice column "%v"`, core.Name, key.GetName()))
		}
		sourceColumns = append(sourceColumns, IndexGroupSourceColumn{Column: baseColumn})
	}
	core.IndexGroups = append(core.IndexGroups, IndexGroupInfo{
		Name:          strings.Join(sourceColumnNames, "_"),
		IndexID:       indexID,
		SourceColumns: sourceColumns,
		PackBits:      core.indexGroupPackBits(indexCfg.Keys),
		// Placeholder until resolveNoIndexBaseGroups names the real one: it marks the group as no-index.
		BaseGroup: &IndexGroupInfo{},
	})
}

// resolveNoIndexBaseGroups gives every TypeCacheGroupNoIndex group its base: the FromKey group with
// the most Keys, all of them among the group's own Keys. The base must leave at least one Key to
// filter, otherwise the group would be the base group under another IndexID.
func (core *TableCore) resolveNoIndexBaseGroups() {
	for groupIndex := range core.IndexGroups {
		noIndexGroup := &core.IndexGroups[groupIndex]
		if noIndexGroup.BaseGroup == nil {
			continue
		}
		groupColumnNames := map[string]bool{}
		for _, sourceColumn := range noIndexGroup.SourceColumns {
			groupColumnNames[sourceColumn.Column.GetName()] = true
		}
		var baseGroup *IndexGroupInfo
		for candidateIndex := range core.IndexGroups {
			candidate := core.IndexGroups[candidateIndex]
			isContained := candidate.InheritFromKey && len(candidate.SourceColumns) < len(noIndexGroup.SourceColumns) &&
				!slices.ContainsFunc(candidate.SourceColumns, func(sourceColumn IndexGroupSourceColumn) bool {
					return !groupColumnNames[sourceColumn.Column.GetName()]
				})
			if isContained && (baseGroup == nil || len(candidate.SourceColumns) > len(baseGroup.SourceColumns)) {
				baseGroup = &candidate
			}
		}
		if baseGroup == nil {
			panic(fmt.Sprintf(`Table "%v": TypeCacheGroupNoIndex "%v" needs a TypeCacheGroupFromKey group whose Keys are all among its own, and fewer`,
				core.Name, noIndexGroup.Name))
		}
		noIndexGroup.BaseGroup = baseGroup
	}
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
		PackBits:       core.indexGroupPackBits(indexCfg.Keys),
	})
}

// indexGroupPackBits sizes each key's slot in the group's packed value: Size() on the key wins, then
// the bits of its FixedValues maximum, then its integer type's width (an int32 takes 31). The slots
// must fit the 64 bits of the packed value; a wide group sizes its keys with Size().
func (core *TableCore) indexGroupPackBits(keys []Coln) []int64 {
	typeBits := map[string]int64{"int64": 63, "int32": 31, "int16": 15, "int8": 7}
	packBits := make([]int64, len(keys))
	for position, key := range keys {
		keyInfo := key.GetInfo()
		column := core.ColumnsMap[keyInfo.Name]
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`Table "%v": cache group column "%v" was not found`, core.Name, keyInfo.Name))
		}
		elementType := strings.TrimPrefix(strings.TrimPrefix(column.GetType().FieldType, "*"), "[]")
		if valueRange, isDeclared := core.FixedValueRanges[keyInfo.Name]; keyInfo.SlotBits > 0 {
			packBits[position] = int64(keyInfo.SlotBits)
		} else if isDeclared {
			packBits[position] = max(1, int64(bits.Len64(uint64(valueRange.MaxValue))))
		} else if packBits[position] = typeBits[elementType]; packBits[position] == 0 {
			panic(fmt.Sprintf(`Table "%v": cache group column "%v" is a %v: a group key must be an integer`, core.Name, keyInfo.Name, elementType))
		}
	}
	if totalBits := SumSlotBits(packBits, 0); totalBits > PackedVirtualInt64Bits {
		panic(fmt.Sprintf(`Table "%v": the cache group on %v packs into %v bits %v, over the %v of its packed value: narrow its keys with Size()`,
			core.Name, keysNames(keys), totalBits, packBits, PackedVirtualInt64Bits))
	}
	return packBits
}

func keysNames(keys []Coln) []string {
	names := make([]string, len(keys))
	for position, key := range keys {
		names[position] = key.GetName()
	}
	return names
}
