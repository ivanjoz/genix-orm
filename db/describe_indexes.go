package db

import (
	"fmt"
	"strings"
	"unsafe"
)

// HasCompositeBucketing reports whether any key of the index is a CompositeBucketing column.
func HasCompositeBucketing(indexCfg Index) bool {
	for _, key := range indexCfg.Keys {
		if len(key.GetInfo().CompositeBucketSizes) > 0 {
			return true
		}
	}
	return false
}

// ResolveSchemaIndexType returns the declared index type, inferring it when the declaration names
// none: a Cols projection makes a view, anything else a local index.
func ResolveSchemaIndexType(indexCfg Index) int8 {
	if indexCfg.Type != 0 {
		return indexCfg.Type
	}
	if len(indexCfg.Cols) > 0 {
		return TypeView
	}
	return TypeLocalIndex
}

// DescribeIndexes describes every index declared in the schema, in declaration order, and returns the
// sources it added to IndexSources: the ones a driver must materialize. A declaration that needs
// none (a key-range cache group, or a local index that already exists) adds nothing.
func (core *TableCore) DescribeIndexes(idxCount *int8, indexes []Index) []*IndexSource {
	firstNewSource := len(core.IndexSources)
	for _, indexCfg := range indexes {
		schemaIndexType := ResolveSchemaIndexType(indexCfg)
		// TypeDelta is the one kind that means something with no keys: a log of the whole partition.
		if len(indexCfg.Keys) == 0 && schemaIndexType != TypeDelta {
			panic(fmt.Sprintf(`Table "%v": Indexes entry must not be empty`, core.Name))
		}
		// Only views materialize their own partition, so an override is meaningless elsewhere.
		if IndexPartitionColumnName(indexCfg) != "" && schemaIndexType != TypeView {
			panic(fmt.Sprintf(`Table "%v": Partition is only supported on TypeView indexes`, core.Name))
		}
		if schemaIndexType == TypeCacheGroup || schemaIndexType == TypeCacheGroupS3 ||
			schemaIndexType == TypeCacheGroupFromKey || schemaIndexType == TypeCacheGroupNoIndex {
			core.DescribeIndexGroup(idxCount, indexCfg)
			continue
		}
		if HasCompositeBucketing(indexCfg) {
			core.describeCompositeBucketIndex(idxCount, indexCfg)
			continue
		}

		switch schemaIndexType {
		case TypeGlobalIndex:
			core.DescribeGlobalIndex(idxCount, indexCfg)
		case TypeLocalIndex:
			core.DescribeLocalIndex(idxCount, indexCfg)
		case TypeViewTable:
			core.DescribeViewTable(indexCfg)
		case TypeView:
			core.DescribeView(indexCfg)
		case TypeDelta:
			core.DescribeDelta(indexCfg)
		default:
			panic(fmt.Sprintf(`Table "%v": unsupported index type %d`, core.Name, schemaIndexType))
		}
	}
	core.resolveNoIndexBaseGroups()
	return core.IndexSources[firstNewSource:]
}

// describeCompositeBucketIndex describes an index with one CompositeBucketing column: per bucket
// size, a generated column holding the []int32 hashes of the record's buckets and a global index on
// it. The column's value functions return the neutral []int32; a driver encodes the collection.
func (core *TableCore) describeCompositeBucketIndex(idxCount *int8, indexCfg Index) {
	indexColumns := indexCfg.Keys
	if len(indexColumns) < 2 || len(indexColumns) > 3 {
		panic(fmt.Sprintf(`Table "%v": composite-bucketing index entries must have 2 to 3 columns. Found: %v`, core.Name, len(indexColumns)))
	}

	sourceColumns := make([]IColInfo, 0, len(indexColumns))
	var bucketColumn IColInfo
	bucketIsWeek := false
	var bucketSizes []int8
	sourceColumnNames := make([]string, 0, len(indexColumns))

	for _, indexColumn := range indexColumns {
		indexColumnInfo := indexColumn.GetInfo()
		column := core.ColumnsMap[indexColumnInfo.Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": composite-bucketing column "%v" was not found`, core.Name, indexColumnInfo.Name))
		}
		if !IsCompositeNumericFieldType(column.GetType().FieldType) {
			panic(fmt.Sprintf(`Table "%v": composite-bucketing column "%v" must be integer scalar/slice. Found: %v`, core.Name, column.GetName(), column.GetType().FieldType))
		}
		sourceColumns = append(sourceColumns, column)
		sourceColumnNames = append(sourceColumnNames, column.GetName())

		bucketDefs := indexColumnInfo.CompositeBucketSizes
		if len(bucketDefs) > 0 {
			if bucketColumn != nil {
				panic(fmt.Sprintf(`Table "%v": composite-bucketing supports exactly one CompositeBucketing column per index`, core.Name))
			}
			if column.GetType().IsSlice {
				panic(fmt.Sprintf(`Table "%v": CompositeBucketing column "%v" must be numeric (not a slice)`, core.Name, column.GetName()))
			}
			bucketColumn = column
			bucketIsWeek = indexColumnInfo.IsWeek
			bucketSizes = NormalizeCompositeBucketSizes(bucketDefs)
		}
	}

	if bucketColumn == nil {
		panic(fmt.Sprintf(`Table "%v": composite-bucketing requires one column marked with CompositeBucketing`, core.Name))
	}

	compositeIndex := CompositeBucketIndex{
		Name:                 strings.Join(sourceColumnNames, "_"),
		SourceColumns:        sourceColumns,
		BucketColumn:         bucketColumn,
		BucketIsWeek:         bucketIsWeek,
		BucketSizes:          bucketSizes,
		VirtualColumnsBySize: map[int8]IColInfo{},
	}

	for _, bucketSize := range bucketSizes {
		virtualColName := fmt.Sprintf("zz_hb_%s_b%d", strings.Join(sourceColumnNames, "_"), bucketSize)
		if _, exists := core.ColumnsMap[virtualColName]; exists {
			panic(fmt.Sprintf(`Table "%v": generated virtual composite bucket column already exists: %v`, core.Name, virtualColName))
		}

		bucketSizeLocal := bucketSize
		virtualColumn := &ColumnInfo{
			ColInfo: ColInfo{Name: virtualColName, FieldName: virtualColName, IsVirtual: true, Idx: core.MaxColIdx},
			ColType: GetColTypeByName("[]int32"),
		}
		bucketHashes := func(ptr unsafe.Pointer) any {
			return ComputeCompositeHashSet(ptr, sourceColumns, bucketColumn, bucketSizeLocal, compositeIndex.BucketIsWeek)
		}
		virtualColumn.GetRawValueFn = bucketHashes
		virtualColumn.GetValueFn = bucketHashes

		core.MaxColIdx++
		core.ColumnsMap[virtualColumn.GetName()] = virtualColumn
		compositeIndex.VirtualColumnsBySize[bucketSize] = virtualColumn

		core.addIndexSource(idxCount, &IndexSource{
			Kind:    IndexKindCompositeBucket,
			Type:    1,
			Name:    fmt.Sprintf(`%v__%v_index_0`, core.Name, virtualColumn.GetName()),
			Column:  virtualColumn,
			Columns: []string{virtualColumn.GetName()},
		})
	}

	core.CompositeBucketIndexes = append(core.CompositeBucketIndexes, compositeIndex)
	fmt.Printf("CompositeBucketing index registered: table=%s index=%s bucketSizes=%v\n", core.Name, compositeIndex.Name, compositeIndex.BucketSizes)
}
