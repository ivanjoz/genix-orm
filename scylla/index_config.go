package scylla

import "fmt"

func hasCompositeBucketing(indexCfg Index) bool {
	for _, key := range indexCfg.Keys {
		if len(key.GetInfo().CompositeBucketSizes) > 0 {
			return true
		}
	}
	return false
}

func resolveSchemaIndexType(indexCfg Index) int8 {
	if indexCfg.Type != 0 {
		return indexCfg.Type
	}
	if len(indexCfg.Cols) > 0 {
		return TypeView
	}
	if len(indexCfg.Keys) == 1 {
		return TypeLocalIndex
	}
	return TypeLocalIndex
}

func registerSchemaLocalIndex(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	if len(indexCfg.Keys) == 1 {
		colCfg := indexCfg.Keys[0]
		colInfo := colCfg.GetInfo()
		column := dbTable.ColumnsMap[colInfo.Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": Indexes column "%v" was not found`, dbTable.Name, colInfo.Name))
		}
		// Scylla rejects a local index on a collection ("not implemented yet"): fail here, not on deploy.
		if column.GetType().IsSlice {
			panic(fmt.Sprintf(`Table "%v": column "%v" is a slice and Scylla has no local index on a collection; use TypeGlobalIndex, or a TypeCacheGroup of two or more Keys`,
				dbTable.Name, column.GetName()))
		}

		indexName := fmt.Sprintf(`%v__%v_index_1`, dbTable.Name, column.GetName())
		if _, exists := dbTable.indexes[indexName]; exists {
			return
		}

		index := &viewInfo{
			Type:    2,
			name:    indexName,
			idx:     *idxCount,
			column:  column,
			columns: []string{dbTable.GetPartKey().GetName(), column.GetName()},
		}
		index.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`,
				indexName, dbTable.GetFullName(), dbTable.GetPartKey().GetName(), column.GetName())
		}

		*idxCount = *idxCount + 1
		dbTable.indexes[index.name] = index
		return
	}

	registerPackedIndex(dbTable, idxCount, indexCfg.Keys, packedIndexBuildConfig{
		scope:               packedIndexScopeLocal,
		schemaFieldName:     "Indexes",
		virtualColumnPrefix: "zz_ixp_",
		indexType:           2,
		requireInOnlyFirst:  false,
	})
}

func registerSchemaGlobalIndex(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	if len(indexCfg.Keys) == 1 {
		colCfg := indexCfg.Keys[0]
		colInfo := colCfg.GetInfo()
		column := dbTable.ColumnsMap[colInfo.Name]
		if column == nil {
			panic(fmt.Sprintf(`Table "%v": Indexes column "%v" was not found`, dbTable.Name, colInfo.Name))
		}

		indexName := fmt.Sprintf(`%v__%v_index_0`, dbTable.Name, column.GetName())
		if _, exists := dbTable.indexes[indexName]; exists {
			return
		}

		index := &viewInfo{
			Type:    1,
			name:    indexName,
			idx:     *idxCount,
			column:  column,
			columns: []string{column.GetName()},
		}
		index.getCreateScript = func() string {
			colName := column.GetName()
			if column.GetType().IsSlice {
				colName = fmt.Sprintf("VALUES(%v)", colName)
			}
			return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, indexName, dbTable.GetFullName(), colName)
		}

		*idxCount = *idxCount + 1
		dbTable.indexes[index.name] = index
		return
	}

	registerPackedIndex(dbTable, idxCount, indexCfg.Keys, packedIndexBuildConfig{
		scope:               packedIndexScopeGlobal,
		schemaFieldName:     "Indexes",
		virtualColumnPrefix: "zz_gixp_",
		indexType:           1,
		requireInOnlyFirst:  true,
	})
}
