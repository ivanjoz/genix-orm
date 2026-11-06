package scylla

import (
	"fmt"

	"github.com/ivanjoz/genix-orm/db"
)

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

// registerSchemaLocalIndex and registerSchemaGlobalIndex describe the index in db (the neutral
// half) and materialize it as a Scylla secondary index.
func registerSchemaLocalIndex(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	if source := dbTable.DescribeLocalIndex(idxCount, indexCfg); source != nil {
		dbTable.indexes[source.Name] = materializeIndexSource(dbTable, source)
	}
}

func registerSchemaGlobalIndex(dbTable *ScyllaTable, idxCount *int8, indexCfg Index) {
	if source := dbTable.DescribeGlobalIndex(idxCount, indexCfg); source != nil {
		dbTable.indexes[source.Name] = materializeIndexSource(dbTable, source)
	}
}

// materializeIndexSource wraps a described index in the CQL that creates and queries it.
func materializeIndexSource(dbTable *ScyllaTable, source *db.IndexSource) *viewInfo {
	view := &viewInfo{IndexSource: source}
	switch source.Kind {
	case db.IndexKindSingleColumn:
		materializeSingleColumnIndex(dbTable, view)
	case db.IndexKindPacked:
		materializePackedIndex(dbTable, view)
	case db.IndexKindGroupHash:
		materializeGroupHashIndex(dbTable, view)
	default:
		panic(fmt.Sprintf(`Table "%v": index %v has kind %d, which this driver does not materialize`, dbTable.Name, source.Name, source.Kind))
	}
	return view
}

// materializeSingleColumnIndex: a local index is keyed on (partition, column); a global index on a
// slice column indexes its VALUES.
func materializeSingleColumnIndex(dbTable *ScyllaTable, view *viewInfo) {
	indexName, column := view.Name, view.Column
	if view.Type == 2 {
		view.getCreateScript = func() string {
			return fmt.Sprintf(`CREATE INDEX %v ON %v ((%v),%v)`,
				indexName, dbTable.GetFullName(), dbTable.GetPartKey().GetName(), column.GetName())
		}
		return
	}
	view.getCreateScript = func() string {
		colName := column.GetName()
		if column.GetType().IsSlice {
			colName = fmt.Sprintf("VALUES(%v)", colName)
		}
		return fmt.Sprintf(`CREATE INDEX %v ON %v (%v)`, indexName, dbTable.GetFullName(), colName)
	}
}
