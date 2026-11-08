package scylla

import (
	"fmt"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// registerMaterializedIndex materializes a described index and files it where the driver looks it up:
// views and view tables (their own tables or materialized views) apart from secondary indexes.
func registerMaterializedIndex(dbTable *ScyllaTable, source *db.IndexSource) {
	view := materializeIndexSource(dbTable, source)
	switch source.Kind {
	case db.IndexKindView, db.IndexKindViewTable, db.IndexKindDelta:
		dbTable.views[view.Name] = view
		if source.Kind == db.IndexKindViewTable {
			dbTable.hasTableBackedViews = true
		}
	default:
		dbTable.indexes[view.Name] = view
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
	case db.IndexKindCompositeBucket:
		materializeCompositeBucketIndex(view, dbTable)
	case db.IndexKindView:
		materializeView(dbTable, view)
	case db.IndexKindViewTable:
		materializeViewTable(dbTable, view)
	case db.IndexKindDelta:
		materializeDeltaView(dbTable, view)
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

// materializeCompositeBucketIndex indexes the VALUES of a bucket-hash set<int> column. The described
// value is the []int32 hashes; the statement carries them as a collection literal.
func materializeCompositeBucketIndex(view *viewInfo, dbTable *ScyllaTable) {
	hashColumn := view.Column.(*columnInfo)
	describedHashes := hashColumn.GetValueFn
	hashColumn.GetValueFn = func(ptr unsafe.Pointer) any {
		return makeSignedIntCollectionLiteral(hashColumn.DBType, describedHashes(ptr).([]int32))
	}
	view.getCreateScript = func() string {
		return fmt.Sprintf(`CREATE INDEX %v ON %v (VALUES(%v))`, view.Name, dbTable.GetFullName(), hashColumn.GetName())
	}
}
