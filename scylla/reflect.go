package scylla

import (
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

var rangeOperators = []string{">", "<", ">=", "<="}

const (
	managedCreatedColumnName = "created"
	managedUpdatedColumnName = "updated"
	managedVersionColumnName = db.ColumnNameVersion
)

// LZ4: every flush, compaction and uncached read pays the codec's CPU, and on a 2-core VPS the
// database is CPU-bound well before it is disk-bound; Zstd's smaller files cost more than they save.
var makeStatementWith string = `	WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
	and compaction = {'class': 'SizeTieredCompactionStrategy'}
	and compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
	and dclocal_read_repair_chance = 0
	and speculative_retry = '99.0PERCENTILE'`

// makeTable is deliberately NOT generic. It is 400+ lines that work purely through the schema
// and reflection, so a type parameter here would stencil the whole body once per table type —
// it cost ~850 KB of binary across 54 tables. The caller does the two lines of type-specific
// work (GetSchema, reflect.ValueOf) and hands the results in.
func makeTable(schema db.TableSchema, structRefValue reflect.Value) ScyllaTable {
	dbTable := ScyllaTable{
		indexes:              map[string]*viewInfo{},
		views:                map[string]*viewInfo{},
		selectStatementCache: newSelectPlanCache(),
	}
	// db compiles what the table means to every driver; below are the parts only CQL has.
	db.RefuseExpandedSlices(schema, "ScyllaDB")
	dbTable.CompileFromSchema(schema, structRefValue)
	if dbTable.Namespace == "" {
		dbTable.Namespace = connParams.Keyspace
	}
	for _, column := range dbTable.ColumnsMap {
		if columnMetadata, isColumnInfo := column.(*columnInfo); isColumnInfo {
			assertColumnIsEncodable(dbTable.Name, columnMetadata)
		}
	}
	if schema.SequenceColumn != nil {
		dbTable.ColumnsMap[schema.SequenceColumn.GetName()].GetType().DBType = "counter"
	}
	if len(dbTable.KeyConcatenated) > 0 {
		// A concatenated key is bound as a quoted CQL text literal.
		keyColumn := dbTable.Keys[0].(*columnInfo)
		keyColumn.GetValueFn = func(ptr unsafe.Pointer) any {
			return "'" + strings.ReplaceAll(keyColumn.GetRawValueFn(ptr).(string), "'", "''") + "'"
		}
	}
	configureDataFrames(&dbTable, schema, structRefValue)

	// db describes every declared index (neutral: columns, layouts, validation); the driver then
	// materializes each described source as the CQL that creates and queries it.
	idxCount := int8(1)
	for _, source := range dbTable.DescribeIndexes(&idxCount, schema.Indexes) {
		registerMaterializedIndex(&dbTable, source)
	}
	if len(dbTable.IndexGroups) > 0 && dbTable.indexUpdatedTable == nil {
		dbTable.indexUpdatedTable = &indexUpdatedTableInfo{name: dbTable.Name + "__index_updated"}
	}
	dbTable.FinalizeColumns()

	for _, e := range dbTable.indexes {
		dbTable.indexViews = append(dbTable.indexViews, e)
	}
	for _, e := range dbTable.views {
		dbTable.indexViews = append(dbTable.indexViews, e)
	}
	// Both sources above are maps, so their iteration order changes every process. Capability
	// matching breaks priority ties by first-seen, which would make a query plan differ between
	// runs whenever two sources advertise the same signature at the same priority. Sorting by name
	// pins the tie-break to something stable.
	slices.SortFunc(dbTable.indexViews, func(leftView, rightView *viewInfo) int {
		return strings.Compare(leftView.Name, rightView.Name)
	})

	for _, idxview := range dbTable.indexViews {
		for _, colname := range idxview.Columns {
			col := dbTable.ColumnsMap[colname]
			idxview.ColumnsIdx = append(idxview.ColumnsIdx, col.GetInfo().Idx)
		}
	}

	dbTable.capabilities = dbTable.ComputeCapabilities()

	// Validates the by-IDs table shape once per table build, not per query/write call.
	configureCacheByIDs(&dbTable)
	// Precompiles the generic by-IDs access plan; depends on the validation above.
	configureGenericRecordFields(&dbTable, schema)
	// Needs the final Columns: every stored one must survive colbin.
	requireCacheGroupSnapshotCodec(&dbTable, structRefValue)

	return dbTable
}
