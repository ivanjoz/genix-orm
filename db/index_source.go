package db

// IndexSource is what a compiled index IS, independent of how a storage engine materializes it:
// which columns it is keyed on, how a packed key is laid out, which operators it answers and
// whether its answer needs an exact post-filter. The query planner matches statements against
// IndexSources, so every driver picks the same index for the same query. A driver wraps it with
// its own materialization (the Scylla driver's viewInfo adds the CQL create script and WHERE
// rendering; the FDB driver writes index entries).
type IndexSource struct {
	// Kind says which compiler described the index, so a driver knows how to materialize it.
	Kind IndexKind
	// Type: 1 = global index, 2 = local index, 3 = hash / cache-group index, 6 = simple view,
	// 7 = hash view, 8 = range / radix (packed) view, delta views included, 9 = view table (fan-out).
	Type int8
	Name string
	Idx  int8
	// Column is the indexed column; for a packed or hashed index it is the generated virtual column.
	Column IColInfo
	// Columns is the key in order, partition first when the index is partitioned.
	Columns       []string
	ColumnsNoPart []string
	ColumnsIdx    []int16
	// AvailableColumns tracks the real base-table columns a projected view can serve.
	// Rationale: projected views cannot satisfy default "select all columns" reads.
	AvailableColumns []string
	// PackedSourceColumns keeps the original key columns behind a packed range index, so prefix
	// ranges can be planned and scanned values decomposed back.
	PackedSourceColumns []IColInfo
	// PackedSlotBitsPerColumn mirrors PackedSourceColumns.
	PackedSlotBitsPerColumn []int64
	// PackedIsInt32 tells the packed column is a 32-bit integer.
	PackedIsInt32 bool
	Operators     []string
	// RequiresPostFilter: the index can overfetch (hash-style plans trade exact routing for
	// bounded overfetch), so its rows are filtered exactly in memory.
	RequiresPostFilter bool
	// IsDeltaView marks a TypeDelta index: its packed column is always rewritten by the write path,
	// so the "columns updated together" check skips it.
	IsDeltaView bool
	// FanoutColumnName, TableColumns and TableKeyColumns describe a TypeViewTable: one index row per
	// element of the fan-out slice column.
	FanoutColumnName string
	TableColumns     []ViewTableColumnInfo
	TableKeyColumns  []ViewTableColumnInfo
	// DeclaredColumns, ProjectedColumns and SelectableColumns describe a TypeView: the declared keys,
	// the Cols projection (empty = every stored column) and every real column the view can serve.
	// PartitionColumn is the partition the view is keyed on (the base partition unless overridden) and
	// KeepsBasePartition says whether it is the base table's.
	DeclaredColumns    []IColInfo
	ProjectedColumns   []IColInfo
	SelectableColumns  []IColInfo
	PartitionColumn    IColInfo
	KeepsBasePartition bool
	// MaintenanceIDColumn is the base-row ID a view table is rebuilt by, and RebuildColumnNames the
	// base columns whose change forces its rows to be rebuilt.
	MaintenanceIDColumn IColInfo
	RebuildColumnNames  map[string]bool
	// PackedIndex is set on an IndexKindPacked index: the layout of its packed virtual column.
	// It is deliberately not PackedSourceColumns, which marks a range view to the planner.
	PackedIndex *PackedIndexInfo
	// IndexGroup is set on an IndexKindGroupHash index: the group whose hash column it indexes.
	IndexGroup *IndexGroupInfo
}

// IndexKind names the compiler an IndexSource came from.
type IndexKind int8

const (
	// IndexKindSingleColumn is a local or global index on one stored column.
	IndexKindSingleColumn IndexKind = iota + 1
	// IndexKindPacked is a local or global index on a generated column packing several integers.
	IndexKindPacked
	// IndexKindGroupHash is a cache group over several columns (or a slice): a generated hash column
	// the group's grouped reads key on. Its IndexGroup holds the group.
	IndexKindGroupHash
)

// IndexSourceNamed returns the compiled index with that name, or nil.
func (core *TableCore) IndexSourceNamed(name string) *IndexSource {
	for _, source := range core.IndexSources {
		if source.Name == name {
			return source
		}
	}
	return nil
}

// ViewTableColumnInfo is one column of a fan-out view table, read from SourceColumn (one element of
// it when UsesSliceElement).
type ViewTableColumnInfo struct {
	SourceColumn     IColInfo
	UsesSliceElement bool
}
