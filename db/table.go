package db

import (
	"fmt"
	"unsafe"
)

// Table is a compiled table: the immutable metadata a driver derived from a
// TableSchema declaration. Code that only needs to identify a table and read its
// columns — controllers, backup and restore, admin tooling, the name registry —
// depends on this interface rather than on any driver's concrete type. Each driver
// keeps its own richer form (materialized views, packed keys, GSI slots) behind it.
type Table interface {
	GetName() string
	GetFullName() string
	GetColumns() map[string]IColInfo
	GetKeys() []IColInfo
	GetPartKey() IColInfo
	// GetPartValue reads the partition value out of a record.
	GetPartValue(ptr unsafe.Pointer) int64
	// GetKeyValues reads the key values out of a record, in key order.
	GetKeyValues(ptr unsafe.Pointer) []any
}

// TableCore is the part of a compiled table that means the same thing to every
// storage engine: its identity, its columns, its key and partition, and the
// columns the ORM manages itself (timestamps, counters, sequences, cache
// versions). Each driver embeds it and adds its own metadata alongside.
//
// The fields are exported because drivers live in other packages; Go has no
// cross-package unexported access, embedding included.
type TableCore struct {
	Name string
	// ID is the table's hand-assigned identity; it names the table's cache files.
	ID int16
	// Namespace is the logical grouping the table lives in — a keyspace on Scylla.
	Namespace     string
	Keys          []IColInfo
	PartKey       IColInfo
	KeysIdx       []int16
	Columns       []IColInfo
	ColumnsMap    map[string]IColInfo
	ColumnsIdxMap map[int16]IColInfo
	// CacheByIDs enables the by-IDs slot hooks on writes for this table.
	CacheByIDs bool
	// Columns the ORM writes on the caller's behalf. VersionCol is nil on a table without a
	// TypeDelta index.
	CreatedCol IColInfo
	UpdatedCol IColInfo
	VersionCol IColInfo
	// Sequence and autoincrement metadata for generated keys.
	UseSequences      bool
	SequencePartCol   IColInfo
	AutoincrementCol  IColInfo
	AutoincrementPart IColInfo
	// MaxColIdx is the highest column index handed out, including virtual columns,
	// so drivers can keep allocating unique indexes as they add their own.
	MaxColIdx int16

	// Index state compiled from the schema (compiled_state.go). Every driver builds it the same way
	// because it decides what an index entry contains.
	KeyConcatenated []IColInfo
	KeyIntPacking   []IColInfo
	// KeyIntPackingSlotBits is the resolved bit layout of KeyIntPacking (KeyIntPackingSlotBits).
	KeyIntPackingSlotBits []int64
	// FixedValueRanges holds the schema's FixedValues per column name.
	FixedValueRanges       map[string]ColumnValueRange
	PackedIndexes          []*PackedIndexInfo
	DeltaIndexes           []DeltaIndexInfo
	CompositeBucketIndexes []CompositeBucketIndex
	IndexGroups            []IndexGroupInfo
	// IndexGroupIDs prevents per-table collisions when logical IndexGroup names hash to the same int16.
	IndexGroupIDs map[int16]string
	// ComputedColumns are the TableSchema.Computed columns (computed_columns.go).
	ComputedColumns []ComputedColumnInfo
	// TextSearchIndex is nil unless the schema declares TextSearch (text_search_info.go).
	TextSearchIndex *TextSearchIndexInfo
	// IndexSources are every compiled index and view, in compile order (describe_index.go). A
	// driver materializes each one its own way.
	IndexSources []*IndexSource
}

func (e TableCore) GetName() string {
	return e.Name
}

func (e TableCore) GetFullName() string {
	return fmt.Sprintf("%v.%v", e.Namespace, e.Name)
}

func (e TableCore) GetColumns() map[string]IColInfo {
	return e.ColumnsMap
}

func (e TableCore) GetKeys() []IColInfo {
	return e.Keys
}

func (e TableCore) GetPartKey() IColInfo {
	return e.PartKey
}

func (e TableCore) GetPartValue(ptr unsafe.Pointer) int64 {
	if e.PartKey == nil || e.PartKey.IsNil() {
		return 0
	}
	return ToInt64(e.PartKey.GetRawValue(ptr))
}

func (e TableCore) GetKeyValues(ptr unsafe.Pointer) []any {
	if len(e.Keys) == 0 {
		return nil
	}
	keyValues := make([]any, 0, len(e.Keys))
	for _, keyColumn := range e.Keys {
		keyValues = append(keyValues, keyColumn.GetRawValue(ptr))
	}
	return keyValues
}
