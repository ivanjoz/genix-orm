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
	// ID is the table's hand-assigned identity, packed into the by-IDs cache key.
	ID int16
	// Namespace is the logical grouping the table lives in — a keyspace on Scylla.
	Namespace     string
	Keys          []IColInfo
	PartKey       IColInfo
	KeysIdx       []int16
	Columns       []IColInfo
	ColumnsMap    map[string]IColInfo
	ColumnsIdxMap map[int16]IColInfo
	// SaveUpdatedVersion enables the by-IDs slot-version hooks on writes for this table.
	SaveUpdatedVersion bool
	// By-IDs cache metadata is precomputed during table creation.
	SlotVersionPartitionCol IColInfo
	SlotVersionKeyCol       IColInfo
	// Columns the ORM writes on the caller's behalf.
	CreatedCol        IColInfo
	UpdatedCol        IColInfo
	UpdatedVersionCol IColInfo
	// Sequence and autoincrement metadata for generated keys.
	UseSequences      bool
	SequencePartCol   IColInfo
	AutoincrementCol  IColInfo
	AutoincrementPart IColInfo
	// MaxColIdx is the highest column index handed out, including virtual columns,
	// so drivers can keep allocating unique indexes as they add their own.
	MaxColIdx int16
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
