package dynamo

import (
	"fmt"
	"unsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// Deriving physical key attributes from a record.
//
//	pk  = "<entity>#" + composite(partition columns)
//	sk  = composite(sort columns)                      (order-preserving)
//	nN  = <single integer column>                      (native DynamoDB number)
//	sN  = "<entity>#" + composite(string-slot columns) (order-preserving)
//
// All field reads go through the column's precompiled xunsafe accessor, so these
// hot paths never touch reflection. Every declared index slot is always written
// (non-sparse): each item of the entity participates in its indexes.
// ─────────────────────────────────────────────────────────────────────────────

// keyPartsFor builds the composite parts for a list of key columns.
func (m *tableMeta) keyPartsFor(ptr unsafe.Pointer, cols []keyCol) []keyPart {
	parts := make([]keyPart, 0, len(cols))
	for _, kc := range cols {
		switch kc.kind {
		case kindString:
			parts = append(parts, stringPart(kc.acc.getStr(ptr)))
		case kindInt, kindUint:
			parts = append(parts, numberPart(kc.acc.getU64(ptr), kc.base))
		default:
			panic(fmt.Sprintf("db: key column %q has unsupported type for a composite key", kc.fieldName))
		}
	}
	return parts
}

// pkValue builds the base-table partition key.
func (m *tableMeta) pkValue(ptr unsafe.Pointer) string {
	parts := append([]keyPart{stringPart(m.entity)}, m.keyPartsFor(ptr, m.partition)...)
	return buildCompositeKey(parts)
}

// skValue builds the base-table sort key.
func (m *tableMeta) skValue(ptr unsafe.Pointer) string {
	return buildCompositeKey(m.keyPartsFor(ptr, m.sort))
}

// slotValue builds one index slot's stored value. For numeric slots it returns
// the raw integer (as int64) so DynamoDB stores a native, range-ordered number;
// for string slots it returns the entity-prefixed composite string.
func (m *tableMeta) slotValue(ptr unsafe.Pointer, idx indexMeta) any {
	if idx.slot.isNumber {
		return idx.keys[0].acc.getI64(ptr)
	}
	parts := append([]keyPart{stringPart(m.entity)}, m.keyPartsFor(ptr, idx.keys)...)
	return buildCompositeKey(parts)
}
