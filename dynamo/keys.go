package dynamo

import (
	"fmt"
	"strings"
	"unsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// Deriving physical key attributes from a record.
//
//	pk  = TableID ‖ decimal(partition columns)          (DynamoDB number)
//	sk  = composite(sort columns)                       (order-preserving string)
//	nN  = TableID ‖ decimal(the one slot column)        (DynamoDB number)
//	sN  = "<TableID>#" + composite(string-slot columns) (order-preserving string)
//
// Array index rows (array_index.go) use pk ‖ the column's cb id as their pk.
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
			parts = append(parts, numberPart(kc.acc.getU64(ptr), kc.bits))
		default:
			panic(fmt.Sprintf("db: key column %q has unsupported type for a composite key", kc.fieldName))
		}
	}
	return parts
}

// numericKey packs the TableID and integer column values into one decimal number
// string: TableID * 10^w1 + v1, then * 10^w2 + v2, and so on (see encoding.go).
func (m *tableMeta) numericKey(values []uint64, cols []keyCol) string {
	var b strings.Builder
	b.WriteString(m.tableID)
	for i, kc := range cols {
		b.WriteString(decimalDigits(values[i], kc.bits))
	}
	return b.String()
}

// pkValue builds the base-table partition key (a DynamoDB number).
func (m *tableMeta) pkValue(ptr unsafe.Pointer) string {
	values := make([]uint64, len(m.partition))
	for i, kc := range m.partition {
		values[i] = kc.acc.getU64(ptr)
	}
	return m.numericKey(values, m.partition)
}

// skValue builds the base-table sort key.
func (m *tableMeta) skValue(ptr unsafe.Pointer) string {
	return buildCompositeKey(m.keyPartsFor(ptr, m.sort))
}

// slotValue builds one index slot's stored value: a decimal number string for
// numeric slots, the TableID-prefixed composite for string slots.
func (m *tableMeta) slotValue(ptr unsafe.Pointer, idx indexMeta) string {
	if idx.slot.isNumber {
		return m.numericKey([]uint64{idx.keys[0].acc.getU64(ptr)}, idx.keys)
	}
	parts := append([]keyPart{stringPart(m.tableID)}, m.keyPartsFor(ptr, idx.keys)...)
	return buildCompositeKey(parts)
}

// partitionRange is the [lo, hi] span of every pk this entity can produce with
// extraDigits appended after the partition columns (0 for base rows, the cb id
// width for array index rows). Scans filter on it, since a number key has no
// begins_with.
func (m *tableMeta) partitionRange(extraDigits int) (string, string) {
	width := m.partitionDigits + extraDigits
	return m.tableID + strings.Repeat("0", width), m.tableID + strings.Repeat("9", width)
}
