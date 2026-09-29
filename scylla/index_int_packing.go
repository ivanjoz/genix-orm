package scylla

import (
	"fmt"

	"github.com/ivanjoz/genix-orm/db"
)

// packedIndexInfo stores schema-time metadata for TableSchema.Indexes packed local indexes.
// It is used for capability generation and for query WHERE rewriting.
type packedIndexInfo struct {
	indexName         string
	packedColumnName  string
	sourceColumnNames []string
	// partitionColumnName is non-empty when the packed index is local (partition + packed).
	// Empty means "global index on packed column only".
	partitionColumnName string
	// slotBitsPerColumn aligns with sourceColumnNames (same length).
	slotBitsPerColumn []int64
	// isInt32Packed indicates the packed column is CQL int (Go int32).
	isInt32Packed bool
}

// Virtual packed columns (range views, delta views, packed indexes) never reach the application, so
// they spend the sign bit as well: the whole 64 (or 32) bits hold the unsigned layout.
const (
	packedVirtualInt64Bits = 64
	packedVirtualInt32Bits = 32
)

// packedVirtualBudgetBits is the bit budget of a virtual packed column.
func packedVirtualBudgetBits(isInt32 bool) int64 {
	if isInt32 {
		return packedVirtualInt32Bits
	}
	return packedVirtualInt64Bits
}

// storeVirtualPacked turns an unsigned layout into the value bound to the CQL int/bigint column.
// Scylla compares those as signed, so the top bit is flipped: that maps unsigned order onto signed
// order, and range scans over the stored value follow the layout's order. Every write and every
// query bound on a virtual packed column goes through here.
func storeVirtualPacked(packed uint64, isInt32 bool) any {
	if isInt32 {
		return int32(uint32(packed) ^ (1 << 31))
	}
	return int64(packed ^ (1 << 63))
}

// packRowValues packs one row's source column values. It checks every slot first so a value that
// does not fit panics naming the table, the packed column and the offending source column, instead
// of the anonymous slot index db.PackSlotValues can report.
func packRowValues(tableName, packedColumnName string, sourceColumns []IColInfo, values []int64, slotBits []int64) uint64 {
	for slotIndex, value := range values {
		if value < 0 || uint64(value) > db.SlotMaxValue(slotBits[slotIndex]) {
			panic(fmt.Sprintf(`Table "%v": packed column "%v" cannot hold %v in "%v": its %v-bit slot takes 0..%v`,
				tableName, packedColumnName, value, sourceColumns[slotIndex].GetName(), slotBits[slotIndex], db.SlotMaxValue(slotBits[slotIndex])))
		}
	}
	return db.PackSlotValues(values, slotBits)
}

// sumSlotBits adds the widths of the slots from fromIndex to the end.
func sumSlotBits(slotBits []int64, fromIndex int) int64 {
	totalBits := int64(0)
	for _, bits := range slotBits[fromIndex:] {
		totalBits += bits
	}
	return totalBits
}

// loadVirtualPacked is the inverse of storeVirtualPacked for a value scanned back from Scylla.
func loadVirtualPacked(stored int64, isInt32 bool) uint64 {
	if isInt32 {
		return uint64(uint32(int32(stored)) ^ (1 << 31))
	}
	return uint64(stored) ^ (1 << 63)
}
