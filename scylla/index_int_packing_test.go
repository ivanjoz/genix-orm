package scylla

import (
	"math"
	"testing"
)

// Scylla compares int/bigint as signed, so the stored values must keep the unsigned layout's order
// across the sign boundary, and load back unchanged.
func TestStoreVirtualPackedKeepsUnsignedOrder(t *testing.T) {
	cases := []struct {
		isInt32 bool
		ordered []uint64
	}{
		{isInt32: true, ordered: []uint64{0, 1, 1<<31 - 1, 1 << 31, math.MaxUint32}},
		{isInt32: false, ordered: []uint64{0, 1, 1<<63 - 1, 1 << 63, math.MaxUint64}},
	}
	for _, testCase := range cases {
		previousStored := int64(math.MinInt64)
		for index, packed := range testCase.ordered {
			stored := convertToInt64(storeVirtualPacked(packed, testCase.isInt32))
			if index > 0 && stored <= previousStored {
				t.Fatalf("isInt32=%v: %v stored as %v, not above the previous %v", testCase.isInt32, packed, stored, previousStored)
			}
			if loaded := loadVirtualPacked(stored, testCase.isInt32); loaded != packed {
				t.Fatalf("isInt32=%v: %v loaded back as %v", testCase.isInt32, packed, loaded)
			}
			previousStored = stored
		}
	}
}
