package db

import (
	"math"
	"slices"
	"strings"
	"testing"
)

func TestPackSlotValuesRoundTrips(t *testing.T) {
	slotBits := []int64{17, 30, 14}
	values := []int64{131071, 123_456_789, 16383}

	packed := PackSlotValues(values, slotBits)
	if packed != 131071<<44|123_456_789<<14|16383 {
		t.Fatalf("unexpected packed value %v", packed)
	}
	if unpacked := UnpackSlotValues(packed, slotBits); !slices.Equal(unpacked, values) {
		t.Fatalf("expected %v back, got %v", values, unpacked)
	}
}

// A full 64-bit layout must not overflow: its max is MaxUint64, not a wrapped 0.
func TestPackSlotPrefixBoundFillsAFull64BitLayout(t *testing.T) {
	slotBits := []int64{33, 31}
	if upper := PackSlotPrefixBound(nil, slotBits, true); upper != math.MaxUint64 {
		t.Fatalf("expected MaxUint64, got %v", upper)
	}
	lower, upper := PackSlotPrefixBound([]int64{5}, slotBits, false), PackSlotPrefixBound([]int64{5}, slotBits, true)
	if lower != 5<<31 || upper != 5<<31|(1<<31-1) {
		t.Fatalf("unexpected prefix block [%v, %v]", lower, upper)
	}
}

// Size(n) is a range cap: a value past it panics instead of being truncated.
func TestPackSlotValuesPanicsOnAValueWiderThanItsSlot(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "does not fit its 7-bit slot") {
			t.Fatalf("expected an overflow panic, got %v", recovered)
		}
	}()
	PackSlotValues([]int64{1, 128}, []int64{8, 7})
}

func TestKeyIntPackingSlotBitsGivesTheRemainderToTheLastColumn(t *testing.T) {
	keyIntPacking := []Coln{
		colRef{ColumnInfo{SlotBits: 15}},
		colRef{ColumnInfo{SlotBits: 17}},
		colRef{ColumnInfo{AutoincrementRandBits: 8}},
	}
	if slotBits := KeyIntPackingSlotBits("t", keyIntPacking); !slices.Equal(slotBits, []int64{15, 17, 31}) {
		t.Fatalf("expected [15 17 31], got %v", slotBits)
	}
}

func TestDecodeKeyIntPackingSplitsTheAutoincrementSlot(t *testing.T) {
	schema := TableSchema{
		Name: "movements",
		KeyIntPacking: []Coln{
			colRef{ColumnInfo{ColInfo: ColInfo{Name: "date"}, SlotBits: 15}},
			colRef{ColumnInfo{AutoincrementRandBits: 8}},
		},
	}
	packedKey := int64(20725)<<48 | 42<<8 | 201

	components := DecodeKeyIntPacking(schema, packedKey)
	expected := []PackedKeyComponent{
		{Column: "date", Value: 20725},
		{Column: "autoincrement", Value: 42<<8 | 201},
		{Column: "autoincrement.sequence", Value: 42},
		{Column: "autoincrement.random", Value: 201},
	}
	if !slices.Equal(components, expected) {
		t.Fatalf("expected %v, got %v", expected, components)
	}
}
