package db

import (
	"slices"
	"strings"
	"testing"
)

func TestNormalizeCompositeBucketSizes(t *testing.T) {
	if got := NormalizeCompositeBucketSizes([]int8{4, 1, 4, 2}); !slices.Equal(got, []int8{1, 2, 4}) {
		t.Errorf("sizes are deduplicated and ordered, got %v", got)
	}
	for name, sizes := range map[string][]int8{"empty": nil, "zero": {0, 2}, "negative": {-1}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s bucket sizes must panic", name)
				}
			}()
			NormalizeCompositeBucketSizes(sizes)
		}()
	}
}

func TestIsCompositeNumericFieldType(t *testing.T) {
	for _, accepted := range []string{"int8", "int64", "[]int32"} {
		if !IsCompositeNumericFieldType(accepted) {
			t.Errorf("%s must be accepted", accepted)
		}
	}
	for _, rejected := range []string{"string", "float64", "[]string", "uint8"} {
		if IsCompositeNumericFieldType(rejected) {
			t.Errorf("%s must be rejected", rejected)
		}
	}
}

// The group ID is stored inside every cache-group freshness row and client cache key: it must stay
// the hash of the logical name, and two names that collide must both stay addressable.
func TestAllocateIndexGroupIDIsStableAndProbesPastCollisions(t *testing.T) {
	core := &TableCore{}
	first := core.AllocateIndexGroupID([]string{"company_id", "status"})
	if again := core.AllocateIndexGroupID([]string{"company_id", "status"}); again != first {
		t.Errorf("the same name must keep its ID: %d then %d", first, again)
	}
	if want := int16(BasicHashInt("company_id_status")); first != want {
		t.Errorf("the ID is the name's hash, got %d want %d", first, want)
	}

	// Force a collision: another logical name already owns the candidate ID.
	collidingCore := &TableCore{IndexGroupIDs: map[int16]string{first: "someone_else"}}
	if probed := collidingCore.AllocateIndexGroupID([]string{"company_id", "status"}); probed == first {
		t.Error("a taken ID must be probed past")
	}
}

func TestBasicHashIntReservesZero(t *testing.T) {
	if BasicHashInt("") == 0 || BasicHashInt("x") == 0 {
		t.Error("0 is the invalid sentinel and must never be returned")
	}
}

func TestMakeIndexGroupCacheKeyAndColumnName(t *testing.T) {
	if MakeIndexGroupCacheKey(1, -1) != (1<<32)|0xFFFFFFFF {
		t.Error("the hash occupies the low 32 bits as unsigned")
	}
	if got := MakeIndexGroupVirtualColumnName([]string{"a", "b"}, false); got != "zz_ig_a_b" {
		t.Errorf("scalar group column, got %s", got)
	}
	if got := MakeIndexGroupVirtualColumnName([]string{"a"}, true); !strings.HasPrefix(got, "zz_igs_") {
		t.Errorf("collection group column, got %s", got)
	}
}

func TestDeltaSlotBitsEndWithUpdated(t *testing.T) {
	deltaIndex := DeltaIndexInfo{PinnedSlotBits: []int64{8, 12}}
	if got := deltaIndex.SlotBits(); !slices.Equal(got, []int64{8, 12, DeltaUpdatedSlotBits}) {
		t.Errorf("pinned slots then updated, got %v", got)
	}
	if len(deltaIndex.PinnedSlotBits) != 2 {
		t.Error("SlotBits must not mutate the index")
	}
}
