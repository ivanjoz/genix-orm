package cache

import (
	"testing"
)

func TestDeltaSnapshotKeyNamesThePrefixAndTheLimit(t *testing.T) {
	cases := []struct {
		prefix   []int64
		limit    int32
		expected string
	}{
		{[]int64{5, 1}, 0, "delta-snapshot/sale_orders/7/warehouse_id_status/5_1.l0.cb"},
		{[]int64{-3}, 500, "delta-snapshot/sale_orders/7/warehouse_id_status/-3.l500.cb"},
		{nil, 0, "delta-snapshot/sale_orders/7/warehouse_id_status/all.l0.cb"},
	}
	for _, keyCase := range cases {
		if key := DeltaSnapshotKey("sale_orders", 7, "warehouse_id_status", keyCase.prefix, keyCase.limit); key != keyCase.expected {
			t.Fatalf("got %q, expected %q", key, keyCase.expected)
		}
	}
}

// The held hash counts the rows inside [from, to) only, and leaves out the excluded IDs: it is the
// value genix-ui computes over its records, so the server can check a snapshot the same way.
func TestDeltaHeldWindowHash(t *testing.T) {
	rows := []DeltaHashRow{
		{ID: 1, Updated: 100, Version: 1}, // window start, counted
		{ID: 2, Updated: 149, Version: 3}, // counted
		{ID: 3, Updated: 150, Version: 1}, // window end is excluded
		{ID: 4, Updated: 99, Version: 1},  // before the window
		{ID: 5, Updated: 120, Version: 2}, // excluded: the sync brought it
	}
	got := DeltaHeldWindowHash(rows, 100, 150, map[int64]bool{5: true})
	if expected := DeltaRecordHash(1, 1) + DeltaRecordHash(2, 3); got != expected {
		t.Fatalf("got %08x, expected %08x", got, expected)
	}
	if DeltaHeldWindowHash(nil, 100, 150, nil) != 0 {
		t.Fatal("an empty window hashes to 0")
	}
}
