package cache

import (
	"strings"
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
		if key := DeltaSnapshotKey("sale_orders", 7, "warehouse_id_status", keyCase.prefix, keyCase.limit, nil); key != keyCase.expected {
			t.Fatalf("got %q, expected %q", key, keyCase.expected)
		}
	}
}

// A projected read has its own file per column list, and never the unprojected one.
func TestDeltaSnapshotKeyNamesTheProjection(t *testing.T) {
	whole := DeltaSnapshotKey("products", 7, "status", nil, 0, nil)
	projected := DeltaSnapshotKey("products", 7, "status", nil, 0, []string{"company_id", "id", "name"})
	again := DeltaSnapshotKey("products", 7, "status", nil, 0, []string{"company_id", "id", "name"})
	other := DeltaSnapshotKey("products", 7, "status", nil, 0, []string{"company_id", "id", "price"})
	if projected == whole || projected == other || projected != again {
		t.Fatalf("whole %q, projected %q, again %q, other %q", whole, projected, again, other)
	}
	if !strings.HasPrefix(projected, "delta-snapshot/products/7/status/all.l0.p") {
		t.Fatalf("got %q", projected)
	}
}

// Both snapshot kinds are recognised, and the protocol files are not.
func TestIsRecordSnapshotKey(t *testing.T) {
	for _, snapshotKey := range []string{
		DeltaSnapshotKey("products", 7, "status", nil, 0, []string{"id"}),
		CacheGroupSnapshotKey("sale_orders", 7, 2, -184467),
	} {
		if !IsRecordSnapshotKey(snapshotKey) {
			t.Fatalf("%q is a snapshot", snapshotKey)
		}
	}
	for _, protocolKey := range []string{"delta-log/products/7.log", "ids-cache/products/7/1.bin"} {
		if IsRecordSnapshotKey(protocolKey) {
			t.Fatalf("%q is not a snapshot", protocolKey)
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
