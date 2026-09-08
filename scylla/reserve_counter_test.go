package scylla

import (
	"errors"
	"testing"
)

// installReserveCounterRange points the allocator hook at a recorder for one test and restores
// whatever was there before.
func installReserveCounterRange(
	t *testing.T, reserve func(keyspace, name string, increment int) (int64, error),
) {
	t.Helper()
	previous := ReserveCounterRange
	ReserveCounterRange = reserve
	t.Cleanup(func() { ReserveCounterRange = previous })
}

// The hook has to receive the counter name verbatim: the external allocator writes it into the same
// `sequences` row the ORM would have, so a name mangled on the way there would silently start a
// second, parallel counter for the same table.
func TestReserveCounterRangeReceivesTheRequestUnchanged(t *testing.T) {
	var gotKeyspace, gotName string
	var gotIncrement int
	installReserveCounterRange(t, func(keyspace, name string, increment int) (int64, error) {
		gotKeyspace, gotName, gotIncrement = keyspace, name, increment
		return 4242, nil
	})

	start, err := reserveCounter("genix", "x12_productos_0", 7)
	if err != nil {
		t.Fatal(err)
	}
	if start != 4242 {
		t.Fatalf("start = %d; want the value the allocator returned", start)
	}
	if gotKeyspace != "genix" || gotName != "x12_productos_0" || gotIncrement != 7 {
		t.Fatalf("hook saw (%q, %q, %d); want (\"genix\", \"x12_productos_0\", 7)",
			gotKeyspace, gotName, gotIncrement)
	}
}

// GetAutoincrementID is the public entry point for application counters, and it must route through
// the same allocator as the insert path — a counter served by two allocators is the exact collision
// the hook exists to remove.
func TestGetAutoincrementIDRoutesThroughTheHook(t *testing.T) {
	var gotName string
	var gotIncrement int
	installReserveCounterRange(t, func(_ string, name string, increment int) (int64, error) {
		gotName, gotIncrement = name, increment
		return 900, nil
	})

	start, err := GetAutoincrementID("images_55", 3)
	if err != nil {
		t.Fatal(err)
	}
	if start != 900 || gotName != "images_55" || gotIncrement != 3 {
		t.Fatalf("GetAutoincrementID gave %d for (%q, %d); want 900 for (\"images_55\", 3)",
			start, gotName, gotIncrement)
	}
}

// A recordsSize below one still has to reserve something, because the caller is going to use the
// value it gets back as an id.
func TestGetAutoincrementIDNormalizesAnEmptyRequest(t *testing.T) {
	var gotIncrement int
	installReserveCounterRange(t, func(_ string, _ string, increment int) (int64, error) {
		gotIncrement = increment
		return 1, nil
	})

	if _, err := GetAutoincrementID("counter", 0); err != nil {
		t.Fatal(err)
	}
	if gotIncrement != 1 {
		t.Fatalf("increment = %d; want it normalized to 1", gotIncrement)
	}
}

// Fails closed. There is deliberately no fallback to GetCounter: once an external allocator owns
// the row, the direct path would hand out a range that allocator already believes is its own.
func TestReserveCounterRangeErrorsPropagateInsteadOfFallingBack(t *testing.T) {
	allocatorDown := errors.New("fareward is unavailable")
	installReserveCounterRange(t, func(string, string, int) (int64, error) {
		return 0, allocatorDown
	})

	start, err := reserveCounter("genix", "x1_ventas_0", 1)
	if !errors.Is(err, allocatorDown) {
		t.Fatalf("error = %v; want the allocator's own error", err)
	}
	if start != 0 {
		t.Fatalf("start = %d; want no value when the allocator refused", start)
	}
}

func installSetCounterValue(
	t *testing.T, set func(keyspace, name string, value int64) (int64, error),
) {
	t.Helper()
	previous := SetCounterValue
	SetCounterValue = set
	t.Cleanup(func() { SetCounterValue = previous })
}

// ResetCounter must hand the move to the allocator when one is installed, and must not touch the
// row itself. Doing the read-and-write here would leave the allocator serving ids from a range
// derived from the value just erased, and the next range it claimed would repeat them.
//
// The absence of a database connection in this test is the assertion: the direct path would panic
// reaching for one, so reaching the end proves nothing but the hook ran.
func TestResetCounterDelegatesToTheAllocator(t *testing.T) {
	var gotKeyspace, gotName string
	var gotValue int64
	installSetCounterValue(t, func(keyspace, name string, value int64) (int64, error) {
		gotKeyspace, gotName, gotValue = keyspace, name, value
		return 900, nil
	})

	previous, err := applyCounterReset("genix", "x7_ventas_0", 42)
	if err != nil {
		t.Fatal(err)
	}
	if previous != 900 {
		t.Fatalf("previous = %d; want what the allocator reported it replaced", previous)
	}
	if gotKeyspace != "genix" || gotName != "x7_ventas_0" || gotValue != 42 {
		t.Fatalf("allocator saw (%q, %q, %d); want (\"genix\", \"x7_ventas_0\", 42)",
			gotKeyspace, gotName, gotValue)
	}
}

// A refused assignment must abort the reset rather than fall through to the direct write, which
// would move the counter behind the allocator's back — the precise corruption this path avoids.
func TestResetCounterPropagatesAnAllocatorRefusal(t *testing.T) {
	installSetCounterValue(t, func(string, string, int64) (int64, error) {
		return 0, errors.New("fareward is unavailable")
	})

	if _, err := applyCounterReset("genix", "x7_ventas_0", 42); err == nil {
		t.Fatal("a refused assignment must not report the counter as reset")
	}
}

// The write path reaches the allocator through getWriteCounterValue, so the hook has to be visible
// from there too — that variable is what the updated_version sequence and autoincrement keys share.
func TestTheWritePathUsesTheHook(t *testing.T) {
	installReserveCounterRange(t, func(_ string, name string, _ int) (int64, error) {
		if name != "x3_ventas_updated" {
			t.Fatalf("counter name = %q; want the write sequence's own name", name)
		}
		return 77, nil
	})

	start, err := getWriteCounterValue("genix", "x3_ventas_updated", 1)
	if err != nil {
		t.Fatal(err)
	}
	if start != 77 {
		t.Fatalf("start = %d; want 77", start)
	}
}
