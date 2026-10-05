package dataframe

import (
	"errors"
	"testing"
	"time"
)

// TestFrameLock walks a lock's life: taken, refused while live, renewed by its holder's writes, taken
// over once expired (the old holder's writes and release then fail to touch it), released and taken
// again at once, and renewed by a holder whose lock expired but nobody took over.
func TestFrameLock(t *testing.T) {
	store := NewMemoryStore()
	frame := &Frame{Folder: "entity/frame/"}
	clock := time.Unix(1_000_000_000, 0)
	now := func() time.Time { return clock }
	assertTaken := func(step string, isTakenWanted bool) *Lock {
		t.Helper()
		lock, err := TakeLock(store, frame, now)
		if err != nil || (lock != nil) != isTakenWanted {
			t.Fatalf("%s: taken %v, err %v; want taken %v", step, lock != nil, err, isTakenWanted)
		}
		return lock
	}

	first := assertTaken("a free lock", true)
	assertTaken("a live lock", false)
	// With less than lockRenewBelow left, a write renews the lock past its first expiry.
	clock = clock.Add(LockDuration - lockRenewBelow + time.Second)
	if err := first.put("entity/frame/1", []byte{1}); err != nil {
		t.Fatalf("a write of the holder: %v", err)
	}
	clock = clock.Add(lockRenewBelow)
	assertTaken("a renewed lock, past its first expiry", false)

	clock = clock.Add(LockDuration)
	second := assertTaken("an expired lock", true)
	if err := first.put("entity/frame/1", []byte{2}); !errors.Is(err, ErrLockLost) {
		t.Fatalf("a write of the holder whose lock was taken over: err %v, want ErrLockLost", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("the release of a lock taken over: %v", err)
	}
	assertTaken("a lock the previous holder released after losing it", false)

	if err := second.Release(); err != nil {
		t.Fatalf("a release: %v", err)
	}
	third := assertTaken("a released lock", true)
	clock = clock.Add(2 * LockDuration)
	if err := third.put("entity/frame/1", []byte{3}); err != nil {
		t.Fatalf("a write of a holder whose lock expired but nobody took over: %v", err)
	}
	assertTaken("a lock renewed after it expired", false)
}
