package dataframe

import (
	"context"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// TestFileStateCommitsOnlyOverWhatItRead: the state file starts as "not built", a commit lands under
// the lock that read it, a held lock refuses a second taker while another partition's lock is free,
// a partition's state is its own, and a holder overtaken by another commit (its lock lapsed) fails
// its own.
func TestFileStateCommitsOnlyOverWhatItRead(t *testing.T) {
	table := compileTestTable(t)
	frame := &table.Frames[0]
	store := utils.NewMemoryFileStore()
	isHeldByName := map[string]bool{}
	newFileState := func() *FileState {
		return &FileState{Table: table, Store: store, AcquireLock: func(lockName string, wait, lease time.Duration) (func(), bool, error) {
			if isHeldByName[lockName] {
				return nil, false, nil
			}
			isHeldByName[lockName] = true
			return func() { delete(isHeldByName, lockName) }, true, nil
		}}
	}
	ctx, now := context.Background(), time.Now()
	const partition, otherPartition = 4, 5

	first := newFileState()
	state, isTaken, err := first.TakeLock(frame, partition, "a", now, now.Add(time.Minute), 0)
	if err != nil || !isTaken || state != (State{}) {
		t.Fatalf("first lock: %+v %v %v", state, isTaken, err)
	}
	if _, isTaken, _ := newFileState().TakeLock(frame, partition, "b", now, now.Add(time.Minute), 0); isTaken {
		t.Fatal("a held lock was taken twice")
	}
	other := newFileState()
	if _, isTaken, _ := other.TakeLock(frame, otherPartition, "c", now, now.Add(time.Minute), 0); !isTaken {
		t.Fatal("another partition's lock must be free")
	}
	other.ReleaseLock(frame, otherPartition, "c")
	if err := first.CommitGeneration(ctx, frame, partition, "a", "g1", 100); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitSnapshot(ctx, frame, partition, "a", State{Generation: "g1", Shape: frame.Shape, Snapshot: 100}, 160); err != nil {
		t.Fatal(err)
	}
	if state, _ := newFileState().ReadState(frame, partition); state != (State{Generation: "g1", Shape: frame.Shape, Snapshot: 160}) {
		t.Fatalf("stored state: %+v", state)
	}
	if state, _ := newFileState().ReadState(frame, otherPartition); state != (State{}) {
		t.Fatalf("another partition's state: %+v", state)
	}

	// The lock lapses: a second run takes it and commits; the first one's next commit must fail.
	clear(isHeldByName)
	second := newFileState()
	state, _, _ = second.TakeLock(frame, partition, "b", now, now.Add(time.Minute), 0)
	if err := second.CommitSnapshot(ctx, frame, partition, "b", state, 200); err != nil {
		t.Fatal(err)
	}
	if err := first.CommitSnapshot(ctx, frame, partition, "a", State{Generation: "g1", Shape: frame.Shape, Snapshot: 160}, 220); err == nil {
		t.Fatal("an overtaken holder committed")
	}
	first.ReleaseLock(frame, partition, "a")
	second.ReleaseLock(frame, partition, "b")
	if len(isHeldByName) != 0 {
		t.Fatal("ReleaseLock left the lock held")
	}
}
