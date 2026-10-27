package dataframe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// FileState keeps the State of each partition of a frame in the file store, for a
// backend with no conditional write of its own (scylla, whose LWT does not run on
// tablets):
//
//	dtf/<table id>/<frame>/<partition>.state   {"g": generation, "s": shape, "w": snapshot}
//
// beside the partition's folder (<partition>/), never inside it, so a rebuild that
// clears the folder keeps it.
//
// The lock is an external one (AcquireLock: fareward in genix), one per partition,
// there so two compactions don't do the same work and so a read that finds the
// partition due waits for the compaction under way instead of starting another.
// Correctness does not rest on it: a commit is written conditioned on the ETag the
// state file had when the lock was taken, so a compaction whose lock lapsed and that
// another one overtook fails its commit, as its file writes already fail on their own
// ETags.
// ─────────────────────────────────────────────────────────────────────────────

// FileState implements the state half of Backend (ReadState, TakeLock, ReleaseLock, CommitSnapshot,
// CommitGeneration). A backend embeds one per Compact, Rebuild or Read call.
type FileState struct {
	Table *Table
	Store utils.FileStore
	// AcquireLock takes a lock, queuing up to wait behind its holder: release frees it, false when
	// the holder kept it past wait. lockName names the frame partition across tables (its folder).
	AcquireLock func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)

	mutex sync.Mutex
	// heldByHolder is, per lock this FileState took, how to release it and the ETag its commits are
	// conditioned on.
	heldByHolder map[string]*heldFileState
}

type heldFileState struct {
	release func()
	etag    string
}

// stateFileContent is the state file: short names, it is read on every frame read.
type stateFileContent struct {
	Generation string `json:"g,omitempty"`
	Shape      string `json:"s,omitempty"`
	Snapshot   int64  `json:"w,omitempty"`
}

// stateKeyOf is the store key of a partition's state file, beside the partition's folder.
func (frame *Frame) stateKeyOf(partition int64) string {
	return frame.Folder + strconv.FormatInt(partition, 10) + ".state"
}

// readStateFile reads a partition's state and its ETag; a missing file is a partition not built ("" ETag).
func (fileState *FileState) readStateFile(frame *Frame, partition int64) (State, string, error) {
	content, etag, err := fileState.Store.GetFile(frame.stateKeyOf(partition))
	if errors.Is(err, utils.ErrFileNotFound) {
		return State{}, "", nil
	}
	if err != nil {
		return State{}, "", fmt.Errorf("dataframe: %s reading the state of frame %q partition %d: %w", fileState.Table.Name, frame.Name, partition, err)
	}
	stored := stateFileContent{}
	if err := json.Unmarshal(content, &stored); err != nil {
		return State{}, "", fmt.Errorf("dataframe: %s the state of frame %q partition %d is corrupt: %w", fileState.Table.Name, frame.Name, partition, err)
	}
	return State{Generation: stored.Generation, Shape: stored.Shape, Snapshot: stored.Snapshot}, etag, nil
}

func (fileState *FileState) ReadState(frame *Frame, partition int64) (State, error) {
	state, _, err := fileState.readStateFile(frame, partition)
	return state, err
}

func (fileState *FileState) TakeLock(frame *Frame, partition int64, holderID string, now, lockedUntil time.Time, wait time.Duration) (State, bool, error) {
	release, isTaken, err := fileState.AcquireLock(frame.partitionFolderOf(partition), wait, lockedUntil.Sub(now))
	if err != nil || !isTaken {
		return State{}, false, err
	}
	state, etag, err := fileState.readStateFile(frame, partition)
	if err != nil {
		release()
		return State{}, false, err
	}
	fileState.mutex.Lock()
	defer fileState.mutex.Unlock()
	if fileState.heldByHolder == nil {
		fileState.heldByHolder = map[string]*heldFileState{}
	}
	fileState.heldByHolder[holderID] = &heldFileState{release: release, etag: etag}
	return state, true, nil
}

func (fileState *FileState) ReleaseLock(frame *Frame, partition int64, holderID string) {
	fileState.mutex.Lock()
	held := fileState.heldByHolder[holderID]
	delete(fileState.heldByHolder, holderID)
	fileState.mutex.Unlock()
	if held != nil {
		held.release()
	}
}

// CommitSnapshot moves w. The state file's ETag being the one read with the lock is what proves the
// generation and the snapshot are still state's.
func (fileState *FileState) CommitSnapshot(ctx context.Context, frame *Frame, partition int64, holderID string, state State, snapshot int64) error {
	return fileState.writeStateFile(ctx, frame, partition, holderID, stateFileContent{Generation: state.Generation, Shape: state.Shape, Snapshot: snapshot})
}

func (fileState *FileState) CommitGeneration(ctx context.Context, frame *Frame, partition int64, holderID string, generation string, snapshot int64) error {
	return fileState.writeStateFile(ctx, frame, partition, holderID, stateFileContent{Generation: generation, Shape: frame.Shape, Snapshot: snapshot})
}

// writeStateFile writes the state conditioned on the ETag the holder read, and keeps the new one for
// the holder's next commit.
func (fileState *FileState) writeStateFile(ctx context.Context, frame *Frame, partition int64, holderID string, content stateFileContent) error {
	fileState.mutex.Lock()
	held := fileState.heldByHolder[holderID]
	fileState.mutex.Unlock()
	if held == nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d: the lock is not held", fileState.Table.Name, frame.Name, partition)
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return err
	}
	etag, err := fileState.Store.PutFileIfMatch(ctx, frame.stateKeyOf(partition), encoded, held.etag)
	if err != nil {
		return fmt.Errorf("dataframe: %s writing the state of frame %q partition %d: %w", fileState.Table.Name, frame.Name, partition, err)
	}
	held.etag = etag
	return nil
}
