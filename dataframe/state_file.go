package dataframe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// FileState keeps each frame's State in the file store, for a backend with no
// conditional write of its own (scylla, whose LWT does not run on tablets):
//
//	dtf/<table id>/<frame>.state   {"g": generation, "s": shape, "w": snapshot}
//
// The lock is an external one (AcquireLock: fareward in genix), there so two runs
// don't do the same work. Correctness does not rest on it: a commit is written
// conditioned on the ETag the state file had when the lock was taken, so a run whose
// lock lapsed and that another run overtook fails its commit, as its file writes
// already fail on their own ETags.
// ─────────────────────────────────────────────────────────────────────────────

// FileState implements the state half of Backend (ReadState, TakeLock, ReleaseLock, CommitSnapshot,
// CommitGeneration). A backend embeds one per Materialize, Rebuild or Read call.
type FileState struct {
	Table *Table
	Store utils.FileStore
	// AcquireLock takes the frame's lock without waiting: release frees it, false when another holder
	// has it. lockName names the frame across tables (its folder).
	AcquireLock func(lockName string, lease time.Duration) (release func(), isTaken bool, err error)

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

// stateKeyOf is the store key of a frame's state file, beside the frame's folder and never inside one.
func (table *Table) stateKeyOf(frame *Frame) string {
	return table.Folder + frame.Name + ".state"
}

// readStateFile reads a frame's state and its ETag; a missing file is a frame not built ("" ETag).
func (fileState *FileState) readStateFile(frame *Frame) (State, string, error) {
	content, etag, err := fileState.Store.GetFile(fileState.Table.stateKeyOf(frame))
	if errors.Is(err, utils.ErrFileNotFound) {
		return State{}, "", nil
	}
	if err != nil {
		return State{}, "", fmt.Errorf("dataframe: %s reading the state of frame %q: %w", fileState.Table.Name, frame.Name, err)
	}
	stored := stateFileContent{}
	if err := json.Unmarshal(content, &stored); err != nil {
		return State{}, "", fmt.Errorf("dataframe: %s the state of frame %q is corrupt: %w", fileState.Table.Name, frame.Name, err)
	}
	return State{Generation: stored.Generation, Shape: stored.Shape, Snapshot: stored.Snapshot}, etag, nil
}

func (fileState *FileState) ReadState(frame *Frame) (State, error) {
	state, _, err := fileState.readStateFile(frame)
	return state, err
}

func (fileState *FileState) TakeLock(frame *Frame, holderID string, now, lockedUntil time.Time) (State, bool, error) {
	release, isTaken, err := fileState.AcquireLock(frame.Folder, lockedUntil.Sub(now))
	if err != nil || !isTaken {
		return State{}, false, err
	}
	state, etag, err := fileState.readStateFile(frame)
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

func (fileState *FileState) ReleaseLock(frame *Frame, holderID string) {
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
func (fileState *FileState) CommitSnapshot(ctx context.Context, frame *Frame, holderID string, state State, snapshot int64) error {
	return fileState.writeStateFile(ctx, frame, holderID, stateFileContent{Generation: state.Generation, Shape: state.Shape, Snapshot: snapshot})
}

func (fileState *FileState) CommitGeneration(ctx context.Context, frame *Frame, holderID string, generation string, snapshot int64) error {
	return fileState.writeStateFile(ctx, frame, holderID, stateFileContent{Generation: generation, Shape: frame.Shape, Snapshot: snapshot})
}

// writeStateFile writes the state conditioned on the ETag the holder read, and keeps the new one for
// the holder's next commit.
func (fileState *FileState) writeStateFile(ctx context.Context, frame *Frame, holderID string, content stateFileContent) error {
	fileState.mutex.Lock()
	held := fileState.heldByHolder[holderID]
	fileState.mutex.Unlock()
	if held == nil {
		return fmt.Errorf("dataframe: %s frame %q: the lock is not held", fileState.Table.Name, frame.Name)
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return err
	}
	etag, err := fileState.Store.PutFileIfMatch(ctx, fileState.Table.stateKeyOf(frame), encoded, held.etag)
	if err != nil {
		return fmt.Errorf("dataframe: %s writing the state of frame %q: %w", fileState.Table.Name, frame.Name, err)
	}
	held.etag = etag
	return nil
}
