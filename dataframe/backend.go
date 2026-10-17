package dataframe

import (
	"context"
	"errors"
	"time"
)

// The frame files live in genix-orm's file store (utils/filestore): the same one, and the same S3
// bucket, as the backend's other files.

// ErrFrameNotBuilt: the frame has no files in its current shape yet. The next Materialize builds it.
var ErrFrameNotBuilt = errors.New("db: the DataFrame is not built yet: the next MaterializeDataFrames builds it")

// State is a frame's state, which the backend keeps (dynamo: one item per frame).
type State struct {
	// Generation is the folder the current files are in; "" when the frame is not built.
	Generation string
	// Shape is the Frame.Shape that generation was built with: another one is a rebuild.
	Shape string
	// Snapshot (w) is a SUnixTime: every file holds the records at least as of it.
	Snapshot int64
}

// Backend is what a database gives the frames of one table: their state and lock, and its records as
// Histories. Every write of the table must keep the records' changelogs (Table.NextChangelog) and
// their created moment, and never delete a record physically.
type Backend interface {
	ReadState(frame *Frame) (State, error)
	// TakeLock sets the frame's lock to holderID until lockedUntil when it is free (absent or expired
	// at now) and returns the state; false when another holder has it.
	TakeLock(frame *Frame, holderID string, now, lockedUntil time.Time) (State, bool, error)
	// ReleaseLock frees the lock if holderID still has it (best effort).
	ReleaseLock(frame *Frame, holderID string)
	// CommitSnapshot moves the snapshot from state.Snapshot to snapshot, only while holderID holds
	// the lock and the generation and snapshot are still state's.
	CommitSnapshot(ctx context.Context, frame *Frame, holderID string, state State, snapshot int64) error
	// CommitGeneration makes generation current, with frame.Shape and snapshot, while holderID holds the lock.
	CommitGeneration(ctx context.Context, frame *Frame, holderID string, generation string, snapshot int64) error
	// HistoriesWrittenAfter reads every record whose Updated is after moment (dynamo: a GSI on
	// Updated). It may miss a write from the last couple of minutes, never an older one.
	HistoriesWrittenAfter(moment int64) ([]History, error)
	// ScanHistories reads every record of the table, page by page.
	ScanHistories(onPage func(histories []History) error) error
}
