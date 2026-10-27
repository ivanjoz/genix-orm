package dataframe

import (
	"context"
	"errors"
	"time"
)

// The frame files live in genix-orm's file store (utils/filestore): the same one, and the same S3
// bucket, as the backend's other files.

// ErrFrameNotBuilt: the partition has no files in the frame's current shape, and the read that found
// it could not build them (another holder kept the lock past the wait).
var ErrFrameNotBuilt = errors.New("db: the DataFrame is not built yet for this partition: retry")

// State is the state of one partition of a frame, which the backend keeps (scylla: a file per
// partition; dynamo: one item per frame, its only partition).
type State struct {
	// Generation is the folder of the partition's current files; "" when it is not built.
	Generation string
	// Shape is the Frame.Shape that generation was built with: another one is a rebuild.
	Shape string
	// Snapshot (w) is a SUnixTime: every file holds the records at least as of it.
	Snapshot int64
}

// Backend is what a database gives the frames of one table: their state and lock, and its records as
// Histories. Every write of the table must keep what its records held over the last ~36 h (dynamo:
// Table.NextChangelog and the created moment; scylla: a change log table), and never delete a record
// physically.
//
// A frame follows the table's partition: every History names its partition, and each partition has
// its own files, versions file, state and lock, so partitions are compacted and rebuilt apart. A
// backend without partitions puts every record in partition 0.
type Backend interface {
	ReadState(frame *Frame, partition int64) (State, error)
	// TakeLock sets the partition's lock to holderID until lockedUntil, waiting up to wait for it to be
	// free (absent or expired), and returns the state; false when another holder kept it.
	TakeLock(frame *Frame, partition int64, holderID string, now, lockedUntil time.Time, wait time.Duration) (State, bool, error)
	// ReleaseLock frees the lock if holderID still has it (best effort).
	ReleaseLock(frame *Frame, partition int64, holderID string)
	// CommitSnapshot moves the snapshot from state.Snapshot to snapshot, only while holderID holds
	// the lock and the generation and snapshot are still state's.
	CommitSnapshot(ctx context.Context, frame *Frame, partition int64, holderID string, state State, snapshot int64) error
	// CommitGeneration makes generation current, with frame.Shape and snapshot, while holderID holds the lock.
	CommitGeneration(ctx context.Context, frame *Frame, partition int64, holderID string, generation string, snapshot int64) error
	// PartitionHistoriesWrittenAfter reads every record of one partition written after moment (dynamo:
	// a GSI on Updated). It may miss a write from the last couple of minutes, never an older one.
	PartitionHistoriesWrittenAfter(partition int64, moment int64) ([]History, error)
	// ScanPartitionHistories reads every record of one partition, page by page.
	ScanPartitionHistories(partition int64, onPage func(histories []History) error) error
}
