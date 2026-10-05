package dataframe

import (
	"context"
	"time"
)

// Table is what a frame's runs, rebuilds and reads need of the database that holds its records: the
// frame's state record, the table's write sequence, and four record reads. A driver implements it once
// per table (dynamo: frameTable). Every read is consistent: a run that misses a write it should see
// leaves the files wrong until a rebuild.
type Table interface {
	// Now is the driver's clock.
	Now() time.Time

	// ReadState reads the frame's state record (State, ReadAt left to the caller).
	ReadState(frame *Frame) (State, error)
	// PushCheckpoint sets the newest checkpoint to checkpoint, read at checkpointTime, and the previous
	// one to seen's newest, conditioned on the newest still being seen's (none, when seen has none). It
	// returns the state after it, and false with no error when another pusher went first.
	PushCheckpoint(frame *Frame, seen State, checkpoint, checkpointTime int64) (State, bool, error)
	// ResetState starts the frame over at its current Shape: it sets the shape and the newest
	// checkpoint, and removes the snapshot, the previous checkpoint and the extended days. ctx is the
	// lock's write context.
	ResetState(ctx context.Context, frame *Frame, checkpoint, checkpointTime int64) error
	// CommitSnapshot sets the frame's snapshot, adds addedDays to the extended days and removes
	// mergedDays from them (one of the two is empty). ctx is the lock's write context.
	CommitSnapshot(ctx context.Context, frame *Frame, snapshot int64, addedDays, mergedDays []int64) error

	// CurrentWriteVersion reads the table's UpdatedVersion sequence: the last version reserved.
	CurrentWriteVersion() (int64, error)

	// ReadAllRecords reads every record of the table.
	ReadAllRecords(frame *Frame) ([]RecordState, error)
	// ReadRecordsInRange reads the records whose Keys[0] is in [fromKey, toKey].
	ReadRecordsInRange(frame *Frame, fromKey, toKey int64) ([]RecordState, error)
	// ReadRecordsWrittenAfter reads every record whose UpdatedVersion is above snapshot, including one
	// rewritten while the read ran.
	ReadRecordsWrittenAfter(frame *Frame, snapshot int64) ([]RecordState, error)
	// ReadRecordsBySK reads records by their sk; missing ones are left out.
	ReadRecordsBySK(frame *Frame, sks []string) ([]RecordState, error)
}

// State is a frame's state record: the snapshot its files have reached and the checkpoints its
// compactions target. Only the holder of the frame's lock moves Snapshot, ExtendedDays and Shape;
// anyone may push a checkpoint.
type State struct {
	// ReadAt is when the state was read (unix seconds, taken before the read).
	ReadAt int64
	// Snapshot (w) is the snapshot every file holds; none: not built (a new frame, or a new shape).
	HasSnapshot bool
	Snapshot    int64
	// Target (nx) is the newest checkpoint, a value of the write sequence, read at TargetTime (nxt, unix
	// seconds).
	HasTarget  bool
	Target     int64
	TargetTime int64
	// PreviousTarget (px) is the checkpoint before it, settled.
	HasPreviousTarget bool
	PreviousTarget    int64
	// ExtendedDays (ix) are the days whose _ixt has entries to merge into their _idx.
	ExtendedDays []int64
	// Shape (sh) is the shape the files were built with.
	HasShape bool
	Shape    uint32
}

// IsBuiltAs reports whether the frame's files are built, in its current shape.
func (state State) IsBuiltAs(frame *Frame) bool {
	return state.HasSnapshot && state.HasShape && state.Shape == frame.Shape
}

// settledTarget is the newest checkpoint settled at now: Target once it is more than settle old
// (TargetTime was rounded down), else PreviousTarget, which a push replaces only once Target has
// settled. false when there is neither.
func (state State) settledTarget(now int64) (int64, bool) {
	if state.HasTarget && now-state.TargetTime > settleSeconds() {
		return state.Target, true
	}
	return state.PreviousTarget, state.HasPreviousTarget
}

// readState reads the frame's state, stamped with when the read began.
func readState(table Table, frame *Frame) (State, error) {
	readAt := table.Now().Unix()
	state, err := table.ReadState(frame)
	state.ReadAt = readAt
	return state, err
}

// pushCheckpoint pushes a checkpoint once the newest has settled, or when there is none: the write
// sequence value now becomes the newest, and the newest the previous one. The sequence is read before
// the clock, so every version up to the value was reserved by the time recorded. Of two concurrent
// pushers one goes through, and the other keeps the state it read, whose checkpoints are as valid. It
// returns the state after.
func pushCheckpoint(table Table, frame *Frame, state State) (State, error) {
	if state.HasTarget && table.Now().Unix()-state.TargetTime <= settleSeconds() {
		return state, nil
	}
	checkpoint, err := table.CurrentWriteVersion()
	if err != nil {
		return state, err
	}
	now := table.Now().Unix()
	pushed, isPushed, err := table.PushCheckpoint(frame, state, checkpoint, now)
	if err != nil || !isPushed {
		return state, err
	}
	pushed.ReadAt = now
	return pushed, nil
}
