package dataframe

import (
	"errors"
	"fmt"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// The runs, rebuilds and reads of a frame, over a driver's Table (../DATA_FRAMES.md,
// sections 4 and 7 to 10). A version is stamped before its write lands, so a
// compaction targets a checkpoint only once it is settle old: every write that took
// a version up to it has landed or given up (write.go). Every compaction (the
// scheduled run, a rebuild, the express compaction of a fresh read) holds the
// frame's lock while it writes the files and the snapshot, so one writes at a time
// (lock.go). The file work is run.go's.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// A rebuild waits up to a minute for the compaction holding the lock: it takes seconds, and a
	// crashed one's lock expires within LockDuration.
	rebuildLockWaitAttempts = 12
	rebuildLockWaitInterval = 5 * time.Second
	// freshReadAttempts bounds the fresh reads a run committing meanwhile restarts.
	freshReadAttempts = 3
	// expressMinChanges is how many records must have changed between the snapshot and the target for
	// a fresh read to write them into the files.
	expressMinChanges = 100
)

// Materialize brings the frame up to date, as the scheduled run: it takes the frame's lock and
// compacts from its snapshot to its newest settled checkpoint, then pushes a new checkpoint for the
// next run. A frame whose lock another compaction holds is skipped, with no error.
func Materialize(table Table, frame *Frame) error {
	store, err := configured()
	if err != nil {
		return err
	}
	lock, err := TakeLock(store, frame, table.Now)
	if err != nil || lock == nil {
		return err
	}
	// Released after a failure too, so the next tick retries; a lock not released expires on its own.
	defer lock.Release()
	// Read once the lock is held: until it is released, no other compaction moves the snapshot.
	state, err := readState(table, frame)
	if err != nil {
		return err
	}
	return runFrame(table, store, frame, lock, state)
}

// runFrame is one run of a frame, as lock's holder.
func runFrame(table Table, store Store, frame *Frame, lock *Lock, state State) error {
	// A new frame, or one whose shape changed: the old shape's log goes, the snapshot is removed with
	// the checkpoints and the extended days, and the first run after the checkpoint read now settles
	// rebuilds every file at it.
	if !state.HasShape || state.Shape != frame.Shape {
		if state.HasShape {
			if err := lock.Delete(ShapeLogKey(frame.Folder, state.Shape)); err != nil {
				return err
			}
		}
		checkpoint, err := table.CurrentWriteVersion()
		if err != nil {
			return err
		}
		ctx, cancel, err := lock.WriteContext()
		if err != nil {
			return err
		}
		defer cancel()
		return table.ResetState(ctx, frame, checkpoint, table.Now().Unix())
	}
	state, err := pushCheckpoint(table, frame, state)
	if err != nil {
		return err
	}
	target, hasTarget := state.settledTarget(table.Now().Unix())

	snapshot := state.Snapshot
	switch {
	case !state.HasSnapshot && !hasTarget:
		return nil
	case !state.HasSnapshot:
		var records []RecordState
		if records, err = table.ReadAllRecords(frame); err == nil {
			err = RebuildAllFiles(lock, frame, records, target)
		}
		snapshot = target
	default:
		// An express compaction may already have moved the snapshot past the target: then only the days
		// in ExtendedDays are merged.
		if hasTarget && target > snapshot {
			snapshot = target
		}
		readWrittenAfter, readBySK := recordReaders(table, frame)
		err = CompactFrame(lock, frame, state.Snapshot, snapshot, state.ExtendedDays, readWrittenAfter, readBySK)
	}
	if err != nil {
		return err
	}
	if err := commitSnapshot(table, frame, lock, snapshot, nil, state.ExtendedDays); err != nil {
		return err
	}
	// After the commit: a crash here leaves entries at or below the snapshot, which every reader ignores.
	return TruncateLog(store, frame, snapshot)
}

// commitSnapshot is Table.CommitSnapshot as lock's holder: bounded as the holder's file writes are
// (Lock.WriteContext), so it lands before another compaction can take the lock over.
func commitSnapshot(table Table, frame *Frame, lock *Lock, snapshot int64, addedDays, mergedDays []int64) error {
	ctx, cancel, err := lock.WriteContext()
	if err != nil {
		return err
	}
	defer cancel()
	return table.CommitSnapshot(ctx, frame, snapshot, addedDays, mergedDays)
}

// recordReaders are the record reads CompactFrame and ReadFreshFiles take, bound to the frame.
func recordReaders(table Table, frame *Frame) (
	readWrittenAfter func(snapshot int64) ([]RecordState, error), readBySK func(sks []string) ([]RecordState, error)) {
	readWrittenAfter = func(snapshot int64) ([]RecordState, error) { return table.ReadRecordsWrittenAfter(frame, snapshot) }
	readBySK = func(sks []string) ([]RecordState, error) { return table.ReadRecordsBySK(frame, sks) }
	return readWrittenAfter, readBySK
}

// ─────────────────────────────────────────────────────────────────────────────
// Rebuilds
// ─────────────────────────────────────────────────────────────────────────────

// RebuildRange recomputes from the records, at the frame's snapshot, the files whose Keys[0] is in
// [fromKey, toKey] (at most MaxFirstKeys values), and writes only those that differ. It is the fix for
// drift: a write from a Lambda still on older code, an insert of a stored record without a stored read,
// files edited by hand. It takes the frame's lock, waiting up to a minute for the compaction holding it.
func RebuildRange(table Table, frame *Frame, fromKey, toKey int64) error {
	if fromKey < 0 || toKey < fromKey || toKey-fromKey >= MaxFirstKeys {
		return fmt.Errorf("a rebuild takes 0 <= fromKey <= toKey, at most %d values", MaxFirstKeys)
	}
	return rebuildUnderLock(table, frame, func(lock *Lock, snapshot int64) error {
		records, err := table.ReadRecordsInRange(frame, fromKey, toKey)
		if err != nil {
			return err
		}
		return RebuildFilesInRange(lock, frame, records, snapshot, fromKey, toKey)
	})
}

// RebuildAll recomputes every file of the frame from all the records, at the frame's snapshot, and
// deletes the files nothing produces. It reads the whole table and rewrites every file.
func RebuildAll(table Table, frame *Frame) error {
	return rebuildUnderLock(table, frame, func(lock *Lock, snapshot int64) error {
		records, err := table.ReadAllRecords(frame)
		if err != nil {
			return err
		}
		return RebuildAllFiles(lock, frame, records, snapshot)
	})
}

// rebuildUnderLock runs rebuild under the frame's lock, at the frame's snapshot. The snapshot does not
// move, so the next run goes on from where the files now are.
func rebuildUnderLock(table Table, frame *Frame, rebuild func(lock *Lock, snapshot int64) error) error {
	store, err := configured()
	if err != nil {
		return err
	}
	var lock *Lock
	for attempt := 1; lock == nil; attempt++ {
		if lock, err = TakeLock(store, frame, table.Now); err != nil {
			return err
		}
		if lock == nil && attempt == rebuildLockWaitAttempts {
			return errors.New("the frame is held by a compaction: retry in a few minutes")
		}
		if lock == nil {
			time.Sleep(rebuildLockWaitInterval)
		}
	}
	state, rebuildErr := readState(table, frame)
	if rebuildErr == nil && !state.IsBuiltAs(frame) {
		rebuildErr = ErrNotBuilt
	}
	if rebuildErr == nil {
		rebuildErr = rebuild(lock, state.Snapshot)
	}
	return errors.Join(rebuildErr, lock.Release())
}

// ─────────────────────────────────────────────────────────────────────────────
// Reads
// ─────────────────────────────────────────────────────────────────────────────

// Read reads the files whose Keys[0] is in [fromKey, toKey] and whose later Keys start with
// pinnedKeys, in key order: as the frame's last compaction left them (10 to 30 minutes behind the
// records) or, isFresh, as the records hold them now. selectFiles, when set, gets every file the read
// would GET, before the first GET, and returns the ones to read; its error ends the read as is. It
// returns the snapshot the files hold. A frame not built in its current shape returns ErrNotBuilt.
func Read(table Table, frame *Frame, fromKey, toKey int64, pinnedKeys []int64, isFresh bool,
	selectFiles func(fileKeys [][MaxKeys]int64) ([][MaxKeys]int64, error)) ([][MaxKeys]int64, []File, int64, error) {
	if toKey < fromKey || toKey-fromKey >= MaxFirstKeys || len(pinnedKeys) >= frame.KeyCount {
		return nil, nil, 0, fmt.Errorf("a read takes at most %d values of %s and pins at most %d later Keys", MaxFirstKeys, frame.Keys[0].FieldName, frame.KeyCount-1)
	}
	store, err := configured()
	if err != nil {
		return nil, nil, 0, err
	}
	state, err := readState(table, frame)
	if err != nil {
		return nil, nil, 0, err
	}
	if !state.IsBuiltAs(frame) {
		return nil, nil, 0, ErrNotBuilt
	}
	if isFresh {
		fresh, snapshot, err := readFresh(table, store, frame, state, fromKey, toKey, pinnedKeys, selectFiles)
		if err != nil {
			return nil, nil, 0, err
		}
		return fresh.FileKeys, fresh.Files, snapshot, nil
	}
	fileKeys, err := SelectFileKeys(store, frame, fromKey, toKey, pinnedKeys)
	if err == nil && selectFiles != nil {
		fileKeys, err = selectFiles(fileKeys)
	}
	if err != nil {
		return nil, nil, 0, err
	}
	files, err := ReadFiles(store, frame, fileKeys)
	return fileKeys, files, state.Snapshot, err
}

// readFresh is ReadFreshFiles from the frame's snapshot, and returns the snapshot the read started
// from. A run that commits meanwhile truncates the log up to its new snapshot, maybe before the read
// got the entries it needed: when the snapshot moved, the read starts over from the new one. A read
// that held still goes on to expressCompact.
func readFresh(table Table, store Store, frame *Frame, state State, fromKey, toKey int64, pinnedKeys []int64,
	selectFiles func(fileKeys [][MaxKeys]int64) ([][MaxKeys]int64, error)) (*FreshRead, int64, error) {
	readWrittenAfter, readBySK := recordReaders(table, frame)
	for attempt := 1; ; attempt++ {
		fresh, err := ReadFreshFiles(store, frame, state.Snapshot, fromKey, toKey, pinnedKeys, selectFiles, readWrittenAfter, readBySK)
		if err != nil {
			return nil, 0, err
		}
		stateAfter, err := readState(table, frame)
		if err != nil {
			return nil, 0, err
		}
		if !stateAfter.IsBuiltAs(frame) {
			return nil, 0, ErrNotBuilt
		}
		if stateAfter.Snapshot == state.Snapshot {
			return fresh, state.Snapshot, expressCompact(table, store, frame, state, stateAfter, fresh)
		}
		if attempt == freshReadAttempts {
			return nil, 0, fmt.Errorf("the frame's compactions kept committing during %d fresh reads: retry", attempt)
		}
		state = stateAfter
	}
}

// expressCompact runs after a fresh read, verified by reading startState before it and endState
// after. It pushes a checkpoint when the newest has settled. Then, when more than expressMinChanges
// records changed between the snapshot and M, the newest checkpoint settled when the read began, it
// writes those changes into the files: every write up to M had landed by then, so the read already
// holds what each record held at M. It takes the frame's lock, and skips while another compaction holds
// it or once one moved the snapshot since startState: the read's changes start at that snapshot. One
// that loses its lock is not an error: the files it wrote are ahead of the snapshot, as a crashed one's.
func expressCompact(table Table, store Store, frame *Frame, startState, endState State, fresh *FreshRead) error {
	if _, err := pushCheckpoint(table, frame, endState); err != nil {
		return err
	}
	target, hasTarget := startState.settledTarget(startState.ReadAt)
	if !hasTarget || target <= startState.Snapshot || fresh.ChangedRecords(target) <= expressMinChanges {
		return nil
	}
	lock, err := TakeLock(store, frame, table.Now)
	if err != nil || lock == nil {
		return err
	}
	defer lock.Release()
	state, err := readState(table, frame)
	if err != nil || !state.IsBuiltAs(frame) || state.Snapshot != startState.Snapshot {
		return err
	}
	extendedDays, err := fresh.CompactTo(lock, frame, target)
	if err == nil {
		err = commitSnapshot(table, frame, lock, target, extendedDays, nil)
	}
	if errors.Is(err, ErrLockLost) {
		return nil
	}
	return err
}

// FrameSource is one frame's live read, what FrameSQL (framesql.Source) reads a frame through. It
// always reads fresh, and hands over each file as decoded.
type FrameSource struct {
	table Table
	frame *Frame
}

// NewFrameSource is the live read of frame over table. A driver builds it once, at boot.
func NewFrameSource(table Table, frame *Frame) *FrameSource {
	return &FrameSource{table: table, frame: frame}
}

// Scan reads, as the records hold them now, the files whose Keys[0] is in [fromKey, toKey] and whose
// later Keys start with pinnedKeys. selectFiles gets every file the read would GET, before the first
// GET, and returns the ones to read; its error comes back as is. fn gets each file once, from this
// goroutine, after the read has checked that no compaction committed under it: a read that starts over
// never hands a file twice. It returns the frame snapshot the read started from.
func (source *FrameSource) Scan(fromKey, toKey int64, pinnedKeys []int64,
	selectFiles func(fileKeys [][MaxKeys]int64) ([][MaxKeys]int64, error),
	fn func(keys [MaxKeys]int64, file File) error) (int64, error) {
	frame := source.frame
	// The caller's error (a file cap) is its own message: it must not come back wrapped in the ORM's.
	var selectError error
	selectFilesOnce := func(fileKeys [][MaxKeys]int64) ([][MaxKeys]int64, error) {
		selected, err := selectFiles(fileKeys)
		selectError = err
		return selected, err
	}
	fileKeys, files, snapshot, err := Read(source.table, frame, fromKey, toKey, pinnedKeys, true, selectFilesOnce)
	if selectError != nil {
		return 0, selectError
	}
	if err != nil {
		return 0, fmt.Errorf("db: %s FrameSource(%q): %w", frame.RecordName, frame.Name, err)
	}
	for i, file := range files {
		if err := fn(fileKeys[i], file); err != nil {
			return 0, err
		}
	}
	return snapshot, nil
}
