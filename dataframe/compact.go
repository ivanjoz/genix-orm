package dataframe

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Compaction: bringing one partition's files of a frame up to now − TargetLag, so its
// reads replay a short log. No schedule runs it. A read compacts the partition it reads
// before answering:
//
//   - always when the partition has no files in the frame's shape, or its w is out of
//     the log's reach: the read could not be exact otherwise;
//   - when the frame's CompactOn says it is due: Records records changed since w, or
//     Seconds since w. Without it the files and the log still answer the read exactly,
//     so a failed compaction is only logged.
//
// A read that finds it due takes the partition's lock, queuing up to lockWait behind a
// compaction under way. Once it holds the lock, a state other than the one it found
// means the holder before it compacted: it releases the lock and reads those files.
// Compact is the manual one, of every frame of a partition.
// ─────────────────────────────────────────────────────────────────────────────

// CompactOn is when a read compacts the partition it reads: Records records changed since the last
// compaction, or Seconds since it. Zero values never trigger.
type CompactOn struct {
	Records int
	Seconds int
}

// needsRebuild says the partition can only be read after a rebuild: it has no files in the frame's
// shape, or its w is so far back the changelogs may no longer reach it.
func (frame *Frame) needsRebuild(state State, now time.Time) bool {
	return frame.rebuildReason(state, now) != ""
}

// rebuildReason names why needsRebuild holds, for the compaction log; "" when it doesn't.
func (frame *Frame) rebuildReason(state State, now time.Time) string {
	switch {
	case state.Generation == "":
		return "no files yet"
	case state.Shape != frame.Shape:
		return "the frame's shape changed"
	case state.Snapshot < utils.SUnixTimeOf(now)-MaxDelta+rebuildMargin:
		return "w is out of the changelogs' reach"
	}
	return ""
}

// isCompactDue applies the frame's CompactOn to a partition at state, whose records written since w
// are histories. Only the changes a compaction would take count (up to its target, now − TargetLag),
// so the records written in the last minutes don't make every read compact again.
func (frame *Frame) isCompactDue(state State, histories []History, now time.Time) bool {
	target := utils.SUnixTimeOf(now) - TargetLag
	compactOn := frame.CompactOn
	if compactOn.Seconds > 0 && (target-state.Snapshot)*2 >= int64(compactOn.Seconds) {
		return true
	}
	if compactOn.Records == 0 {
		return false
	}
	compactableRecords := 0
	for _, history := range histories {
		isInsertedBefore := history.created > state.Snapshot && history.created <= target
		if isInsertedBefore || len(history.changeMoments(state.Snapshot, target)) > 0 {
			compactableRecords++
		}
	}
	return compactableRecords >= compactOn.Records
}

// Compact brings every frame of one partition up to date: it rebuilds the ones that need it and moves
// the others' files to now − TargetLag. It is the manual compaction; reads compact on their own.
func Compact(table *Table, backend Backend, store utils.FileStore, partition int64, now time.Time) error {
	var frameErrors []error
	for i := range table.Frames {
		frame := &table.Frames[i]
		state, err := backend.ReadState(frame, partition)
		if err == nil {
			err = table.compactFrame(backend, store, frame, partition, state, now)
		}
		frameErrors = append(frameErrors, err)
	}
	return errors.Join(frameErrors...)
}

// compactFrame takes the partition's lock, waiting behind a compaction under way, and, unless the
// holder before it moved the state away from seenState (it did this compaction already), rebuilds the
// partition when it needs it or moves its files to now − TargetLag.
func (table *Table) compactFrame(backend Backend, store utils.FileStore, frame *Frame, partition int64, seenState State, now time.Time) error {
	// A rebuild or compaction runs inside a read and is most of its latency: each one is logged with
	// its cause and duration, so a slow read is explained by the log.
	startedAt := time.Now()
	logName := fmt.Sprintf("dataframe: %s frame %q partition %d", table.Name, frame.Name, partition)
	lock, state, err := takeLock(backend, frame, partition, now)
	if err != nil {
		return err
	}
	if lock == nil {
		return fmt.Errorf("%s: another compaction kept it past %v", logName, lockWait)
	}
	defer releaseLock(backend, frame, partition, lock)
	if state != seenState {
		log.Printf("%s: waited %v for a compaction another read ran", logName, time.Since(startedAt))
		return nil
	}
	if rebuildReason := frame.rebuildReason(state, now); rebuildReason != "" {
		err := table.rebuildFrame(backend, store, frame, partition, lock, now)
		log.Printf("%s: rebuilt from its records (%s) in %v, err: %v", logName, rebuildReason, time.Since(startedAt), err)
		return err
	}
	err = table.runFrame(backend, store, frame, partition, lock, state, now)
	snapshotAge := time.Duration(utils.SUnixTimeOf(now)-state.Snapshot) * 2 * time.Second
	log.Printf("%s: compacted %v of changes in %v, err: %v", logName, snapshotAge, time.Since(startedAt), err)
	return err
}

// compactDueFrame is the compaction a read does when CompactOn says it is due; true when it compacted,
// so the read starts over on the new state. A failure is logged and the read goes on without it.
func (table *Table) compactDueFrame(backend Backend, store utils.FileStore, frame *Frame, partition int64, state State, now time.Time) bool {
	if err := table.compactFrame(backend, store, frame, partition, state, now); err != nil {
		log.Printf("dataframe: %s frame %q partition %d: the compaction before a read failed, the read replays the log: %v", table.Name, frame.Name, partition, err)
		return false
	}
	return true
}

// rebuildBeforeRead builds or rebuilds a partition a read can't answer as it is (needsRebuild).
func (table *Table) rebuildBeforeRead(backend Backend, store utils.FileStore, frame *Frame, partition int64, state State, now time.Time) error {
	if err := table.compactFrame(backend, store, frame, partition, state, now); err != nil {
		return fmt.Errorf("%w: %w", ErrFrameNotBuilt, err)
	}
	return nil
}
