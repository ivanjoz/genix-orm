package dataframe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// Runs and rebuilds. One run or rebuild holds a frame at a time (the backend's lock),
// but the files don't rely on it: each file is written conditioned on the ETag read,
// and the snapshot w moves only if it is still the one read. A run whose lock expired
// loses those conditions and writes nothing more; a file it already wrote is ahead
// of w, and its own snapshot says so.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// lockDuration bounds a run or a rebuild; its writes stop lockMargin before it ends.
	lockDuration = 5 * time.Minute
	lockMargin   = 30 * time.Second
	// TargetLag: a run moves the files up to now − 2 minutes (60 ticks), so every write stamped up to
	// there has landed and the backend's HistoriesWrittenAfter sees it.
	TargetLag = 60
	// rebuildMargin: a frame whose w is within an hour (1800 ticks) of the changelog's reach is
	// rebuilt, not run: the records' changelogs may no longer reach w.
	rebuildMargin = 1800
)

// heldLock is a lock this process holds on a frame: its holder id and the context its writes go through.
type heldLock struct {
	holderID string
	ctx      context.Context
	cancel   context.CancelFunc
}

func takeLock(backend Backend, frame *Frame, now time.Time) (*heldLock, State, error) {
	holderBytes := make([]byte, 8)
	rand.Read(holderBytes)
	lock := &heldLock{holderID: hex.EncodeToString(holderBytes)}
	state, isTaken, err := backend.TakeLock(frame, lock.holderID, now, now.Add(lockDuration))
	if err != nil || !isTaken {
		return nil, State{}, err
	}
	lock.ctx, lock.cancel = context.WithDeadline(context.Background(), now.Add(lockDuration-lockMargin))
	return lock, state, nil
}

func releaseLock(backend Backend, frame *Frame, lock *heldLock) {
	lock.cancel()
	backend.ReleaseLock(frame, lock.holderID)
}

// Materialize brings every frame of the table up to date at now: it rebuilds the ones not built in
// their current shape, or too far behind for the changelogs, and moves the others' files to now − 2
// minutes. A frame another run holds is skipped. Run it every few minutes (berryapps: every 10).
func Materialize(table *Table, backend Backend, store utils.FileStore, now time.Time) error {
	var frameErrors []error
	for i := range table.Frames {
		frame := &table.Frames[i]
		lock, state, err := takeLock(backend, frame, now)
		if err != nil || lock == nil {
			frameErrors = append(frameErrors, err)
			continue
		}
		isOutOfReach := state.Snapshot < utils.SUnixTimeOf(now)-MaxDelta+rebuildMargin
		if state.Generation == "" || state.Shape != frame.Shape || isOutOfReach {
			err = table.rebuildFrame(backend, store, frame, lock, now)
		} else {
			err = table.runFrame(backend, store, frame, lock, state, now)
		}
		releaseLock(backend, frame, lock)
		frameErrors = append(frameErrors, err)
	}
	return errors.Join(frameErrors...)
}

// Rebuild computes the files of the frame frameName ("" every frame) again from the records: the fix
// for drift. It fails when a run holds the frame; retry it.
func Rebuild(table *Table, backend Backend, store utils.FileStore, frameName string, now time.Time) error {
	isRebuilt := false
	for i := range table.Frames {
		frame := &table.Frames[i]
		if frameName != "" && frame.Name != frameName {
			continue
		}
		isRebuilt = true
		lock, _, err := takeLock(backend, frame, now)
		if err != nil {
			return err
		}
		if lock == nil {
			return fmt.Errorf("dataframe: %s frame %q is held by a run: retry in a few minutes", table.Name, frame.Name)
		}
		err = table.rebuildFrame(backend, store, frame, lock, now)
		releaseLock(backend, frame, lock)
		if err != nil {
			return err
		}
	}
	if !isRebuilt {
		return fmt.Errorf("dataframe: %s has no frame %q", table.Name, frameName)
	}
	return nil
}

// runFrame moves every file the records written after w were in, from what they held at w (or at the
// file's own snapshot, past w after a crashed run) to what they hold at the target, then moves w.
func (table *Table) runFrame(backend Backend, store utils.FileStore, frame *Frame, lock *heldLock, state State, now time.Time) error {
	target := utils.SUnixTimeOf(now) - TargetLag
	if target <= state.Snapshot {
		return nil
	}
	histories, err := backend.HistoriesWrittenAfter(state.Snapshot)
	if err != nil {
		return fmt.Errorf("dataframe: %s reading the records written since %d: %w", table.Name, state.Snapshot, err)
	}
	if err := table.moveFiles(lock.ctx, store, frame, state.Generation, histories, state.Snapshot, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q writing its files: %w", table.Name, frame.Name, err)
	}
	if err := backend.CommitSnapshot(lock.ctx, frame, lock.holderID, state, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q moving its snapshot: %w", table.Name, frame.Name, err)
	}
	return nil
}

// moveFiles rewrites, at target, every file the records of histories were in between the snapshot w
// and target, each conditioned on the ETag read. Their slots are raised to target first (the new
// files get theirs), so the versions file is never behind the files.
func (table *Table) moveFiles(ctx context.Context, store utils.FileStore, frame *Frame, generation string, histories []History, snapshot, target int64) error {
	fileKeys := table.filesHeldBetween(frame, generation, histories, snapshot, target)
	loaded, err := loadFiles(store, frame, fileKeys, snapshot)
	if err != nil {
		return err
	}
	table.moveRecords(frame, generation, histories, loaded, target)

	var writtenKeys []string
	var writtenFiles []File
	var rewrittenSlots []uint8
	var newFileIndexes []int
	for _, fileKey := range fileKeys {
		if loaded.snapshotByFileKey[fileKey] >= target {
			continue // a run that crashed after writing it already brought it here
		}
		file := loaded.rowsByFileKey[fileKey].fileAt(target, len(frame.Rows), frame.SumsCount)
		if file.rowCount() == 0 && loaded.etagByFileKey[fileKey] == "" {
			continue // a file no record is left in is not created
		}
		if file.Slot = loaded.slotByFileKey[fileKey]; file.Slot == 0 {
			newFileIndexes = append(newFileIndexes, len(writtenFiles))
		} else {
			rewrittenSlots = append(rewrittenSlots, file.Slot)
		}
		writtenKeys, writtenFiles = append(writtenKeys, fileKey), append(writtenFiles, file)
	}
	if len(writtenFiles) == 0 {
		return nil
	}
	newSlots, err := allocateAndRaiseSlots(ctx, store, len(newFileIndexes), rewrittenSlots, target)
	if err != nil {
		return err
	}
	for i, fileIndex := range newFileIndexes {
		writtenFiles[fileIndex].Slot = newSlots[i]
	}
	return utils.RunParallel(len(writtenFiles), func(fileIndex int) error {
		_, err := store.PutFileIfMatch(ctx, writtenKeys[fileIndex], EncodeFile(writtenFiles[fileIndex]), loaded.etagByFileKey[writtenKeys[fileIndex]])
		return err
	})
}

// frameValuesAt is what a record put in a frame at moment: nil when it was in no file.
func (table *Table) frameValuesAt(frame *Frame, history History, moment int64) *frameValues {
	tracked, existed := table.valuesAt(history, moment)
	if !existed {
		return nil
	}
	return table.valuesOf(frame, tracked)
}

// filesHeldBetween lists the files the records were in at any moment of [from, to]: at from and after
// each of their changes up to to. A run rewrites them all, so a file a crashed run left ahead of w
// moves to the target too.
func (table *Table) filesHeldBetween(frame *Frame, generation string, histories []History, from, to int64) []string {
	isListed := map[string]bool{}
	var fileKeys []string
	for _, history := range histories {
		for _, fileKey := range table.filesOfHistory(frame, generation, history, from, to) {
			if !isListed[fileKey] {
				isListed[fileKey] = true
				fileKeys = append(fileKeys, fileKey)
			}
		}
	}
	return fileKeys
}

// filesOfHistory lists the files one record was in at any moment of [from, to], repeats included: at
// from, at to, after each change and after its insert, which leaves no changelog entry. Without the
// insert, a record inserted and moved out within the window would be in no listed file, and a run
// would leave that file as is while a reader holds a fresh copy that counted the record.
func (table *Table) filesOfHistory(frame *Frame, generation string, history History, from, to int64) []string {
	moments := append([]int64{from, to}, history.changeMoments(from, to)...)
	if history.created > from && history.created <= to {
		moments = append(moments, history.created)
	}
	var fileKeys []string
	for _, moment := range moments {
		if values := table.frameValuesAt(frame, history, moment); values != nil {
			fileKeys = append(fileKeys, frame.fileKeyOf(generation, values.keys))
		}
	}
	return fileKeys
}

// moveRecords brings the loaded files forward to moment: each file takes, of every record, what it
// held at moment minus what it held at appliedThrough[file] (max(the file's snapshot, w)).
func (table *Table) moveRecords(frame *Frame, generation string, histories []History, loaded loadedFiles, moment int64) {
	appliedMoments := slices.Compact(slices.Sorted(maps.Values(loaded.appliedThrough)))
	for _, history := range histories {
		if values := table.frameValuesAt(frame, history, moment); values != nil {
			if rows := loaded.rowsByFileKey[frame.fileKeyOf(generation, values.keys)]; rows != nil {
				rows.add(values, 1)
			}
		}
		for _, appliedMoment := range appliedMoments {
			values := table.frameValuesAt(frame, history, appliedMoment)
			if values == nil {
				continue
			}
			fileKey := frame.fileKeyOf(generation, values.keys)
			if rows := loaded.rowsByFileKey[fileKey]; rows != nil && loaded.appliedThrough[fileKey] == appliedMoment {
				rows.add(values, -1)
			}
		}
	}
}

// loadedFiles are files read to be brought forward: their rows, snapshot, slot, ETag ("" for a
// missing file) and stored bytes, and the moment each one already holds (max(snapshot, w)).
type loadedFiles struct {
	rowsByFileKey     map[string]frameRows
	snapshotByFileKey map[string]int64
	slotByFileKey     map[string]uint8
	appliedThrough    map[string]int64
	etagByFileKey     map[string]string
	contentByFileKey  map[string][]byte
}

func loadFiles(store utils.FileStore, frame *Frame, fileKeys []string, snapshot int64) (loadedFiles, error) {
	files := make([]File, len(fileKeys))
	etags := make([]string, len(fileKeys))
	contents := make([][]byte, len(fileKeys))
	err := utils.RunParallel(len(fileKeys), func(fileIndex int) error {
		content, etag, err := store.GetFile(fileKeys[fileIndex])
		if errors.Is(err, utils.ErrFileNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("dataframe: reading the frame file %s: %w", fileKeys[fileIndex], err)
		}
		if files[fileIndex], err = DecodeFile(content, len(frame.Rows), frame.SumsCount); err != nil {
			return fmt.Errorf("%s: %w", fileKeys[fileIndex], err)
		}
		etags[fileIndex], contents[fileIndex] = etag, content
		return nil
	})
	loaded := loadedFiles{rowsByFileKey: map[string]frameRows{}, snapshotByFileKey: map[string]int64{}, slotByFileKey: map[string]uint8{},
		appliedThrough: map[string]int64{}, etagByFileKey: map[string]string{}, contentByFileKey: map[string][]byte{}}
	for i, fileKey := range fileKeys {
		loaded.rowsByFileKey[fileKey] = rowsOfFile(files[i])
		loaded.snapshotByFileKey[fileKey] = files[i].Snapshot
		loaded.slotByFileKey[fileKey] = files[i].Slot
		loaded.appliedThrough[fileKey] = max(files[i].Snapshot, snapshot)
		loaded.etagByFileKey[fileKey] = etags[i]
		loaded.contentByFileKey[fileKey] = contents[i]
	}
	return loaded, err
}

// rebuildFrame sums what every record held at the target into a new generation and makes it current,
// then deletes everything else in the frame's folder. The changelogs make it exact at the target
// whatever writes land while it reads; the next run takes the records written after it.
func (table *Table) rebuildFrame(backend Backend, store utils.FileStore, frame *Frame, lock *heldLock, now time.Time) error {
	target := utils.SUnixTimeOf(now) - TargetLag
	generation := utils.EncodeUintBase64(uint64(now.UnixMilli()))
	rowsByFileKey := map[string]frameRows{}
	err := backend.ScanHistories(func(histories []History) error {
		table.sumAt(frame, generation, histories, target, rowsByFileKey)
		return nil
	})
	if err != nil {
		return fmt.Errorf("dataframe: %s frame %q reading the records: %w", table.Name, frame.Name, err)
	}

	// Every file of the new generation takes a new slot: the readers holding the old generation
	// fetch the whole frame again anyway.
	fileKeys := slices.Collect(maps.Keys(rowsByFileKey))
	slots, err := allocateAndRaiseSlots(lock.ctx, store, len(fileKeys), nil, target)
	if err != nil {
		return fmt.Errorf("dataframe: %s frame %q: %w", table.Name, frame.Name, err)
	}
	err = utils.RunParallel(len(fileKeys), func(fileIndex int) error {
		file := rowsByFileKey[fileKeys[fileIndex]].fileAt(target, len(frame.Rows), frame.SumsCount)
		file.Slot = slots[fileIndex]
		_, err := store.PutFileIfMatch(lock.ctx, fileKeys[fileIndex], EncodeFile(file), "")
		return err
	})
	if err != nil {
		return fmt.Errorf("dataframe: %s frame %q writing its files: %w", table.Name, frame.Name, err)
	}
	if err := backend.CommitGeneration(lock.ctx, frame, lock.holderID, generation, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q making its new files current: %w", table.Name, frame.Name, err)
	}

	storedKeys, err := store.ListFiles(frame.Folder)
	if err != nil {
		return fmt.Errorf("dataframe: %s listing frame %q: %w", table.Name, frame.Name, err)
	}
	var staleKeys []string
	for _, storedKey := range storedKeys {
		if !strings.HasPrefix(storedKey, frame.Folder+generation+"/") {
			staleKeys = append(staleKeys, storedKey)
		}
	}
	return store.DeleteFiles(lock.ctx, staleKeys...)
}

// sumAt adds what every record held at moment to the files of a generation.
func (table *Table) sumAt(frame *Frame, generation string, histories []History, moment int64, rowsByFileKey map[string]frameRows) {
	for _, history := range histories {
		values := table.frameValuesAt(frame, history, moment)
		if values == nil {
			continue
		}
		fileKey := frame.fileKeyOf(generation, values.keys)
		if rowsByFileKey[fileKey] == nil {
			rowsByFileKey[fileKey] = frameRows{}
		}
		rowsByFileKey[fileKey].add(values, 1)
	}
}
