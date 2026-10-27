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
// Runs and rebuilds, of one partition of a frame (compact.go starts them). One run or
// rebuild holds a partition at a time (the backend's lock), but the files don't rely
// on it: each file is written conditioned on the ETag read, and the snapshot w moves
// only if it is still the one read. A run whose lock expired loses those conditions
// and writes nothing more; a file it already wrote is ahead of w, and its own snapshot
// says so.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// lockLease bounds a run or a rebuild of a partition; its writes stop lockMargin before it ends.
	lockLease  = 40 * time.Second
	lockMargin = 5 * time.Second
	// lockWait is how long a compaction queues behind the one holding the partition: a holder past
	// its lease lets it in.
	lockWait = 40 * time.Second
	// TargetLag: a run moves the files up to now − 2 minutes (60 ticks), so every write stamped up to
	// there has landed and the backend's PartitionHistoriesWrittenAfter sees it.
	TargetLag = 60
	// rebuildMargin: a partition whose w is within an hour (1800 ticks) of the changelog's reach is
	// rebuilt, not run: the records' changelogs may no longer reach w.
	rebuildMargin = 1800
	// maxConcurrentFileTasks bounds the files one step reads or writes at once.
	maxConcurrentFileTasks = 8
)

// heldLock is a lock this process holds on a partition: its holder id and the context its writes go through.
type heldLock struct {
	holderID string
	ctx      context.Context
	cancel   context.CancelFunc
}

func takeLock(backend Backend, frame *Frame, partition int64, now time.Time) (*heldLock, State, error) {
	holderBytes := make([]byte, 8)
	rand.Read(holderBytes)
	lock := &heldLock{holderID: hex.EncodeToString(holderBytes)}
	state, isTaken, err := backend.TakeLock(frame, partition, lock.holderID, now, now.Add(lockLease), lockWait)
	if err != nil || !isTaken {
		return nil, State{}, err
	}
	// The lease starts when the lock is granted, after the wait: the wall clock, not now.
	lock.ctx, lock.cancel = context.WithTimeout(context.Background(), lockLease-lockMargin)
	return lock, state, nil
}

func releaseLock(backend Backend, frame *Frame, partition int64, lock *heldLock) {
	lock.cancel()
	backend.ReleaseLock(frame, partition, lock.holderID)
}

// Rebuild computes one partition's files of the frame frameName ("" every frame) again from its
// records, in a new generation: the fix for drift, or after the partition's rows changed outside
// the write path (a backup restore).
func Rebuild(table *Table, backend Backend, store utils.FileStore, partition int64, frameName string, now time.Time) error {
	isRebuilt := false
	for i := range table.Frames {
		frame := &table.Frames[i]
		if frameName != "" && frame.Name != frameName {
			continue
		}
		isRebuilt = true
		lock, _, err := takeLock(backend, frame, partition, now)
		if err != nil {
			return err
		}
		if lock == nil {
			return fmt.Errorf("dataframe: %s frame %q partition %d is held by a compaction: retry", table.Name, frame.Name, partition)
		}
		err = table.rebuildFrame(backend, store, frame, partition, lock, now)
		releaseLock(backend, frame, partition, lock)
		if err != nil {
			return err
		}
	}
	if !isRebuilt {
		return fmt.Errorf("dataframe: %s has no frame %q", table.Name, frameName)
	}
	return nil
}

// runFrame moves every file the partition's records written after w were in, from what they held at
// w (or at the file's own snapshot, past w after a crashed run) to what they hold at the target, then
// moves w.
func (table *Table) runFrame(backend Backend, store utils.FileStore, frame *Frame, partition int64, lock *heldLock, state State, now time.Time) error {
	target := utils.SUnixTimeOf(now) - TargetLag
	if target <= state.Snapshot {
		return nil
	}
	histories, err := backend.PartitionHistoriesWrittenAfter(partition, state.Snapshot)
	if err != nil {
		return fmt.Errorf("dataframe: %s partition %d reading the records written since %d: %w", table.Name, partition, state.Snapshot, err)
	}
	if err := table.moveFiles(lock.ctx, store, frame, state.Generation, partition, histories, state.Snapshot, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d writing its files: %w", table.Name, frame.Name, partition, err)
	}
	if err := backend.CommitSnapshot(lock.ctx, frame, partition, lock.holderID, state, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d moving its snapshot: %w", table.Name, frame.Name, partition, err)
	}
	return nil
}

// moveFiles rewrites, at target, every file the records of histories were in between the snapshot w
// and target, each conditioned on the ETag read. Their slots are raised to target first (the new
// files get theirs), so the versions file is never behind the files.
func (table *Table) moveFiles(ctx context.Context, store utils.FileStore, frame *Frame, generation string, partition int64, histories []History, snapshot, target int64) error {
	fileKeys := table.filesHeldBetween(frame, generation, histories, snapshot, target)
	loaded, err := loadFiles(store, frame, fileKeys, snapshot)
	if err != nil {
		return err
	}
	table.moveRecords(frame, generation, histories, loaded, target)

	var writtenKeys []string
	var writtenFiles []File
	for _, fileKey := range fileKeys {
		if loaded.snapshotByFileKey[fileKey] >= target {
			continue // a run that crashed after writing it already brought it here
		}
		file := loaded.rowsByFileKey[fileKey].fileAt(target, frame)
		if file.rowCount() == 0 && loaded.etagByFileKey[fileKey] == "" {
			continue // a file no record is left in is not created
		}
		file.Slot = loaded.slotByFileKey[fileKey]
		writtenKeys, writtenFiles = append(writtenKeys, fileKey), append(writtenFiles, file)
	}
	return table.writeFiles(ctx, store, partition, writtenKeys, writtenFiles, loaded.etagByFileKey, target)
}

// writeFiles gives every new file (Slot 0) a slot and raises the slots of the others to moment, in the
// partition's versions file, then writes the files conditioned on etagByFileKey ("" or absent: the
// file must not exist). The slots move first, so the versions file is never behind its files.
func (table *Table) writeFiles(ctx context.Context, store utils.FileStore, partition int64, fileKeys []string, files []File, etagByFileKey map[string]string, moment int64) error {
	if len(files) == 0 {
		return nil
	}
	var newFileIndexes []int
	var rewrittenSlots []uint8
	for fileIndex, file := range files {
		if file.Slot == 0 {
			newFileIndexes = append(newFileIndexes, fileIndex)
		} else {
			rewrittenSlots = append(rewrittenSlots, file.Slot)
		}
	}
	newSlots, err := allocateAndRaiseSlots(ctx, store, table.versionsKeyOf(partition), len(newFileIndexes), rewrittenSlots, moment)
	if err != nil {
		return err
	}
	for i, fileIndex := range newFileIndexes {
		files[fileIndex].Slot = newSlots[i]
	}
	return utils.RunParallelLimit(maxConcurrentFileTasks, len(files), func(fileIndex int) error {
		_, err := store.PutFileIfMatch(ctx, fileKeys[fileIndex], EncodeFile(files[fileIndex]), etagByFileKey[fileKeys[fileIndex]])
		return err
	})
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
		lines, _ := history.linesAt(frame, moment)
		for _, line := range lines {
			fileKeys = append(fileKeys, frame.fileKeyOf(generation, history.Partition, frame.keysOf(line)))
		}
	}
	return fileKeys
}

// moveRecords brings the loaded files forward to moment: each file takes, of every record, the lines
// it held at moment minus the ones it held at appliedThrough[file] (max(the file's snapshot, w)).
func (table *Table) moveRecords(frame *Frame, generation string, histories []History, loaded loadedFiles, moment int64) {
	appliedMoments := slices.Compact(slices.Sorted(maps.Values(loaded.appliedThrough)))
	for _, history := range histories {
		lines, _ := history.linesAt(frame, moment)
		for _, line := range lines {
			if rows := loaded.rowsByFileKey[frame.fileKeyOf(generation, history.Partition, frame.keysOf(line))]; rows != nil {
				rows.add(frame, line, 1)
			}
		}
		for _, appliedMoment := range appliedMoments {
			appliedLines, _ := history.linesAt(frame, appliedMoment)
			for _, line := range appliedLines {
				fileKey := frame.fileKeyOf(generation, history.Partition, frame.keysOf(line))
				if rows := loaded.rowsByFileKey[fileKey]; rows != nil && loaded.appliedThrough[fileKey] == appliedMoment {
					rows.add(frame, line, -1)
				}
			}
		}
	}
}

// loadedFiles are files read to be brought forward: their rows, snapshot, slot, ETag ("" for a
// missing file) and stored bytes, and the moment each one already holds (max(snapshot, w)).
type loadedFiles struct {
	rowsByFileKey     map[string]*frameRows
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
	err := utils.RunParallelLimit(maxConcurrentFileTasks, len(fileKeys), func(fileIndex int) error {
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
	loaded := loadedFiles{rowsByFileKey: map[string]*frameRows{}, snapshotByFileKey: map[string]int64{}, slotByFileKey: map[string]uint8{},
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

// rebuildFrame sums what every record of the partition held at the target into a new generation and
// makes it current, then deletes everything else in the partition's folder. The changelogs make it
// exact at the target whatever writes land while it reads; the next run takes the records written
// after it.
func (table *Table) rebuildFrame(backend Backend, store utils.FileStore, frame *Frame, partition int64, lock *heldLock, now time.Time) error {
	target := utils.SUnixTimeOf(now) - TargetLag
	generation := utils.EncodeUintBase64(uint64(now.UnixMilli()))
	sums := newLineSums(frame)
	err := backend.ScanPartitionHistories(partition, func(histories []History) error {
		for _, history := range histories {
			lines, _ := history.linesAt(frame, target)
			for _, line := range lines {
				sums.add(line)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d reading the records: %w", table.Name, frame.Name, partition, err)
	}

	// Every file of the new generation takes a new slot (Slot 0 here): the readers holding the old
	// generation fetch the whole partition again anyway.
	fileKeys, files := sums.files(generation, partition, target)
	if err := table.writeFiles(lock.ctx, store, partition, fileKeys, files, nil, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d writing its files: %w", table.Name, frame.Name, partition, err)
	}
	if err := backend.CommitGeneration(lock.ctx, frame, partition, lock.holderID, generation, target); err != nil {
		return fmt.Errorf("dataframe: %s frame %q partition %d making its new files current: %w", table.Name, frame.Name, partition, err)
	}

	storedKeys, err := store.ListFiles(frame.partitionFolderOf(partition))
	if err != nil {
		return fmt.Errorf("dataframe: %s listing frame %q partition %d: %w", table.Name, frame.Name, partition, err)
	}
	var staleKeys []string
	for _, storedKey := range storedKeys {
		if !strings.HasPrefix(storedKey, frame.generationFolderOf(generation, partition)) {
			staleKeys = append(staleKeys, storedKey)
		}
	}
	return store.DeleteFiles(lock.ctx, staleKeys...)
}
