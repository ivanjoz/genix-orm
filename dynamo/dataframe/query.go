package dataframe

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// SelectFileKeys lists the files of the days [fromKey, toKey] whose later Keys start with
// pinnedKeys, in key order. With every Key pinned they are named directly; otherwise each day's
// index (_idx and _ixt) lists them.
func SelectFileKeys(store Store, frame *Frame, fromKey, toKey int64, pinnedKeys []int64) ([][MaxKeys]int64, error) {
	dayCount := int(toKey - fromKey + 1)
	if len(pinnedKeys) == frame.KeyCount-1 {
		fileKeys := make([][MaxKeys]int64, dayCount)
		for i := range fileKeys {
			fileKeys[i][0] = fromKey + int64(i)
			copy(fileKeys[i][1:], pinnedKeys)
		}
		return fileKeys, nil
	}
	fileKeysByDay := make([][][MaxKeys]int64, dayCount)
	err := parallel.Run(dayCount, func(offset int) error {
		day := fromKey + int64(offset)
		hashByKeys, _, err := readIndex(store, frame, day)
		if err != nil {
			return err
		}
		for indexKeys := range hashByKeys {
			if slices.Equal(indexKeys[:len(pinnedKeys)], pinnedKeys) {
				fileKeysByDay[offset] = append(fileKeysByDay[offset], [MaxKeys]int64{day, indexKeys[0], indexKeys[1]})
			}
		}
		slices.SortFunc(fileKeysByDay[offset], func(a, b [MaxKeys]int64) int { return slices.Compare(a[:], b[:]) })
		return nil
	})
	return slices.Concat(fileKeysByDay...), err
}

// FreshRead is what ReadFreshFiles read: the selected files as the records hold them now, and the
// changes since the frame's snapshot, which an express compaction (CompactTo) writes into the files.
type FreshRead struct {
	FileKeys [][MaxKeys]int64
	Files    []File
	snapshot int64
	lives    []*incarnation
}

// ChangedRecords counts the records (incarnations) whose frame values changed in (snapshot, target].
func (read *FreshRead) ChangedRecords(target int64) int {
	changed := 0
	for _, life := range read.lives {
		if life.changesBetween(read.snapshot, target) {
			changed++
		}
	}
	return changed
}

// CompactTo is an express compaction (../DATA_FRAMES_PLAN.md, D6), as lock's holder: it brings to
// target every file the changes it read name, then appends their keys and hashes to their days' _ixt,
// and returns those days. It needs no other read: target must have settled before the read began, so
// every write up to it had landed, and the read holds what each record held at target. The frame's w
// must still be the read's snapshot once the lock is held.
func (read *FreshRead) CompactTo(lock *Lock, frame *Frame, target int64) ([]int64, error) {
	fileKeys, fileHashes, err := compactFiles(lock, frame, read.lives, read.snapshot, target)
	if err != nil || frame.KeyCount == 1 {
		return nil, err
	}
	return appendIndexExtensions(lock, frame, fileKeys, fileHashes)
}

// ReadFreshFiles is SelectFileKeys + ReadFiles with the records as they are now, not as the frame's
// last compaction left them: each file plus the changes of the records written since its snapshot,
// computed in memory as a compaction would. snapshot is the frame's: every file holds the frame at
// it, a file a compaction touched since at a later one, and the log keeps every entry above it. A
// write counts once it lands. The files hold no snapshot (0); the readers are CompactFrame's.
func ReadFreshFiles(store Store, frame *Frame, snapshot, fromKey, toKey int64, pinnedKeys []int64,
	readWrittenAfter func(snapshot int64) ([]RecordState, error), readBySK func(sks []string) ([]RecordState, error)) (*FreshRead, error) {
	lives, err := readIncarnations(store, frame, snapshot, readWrittenAfter, readBySK)
	if err != nil {
		return nil, err
	}
	fileKeys, err := SelectFileKeys(store, frame, fromKey, toKey, pinnedKeys)
	if err != nil {
		return nil, err
	}
	// The changes may name files the _idx doesn't list yet: every file a changed record held or
	// holds in the selection is read too.
	isSelected := map[[MaxKeys]int64]bool{}
	for _, keys := range fileKeys {
		isSelected[keys] = true
	}
	selectChangedFile := func(values *Values) {
		if values == nil || isSelected[values.Keys] || values.Keys[0] < fromKey || values.Keys[0] > toKey ||
			!slices.Equal(values.Keys[1:1+len(pinnedKeys)], pinnedKeys) {
			return
		}
		isSelected[values.Keys] = true
		fileKeys = append(fileKeys, values.Keys)
	}
	for _, life := range lives {
		selectChangedFile(life.current)
		for _, entry := range life.entries {
			selectChangedFile(entry.OldValues)
		}
	}
	slices.SortFunc(fileKeys, func(a, b [MaxKeys]int64) int { return slices.Compare(a[:], b[:]) })

	files, err := ReadFiles(store, frame, fileKeys)
	if err != nil {
		return nil, err
	}
	// A file no compaction touched since an older snapshot (or a missing one) holds the frame at
	// snapshot too. One that didn't commit leaves files above it: each file takes the changes after its own.
	deltasByFileSnapshot := map[int64]map[[MaxKeys]int64]rowSums{}
	for i, file := range files {
		fileSnapshot := max(file.Snapshot, snapshot)
		if deltasByFileSnapshot[fileSnapshot] == nil {
			deltasByFileSnapshot[fileSnapshot] = deltasBetween(lives, fileSnapshot, math.MaxInt64)
		}
		rows := rowSumsOf(file)
		addRowSums(rows, deltasByFileSnapshot[fileSnapshot][fileKeys[i]])
		files[i] = fileOf(rows, 0, frame.SumsCount)
	}
	return &FreshRead{FileKeys: fileKeys, Files: files, snapshot: snapshot, lives: lives}, nil
}

// ReadFiles reads the files in parallel, in the order of fileKeys. A missing file has no rows.
func ReadFiles(store Store, frame *Frame, fileKeys [][MaxKeys]int64) ([]File, error) {
	files := make([]File, len(fileKeys))
	err := parallel.Run(len(fileKeys), func(i int) error {
		objectKey := frame.FileKey(fileKeys[i])
		content, _, err := store.Get(objectKey)
		if errors.Is(err, ErrObjectNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if files[i], err = DecodeFile(content, frame.SumsCount); err != nil {
			return fmt.Errorf("db: frame file %s: %w", objectKey, err)
		}
		return nil
	})
	return files, err
}
