package dataframe

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// The day indexes of a 2–3-key frame (../DATA_FRAMES_PLAN.md, D3 and D7). A day
// folder's _idx lists its files and their hashes; its _ixt holds the blocks express
// compactions appended since the last merge, a few bytes each instead of a rewrite
// of the whole _idx. A reader reads _ixt, then _idx, and the _ixt entries win, the
// later ones over the earlier: one compaction at a time holds the frame's lock, so
// the blocks are in the order their files were written. Reading _ixt first never
// misses an entry: a merge rewrites _idx before it deletes _ixt. Only runs and
// rebuilds rewrite _idx and delete _ixt; under the lock, nothing appends meanwhile.
//
// The keys are exact: no compaction deletes a file. The hashes are too, unless a
// compaction crashed between writing a file and indexing it; the next compaction of
// the file indexes it again.
// ─────────────────────────────────────────────────────────────────────────────

// readIndex reads a day folder's files as keys → hash: its _idx with its _ixt over it, both empty when
// missing. hasExtension tells a merge it has an _ixt to delete; it is set even when the error is a
// corrupt index, so a rebuild can replace both.
func readIndex(store Store, frame *Frame, day int64) (hashByKeys map[[MaxKeys - 1]int64]uint32, hasExtension bool, err error) {
	extension, _, err := store.Get(frame.indexExtensionKey(day))
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, false, err
	}
	hasExtension = extension != nil
	content, _, err := store.Get(frame.indexKey(day))
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return nil, hasExtension, err
	}
	var entries []IndexEntry
	if content != nil {
		if entries, err = DecodeIndex(content, frame.KeyCount); err != nil {
			return nil, hasExtension, fmt.Errorf("db: frame index %s: %w: rebuild the frame", frame.indexKey(day), err)
		}
	}
	extensionEntries, err := DecodeIndexExtension(extension, frame.KeyCount)
	if err != nil {
		return nil, hasExtension, fmt.Errorf("db: frame index %s: %w: rebuild the frame", frame.indexExtensionKey(day), err)
	}
	hashByKeys = map[[MaxKeys - 1]int64]uint32{}
	for _, entry := range slices.Concat(entries, extensionEntries) {
		hashByKeys[entry.Keys] = entry.Hash
	}
	return hashByKeys, hasExtension, nil
}

// writeIndex writes a day folder's _idx; a folder left without files loses it.
func writeIndex(lock *Lock, frame *Frame, day int64, hashByKeys map[[MaxKeys - 1]int64]uint32) error {
	if len(hashByKeys) == 0 {
		return lock.Delete(frame.indexKey(day))
	}
	return lock.put(frame.indexKey(day), appendIndex(nil, sortedIndexEntries(hashByKeys), frame.KeyCount))
}

func sortedIndexEntries(hashByKeys map[[MaxKeys - 1]int64]uint32) []IndexEntry {
	entries := make([]IndexEntry, 0, len(hashByKeys))
	for keys, hash := range hashByKeys {
		entries = append(entries, IndexEntry{Keys: keys, Hash: hash})
	}
	slices.SortFunc(entries, func(a, b IndexEntry) int { return slices.Compare(a.Keys[:], b.Keys[:]) })
	return entries
}

// updateIndexes is a run's index work: it rewrites the _idx of the day folder of every file in
// fileKeys with their new hashes, and of every day in extendedDays (the days express compactions
// appended to), merging each day's _ixt into its _idx and then deleting it.
func updateIndexes(lock *Lock, frame *Frame, fileKeys [][MaxKeys]int64, fileHashes []uint32, extendedDays []int64) error {
	touchedFilesByDay := map[int64][]int{}
	for _, day := range extendedDays {
		touchedFilesByDay[day] = nil
	}
	for i, keys := range fileKeys {
		touchedFilesByDay[keys[0]] = append(touchedFilesByDay[keys[0]], i)
	}
	days := slices.Collect(maps.Keys(touchedFilesByDay))
	return parallel.Run(len(days), func(position int) error {
		day := days[position]
		hashByKeys, hasExtension, err := readIndex(lock.store, frame, day)
		if err != nil {
			return err
		}
		isChanged := hasExtension
		for _, i := range touchedFilesByDay[day] {
			indexKeys := [MaxKeys - 1]int64{fileKeys[i][1], fileKeys[i][2]}
			if storedHash, isListed := hashByKeys[indexKeys]; !isListed || storedHash != fileHashes[i] {
				hashByKeys[indexKeys] = fileHashes[i]
				isChanged = true
			}
		}
		if !isChanged {
			return nil
		}
		return writeIndexMerged(lock, frame, day, hashByKeys, hasExtension)
	})
}

// writeIndexMerged writes a day's _idx, then deletes the _ixt merged into it.
func writeIndexMerged(lock *Lock, frame *Frame, day int64, hashByKeys map[[MaxKeys - 1]int64]uint32, hasExtension bool) error {
	if err := writeIndex(lock, frame, day, hashByKeys); err != nil || !hasExtension {
		return err
	}
	return lock.Delete(frame.indexExtensionKey(day))
}

// appendIndexExtensions is an express compaction's index work: one block per day folder of fileKeys,
// appended to its _ixt, with the keys and hashes of its files there. It returns those days.
func appendIndexExtensions(lock *Lock, frame *Frame, fileKeys [][MaxKeys]int64, fileHashes []uint32) ([]int64, error) {
	hashByKeysByDay := map[int64]map[[MaxKeys - 1]int64]uint32{}
	for i, keys := range fileKeys {
		if hashByKeysByDay[keys[0]] == nil {
			hashByKeysByDay[keys[0]] = map[[MaxKeys - 1]int64]uint32{}
		}
		hashByKeysByDay[keys[0]][[MaxKeys - 1]int64{keys[1], keys[2]}] = fileHashes[i]
	}
	days := slices.Sorted(maps.Keys(hashByKeysByDay))
	err := parallel.Run(len(days), func(position int) error {
		block := appendIndexExtensionBlock(nil, sortedIndexEntries(hashByKeysByDay[days[position]]), frame.KeyCount)
		return lock.append(frame.indexExtensionKey(days[position]), block)
	})
	return days, err
}
