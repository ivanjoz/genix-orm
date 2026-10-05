package dataframe

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/ivanjoz/genix-orm/dataframe/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// The file work of a compaction (../DATA_FRAMES.md, sections 4 and 7, and
// after): the scheduled run's (CompactFrame) and the express one of a fresh read
// (FreshRead.CompactTo), which also applies it in memory. A version is reserved
// before its write lands, so the caller targets a version only once it is settled:
// every write that took a version up to it has landed and logged. A compaction
// from W to the target takes out of the files what each incarnation held at W and
// adds what it held at the target (valuesAt).
//
// One compaction at a time holds the frame's lock and writes through it (lock.go).
// One that crashed, or lost its lock, leaves files ahead of W: each file carries
// the snapshot it was written at and takes the changes after it, and a compaction
// rewrites every file a change in its window names, even unchanged, so no file
// keeps a change of a lower target that W has passed ("The lock", rule 2).
// ─────────────────────────────────────────────────────────────────────────────

// conditionalWriteAttempts bounds the truncation of the log, which every write may append to meanwhile.
const conditionalWriteAttempts = 8

// RecordState is a stored record as a run read it: its sk, its CreatedVersion and what it
// contributes to the frame (nil: nothing, as with Status 0).
type RecordState struct {
	SK             string
	CreatedVersion int64
	Values         *Values
}

// incarnation is one life of a record in a frame: an sk and the CreatedVersion of the write that
// inserted it. A record deleted and inserted again with the same Keys is two incarnations.
type incarnation struct {
	sk             string
	createdVersion int64
	// entries are its log entries above the run's base snapshot, by NewVersion.
	entries []LogEntry
	// current is what the stored record holds, when the stored record is this incarnation.
	current *Values
}

// valuesAt is what the incarnation contributed to the frame at snapshot: the old values of its
// first logged change after snapshot, or else its values now. nil when it counted nowhere: created
// after snapshot, deleted, or soft-deleted.
func (life *incarnation) valuesAt(snapshot int64) *Values {
	if life.createdVersion > snapshot {
		return nil
	}
	for _, entry := range life.entries {
		if entry.NewVersion > snapshot {
			return entry.OldValues
		}
	}
	return life.current
}

// changesBetween reports whether the incarnation's frame values changed in (from, to]: a logged change
// in the window, or values that differ at its ends (an insert, which logs nothing).
func (life *incarnation) changesBetween(from, to int64) bool {
	return !EqualValues(life.valuesAt(from), life.valuesAt(to)) ||
		slices.ContainsFunc(life.entries, func(entry LogEntry) bool { return entry.NewVersion > from && entry.NewVersion <= to })
}

// incarnations groups the records a run read and its log entries above baseSnapshot into
// incarnations. An entry voided by a cancel marker is dropped.
func incarnations(records []RecordState, entries []LogEntry, baseSnapshot int64) []*incarnation {
	type incarnationKey struct {
		sk             string
		createdVersion int64
	}
	type writeKey struct {
		sk         string
		newVersion int64
	}
	incarnationByKey := map[incarnationKey]*incarnation{}
	incarnationOf := func(sk string, createdVersion int64) *incarnation {
		key := incarnationKey{sk, createdVersion}
		life := incarnationByKey[key]
		if life == nil {
			life = &incarnation{sk: sk, createdVersion: createdVersion}
			incarnationByKey[key] = life
		}
		return life
	}
	for _, record := range records {
		incarnationOf(record.SK, record.CreatedVersion).current = record.Values
	}
	isCancelled := map[writeKey]bool{}
	for _, entry := range entries {
		if entry.IsCancel {
			isCancelled[writeKey{entry.SK, entry.NewVersion}] = true
		}
	}
	for _, entry := range entries {
		if entry.IsCancel || entry.NewVersion <= baseSnapshot || isCancelled[writeKey{entry.SK, entry.NewVersion}] {
			continue
		}
		life := incarnationOf(entry.SK, entry.CreatedVersion)
		life.entries = append(life.entries, entry)
	}
	lives := make([]*incarnation, 0, len(incarnationByKey))
	for _, life := range incarnationByKey {
		slices.SortFunc(life.entries, func(a, b LogEntry) int { return cmp.Compare(a.NewVersion, b.NewVersion) })
		lives = append(lives, life)
	}
	return lives
}

// rowSums maps the rows of one file to their sums.
type rowSums map[int64][]int64

// addValues adds sign × values to the row of its file in files.
func addValues(files map[[MaxKeys]int64]rowSums, values *Values, sign int64) {
	rows := files[values.Keys]
	if rows == nil {
		rows = rowSums{}
		files[values.Keys] = rows
	}
	sums := rows[values.Row]
	if sums == nil {
		sums = make([]int64, len(values.Sums))
		rows[values.Row] = sums
	}
	for i, value := range values.Sums {
		sums[i] += sign * value
	}
}

// fileOf turns rows into a file at snapshot, ascending, without the rows whose sums are all 0.
func fileOf(rows rowSums, snapshot int64, sumsCount int) File {
	file := File{Snapshot: snapshot, Sums: make([][]int64, sumsCount)}
	for _, rowID := range slices.Sorted(maps.Keys(rows)) {
		if !slices.ContainsFunc(rows[rowID], func(sum int64) bool { return sum != 0 }) {
			continue
		}
		file.RowIDs = append(file.RowIDs, rowID)
		for i, sum := range rows[rowID] {
			file.Sums[i] = append(file.Sums[i], sum)
		}
	}
	return file
}

func rowSumsOf(file File) rowSums {
	rows := make(rowSums, len(file.RowIDs))
	for i, rowID := range file.RowIDs {
		sums := make([]int64, len(file.Sums))
		for j := range file.Sums {
			sums[j] = file.Sums[j][i]
		}
		rows[rowID] = sums
	}
	return rows
}

// expectedFiles sums what the incarnations held at snapshot into files, keeping the ones whose
// Keys[0] isInRange.
func expectedFiles(lives []*incarnation, snapshot int64, isInRange func(firstKey int64) bool) map[[MaxKeys]int64]rowSums {
	files := map[[MaxKeys]int64]rowSums{}
	for _, life := range lives {
		if values := life.valuesAt(snapshot); values != nil && isInRange(values.Keys[0]) {
			addValues(files, values, 1)
		}
	}
	return files
}

// CompactFrame is a run's compaction, as lock's holder: it brings the frame's files from snapshot W to
// target, then rewrites the _idx of the day folders it touched and of extendedDays (the days express
// compactions appended to), merging their _ixt. With target = W it only merges. readWrittenAfter (the
// records whose UpdatedVersion is above a snapshot) and readBySK read the table: DynamoDB, or a
// test's fake one.
func CompactFrame(lock *Lock, frame *Frame, W, target int64, extendedDays []int64,
	readWrittenAfter func(snapshot int64) ([]RecordState, error), readBySK func(sks []string) ([]RecordState, error)) error {
	var fileKeys [][MaxKeys]int64
	var fileHashes []uint32
	if target > W {
		lives, err := readIncarnations(lock.store, frame, W, readWrittenAfter, readBySK)
		if err != nil {
			return err
		}
		if fileKeys, fileHashes, err = compactFiles(lock, frame, lives, W, target); err != nil {
			return err
		}
	}
	if frame.KeyCount == 1 {
		return nil
	}
	return updateIndexes(lock, frame, fileKeys, fileHashes, extendedDays)
}

// readIncarnations reads every incarnation changed after snapshot W. The records are read before the
// log: a write appends its entry before it lands, so every change the record read saw is in the log
// read after it. Records with entries that the first read did not return (deleted, or whose write
// failed after logging) are read by sk, and the log read once more, for the same reason.
func readIncarnations(store Store, frame *Frame, W int64,
	readWrittenAfter func(snapshot int64) ([]RecordState, error), readBySK func(sks []string) ([]RecordState, error)) ([]*incarnation, error) {
	records, err := readWrittenAfter(W)
	if err != nil {
		return nil, err
	}
	entries, err := ReadLog(store, frame)
	if err != nil {
		return nil, err
	}
	isRead := map[string]bool{}
	for _, record := range records {
		isRead[record.SK] = true
	}
	var unreadSKs []string
	for _, entry := range entries {
		if entry.NewVersion > W && !isRead[entry.SK] {
			isRead[entry.SK] = true
			unreadSKs = append(unreadSKs, entry.SK)
		}
	}
	if len(unreadSKs) > 0 {
		unreadRecords, err := readBySK(unreadSKs)
		if err != nil {
			return nil, err
		}
		records = append(records, unreadRecords...)
		if entries, err = ReadLog(store, frame); err != nil {
			return nil, err
		}
	}
	return incarnations(records, entries, W), nil
}

// deltasBetween is, by file, what the incarnations held at to less what they held at from: one
// signed delta per row.
func deltasBetween(lives []*incarnation, from, to int64) map[[MaxKeys]int64]rowSums {
	deltas := map[[MaxKeys]int64]rowSums{}
	for _, life := range lives {
		before, after := life.valuesAt(from), life.valuesAt(to)
		if EqualValues(before, after) {
			continue
		}
		if before != nil {
			addValues(deltas, before, -1)
		}
		if after != nil {
			addValues(deltas, after, 1)
		}
	}
	return deltas
}

// addRowSums adds delta to rows.
func addRowSums(rows, delta rowSums) {
	for rowID, rowDelta := range delta {
		if !slices.ContainsFunc(rowDelta, func(sum int64) bool { return sum != 0 }) {
			continue
		}
		sums := rows[rowID]
		if sums == nil {
			sums = make([]int64, len(rowDelta))
			rows[rowID] = sums
		}
		for i := range sums {
			sums[i] += rowDelta[i]
		}
	}
}

// filesChangedBetween lists every file a change of lives in (from, to] names: the files of every value
// a changed incarnation held in that window.
func filesChangedBetween(lives []*incarnation, from, to int64) [][MaxKeys]int64 {
	isNamed := map[[MaxKeys]int64]bool{}
	name := func(values *Values) {
		if values != nil {
			isNamed[values.Keys] = true
		}
	}
	for _, life := range lives {
		if !life.changesBetween(from, to) {
			continue
		}
		name(life.valuesAt(from))
		name(life.valuesAt(to))
		for _, entry := range life.entries {
			if entry.NewVersion > from && entry.NewVersion <= to {
				name(entry.OldValues)
			}
		}
	}
	return slices.Collect(maps.Keys(isNamed))
}

// compactFiles brings to target every file a change of lives in (W, target] names, and returns their
// keys and their hashes after.
func compactFiles(lock *Lock, frame *Frame, lives []*incarnation, W, target int64) ([][MaxKeys]int64, []uint32, error) {
	fileKeys := filesChangedBetween(lives, W, target)
	deltas := &deltasToTarget{lives: lives, target: target, byFrom: map[int64]map[[MaxKeys]int64]rowSums{}}
	fileHashes := make([]uint32, len(fileKeys))
	err := parallel.Run(len(fileKeys), func(i int) (err error) {
		fileHashes[i], err = compactFile(lock, frame, fileKeys[i], W, target, deltas)
		return err
	})
	return fileKeys, fileHashes, err
}

// deltasToTarget is deltasBetween(lives, from, target) by file, computed once per from: the files of a
// compaction hold few distinct snapshots. Safe for concurrent use.
type deltasToTarget struct {
	mutex  sync.Mutex
	lives  []*incarnation
	target int64
	byFrom map[int64]map[[MaxKeys]int64]rowSums
}

func (deltas *deltasToTarget) of(from int64, keys [MaxKeys]int64) rowSums {
	deltas.mutex.Lock()
	defer deltas.mutex.Unlock()
	if deltas.byFrom[from] == nil {
		deltas.byFrom[from] = deltasBetween(deltas.lives, from, deltas.target)
	}
	return deltas.byFrom[from][keys]
}

// compactFile brings one file to target and returns its hash after. A file at or past target is left
// as it is: a compaction to that target or a later one wrote it, and crashed or lost its lock before
// it committed. A file older than W holds the frame at W too, and a missing one was never written (no
// compaction deletes a file), so it is empty at W; a file past W takes the changes after its own
// snapshot. The file is written even when its rows don't change, and an emptied one is kept, rows 0,
// so its snapshot moves to target.
func compactFile(lock *Lock, frame *Frame, keys [MaxKeys]int64, W, target int64, deltas *deltasToTarget) (uint32, error) {
	objectKey := frame.FileKey(keys)
	content, _, err := lock.store.Get(objectKey)
	isMissing := errors.Is(err, ErrObjectNotFound)
	if err != nil && !isMissing {
		return 0, err
	}
	rows, snapshot := rowSums{}, W
	if !isMissing {
		file, err := DecodeFile(content, frame.SumsCount)
		if err != nil {
			return 0, fmt.Errorf("db: frame file %s: %w: rebuild the frame", objectKey, err)
		}
		if file.Snapshot >= target {
			return FileHash(content), nil
		}
		rows, snapshot = rowSumsOf(file), max(file.Snapshot, W)
	}
	addRowSums(rows, deltas.of(snapshot, keys))
	content = appendFile(nil, fileOf(rows, target, frame.SumsCount), codecOptions{})
	if err := lock.put(objectKey, content); err != nil {
		return 0, err
	}
	return FileHash(content), nil
}

// RebuildAllFiles writes every file and _idx the records produce at snapshot, as lock's holder,
// without reading the stored ones (after a change of shape they are in another format), then deletes
// every other object of the frame but its log and its lock: the files nothing produces, every _ixt,
// and whatever the frame's Keys can't name (another shape's files). records were read before this
// call: the log it reads after them holds the entries of every record deleted or changed since
// snapshot.
func RebuildAllFiles(lock *Lock, frame *Frame, records []RecordState, snapshot int64) error {
	entries, err := ReadLog(lock.store, frame)
	if err != nil {
		return err
	}
	listedKeys, err := lock.store.List(frame.Folder)
	if err != nil {
		return err
	}
	contentByKey := map[string][]byte{}
	hashByKeysByDay := map[int64]map[[MaxKeys - 1]int64]uint32{}
	for keys, rows := range expectedFiles(incarnations(records, entries, snapshot), snapshot, func(int64) bool { return true }) {
		file := fileOf(rows, snapshot, frame.SumsCount)
		if len(file.RowIDs) == 0 {
			continue
		}
		content := appendFile(nil, file, codecOptions{})
		contentByKey[frame.FileKey(keys)] = content
		if frame.KeyCount > 1 {
			if hashByKeysByDay[keys[0]] == nil {
				hashByKeysByDay[keys[0]] = map[[MaxKeys - 1]int64]uint32{}
			}
			hashByKeysByDay[keys[0]][[MaxKeys - 1]int64{keys[1], keys[2]}] = FileHash(content)
		}
	}
	for day, hashByKeys := range hashByKeysByDay {
		contentByKey[frame.indexKey(day)] = appendIndex(nil, sortedIndexEntries(hashByKeys), frame.KeyCount)
	}
	objectKeys := slices.Collect(maps.Keys(contentByKey))
	if err := parallel.Run(len(objectKeys), func(i int) error { return lock.put(objectKeys[i], contentByKey[objectKeys[i]]) }); err != nil {
		return err
	}
	var staleKeys []string
	for _, listedKey := range listedKeys {
		if _, isWritten := contentByKey[listedKey]; !isWritten && listedKey != frame.LogKey() && listedKey != frame.lockKey() {
			staleKeys = append(staleKeys, listedKey)
		}
	}
	if len(staleKeys) == 0 {
		return nil
	}
	return lock.Delete(staleKeys...)
}

// RebuildFilesInRange rewrites, as lock's holder, the files whose Keys[0] is in [fromKey, toKey] as
// the records held them at snapshot; records are those of the range, read before this call (a record
// moved out of the range since snapshot is found through its log entry). On a 1-key frame it writes
// each day's file, or deletes it when nothing produces it. On a 2–3-key frame it writes the files
// whose hash differs from the day's index (its _idx with its _ixt merged), deletes the listed files
// nothing produces, then rewrites _idx and deletes _ixt.
func RebuildFilesInRange(lock *Lock, frame *Frame, records []RecordState, snapshot, fromKey, toKey int64) error {
	entries, err := ReadLog(lock.store, frame)
	if err != nil {
		return err
	}
	contentByKeysByDay := map[int64]map[[MaxKeys - 1]int64][]byte{}
	isInRange := func(firstKey int64) bool { return firstKey >= fromKey && firstKey <= toKey }
	for keys, rows := range expectedFiles(incarnations(records, entries, snapshot), snapshot, isInRange) {
		file := fileOf(rows, snapshot, frame.SumsCount)
		if len(file.RowIDs) == 0 {
			continue
		}
		if contentByKeysByDay[keys[0]] == nil {
			contentByKeysByDay[keys[0]] = map[[MaxKeys - 1]int64][]byte{}
		}
		contentByKeysByDay[keys[0]][[MaxKeys - 1]int64{keys[1], keys[2]}] = appendFile(nil, file, codecOptions{})
	}
	return parallel.Run(int(toKey-fromKey+1), func(offset int) error {
		day := fromKey + int64(offset)
		contentByKeys := contentByKeysByDay[day]
		if frame.KeyCount == 1 {
			fileKey := frame.FileKey([MaxKeys]int64{day})
			if content := contentByKeys[[MaxKeys - 1]int64{}]; content != nil {
				return lock.put(fileKey, content)
			}
			return lock.Delete(fileKey)
		}
		// A corrupt index lists nothing: the rebuild is what repairs it.
		storedHashByKeys, hasExtension, err := readIndex(lock.store, frame, day)
		if errors.Is(err, errCorrupt) {
			storedHashByKeys, err = map[[MaxKeys - 1]int64]uint32{}, nil
		}
		if err != nil {
			return err
		}
		hashByKeys := map[[MaxKeys - 1]int64]uint32{}
		for indexKeys, content := range contentByKeys {
			hashByKeys[indexKeys] = FileHash(content)
			if storedHash, isListed := storedHashByKeys[indexKeys]; isListed && storedHash == hashByKeys[indexKeys] {
				continue
			}
			if err := lock.put(frame.FileKey([MaxKeys]int64{day, indexKeys[0], indexKeys[1]}), content); err != nil {
				return err
			}
		}
		var staleKeys []string
		for indexKeys := range storedHashByKeys {
			if _, isProduced := hashByKeys[indexKeys]; !isProduced {
				staleKeys = append(staleKeys, frame.FileKey([MaxKeys]int64{day, indexKeys[0], indexKeys[1]}))
			}
		}
		if len(staleKeys) > 0 {
			if err := lock.Delete(staleKeys...); err != nil {
				return err
			}
		}
		if maps.Equal(storedHashByKeys, hashByKeys) && !hasExtension {
			return nil
		}
		return writeIndexMerged(lock, frame, day, hashByKeys, hasExtension)
	})
}

// AppendLog appends entries to the frame's log in one append. Writes call it before their base
// write, so a change a run sees in the table always has its entry; ctx carries the write's deadline.
func AppendLog(ctx context.Context, store Store, frame *Frame, entries []LogEntry) error {
	var encoded []byte
	for _, entry := range entries {
		encoded = appendLogEntry(encoded, entry, frame.KeyCount)
	}
	return store.Append(ctx, frame.LogKey(), encoded)
}

// ReadLog reads every entry of the frame's log; none when it is missing.
func ReadLog(store Store, frame *Frame) ([]LogEntry, error) {
	content, _, err := store.Get(frame.LogKey())
	if errors.Is(err, ErrObjectNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := decodeLog(content, frame.KeyCount, frame.SumsCount)
	if err != nil {
		return nil, fmt.Errorf("db: frame log %s: %w", frame.LogKey(), err)
	}
	return entries, nil
}

// TruncateLog drops the entries at or below snapshot, which no run reads again. A write appending
// meanwhile changes the ETag: the log is read again and the truncation retried.
func TruncateLog(store Store, frame *Frame, snapshot int64) error {
	for attempt := 1; ; attempt++ {
		content, etag, err := store.Get(frame.LogKey())
		if errors.Is(err, ErrObjectNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		entries, err := decodeLog(content, frame.KeyCount, frame.SumsCount)
		if err != nil {
			return fmt.Errorf("db: frame log %s: %w", frame.LogKey(), err)
		}
		var keptContent []byte
		for _, entry := range entries {
			if entry.NewVersion > snapshot {
				keptContent = appendLogEntry(keptContent, entry, frame.KeyCount)
			}
		}
		if len(keptContent) == len(content) {
			return nil
		}
		_, err = store.PutIfMatch(context.Background(), frame.LogKey(), keptContent, etag)
		if !errors.Is(err, ErrPreconditionFailed) || attempt == conditionalWriteAttempts {
			return err
		}
		time.Sleep(time.Duration(attempt*attempt) * 10 * time.Millisecond)
	}
}
