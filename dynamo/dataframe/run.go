package dataframe

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/ivanjoz/genix-orm/dynamo/internal/parallel"
)

// ─────────────────────────────────────────────────────────────────────────────
// The file work of a run (../DATA_FRAMES_PLAN.md, "The snapshot rule" and after),
// also used by the fresh read (ReadFreshFiles), which applies it in memory.
// A version is reserved before its write lands, so the caller targets a version
// only once it is settled: every write that took a version up to it has landed and
// logged. A run from W to the target takes out of the files what each incarnation
// held at W and adds what it held at the target (valuesAt). A file carries the
// snapshot it was written at, so a run that crashed halfway is retried to the same
// target and skips the files it already wrote.
// ─────────────────────────────────────────────────────────────────────────────

// logTruncateAttempts bounds the conditional rewrite of the log, which every write may append to.
const logTruncateAttempts = 8

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

// CompactFrame brings the frame's files from snapshot W to target. readWrittenAfter (the records
// whose UpdatedVersion is above a snapshot) and readBySK read the table: DynamoDB, or a test's fake one.
func CompactFrame(store Store, frame *Frame, W, target int64,
	readWrittenAfter func(snapshot int64) ([]RecordState, error), readBySK func(sks []string) ([]RecordState, error)) error {
	lives, err := readIncarnations(store, frame, W, readWrittenAfter, readBySK)
	if err != nil {
		return err
	}
	return compactFiles(store, frame, lives, W, target)
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

// addRowSums adds delta to rows and reports whether any sum changed.
func addRowSums(rows, delta rowSums) bool {
	isChanged := false
	for rowID, rowDelta := range delta {
		if !slices.ContainsFunc(rowDelta, func(sum int64) bool { return sum != 0 }) {
			continue
		}
		isChanged = true
		sums := rows[rowID]
		if sums == nil {
			sums = make([]int64, len(rowDelta))
			rows[rowID] = sums
		}
		for i := range sums {
			sums[i] += rowDelta[i]
		}
	}
	return isChanged
}

// compactFiles brings the files the incarnations touch from snapshot W to target, then the _idx
// of their folders.
func compactFiles(store Store, frame *Frame, lives []*incarnation, W, target int64) error {
	deltas := deltasBetween(lives, W, target)
	fileKeys := slices.Collect(maps.Keys(deltas))
	fileHashes := make([]*uint32, len(fileKeys))
	err := parallel.Run(len(fileKeys), func(i int) (err error) {
		fileHashes[i], err = compactFile(store, frame, fileKeys[i], deltas[fileKeys[i]], target)
		return err
	})
	if err != nil || frame.KeyCount == 1 {
		return err
	}
	return updateIndexes(store, frame, fileKeys, fileHashes)
}

// compactFile adds delta to one file at target and returns its hash after, nil when there is no
// file. A file already at target was written by a run that crashed before its commit, to this same
// target: it is left as it is. A missing file is empty: nothing at W counted in it (a rebuild
// deletes the files it leaves empty). An emptied file is kept, rows 0, so the snapshot of its last
// change survives.
func compactFile(store Store, frame *Frame, keys [MaxKeys]int64, delta rowSums, target int64) (*uint32, error) {
	objectKey := frame.FileKey(keys)
	content, _, err := store.Get(objectKey)
	isMissing := errors.Is(err, ErrObjectNotFound)
	if err != nil && !isMissing {
		return nil, err
	}
	rows := rowSums{}
	if !isMissing {
		file, err := DecodeFile(content, frame.SumsCount)
		if err != nil {
			return nil, fmt.Errorf("db: frame file %s: %w: rebuild the frame", objectKey, err)
		}
		if file.Snapshot >= target {
			hash := FileHash(content)
			return &hash, nil
		}
		rows = rowSumsOf(file)
	}
	if !addRowSums(rows, delta) {
		if isMissing {
			return nil, nil
		}
		hash := FileHash(content)
		return &hash, nil
	}
	content = appendFile(nil, fileOf(rows, target, frame.SumsCount), codecOptions{})
	if err := store.Put(objectKey, content); err != nil {
		return nil, err
	}
	hash := FileHash(content)
	return &hash, nil
}

// updateIndexes sets in the _idx of each day folder the hash of every touched file of it, or
// drops the file when there is none.
func updateIndexes(store Store, frame *Frame, fileKeys [][MaxKeys]int64, fileHashes []*uint32) error {
	touchedFilesByDay := map[int64][]int{}
	for i, keys := range fileKeys {
		touchedFilesByDay[keys[0]] = append(touchedFilesByDay[keys[0]], i)
	}
	days := slices.Collect(maps.Keys(touchedFilesByDay))
	return parallel.Run(len(days), func(position int) error {
		day := days[position]
		hashByKeys, err := readIndex(store, frame, day)
		if err != nil {
			return err
		}
		isChanged := false
		for _, i := range touchedFilesByDay[day] {
			indexKeys := [MaxKeys - 1]int64{fileKeys[i][1], fileKeys[i][2]}
			storedHash, isListed := hashByKeys[indexKeys]
			switch {
			case fileHashes[i] == nil && isListed:
				delete(hashByKeys, indexKeys)
				isChanged = true
			case fileHashes[i] != nil && (!isListed || storedHash != *fileHashes[i]):
				hashByKeys[indexKeys] = *fileHashes[i]
				isChanged = true
			}
		}
		if !isChanged {
			return nil
		}
		return writeIndex(store, frame, day, hashByKeys)
	})
}

// readIndex reads a day folder's _idx as file keys → hash; empty when it is missing.
func readIndex(store Store, frame *Frame, day int64) (map[[MaxKeys - 1]int64]uint32, error) {
	hashByKeys := map[[MaxKeys - 1]int64]uint32{}
	content, _, err := store.Get(frame.indexKey(day))
	if errors.Is(err, ErrObjectNotFound) {
		return hashByKeys, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := DecodeIndex(content, frame.KeyCount)
	if err != nil {
		return nil, fmt.Errorf("db: frame index %s: %w: rebuild the frame", frame.indexKey(day), err)
	}
	for _, entry := range entries {
		hashByKeys[entry.Keys] = entry.Hash
	}
	return hashByKeys, nil
}

// writeIndex writes a day folder's _idx, sorted by keys; a folder left without files loses it.
func writeIndex(store Store, frame *Frame, day int64, hashByKeys map[[MaxKeys - 1]int64]uint32) error {
	if len(hashByKeys) == 0 {
		return store.Delete(frame.indexKey(day))
	}
	return store.Put(frame.indexKey(day), encodeIndex(frame, hashByKeys))
}

func encodeIndex(frame *Frame, hashByKeys map[[MaxKeys - 1]int64]uint32) []byte {
	entries := make([]IndexEntry, 0, len(hashByKeys))
	for keys, hash := range hashByKeys {
		entries = append(entries, IndexEntry{Keys: keys, Hash: hash})
	}
	slices.SortFunc(entries, func(a, b IndexEntry) int { return slices.Compare(a.Keys[:], b.Keys[:]) })
	return appendIndex(nil, entries, frame.KeyCount)
}

// RebuildAllFiles writes every file and _idx the records produce at snapshot, without reading the
// stored ones (after a change of shape they are in another format), then deletes every other object
// of the frame but its log. records were read before this call: the log it reads after them holds
// the entries of every record deleted or changed since snapshot.
func RebuildAllFiles(store Store, frame *Frame, records []RecordState, snapshot int64) error {
	entries, err := ReadLog(store, frame)
	if err != nil {
		return err
	}
	listedKeys, err := store.List(frame.Folder)
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
		contentByKey[frame.indexKey(day)] = encodeIndex(frame, hashByKeys)
	}
	objectKeys := slices.Collect(maps.Keys(contentByKey))
	if err := parallel.Run(len(objectKeys), func(i int) error { return store.Put(objectKeys[i], contentByKey[objectKeys[i]]) }); err != nil {
		return err
	}
	var staleKeys []string
	for _, listedKey := range listedKeys {
		if _, isWritten := contentByKey[listedKey]; !isWritten && listedKey != frame.LogKey() {
			staleKeys = append(staleKeys, listedKey)
		}
	}
	if len(staleKeys) == 0 {
		return nil
	}
	return store.Delete(staleKeys...)
}

// RebuildFilesInRange rewrites the files whose Keys[0] is in [fromKey, toKey] as the records held
// them at snapshot; records are those of the range, read before this call (a record moved out of
// the range since snapshot is found through its log entry). On a 1-key frame it writes each day's
// file, or deletes it when nothing produces it. On a 2–3-key frame it writes the files whose hash
// differs from the day's _idx and deletes the ones it lists that nothing produces anymore.
func RebuildFilesInRange(store Store, frame *Frame, records []RecordState, snapshot, fromKey, toKey int64) error {
	entries, err := ReadLog(store, frame)
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
			if content := contentByKeys[[MaxKeys - 1]int64{}]; content != nil {
				return store.Put(frame.FileKey([MaxKeys]int64{day}), content)
			}
			return store.Delete(frame.FileKey([MaxKeys]int64{day}))
		}
		// A corrupt index lists nothing: the rebuild is what repairs it.
		storedHashByKeys, err := readIndex(store, frame, day)
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
			if err := store.Put(frame.FileKey([MaxKeys]int64{day, indexKeys[0], indexKeys[1]}), content); err != nil {
				return err
			}
		}
		var staleKeys []string
		for indexKeys := range storedHashByKeys {
			if _, isExpected := hashByKeys[indexKeys]; !isExpected {
				staleKeys = append(staleKeys, frame.FileKey([MaxKeys]int64{day, indexKeys[0], indexKeys[1]}))
			}
		}
		if len(staleKeys) > 0 {
			if err := store.Delete(staleKeys...); err != nil {
				return err
			}
		}
		if maps.Equal(storedHashByKeys, hashByKeys) {
			return nil
		}
		return writeIndex(store, frame, day, hashByKeys)
	})
}

// AppendLog appends entries to the frame's log in one append. Writes call it before their base
// write, so a change a run sees in the table always has its entry.
func AppendLog(store Store, frame *Frame, entries []LogEntry) error {
	var encoded []byte
	for _, entry := range entries {
		encoded = appendLogEntry(encoded, entry, frame.KeyCount)
	}
	return store.Append(frame.LogKey(), encoded)
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
		err = store.PutIfMatch(frame.LogKey(), keptContent, etag)
		if !errors.Is(err, ErrPreconditionFailed) || attempt == logTruncateAttempts {
			return err
		}
		time.Sleep(time.Duration(attempt*attempt) * 10 * time.Millisecond)
	}
}
