package dataframe

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/ivanjoz/genix-orm/utils"
)

const (
	// MaxFirstKeys bounds the values of Keys[0] one read selects: one GET (or LIST) each.
	MaxFirstKeys = 400
	// maxReadAttempts: a fresh read starts over when a run moved w while it read.
	maxReadAttempts = 3
)

// Selection is the files a read wants: Keys[0] in [FromKey, ToKey], then PinnedKeys, the values of a
// leading run of the later Keys. With every Key pinned the files are named; otherwise the folder of
// each Keys[0] value is listed.
type Selection struct {
	FromKey, ToKey int64
	PinnedKeys     []int64
}

func (selection Selection) matches(keys []int64) bool {
	return keys[0] >= selection.FromKey && keys[0] <= selection.ToKey && slices.Equal(keys[1:1+len(selection.PinnedKeys)], selection.PinnedKeys)
}

// matchesFile says whether a file key is a file of the partition's generation the selection takes.
func (selection Selection) matchesFile(frame *Frame, generation string, partition int64, fileKey string) bool {
	keys, isFrameFile := frame.keysOfFile(generation, partition, fileKey)
	return isFrameFile && selection.matches(keys)
}

// Row is one row of a frame file: the file's Keys, its Rows values (Frame.Rows in order) and the sums
// (Frame.Sums in order, then the record count when the frame counts records).
type Row struct {
	Keys      []int64
	RowValues []int64
	Sums      []int64
}

// Read returns the selected rows of one partition, sorted by Keys then by RowValues, as the last
// compaction left them; isFresh also moves the partition's records written since the frame's snapshot
// to their current values. It compacts the partition first when it must or is due (compact.go): a
// read that isn't fresh reads no log, so only CompactOn.Seconds makes it due.
func Read(table *Table, frame *Frame, backend Backend, store utils.FileStore, partition int64, selection Selection, isFresh bool, now time.Time) ([]Row, error) {
	readName := fmt.Sprintf("dataframe: %s frame %q partition %d", table.Name, frame.Name, partition)
	if err := frame.checkSelection(readName, selection); err != nil {
		return nil, err
	}
	isCompacted := false
	for attempt := 1; ; attempt++ {
		state, err := backend.ReadState(frame, partition)
		if err != nil {
			return nil, err
		}
		if frame.needsRebuild(state, now) {
			if isCompacted {
				return nil, fmt.Errorf("%s: %w", readName, ErrFrameNotBuilt)
			}
			if err := table.rebuildBeforeRead(backend, store, frame, partition, state, now); err != nil {
				return nil, fmt.Errorf("%s: %w", readName, err)
			}
			isCompacted = true
			continue
		}
		var histories []History
		if isFresh {
			if histories, err = backend.PartitionHistoriesWrittenAfter(partition, state.Snapshot); err != nil {
				return nil, err
			}
		}
		if !isCompacted && frame.isCompactDue(state, histories, now) {
			isCompacted = true
			if table.compactDueFrame(backend, store, frame, partition, state, now) {
				continue
			}
		}
		fileKeys, err := frame.selectedFileKeys(store, state.Generation, partition, selection)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", readName, err)
		}
		if isFresh {
			// A file a change creates is not listed yet.
			for _, fileKey := range table.filesHeldBetween(frame, state.Generation, histories, state.Snapshot, math.MaxInt64) {
				if selection.matchesFile(frame, state.Generation, partition, fileKey) && !slices.Contains(fileKeys, fileKey) {
					fileKeys = append(fileKeys, fileKey)
				}
			}
		}
		loaded, err := loadFiles(store, frame, fileKeys, state.Snapshot)
		if err != nil {
			return nil, err
		}
		if isFresh {
			table.moveRecords(frame, state.Generation, histories, loaded, math.MaxInt64)
			// A run that moved w meanwhile wrote files this read took as of the old w.
			stateAfter, err := backend.ReadState(frame, partition)
			if err != nil {
				return nil, err
			}
			if stateAfter != state {
				if attempt == maxReadAttempts {
					return nil, fmt.Errorf("%s: the frame kept moving while it was read: retry", readName)
				}
				continue
			}
		}
		return frame.rowsOf(state.Generation, partition, fileKeys, loaded.rowsByFileKey), nil
	}
}

func (frame *Frame) checkSelection(readName string, selection Selection) error {
	if selection.FromKey < 0 || selection.ToKey < selection.FromKey || selection.ToKey-selection.FromKey >= MaxFirstKeys || len(selection.PinnedKeys) >= len(frame.Keys) {
		return fmt.Errorf("%s takes 1 to %d non-negative values of %s and at most %d later Keys", readName, MaxFirstKeys, frame.Keys[0].FieldName, len(frame.Keys)-1)
	}
	return nil
}

// selectedFileKeys names the selected files: directly when every Key is pinned, otherwise by listing
// the folder of each Keys[0] value.
func (frame *Frame) selectedFileKeys(store utils.FileStore, generation string, partition int64, selection Selection) ([]string, error) {
	firstKeyCount := int(selection.ToKey - selection.FromKey + 1)
	if len(selection.PinnedKeys) == len(frame.Keys)-1 {
		fileKeys := make([]string, firstKeyCount)
		for i := range fileKeys {
			fileKeys[i] = frame.fileKeyOf(generation, partition, append([]int64{selection.FromKey + int64(i)}, selection.PinnedKeys...))
		}
		return fileKeys, nil
	}
	listedByFirstKey := make([][]string, firstKeyCount)
	err := utils.RunParallelLimit(maxConcurrentFileTasks, firstKeyCount, func(firstKeyIndex int) error {
		folder := frame.generationFolderOf(generation, partition) + strconv.FormatInt(selection.FromKey+int64(firstKeyIndex), 10) + "/"
		listedKeys, err := store.ListFiles(folder)
		for _, listedKey := range listedKeys {
			if selection.matchesFile(frame, generation, partition, listedKey) {
				listedByFirstKey[firstKeyIndex] = append(listedByFirstKey[firstKeyIndex], listedKey)
			}
		}
		return err
	})
	return slices.Concat(listedByFirstKey...), err
}

// rowsOf turns the files into rows, by Keys then by RowValues.
func (frame *Frame) rowsOf(generation string, partition int64, fileKeys []string, rowsByFileKey map[string]*frameRows) []Row {
	keysByFileKey := map[string][]int64{}
	for _, fileKey := range fileKeys {
		keysByFileKey[fileKey], _ = frame.keysOfFile(generation, partition, fileKey)
	}
	slices.SortFunc(fileKeys, func(a, b string) int { return slices.Compare(keysByFileKey[a], keysByFileKey[b]) })
	var rows []Row
	rowColumnCount := len(frame.Rows)
	for _, fileKey := range fileKeys {
		file := rowsByFileKey[fileKey].fileAt(0, frame)
		// One backing array per file for every row's values and sums: two allocations, not two per row.
		rowWidth := rowColumnCount + frame.SumsCount
		backing := make([]int64, file.rowCount()*rowWidth)
		for i := range file.rowCount() {
			rowBacking := backing[i*rowWidth : (i+1)*rowWidth : (i+1)*rowWidth]
			for c := range rowColumnCount {
				rowBacking[c] = file.Rows[c][i]
			}
			for s := range frame.SumsCount {
				rowBacking[rowColumnCount+s] = file.Sums[s][i]
			}
			rows = append(rows, Row{Keys: keysByFileKey[fileKey], RowValues: rowBacking[:rowColumnCount:rowColumnCount], Sums: rowBacking[rowColumnCount:]})
		}
	}
	return rows
}

// frameRows is the content of one file while it is being changed: the rows it was read with, as
// decoded (ascending, never a row of zero sums), and what the lines moved since add to each row they
// touch. A run moves a few rows of many files, so the read rows are never put in a map: fileAt merges
// the two once, when the file is written.
type frameRows struct {
	read  File
	moved map[rowKey][]int64
}

func rowsOfFile(file File) *frameRows { return &frameRows{read: file} }

// add moves the row of a line of the frame by sign (+1 joins it, −1 leaves it): its Sums, and 1 to
// the count when the frame counts records.
func (rows *frameRows) add(frame *Frame, line Line, sign int64) {
	if rows.moved == nil {
		rows.moved = map[rowKey][]int64{}
	}
	row := frame.rowOf(line)
	sums := rows.moved[row]
	if sums == nil {
		sums = make([]int64, frame.SumsCount)
		rows.moved[row] = sums
	}
	for i, sum := range frame.sumsOf(line) {
		sums[i] += sign * sum
	}
	if frame.CountsRecords {
		sums[frame.SumsCount-1] += sign
	}
}

// fileAt is the rows as a file of the frame at snapshot, rows ascending; a row whose sums are all 0 is
// dropped. A nil frameRows is a file without rows.
func (rows *frameRows) fileAt(snapshot int64, frame *Frame) File {
	rowColumnCount, sumsCount := len(frame.Rows), frame.SumsCount
	file := File{Snapshot: snapshot, Rows: make([][]int64, rowColumnCount), Sums: make([][]int64, sumsCount), UsesZeroSumsMask: frame.UsesZeroSumsMask}
	if rows == nil {
		return file
	}
	movedRows := slices.SortedFunc(maps.Keys(rows.moved), compareRowKeys)
	readCount := rows.read.rowCount()
	capacity := readCount + len(movedRows)
	if capacity == 0 {
		return file
	}
	backing := make([]int64, capacity*(rowColumnCount+sumsCount))
	for c := range file.Rows {
		file.Rows[c], backing = backing[:0:capacity], backing[capacity:]
	}
	for s := range file.Sums {
		file.Sums[s], backing = backing[:0:capacity], backing[capacity:]
	}
	readRowAt := func(r int) rowKey {
		var row rowKey
		for c := range rowColumnCount {
			row[c] = rows.read.Rows[c][r]
		}
		return row
	}
	sums := make([]int64, sumsCount)
	for r, m := 0, 0; r < readCount || m < len(movedRows); {
		var row rowKey
		order := -1 // the read row comes first
		if r == readCount {
			order = 1
		} else if row = readRowAt(r); m < len(movedRows) {
			order = compareRowKeys(row, movedRows[m])
		}
		clear(sums)
		if order <= 0 {
			for s := range sums {
				sums[s] = rows.read.Sums[s][r]
			}
			r++
		}
		if order >= 0 {
			row = movedRows[m]
			for s, sum := range rows.moved[row] {
				sums[s] += sum
			}
			m++
		}
		if !slices.ContainsFunc(sums, func(sum int64) bool { return sum != 0 }) {
			continue
		}
		for c := range file.Rows {
			file.Rows[c] = append(file.Rows[c], row[c])
		}
		for s, sum := range sums {
			file.Sums[s] = append(file.Sums[s], sum)
		}
	}
	return file
}
