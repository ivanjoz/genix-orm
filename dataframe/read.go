package dataframe

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"

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

// Row is one row of a frame file: the file's Keys, the Rows value and the sums (Frame.Sums in order,
// then the record count when the frame counts records).
type Row struct {
	Keys  []int64
	RowID int64
	Sums  []int64
}

// Read returns the selected rows, sorted by Keys then by RowID, as the last run left them; isFresh
// also moves the records written since the frame's snapshot to their current values.
func Read(table *Table, frame *Frame, backend Backend, store utils.FileStore, selection Selection, isFresh bool) ([]Row, error) {
	readName := fmt.Sprintf("dataframe: %s frame %q", table.Name, frame.Name)
	if selection.FromKey < 0 || selection.ToKey < selection.FromKey || selection.ToKey-selection.FromKey >= MaxFirstKeys || len(selection.PinnedKeys) >= len(frame.Keys) {
		return nil, fmt.Errorf("%s takes 1 to %d non-negative values of %s and at most %d later Keys", readName, MaxFirstKeys, frame.Keys[0].FieldName, len(frame.Keys)-1)
	}
	for attempt := 1; ; attempt++ {
		state, err := backend.ReadState(frame)
		if err != nil {
			return nil, err
		}
		if state.Generation == "" || state.Shape != frame.Shape {
			return nil, fmt.Errorf("%s: %w", readName, ErrFrameNotBuilt)
		}
		fileKeys, err := frame.selectedFileKeys(store, state.Generation, selection)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", readName, err)
		}
		var histories []History
		if isFresh {
			if histories, err = backend.HistoriesWrittenAfter(state.Snapshot); err != nil {
				return nil, err
			}
			// A file a change creates is not listed yet.
			for _, fileKey := range table.filesHeldBetween(frame, state.Generation, histories, state.Snapshot, math.MaxInt64) {
				keys, _ := frame.keysOfFile(state.Generation, fileKey)
				if selection.matches(keys) && !slices.Contains(fileKeys, fileKey) {
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
			stateAfter, err := backend.ReadState(frame)
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
		return frame.rowsOf(state.Generation, fileKeys, loaded.rowsByFileKey), nil
	}
}

// selectedFileKeys names the selected files: directly when every Key is pinned, otherwise by listing
// the folder of each Keys[0] value.
func (frame *Frame) selectedFileKeys(store utils.FileStore, generation string, selection Selection) ([]string, error) {
	firstKeyCount := int(selection.ToKey - selection.FromKey + 1)
	if len(selection.PinnedKeys) == len(frame.Keys)-1 {
		fileKeys := make([]string, firstKeyCount)
		for i := range fileKeys {
			fileKeys[i] = frame.fileKeyOf(generation, append([]int64{selection.FromKey + int64(i)}, selection.PinnedKeys...))
		}
		return fileKeys, nil
	}
	listedByFirstKey := make([][]string, firstKeyCount)
	err := utils.RunParallel(firstKeyCount, func(firstKeyIndex int) error {
		folder := frame.Folder + generation + "/" + strconv.FormatInt(selection.FromKey+int64(firstKeyIndex), 10) + "/"
		listedKeys, err := store.ListFiles(folder)
		for _, listedKey := range listedKeys {
			if keys, isFrameFile := frame.keysOfFile(generation, listedKey); isFrameFile && selection.matches(keys) {
				listedByFirstKey[firstKeyIndex] = append(listedByFirstKey[firstKeyIndex], listedKey)
			}
		}
		return err
	})
	return slices.Concat(listedByFirstKey...), err
}

// rowsOf turns the files into rows, by Keys then by RowID.
func (frame *Frame) rowsOf(generation string, fileKeys []string, rowsByFileKey map[string]frameRows) []Row {
	keysByFileKey := map[string][]int64{}
	for _, fileKey := range fileKeys {
		keysByFileKey[fileKey], _ = frame.keysOfFile(generation, fileKey)
	}
	slices.SortFunc(fileKeys, func(a, b string) int { return slices.Compare(keysByFileKey[a], keysByFileKey[b]) })
	var rows []Row
	for _, fileKey := range fileKeys {
		file := rowsByFileKey[fileKey].fileAt(0, frame.SumsCount)
		for i, rowID := range file.RowIDs {
			row := Row{Keys: keysByFileKey[fileKey], RowID: rowID, Sums: make([]int64, frame.SumsCount)}
			for s := range row.Sums {
				row.Sums[s] = file.Sums[s][i]
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// frameRows is the content of one file while it is being changed: the sums per row.
type frameRows map[int64][]int64

func rowsOfFile(file File) frameRows {
	rows := frameRows{}
	for i, rowID := range file.RowIDs {
		sums := make([]int64, len(file.Sums))
		for j := range file.Sums {
			sums[j] = file.Sums[j][i]
		}
		rows[rowID] = sums
	}
	return rows
}

// add moves the row of values by sign (+1 joins it, −1 leaves it).
func (rows frameRows) add(values *frameValues, sign int64) {
	sums := rows[values.row]
	if sums == nil {
		sums = make([]int64, len(values.sums))
		rows[values.row] = sums
	}
	for i, sum := range values.sums {
		sums[i] += sign * sum
	}
}

// fileAt encodes the rows as a file at snapshot, rows ascending; a row whose sums are all 0 is dropped.
func (rows frameRows) fileAt(snapshot int64, sumsCount int) File {
	file := File{Snapshot: snapshot, Sums: make([][]int64, sumsCount)}
	for _, rowID := range slices.Sorted(maps.Keys(rows)) {
		sums := rows[rowID]
		if !slices.ContainsFunc(sums, func(sum int64) bool { return sum != 0 }) {
			continue
		}
		file.RowIDs = append(file.RowIDs, rowID)
		for j, sum := range sums {
			file.Sums[j] = append(file.Sums[j], sum)
		}
	}
	return file
}
