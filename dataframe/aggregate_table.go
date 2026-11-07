package dataframe

import (
	"cmp"
	"fmt"
	"hash/crc32"
	"slices"
	"strconv"
)

// ─────────────────────────────────────────────────────────────────────────────
// SQL aggregate tables: how genix-orm's SQL drivers (pgdriver, mariadbdriver) keep a frame. Instead
// of files in a store, compacted from a change log, one table per frame holds every row of every
// file, already summed:
//
//	(partition, packed key) → one BIGINT per summed column, updated
//
// The write path keeps it exact in the record's own transaction: it subtracts the lines the stored
// record gave and adds the lines the written one gives (AddLines), one upsert per touched row. So a
// read is a range scan, with nothing to compact, no lock and no store.
//
// The packed key is the line's Keys then Rows, each in its Column.Bits, the first Key in the high
// bits: rows sort by Keys then Rows, a range of the first Key is a range of packed keys, and a file
// is a contiguous run. The frame's columns must fit 63 bits (CheckPackedKey).
//
// A row whose sums net to zero stays, with the moment it emptied: ReadChanged must still tell a
// client holding the file that the row is gone.
// ─────────────────────────────────────────────────────────────────────────────

// MaxPackedKeyBits is the width of a packed key: a BIGINT's non-negative range.
const MaxPackedKeyBits = 63

// AggregateRow is one row of an aggregate table: its packed key, its sums (Frame.SumsCount of them:
// the Sums, then the record count) and the SUnixTime of its last change.
type AggregateRow struct {
	PackedKey int64
	Sums      []int64
	Updated   int64
}

// keyColumns are the frame's packed-key columns: its Keys, then its Rows.
func (frame *Frame) keyColumns() []Column { return slices.Concat(frame.Keys, frame.Rows) }

// CheckPackedKey refuses a frame whose Keys and Rows don't fit a packed key: a SQL driver calls it
// when the schema compiles.
func (frame *Frame) CheckPackedKey(tableName string) error {
	totalBits := 0
	for _, column := range frame.keyColumns() {
		if column.Bits < 1 {
			return fmt.Errorf(`dataframe: %s frame %q column %s has no packed-key width: declare it (frame:"row,20")`, tableName, frame.Name, column.FieldName)
		}
		totalBits += column.Bits
	}
	if totalBits > MaxPackedKeyBits {
		return fmt.Errorf(`dataframe: %s frame %q Keys and Rows take %d bits, a packed key holds %d: declare narrower widths (frame:"row,20")`,
			tableName, frame.Name, totalBits, MaxPackedKeyBits)
	}
	return nil
}

// PackKey is the packed key of a line's Keys and Rows. A value past its column's width fails: the
// write refuses it before anything commits.
func (frame *Frame) PackKey(line Line) (int64, error) {
	packed := int64(0)
	for position, column := range frame.keyColumns() {
		value := line[position]
		if value < 0 || value >= int64(1)<<column.Bits {
			return 0, fmt.Errorf("dataframe: frame %q column %s holds %d, past its %d-bit packed-key width", frame.Name, column.FieldName, value, column.Bits)
		}
		packed = packed<<column.Bits | value
	}
	return packed, nil
}

// unpackKey splits a packed key into its Keys and its Rows values.
func (frame *Frame) unpackKey(packed int64) (keys, rowValues []int64) {
	columns := frame.keyColumns()
	values := make([]int64, len(columns))
	for position := len(columns) - 1; position >= 0; position-- {
		values[position] = packed & (int64(1)<<columns[position].Bits - 1)
		packed >>= columns[position].Bits
	}
	return values[:len(frame.Keys)], values[len(frame.Keys):]
}

// PackedKeyRange is the inclusive range of packed keys a selection's files lie in: Keys[0] from
// FromKey to ToKey, with the pinned Keys after it. With pinned Keys and more than one Keys[0] value
// the range also spans unpinned files, which RowsOfAggregate drops.
func (frame *Frame) PackedKeyRange(selection Selection) (from, to int64) {
	columns := frame.keyColumns()
	bound := func(firstKey int64, fillWithMax bool) int64 {
		packed := int64(0)
		for position, column := range columns {
			value := int64(0)
			switch {
			case position == 0:
				value = firstKey
			case position <= len(selection.PinnedKeys):
				value = selection.PinnedKeys[position-1]
			case fillWithMax:
				value = int64(1)<<column.Bits - 1
			}
			value = min(max(value, 0), int64(1)<<column.Bits-1)
			packed = packed<<column.Bits | value
		}
		return packed
	}
	return bound(selection.FromKey, false), bound(selection.ToKey, true)
}

// AddLines adds lines to the deltas by packed key, each multiplied by sign: +1 for the lines a
// written record gives, -1 for the lines its stored version gave. The record count, when the frame
// keeps one, moves by sign per line.
func (frame *Frame) AddLines(deltas map[int64][]int64, lines []Line, sign int64) error {
	for _, line := range lines {
		packed, err := frame.PackKey(line)
		if err != nil {
			return err
		}
		sums := deltas[packed]
		if sums == nil {
			sums = make([]int64, frame.SumsCount)
			deltas[packed] = sums
		}
		for position, value := range frame.sumsOf(line) {
			sums[position] += sign * value
		}
		if frame.CountsRecords {
			sums[frame.SumsCount-1] += sign
		}
	}
	return nil
}

// SortedDeltas are the deltas that move a sum, by ascending packed key: the order every write
// upserts them in, so two writes of one partition lock their shared rows in the same order and
// never deadlock on them.
func SortedDeltas(deltas map[int64][]int64) []AggregateRow {
	rows := make([]AggregateRow, 0, len(deltas))
	for packed, sums := range deltas {
		if slices.ContainsFunc(sums, func(sum int64) bool { return sum != 0 }) {
			rows = append(rows, AggregateRow{PackedKey: packed, Sums: sums})
		}
	}
	slices.SortFunc(rows, func(left, right AggregateRow) int { return cmp.Compare(left.PackedKey, right.PackedKey) })
	return rows
}

// RowsOfAggregate turns the aggregate rows of a selection's range (ascending packed keys) into the
// frame's rows: the selected files only, rows that net to zero dropped.
func (frame *Frame) RowsOfAggregate(aggregateRows []AggregateRow, selection Selection) []Row {
	rows := make([]Row, 0, len(aggregateRows))
	for _, aggregateRow := range aggregateRows {
		if !slices.ContainsFunc(aggregateRow.Sums, func(sum int64) bool { return sum != 0 }) {
			continue
		}
		keys, rowValues := frame.unpackKey(aggregateRow.PackedKey)
		if selection.matches(keys) {
			rows = append(rows, Row{Keys: keys, RowValues: rowValues, Sums: aggregateRow.Sums})
		}
	}
	return rows
}

// AggregateShape names an aggregate table's layout: the frame's Shape and its packed-key widths. A
// driver rebuilds the table when it changes.
func (frame *Frame) AggregateShape() string {
	layout := frame.Shape
	for _, column := range frame.keyColumns() {
		layout += "|" + strconv.Itoa(column.Bits)
	}
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte("aggregate-v1|"+layout)))
}

// ChangedFilesOfAggregate answers a client holding raw files (ReadChanged's wire) from the aggregate
// rows of a selection's range, ascending. A held copy at snapshot S is resent when a row of its file
// changed after S; every file goes out when the client holds another generation (the aggregate's
// shape). The files carry the snapshot now − TargetLag, not now: a write stamped just before now may
// commit after this read, and a snapshot below its stamp makes the next read resend its file.
func (frame *Frame) ChangedFilesOfAggregate(aggregateRows []AggregateRow, selection Selection, heldGeneration string, held []HeldFile, now int64) ChangedFiles {
	snapshot := now - TargetLag
	generation := frame.AggregateShape()
	answer := ChangedFiles{Generation: generation, RowColumnCount: uint8(len(frame.Rows)), SumsCount: uint8(frame.SumsCount), Snapshot: snapshot}
	heldSnapshots := map[string]int64{}
	if heldGeneration == generation {
		for _, heldFile := range held {
			heldSnapshots[keysToken(heldFile.Keys)] = heldFile.Snapshot
		}
	}
	isAnswered := map[string]bool{}

	// The rows of one file are a contiguous run of the ascending packed keys.
	for runStart := 0; runStart < len(aggregateRows); {
		keys, _ := frame.unpackKey(aggregateRows[runStart].PackedKey)
		runEnd, lastUpdated := runStart, int64(0)
		file := File{Snapshot: snapshot, Slot: 1, Rows: make([][]int64, len(frame.Rows)), Sums: make([][]int64, frame.SumsCount), UsesZeroSumsMask: frame.UsesZeroSumsMask}
		for ; runEnd < len(aggregateRows); runEnd++ {
			rowKeys, rowValues := frame.unpackKey(aggregateRows[runEnd].PackedKey)
			if !slices.Equal(rowKeys, keys) {
				break
			}
			lastUpdated = max(lastUpdated, aggregateRows[runEnd].Updated)
			if !slices.ContainsFunc(aggregateRows[runEnd].Sums, func(sum int64) bool { return sum != 0 }) {
				continue
			}
			for position, value := range rowValues {
				file.Rows[position] = append(file.Rows[position], value)
			}
			for position, sum := range aggregateRows[runEnd].Sums {
				file.Sums[position] = append(file.Sums[position], sum)
			}
		}
		runStart = runEnd
		if !selection.matches(keys) {
			continue
		}
		token := keysToken(keys)
		isAnswered[token] = true
		heldSnapshot, isHeld := heldSnapshots[token]
		hasRows := len(file.Rows[0]) > 0
		if (isHeld && heldSnapshot >= lastUpdated) || (!isHeld && !hasRows) {
			continue
		}
		changed := ChangedFile{Keys: keys, Snapshot: snapshot, Slot: 1}
		if hasRows {
			changed.Content = EncodeFile(file)
		}
		answer.Files = append(answer.Files, changed)
	}
	// A held file the table has no row of: answered empty.
	for _, heldFile := range held {
		if token := keysToken(heldFile.Keys); !isAnswered[token] && len(heldFile.Keys) == len(frame.Keys) && selection.matches(heldFile.Keys) {
			isAnswered[token] = true
			answer.Files = append(answer.Files, ChangedFile{Keys: heldFile.Keys, Snapshot: snapshot, Slot: 1})
		}
	}
	return answer
}

func keysToken(keys []int64) string { return fmt.Sprint(keys) }
