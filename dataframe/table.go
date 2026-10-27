// Package dataframe keeps group-by aggregates of a table as columnar files in a file store (S3),
// fed by the lines each record puts in them and a changelog of those lines. It holds no database
// code: a backend (genix-orm's dynamo and scylla) declares the frames from its schema, turns its
// records into lines and Histories and keeps the frames' state, through the Backend interface. See
// DATAFRAMES.md.
package dataframe

import (
	"fmt"
	"hash/crc32"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

const (
	// Folder is the store prefix of every frame: dtf/<table id>/<frame>/<partition>/<generation>/...,
	// the table id and the generation in utils.EncodeUintBase64, the partition in decimal. Each
	// partition has its own generation, so it is rebuilt alone.
	Folder = "dtf/"
	// FloatScale turns a float into the int64 frames keep it as: 6 decimals, so adding and
	// subtracting the same value is exact.
	FloatScale = 1e6
	// MaxMagnitude bounds every value of a line, so the sums of a file stay far from overflowing.
	MaxMagnitude = 1 << 60
	// MaxRowColumns bounds a frame's Rows: a row is named by up to 3 values (rowKey).
	MaxRowColumns = 3
)

var frameNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Column is one value of a frame's lines, as the backend describes it: its name, for messages, a
// stable id (genix-orm's cb tag or field position; a frame struct's field position) that names it in
// the frame's shape, and whether it is a float.
type Column struct {
	FieldName string
	ColumnID  int
	IsFloat   bool
}

// Line is one row a record puts in a frame: its Keys values (the file), its Rows values (the row) and
// its Sums, in that order (floats × FloatScale). A record puts 0..N lines in a frame; two lines of one
// file and row add up.
type Line []int64

// RecordLines are the lines one record puts in every frame of its table, by frame index.
type RecordLines [][]Line

// Spec declares one frame: one file per value of Keys (1 to 3 integer columns), one row per value of
// Rows (1 to 3 integer columns, the widest group first: department, program, employee) with the Sums
// of the lines that hold it, and their count with Count. The file nests the rows like folders: each
// value of Rows[0] once, with the values of Rows[1] under it, and so on (codec.go).
type Spec struct {
	Name  string
	Keys  []Column
	Rows  []Column
	Sums  []Column
	Count bool
	// Version is part of the shape: bump it when what LinesOf computes changes, which the columns
	// alone do not show (a ToFrames method's body).
	Version int
	// UseZeroSumsMask writes the zero sums mask in every file (codec.go): a summed column empty in a
	// whole file costs nothing. At most MaxMaskedSums summed columns (the Sums, then the count).
	UseZeroSumsMask bool
	// CompactOn makes a read compact the partition it reads when it is due (compact.go). Zero: only
	// Compact compacts it, and a read that finds it out of the log's reach.
	CompactOn CompactOn
	// LinesOf is what the record at the pointer puts in the frame: a backend builds it from the
	// frame's columns (one line, none for a soft-deleted record).
	LinesOf func(record unsafe.Pointer) ([]Line, error)
	// Projection declares the frame by a ToFrames method instead (ProjectionOf): Compile takes the
	// Keys, Rows, Sums and LinesOf from it, and the spec must list none of them.
	Projection *Projection
}

// Frame is one compiled frame.
type Frame struct {
	Name string
	// Index is the frame's position in Table.Frames, and in every RecordLines.
	Index            int
	Keys             []Column
	Rows             []Column
	Sums             []Column
	CountsRecords    bool
	UsesZeroSumsMask bool
	// IsProjected: declared by a ToFrames method, so its columns are a frame struct's, not the record's.
	IsProjected bool
	CompactOn   CompactOn
	// SumsCount is the number of summed columns in its files: the Sums, then the record count.
	SumsCount int
	// Shape names the layout of its files and what fills them (column ids, Count, the mask, the
	// Version, the format): another one is a rebuild.
	Shape string
	// shapeTag names the frame in the changelog: 16 bits of its Shape, so a logged line of another
	// shape is skipped (that frame is rebuilt anyway).
	shapeTag uint16
	Folder   string // dtf/<table id>/<name>/, a folder per partition in it
	linesOf  func(record unsafe.Pointer) ([]Line, error)
}

// Table is the compiled frames of one table.
type Table struct {
	Name string
	// Folder is the table's store prefix, dtf/<table id>/: its frames' folders and the versions file of
	// each partition (versionsKeyOf).
	Folder string
	Frames []Frame
}

// Compile checks the specs and compiles them. tableID names the table's folder (utils.EncodeUintBase64).
func Compile(tableName string, tableID uint64, specs []Spec) (*Table, error) {
	tableFolder := Folder + utils.EncodeUintBase64(tableID) + "/"
	table := &Table{Name: tableName, Folder: tableFolder}
	for _, spec := range specs {
		if spec.Projection != nil {
			if spec.Keys != nil || spec.Rows != nil || spec.Sums != nil || spec.LinesOf != nil {
				return nil, fmt.Errorf("dataframe: %s frame %q is declared by ToFrames: it takes no Keys, Rows, Sums or LinesOf", tableName, spec.Name)
			}
			spec.Keys, spec.Rows, spec.Sums, spec.LinesOf = spec.Projection.Keys, spec.Projection.Rows, spec.Projection.Sums, spec.Projection.linesOf
		}
		if !frameNamePattern.MatchString(spec.Name) || slices.ContainsFunc(table.Frames, func(frame Frame) bool { return frame.Name == spec.Name }) {
			return nil, fmt.Errorf("dataframe: %s frame name %q must be unique and hold only a-z, 0-9 and '-'", tableName, spec.Name)
		}
		if len(spec.Keys) == 0 || len(spec.Keys) > 3 || len(spec.Rows) == 0 || len(spec.Rows) > MaxRowColumns || (len(spec.Sums) == 0 && !spec.Count) {
			return nil, fmt.Errorf("dataframe: %s frame %q needs 1 to 3 Keys, 1 to %d Rows, and Sums or Count", tableName, spec.Name, MaxRowColumns)
		}
		if spec.CompactOn.Records < 0 || spec.CompactOn.Seconds < 0 {
			return nil, fmt.Errorf("dataframe: %s frame %q CompactOn takes no negative Records or Seconds", tableName, spec.Name)
		}
		if spec.LinesOf == nil {
			return nil, fmt.Errorf("dataframe: %s frame %q has no LinesOf", tableName, spec.Name)
		}
		frameColumns := slices.Concat(spec.Keys, spec.Rows, spec.Sums)
		for i, column := range frameColumns {
			if column.IsFloat && i < len(spec.Keys)+len(spec.Rows) {
				return nil, fmt.Errorf("dataframe: %s frame %q Keys and Rows must be integers, %q is a float", tableName, spec.Name, column.FieldName)
			}
			if slices.ContainsFunc(frameColumns[:i], func(earlier Column) bool { return earlier.FieldName == column.FieldName }) {
				return nil, fmt.Errorf("dataframe: %s frame %q lists %q twice", tableName, spec.Name, column.FieldName)
			}
		}
		frame := Frame{Name: spec.Name, Index: len(table.Frames), Keys: spec.Keys, Rows: spec.Rows, Sums: spec.Sums, CountsRecords: spec.Count,
			UsesZeroSumsMask: spec.UseZeroSumsMask, IsProjected: spec.Projection != nil, CompactOn: spec.CompactOn, SumsCount: len(spec.Sums), Folder: tableFolder + spec.Name + "/", linesOf: spec.LinesOf}
		if spec.Count {
			frame.SumsCount++
		}
		if frame.UsesZeroSumsMask && frame.SumsCount > MaxMaskedSums {
			return nil, fmt.Errorf("dataframe: %s frame %q sums %d columns: UseZeroSumsMask takes at most %d", tableName, spec.Name, frame.SumsCount, MaxMaskedSums)
		}
		frame.Shape = shapeOf(frame, spec.Version)
		frame.shapeTag = uint16(crc32.ChecksumIEEE([]byte(frame.Shape)))
		if slices.ContainsFunc(table.Frames, func(earlier Frame) bool { return earlier.shapeTag == frame.shapeTag }) {
			return nil, fmt.Errorf("dataframe: %s frame %q has the changelog tag of another frame: bump its Version", tableName, spec.Name)
		}
		table.Frames = append(table.Frames, frame)
	}
	return table, nil
}

// FrameNamed returns the frame called name, nil when the table has none.
func (table *Table) FrameNamed(name string) *Frame {
	for i := range table.Frames {
		if table.Frames[i].Name == name {
			return &table.Frames[i]
		}
	}
	return nil
}

// shapeOf hashes what the files are laid out by and filled with: the column ids of the Keys, the Rows
// and the Sums (and which are floats), the record count, the zero sums mask, the Version, and the
// layout version (v7: a folder and a state per partition), so a frame built otherwise is rebuilt.
func shapeOf(frame Frame, version int) string {
	columnIDs := func(columns []Column) string {
		ids := make([]string, len(columns))
		for i, column := range columns {
			ids[i] = strconv.Itoa(column.ColumnID)
			if column.IsFloat {
				ids[i] += "f"
			}
		}
		return strings.Join(ids, ".")
	}
	layout := fmt.Sprintf("v7|%d|%s|%s|%s|%t|%t", version, columnIDs(frame.Keys), columnIDs(frame.Rows), columnIDs(frame.Sums), frame.CountsRecords, frame.UsesZeroSumsMask)
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(layout)))
}

// ScaledFloat is a float value of a line: round(v × FloatScale), false when it is NaN or outside what
// a frame keeps (±MaxMagnitude).
func ScaledFloat(value float64) (int64, bool) {
	scaled := math.Round(value * FloatScale)
	if math.IsNaN(scaled) || math.Abs(scaled) >= MaxMagnitude {
		return 0, false
	}
	return int64(scaled), true
}

// LinesOf is what the record at the pointer puts in every frame. It refuses, before the record is
// written, a line a frame can't keep: a value past ±MaxMagnitude, a negative Keys or Rows value (they
// name files and ascend in them), or a line of the wrong length.
func (table *Table) LinesOf(record unsafe.Pointer) (RecordLines, error) {
	recordLines := make(RecordLines, len(table.Frames))
	for i := range table.Frames {
		frame := &table.Frames[i]
		lines, err := frame.linesOf(record)
		if err != nil {
			return nil, fmt.Errorf("dataframe: %s frame %q: %w", table.Name, frame.Name, err)
		}
		lineLength, keysAndRows := frame.lineLength(), len(frame.Keys)+len(frame.Rows)
		for _, line := range lines {
			if len(line) != lineLength {
				return nil, fmt.Errorf("dataframe: %s frame %q takes lines of %d values, got %d", table.Name, frame.Name, lineLength, len(line))
			}
			for v, value := range line {
				if value >= MaxMagnitude || value <= -MaxMagnitude {
					return nil, fmt.Errorf("dataframe: %s frame %q holds %d, outside what a frame keeps (±2^60)", table.Name, frame.Name, value)
				}
				if value < 0 && v < keysAndRows {
					return nil, fmt.Errorf("dataframe: %s frame %q takes no negative Keys or Rows value", table.Name, frame.Name)
				}
			}
		}
		recordLines[i] = lines
	}
	return recordLines, nil
}

// lineLength is how many values a line of the frame holds: its Keys, Rows and Sums.
func (frame *Frame) lineLength() int { return len(frame.Keys) + len(frame.Rows) + len(frame.Sums) }

// rowKey names a row of a file: its Rows values in order, the positions past len(Frame.Rows) 0. A
// fixed array, so it keys a map without an allocation.
type rowKey [MaxRowColumns]int64

func compareRowKeys(a, b rowKey) int { return slices.Compare(a[:], b[:]) }

// keysOf is the file a line goes to: its Keys values.
func (frame *Frame) keysOf(line Line) []int64 { return line[:len(frame.Keys)] }

// rowOf is the row of its file a line goes to.
func (frame *Frame) rowOf(line Line) rowKey {
	var row rowKey
	copy(row[:], line[len(frame.Keys):len(frame.Keys)+len(frame.Rows)])
	return row
}

// sumsOf is what a line adds to its row: its Sums.
func (frame *Frame) sumsOf(line Line) []int64 { return line[len(frame.Keys)+len(frame.Rows):] }

// partitionFolderOf is the folder of one partition's files: <folder><partition>/. Its generations are
// folders in it.
func (frame *Frame) partitionFolderOf(partition int64) string {
	return frame.Folder + strconv.FormatInt(partition, 10) + "/"
}

// generationFolderOf is the folder of one partition's files in a generation: <folder><partition>/<generation>/.
func (frame *Frame) generationFolderOf(generation string, partition int64) string {
	return frame.partitionFolderOf(partition) + generation + "/"
}

// fileKeyOf is the store key of the file of keys of a partition in a generation folder:
// <generation folder><k1> for one key, <generation folder><k1>/<k2>_<k3> for two or three.
func (frame *Frame) fileKeyOf(generation string, partition int64, keys []int64) string {
	fileKey := frame.generationFolderOf(generation, partition) + strconv.FormatInt(keys[0], 10)
	if len(keys) > 1 {
		laterKeys := make([]string, len(keys)-1)
		for i, key := range keys[1:] {
			laterKeys[i] = strconv.FormatInt(key, 10)
		}
		fileKey += "/" + strings.Join(laterKeys, "_")
	}
	return fileKey
}

// keysOfFile parses a file key of a partition's generation folder back into its keys (false: not a
// frame file of it).
func (frame *Frame) keysOfFile(generation string, partition int64, fileKey string) ([]int64, bool) {
	relativeKey, isInFolder := strings.CutPrefix(fileKey, frame.generationFolderOf(generation, partition))
	if !isInFolder {
		return nil, false
	}
	firstKey, laterKeys, hasLaterKeys := strings.Cut(relativeKey, "/")
	parts := []string{firstKey}
	if hasLaterKeys {
		parts = append(parts, strings.Split(laterKeys, "_")...)
	}
	if len(parts) != len(frame.Keys) {
		return nil, false
	}
	keys := make([]int64, len(parts))
	for i, part := range parts {
		key, err := strconv.ParseInt(part, 10, 64)
		if err != nil || key < 0 {
			return nil, false
		}
		keys[i] = key
	}
	return keys, true
}

// versionsKeyOf is the store key of a partition's versions file (versions.go): dtf/<table id>/<partition>.versions,
// beside the frames' folders and never inside one.
func (table *Table) versionsKeyOf(partition int64) string {
	return table.Folder + strconv.FormatInt(partition, 10) + ".versions"
}
