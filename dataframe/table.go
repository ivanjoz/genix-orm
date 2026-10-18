// Package dataframe keeps group-by aggregates of a table as columnar files in a file store (S3),
// fed by a changelog each record carries. It holds no database code: a backend (genix-orm's dynamo,
// later scylla) declares the frames from its schema, reads its records into Histories and keeps the
// frames' state, through the Backend interface. See DATAFRAMES.md.
package dataframe

import (
	"fmt"
	"hash/crc32"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/ivanjoz/genix-orm/utils"
)

const (
	// Folder is the store prefix of every frame: dtf/<table id>/<frame>/<generation>/..., the table id
	// and the generation in utils.EncodeUintBase64.
	Folder = "dtf/"
	// FloatScale turns a float into the int64 frames keep it as: 6 decimals, so adding and
	// subtracting the same value is exact.
	FloatScale = 1e6
	// MaxMagnitude bounds every tracked value: the widest changelog field holds 60 magnitude bits
	// when its entry has a sign.
	MaxMagnitude = 1 << 60
	// maxTrackedColumns: the changelog counts an entry's fields in 6 bits.
	maxTrackedColumns = 63
	// maxColumnID: the changelog names a column by one byte.
	maxColumnID = 255
	// MaxRowColumns bounds a frame's Rows: a row is named by up to 3 values (rowKey).
	MaxRowColumns = 3
)

var frameNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Column is one record field a frame reads, as the backend describes it: its name, for messages,
// its stable column id (genix-orm's cb tag: renaming the field keeps the files and the changelogs),
// and whether it is a float.
type Column struct {
	FieldName string
	ColumnID  int
	IsFloat   bool
}

// Spec declares one frame: one file per value of Keys (1 to 3 integer columns), one row per value of
// Rows (1 to 3 integer columns, the widest group first: department, program, employee) with the Sums
// of the records that hold it, and their count with Count. The file nests the rows like folders: each
// value of Rows[0] once, with the values of Rows[1] under it, and so on (codec.go).
type Spec struct {
	Name  string
	Keys  []Column
	Rows  []Column
	Sums  []Column
	Count bool
}

// FrameColumn is a column of a compiled frame: TrackedIndex is its position in Table.Tracked, the
// order of every tracked-values slice.
type FrameColumn struct {
	Column
	TrackedIndex int
}

// Frame is one compiled frame.
type Frame struct {
	Name          string
	Keys          []FrameColumn
	Rows          []FrameColumn
	Sums          []FrameColumn
	CountsRecords bool
	// SumsCount is the number of summed columns in its files: the Sums, then the record count.
	SumsCount int
	// Shape names the layout of its files (column ids, Count, format): another one is a rebuild.
	Shape  string
	Folder string // dtf/<table id>/<name>/
}

// Table is the compiled frames of one table and the columns their changelog tracks: every frame's
// Keys, Rows and Sums, plus the Status column, in column id order. A backend reads a record into a
// tracked-values slice in that order (floats × FloatScale).
type Table struct {
	Name    string
	Frames  []Frame
	Tracked []Column
	// StatusIndex is the Status column in Tracked, -1 without one: a record with Status 0 is in no file.
	StatusIndex int
}

// Compile checks the specs and compiles them. tableID names the table's folder (utils.EncodeUintBase64); status
// is the record's soft-delete column (nil without one).
func Compile(tableName string, tableID uint64, specs []Spec, status *Column) (*Table, error) {
	table := &Table{Name: tableName}
	tableFolder := Folder + utils.EncodeUintBase64(tableID) + "/"
	var trackedColumns []Column
	for _, spec := range specs {
		if !frameNamePattern.MatchString(spec.Name) || slices.ContainsFunc(table.Frames, func(frame Frame) bool { return frame.Name == spec.Name }) {
			return nil, fmt.Errorf("dataframe: %s frame name %q must be unique and hold only a-z, 0-9 and '-'", tableName, spec.Name)
		}
		if len(spec.Keys) == 0 || len(spec.Keys) > 3 || len(spec.Rows) == 0 || len(spec.Rows) > MaxRowColumns || (len(spec.Sums) == 0 && !spec.Count) {
			return nil, fmt.Errorf("dataframe: %s frame %q needs 1 to 3 Keys, 1 to %d Rows, and Sums or Count", tableName, spec.Name, MaxRowColumns)
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
		trackedColumns = append(trackedColumns, frameColumns...)
		frame := Frame{Name: spec.Name, CountsRecords: spec.Count, SumsCount: len(spec.Sums), Folder: tableFolder + spec.Name + "/"}
		if spec.Count {
			frame.SumsCount++
		}
		for _, column := range spec.Keys {
			frame.Keys = append(frame.Keys, FrameColumn{Column: column})
		}
		for _, column := range spec.Rows {
			frame.Rows = append(frame.Rows, FrameColumn{Column: column})
		}
		for _, column := range spec.Sums {
			frame.Sums = append(frame.Sums, FrameColumn{Column: column})
		}
		frame.Shape = shapeOf(frame)
		table.Frames = append(table.Frames, frame)
	}
	// A change of Status moves a record in or out of every file, so it is tracked too.
	if status != nil {
		trackedColumns = append(trackedColumns, *status)
	}

	for _, column := range trackedColumns {
		if column.ColumnID < 1 || column.ColumnID > maxColumnID {
			return nil, fmt.Errorf("dataframe: %s column %q needs a column id in 1..%d: the changelog names columns by one byte", tableName, column.FieldName, maxColumnID)
		}
		if !slices.ContainsFunc(table.Tracked, func(tracked Column) bool { return tracked.FieldName == column.FieldName }) {
			table.Tracked = append(table.Tracked, column)
		}
	}
	if len(table.Tracked) > maxTrackedColumns {
		return nil, fmt.Errorf("dataframe: %s frames track %d columns: the changelog takes at most %d", tableName, len(table.Tracked), maxTrackedColumns)
	}
	slices.SortFunc(table.Tracked, func(a, b Column) int { return a.ColumnID - b.ColumnID })
	table.StatusIndex = -1
	if status != nil {
		table.StatusIndex = table.trackedIndexOf(status.FieldName)
	}
	for i := range table.Frames {
		frame := &table.Frames[i]
		for j := range frame.Keys {
			frame.Keys[j].TrackedIndex = table.trackedIndexOf(frame.Keys[j].FieldName)
		}
		for j := range frame.Rows {
			frame.Rows[j].TrackedIndex = table.trackedIndexOf(frame.Rows[j].FieldName)
		}
		for j := range frame.Sums {
			frame.Sums[j].TrackedIndex = table.trackedIndexOf(frame.Sums[j].FieldName)
		}
	}
	return table, nil
}

func (table *Table) trackedIndexOf(fieldName string) int {
	return slices.IndexFunc(table.Tracked, func(column Column) bool { return column.FieldName == fieldName })
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

// shapeOf hashes what the files are laid out by: the column ids of the Keys, the Rows and the Sums,
// the record count, and the layout version (v4: nested Rows columns), so a frame built otherwise is rebuilt.
func shapeOf(frame Frame) string {
	columnIDs := func(columns []FrameColumn) string {
		ids := make([]string, len(columns))
		for i, column := range columns {
			ids[i] = strconv.Itoa(column.ColumnID)
		}
		return strings.Join(ids, ".")
	}
	layout := fmt.Sprintf("v4|%s|%s|%s|%t", columnIDs(frame.Keys), columnIDs(frame.Rows), columnIDs(frame.Sums), frame.CountsRecords)
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(layout)))
}

// ScaledFloat is a float tracked value: round(v × FloatScale), false when it is NaN or outside what a
// frame keeps (±MaxMagnitude).
func ScaledFloat(value float64) (int64, bool) {
	scaled := math.Round(value * FloatScale)
	if math.IsNaN(scaled) || math.Abs(scaled) >= MaxMagnitude {
		return 0, false
	}
	return int64(scaled), true
}

// CheckValues refuses, before the record is written, a value its changelog can't keep and a negative
// frame Keys or Rows value: they name files and ascend in them.
func (table *Table) CheckValues(tracked []int64) error {
	for i, value := range tracked {
		if value >= MaxMagnitude || value <= -MaxMagnitude {
			return fmt.Errorf("dataframe: %s column %s holds %d, outside what a frame keeps (±2^60)", table.Name, table.Tracked[i].FieldName, value)
		}
	}
	for i := range table.Frames {
		frame := &table.Frames[i]
		values := table.valuesOf(frame, tracked)
		if values != nil && (slices.ContainsFunc(values.row[:], func(rowValue int64) bool { return rowValue < 0 }) || slices.ContainsFunc(values.keys, func(key int64) bool { return key < 0 })) {
			return fmt.Errorf("dataframe: %s frame %q takes no negative Keys or Rows value", table.Name, frame.Name)
		}
	}
	return nil
}

// rowKey names a row of a file: its Rows values in order, the positions past len(Frame.Rows) 0. A
// fixed array, so it keys a map without an allocation.
type rowKey [MaxRowColumns]int64

func compareRowKeys(a, b rowKey) int { return slices.Compare(a[:], b[:]) }

// frameValues is what one record puts in a frame: the file (keys), the row and the sums.
type frameValues struct {
	keys []int64
	row  rowKey
	sums []int64 // SumsCount values: the Sums, then 1 when the frame counts records
}

// valuesOf reads a frame's values out of a record's tracked values: nil for no record or a deleted
// one (Status 0).
func (table *Table) valuesOf(frame *Frame, tracked []int64) *frameValues {
	if tracked == nil || (table.StatusIndex >= 0 && tracked[table.StatusIndex] == 0) {
		return nil
	}
	values := &frameValues{keys: make([]int64, len(frame.Keys)), sums: make([]int64, frame.SumsCount)}
	for i, key := range frame.Keys {
		values.keys[i] = tracked[key.TrackedIndex]
	}
	for i, rowColumn := range frame.Rows {
		values.row[i] = tracked[rowColumn.TrackedIndex]
	}
	for i, sum := range frame.Sums {
		values.sums[i] = tracked[sum.TrackedIndex]
	}
	if frame.CountsRecords {
		values.sums[frame.SumsCount-1] = 1
	}
	return values
}

// fileKeyOf is the store key of the file of keys in a generation folder:
// <folder><generation>/<k1> for one key, <folder><generation>/<k1>/<k2>_<k3> for two or three.
func (frame *Frame) fileKeyOf(generation string, keys []int64) string {
	fileKey := frame.Folder + generation + "/" + strconv.FormatInt(keys[0], 10)
	if len(keys) > 1 {
		laterKeys := make([]string, len(keys)-1)
		for i, key := range keys[1:] {
			laterKeys[i] = strconv.FormatInt(key, 10)
		}
		fileKey += "/" + strings.Join(laterKeys, "_")
	}
	return fileKey
}

// keysOfFile parses a file key of a generation folder back into its keys (false: not a frame file).
func (frame *Frame) keysOfFile(generation, fileKey string) ([]int64, bool) {
	relativeKey, isInFolder := strings.CutPrefix(fileKey, frame.Folder+generation+"/")
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
