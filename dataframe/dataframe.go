// Package dataframe is genix-orm's DataFrames, for every database driver (../DATA_FRAMES.md): group-by
// aggregates of a table, kept as compact files in a Store (an S3 bucket) and brought up to date by
// runs. It holds everything that does not depend on the database: the compile rules (compile.go),
// the write path's log entries and deadline (write.go), the runs, rebuilds and reads with their state
// machine (materialize.go), the files and their formats, the lock. A driver (dynamo) resolves its
// schema's declarations into Columns, feeds the write path, and implements Table (table.go): the
// frame's state record, the write clock, and the record reads.
//
//	<table>/<frame>/_log.<shape>          the old values of the writes that changed the frame
//	<table>/<frame>/_lock                 the compaction holding the frame, and until when (lock.go)
//	<table>/<frame>/<k0>                  a 1-key frame's file
//	<table>/<frame>/<k0>/<k1>[_<k2>]      a 2–3-key frame's file
//	<table>/<frame>/<k0>/_idx             the folder's keys and file hashes
//	<table>/<frame>/<k0>/_ixt             keys and hashes express compactions appended since (index.go)
//
// <table> and <frame> are short names the driver gives (Frame.Folder); the Keys are decimal.
//
// Every file holds its frame at a snapshot: the sum of what each record held at that
// Updated. A compaction from snapshot W to a target takes out of the files what each record
// held at W and adds what it held at the target (run.go); the formats are in codec.go.
package dataframe

import (
	"fmt"
	"hash/crc32"
	"slices"
	"strconv"
	"unsafe"
)

const (
	// MaxKeys is how many Keys a frame takes: the first names a file or a folder, the others a file in it.
	MaxKeys = 3
	// MaxFirstKeys caps a range of Keys[0] a rebuild or a read covers: one request per value.
	MaxFirstKeys = 400

	indexFileName          = "_idx"
	indexExtensionFileName = "_ixt"
	logFilePrefix          = "_log."
	lockFileName           = "_lock"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Frame is a compiled DataFrame (Compile): where its files are, how they are coded, and the columns
// its values are read from.
type Frame struct {
	Name string
	// RecordName is the record type's name, for messages.
	RecordName string
	// Folder is "<table>/<frame>/", the prefix of every object of the frame.
	Folder    string
	KeyCount  int
	SumsCount int
	// Shape hashes the format version, the folder and the column ids of Keys, Rows and Sums (ShapeOf).
	// It names the log, and a change of it makes the next run rebuild every file.
	Shape uint32

	Keys []Column
	Rows Column
	// Sums are the declared summed columns; with CountsRecords the files hold one more, the record count.
	Sums               []Column
	AllowsNegativeSums bool
	CountsRecords      bool
}

// Column is one integer column of a frame, as the driver resolved it from its schema.
type Column struct {
	// FieldName is the record's Go field: messages, and the driver's own lookups (FrameRow.Sum).
	FieldName string
	// ID is the column's stable id (dynamo: its cb tag), hashed into the shape: renaming the Go field
	// keeps the files.
	ID  string
	Get func(record unsafe.Pointer) int64
	Set func(record unsafe.Pointer, value int64)
}

// ValuesOf reads what the record contributes to the frame: nil for none. The driver passes nil for a
// record that counts nowhere (soft-deleted, Status 0). A counting frame gets a 1 after the Sums.
func (frame *Frame) ValuesOf(record unsafe.Pointer) *Values {
	if record == nil {
		return nil
	}
	values := &Values{Row: frame.Rows.Get(record), Sums: make([]int64, frame.SumsCount)}
	for i, key := range frame.Keys {
		values.Keys[i] = key.Get(record)
	}
	for i, sum := range frame.Sums {
		values.Sums[i] = sum.Get(record)
	}
	if frame.CountsRecords {
		values.Sums[len(frame.Sums)] = 1
	}
	return values
}

// ShapeOf hashes where a frame's files are and what they hold: the format version, the folder and the
// columns, given as their Column.IDs joined. A new codec version or a new folder changes the shape, so the
// next runs rebuild every file, in the new format or the new folder.
func ShapeOf(folder, keysIDs, rowsID, sumsIDs string) uint32 {
	return crc32.Checksum(fmt.Appendf(nil, "v%d|%s|%s|%s|%s", formatVersion, folder, keysIDs, rowsID, sumsIDs), castagnoliTable)
}

// LogKey is the frame's log. The shape is in its name: Lambdas still running the code of another
// shape append to a log no run of this one reads.
func (frame *Frame) LogKey() string { return ShapeLogKey(frame.Folder, frame.Shape) }

// ShapeLogKey is the log of a frame folder at a shape: a run deletes the log of the shape it replaces.
func ShapeLogKey(folder string, shape uint32) string {
	return folder + logFilePrefix + fmt.Sprintf("%08x", shape)
}

// FileKey is the object of the file holding keys.
func (frame *Frame) FileKey(keys [MaxKeys]int64) string {
	switch frame.KeyCount {
	case 1:
		return frame.Folder + strconv.FormatInt(keys[0], 10)
	case 2:
		return frame.dayFolder(keys[0]) + strconv.FormatInt(keys[1], 10)
	default:
		return frame.dayFolder(keys[0]) + strconv.FormatInt(keys[1], 10) + "_" + strconv.FormatInt(keys[2], 10)
	}
}

// dayFolder is the folder of the files sharing Keys[0], on a 2–3-key frame.
func (frame *Frame) dayFolder(firstKey int64) string {
	return frame.Folder + strconv.FormatInt(firstKey, 10) + "/"
}

func (frame *Frame) indexKey(firstKey int64) string {
	return frame.dayFolder(firstKey) + indexFileName
}

func (frame *Frame) indexExtensionKey(firstKey int64) string {
	return frame.dayFolder(firstKey) + indexExtensionFileName
}

func (frame *Frame) lockKey() string { return frame.Folder + lockFileName }

// Values is what one record version contributes to a frame: the file it lands in (Keys), its row
// and the values summed into that row. A record that counts in no file has nil Values.
type Values struct {
	Keys [MaxKeys]int64 // unused trailing keys are 0
	Row  int64
	Sums []int64
}

func EqualValues(a, b *Values) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Keys == b.Keys && a.Row == b.Row && slices.Equal(a.Sums, b.Sums)
}
