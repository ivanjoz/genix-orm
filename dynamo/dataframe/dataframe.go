// Package dataframe is the storage side of genix-orm's DataFrames (../DATA_FRAMES_PLAN.md): group-by
// aggregates of a table, kept as compact files in a Store (an S3 bucket) and brought up to date by
// runs. It knows nothing of DynamoDB: the dynamo package compiles a table's DataFrames into Frames,
// reads the records, holds each frame's state and lease, and calls this package for the files.
//
//	<entity>/<frame>/_log.<shape>          the old values of the writes that changed the frame
//	<entity>/<frame>/<k0>                  a 1-key frame's file
//	<entity>/<frame>/<k0>/<k1>[_<k2>]      a 2–3-key frame's file
//	<entity>/<frame>/<k0>/_idx           the folder's keys and file hashes
//
// Every file holds its frame at a snapshot: the sum of what each record held at that
// UpdatedVersion. A run from snapshot W to a target takes out of the files what each record held at
// W and adds what it held at the target (run.go); the formats are in codec.go.
package dataframe

import (
	"fmt"
	"hash/crc32"
	"slices"
	"strconv"
)

const (
	// MaxKeys is how many Keys a frame takes: the first names a file or a folder, the others a file in it.
	MaxKeys = 3
	// MaxFirstKeys caps a range of Keys[0] a rebuild or a read covers: one request per value.
	MaxFirstKeys = 400

	indexFileName = "_idx"
	logFilePrefix = "_log."
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Frame is what the file work needs of a compiled DataFrame.
type Frame struct {
	Name string
	// Folder is "<entity>/<name>/", the prefix of every object of the frame.
	Folder    string
	KeyCount  int
	SumsCount int
	// Shape hashes the format version and the cb ids of Keys, Rows and Sums (ShapeOf). It names the
	// log, and a change of it makes the next run rebuild every file.
	Shape uint32
}

// ShapeOf hashes a frame's columns, given as their cb ids joined, with the format version: a new
// codec version changes every shape, so the next runs rebuild every file.
func ShapeOf(keysIDs, rowsID, sumsIDs string) uint32 {
	return crc32.Checksum(fmt.Appendf(nil, "v%d|%s|%s|%s", formatVersion, keysIDs, rowsID, sumsIDs), castagnoliTable)
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
