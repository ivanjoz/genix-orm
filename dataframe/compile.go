package dataframe

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var frameNamePattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// Declaration is a DataFrame of a schema with its columns resolved by the driver: each one an integer
// scalar column with a stable id. A Column with no FieldName is one the schema left out.
type Declaration struct {
	Name              string
	Keys              []Column
	Rows              Column
	Sums              []Column
	AllowNegativeSums bool
	Count             bool
}

// Compile checks a record type's frame declarations and compiles them; it panics on a violation, at
// boot. folderOf names a frame's folder in the store from its name; the rules that depend on the
// driver's schema (which column may lead a rebuild's range, the managed columns) are the driver's.
func Compile(recordName string, declarations []Declaration, folderOf func(frameName string) string) []Frame {
	frames := make([]Frame, 0, len(declarations))
	for _, declared := range declarations {
		if !frameNamePattern.MatchString(declared.Name) {
			panic(fmt.Sprintf("db: %s DataFrame name %q must be lowercase letters, digits and '-'", recordName, declared.Name))
		}
		if slices.ContainsFunc(frames, func(frame Frame) bool { return frame.Name == declared.Name }) {
			panic(fmt.Sprintf("db: %s declares DataFrame %q twice", recordName, declared.Name))
		}
		if len(declared.Keys) < 1 || len(declared.Keys) > MaxKeys || declared.Rows.FieldName == "" || (len(declared.Sums) == 0 && !declared.Count) {
			panic(fmt.Sprintf("db: %s DataFrame %q needs 1 to %d Keys, a Rows column and at least one Sums column or Count", recordName, declared.Name, MaxKeys))
		}
		isUsed := map[string]bool{}
		for _, column := range slices.Concat(declared.Keys, []Column{declared.Rows}, declared.Sums) {
			if isUsed[column.FieldName] {
				panic(fmt.Sprintf("db: %s DataFrame %q lists %q twice", recordName, declared.Name, column.FieldName))
			}
			isUsed[column.FieldName] = true
		}

		folder := folderOf(declared.Name)
		if collision := slices.IndexFunc(frames, func(other Frame) bool { return other.Folder == folder }); collision >= 0 {
			panic(fmt.Sprintf("db: %s DataFrames %q and %q hash to the same folder: rename one", recordName, frames[collision].Name, declared.Name))
		}
		sumsCount, sumsIDs := len(declared.Sums), columnIDs(declared.Sums)
		if declared.Count {
			sumsCount, sumsIDs = sumsCount+1, sumsIDs+"+count"
		}
		frames = append(frames, Frame{
			Name:               declared.Name,
			RecordName:         recordName,
			Folder:             folder,
			KeyCount:           len(declared.Keys),
			SumsCount:          sumsCount,
			Shape:              ShapeOf(folder, columnIDs(declared.Keys), declared.Rows.ID, sumsIDs),
			Keys:               declared.Keys,
			Rows:               declared.Rows,
			Sums:               declared.Sums,
			AllowsNegativeSums: declared.AllowNegativeSums,
			CountsRecords:      declared.Count,
		})
	}
	return frames
}

// columnIDs joins the ids the shape hashes.
func columnIDs(columns []Column) string {
	ids := make([]string, len(columns))
	for i, column := range columns {
		ids[i] = column.ID
	}
	return strings.Join(ids, ".")
}
