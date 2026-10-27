package dynamo

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrames: group-by aggregates of a table kept as columnar files in the
// utils.FileStore. The logic lives in the dataframe module (../dataframe, DATAFRAMES.md);
// this file and data_frame_backend.go are what dynamo gives it:
//
//   - the frames, compiled from Schema.DataFrames (compileDataFrames), and the lines a
//     record puts in them: from the frame's Cols, or from its ToFrames method;
//   - every item's crt (created, client.go) and cgl (changelog): the item attributes
//     beside d and ver that say which lines a record put in its frames at any moment of
//     the last ~36 h. putIfVersion writes the next cgl from the stored record
//     (putChangelog);
//   - the frames' state item and lock, and the records as Histories, read through
//     the GSI on Updated or a scan (data_frame_backend.go).
// ─────────────────────────────────────────────────────────────────────────────

const (
	createdColumn   = "crt"
	changelogColumn = "cgl"
	// framePartition is the one partition of every frame: an entity with DataFrames has no Partition.
	framePartition = 0
)

// compileDataFrames resolves the schema's DataFrames into meta.frames. Every column carries a cb tag:
// the files and the changelog name columns by it, so renaming a Go field keeps them. It runs once the
// GSIs are compiled: a frame needs the one on Updated.
func compileDataFrames(schema Schema, recordType reflect.Type, accessors map[string]*colAccessor, meta *tableMeta) {
	if len(schema.DataFrames) == 0 {
		return
	}
	recordName := recordType.Name()
	if len(schema.Partition) > 0 {
		panic(fmt.Sprintf("db: %s declares DataFrames and a Partition: frames need an entity without one", recordName))
	}
	hasUpdatedGSI := slices.ContainsFunc(meta.indexes, func(index indexMeta) bool {
		return len(index.partition) == 0 && index.sortColumns[0].fieldName == updatedFieldName
	})
	if meta.updated == nil || !hasUpdatedGSI {
		panic(fmt.Sprintf("db: %s declares DataFrames and needs Updated and a GSI whose Keys start with it, such as {Slot: G2, Keys: Cols(table.Updated.Size(32))}: the run reads the records written since its snapshot through it", recordName))
	}
	frameColumnOf := func(column Coln, frameName string) dataframe.Column {
		fieldName := column.col().fieldName
		accessor := accessors[fieldName]
		if _, isSlice := column.(SliceColn); isSlice || accessor == nil || !(accessor.kind.isInteger() || accessor.kind == kindFloat) {
			panic(fmt.Sprintf("db: %s DataFrame %q column %q must be an integer or float Col", recordName, frameName, fieldName))
		}
		columnID, _ := strconv.Atoi(cbColumnID(recordType, fieldName))
		return dataframe.Column{FieldName: fieldName, ColumnID: columnID, IsFloat: accessor.kind == kindFloat}
	}
	specs := make([]dataframe.Spec, len(schema.DataFrames))
	for i, declared := range schema.DataFrames {
		specs[i] = dataframe.Spec{Name: declared.Name, Count: declared.Count, Version: declared.Version,
			UseZeroSumsMask: declared.UseZeroSumsMask, Projection: declared.ToFrames}
		if declared.ToFrames != nil {
			continue
		}
		for _, column := range declared.Keys {
			specs[i].Keys = append(specs[i].Keys, frameColumnOf(column, declared.Name))
		}
		for _, column := range declared.Rows {
			specs[i].Rows = append(specs[i].Rows, frameColumnOf(column, declared.Name))
		}
		for _, column := range declared.Sums {
			specs[i].Sums = append(specs[i].Sums, frameColumnOf(column, declared.Name))
		}
		specs[i].LinesOf = meta.columnLinesOf(slices.Concat(specs[i].Keys, specs[i].Rows, specs[i].Sums))
	}
	frames, err := dataframe.Compile(recordName, uint64(resolveTableID(schema)), specs)
	if err != nil {
		panic(err.Error())
	}
	meta.frames = frames
}

// columnLinesOf is the LinesOf of a frame declared by Cols: one line of their values (floats ×
// dataframe.FloatScale), none for a soft-deleted record (Status 0).
func (m *tableMeta) columnLinesOf(lineColumns []dataframe.Column) func(record unsafe.Pointer) ([]dataframe.Line, error) {
	return func(ptr unsafe.Pointer) ([]dataframe.Line, error) {
		if m.status != nil && m.status.getI64(ptr) == 0 {
			return nil, nil
		}
		line := make(dataframe.Line, len(lineColumns))
		for i, column := range lineColumns {
			accessor := m.accessors[column.FieldName]
			if !column.IsFloat {
				line[i] = accessor.getI64(ptr)
				continue
			}
			var isKept bool
			if line[i], isKept = dataframe.ScaledFloat(accessor.getF64(ptr)); !isKept {
				return nil, fmt.Errorf("field %s holds %v, outside what a frame keeps (±1.1e12, 6 decimals)", column.FieldName, accessor.getF64(ptr))
			}
		}
		return []dataframe.Line{line}, nil
	}
}

// checkFrameValues refuses, before the record is written, what its frames can't keep (marshalItem).
func (m *tableMeta) checkFrameValues(ptr unsafe.Pointer) error {
	if m.frames == nil {
		return nil
	}
	if _, err := m.frames.LinesOf(ptr); err != nil {
		return fmt.Errorf("db: %s: %w", m.recordType.Name(), err)
	}
	return nil
}

// putChangelog sets the cgl of item, the record at writtenPtr written over stored (no ptr: a new
// record, which has none).
func (m *tableMeta) putChangelog(item map[string]types.AttributeValue, stored storedRecord, writtenPtr unsafe.Pointer) error {
	if m.frames == nil || stored.ptr == nil {
		return nil
	}
	storedLines, err := m.frames.LinesOf(stored.ptr)
	if err != nil {
		return err
	}
	writtenLines, err := m.frames.LinesOf(writtenPtr)
	if err != nil {
		return err
	}
	changelog, err := m.frames.NextChangelog(stored.changelog, m.updated.acc.getI64(stored.ptr), m.updated.acc.getI64(writtenPtr), storedLines, writtenLines)
	if err != nil {
		return fmt.Errorf("db: %s %s: %w", m.recordType.Name(), m.recordKey(writtenPtr), err)
	}
	if len(changelog) > 0 {
		item[changelogColumn] = &types.AttributeValueMemberB{Value: changelog}
	}
	return nil
}

// changelogOf is the item's cgl, nil without one.
func changelogOf(item map[string]types.AttributeValue) []byte {
	if changelog, hasChangelog := item[changelogColumn].(*types.AttributeValueMemberB); hasChangelog {
		return changelog.Value
	}
	return nil
}

// historyOf reads the History of a stored item decoded into the record at ptr.
func (m *tableMeta) historyOf(item map[string]types.AttributeValue, ptr unsafe.Pointer) (dataframe.History, error) {
	lines, err := m.frames.LinesOf(ptr)
	if err != nil {
		return dataframe.History{}, err
	}
	return m.frames.NewHistory(framePartition, m.recordKey(ptr), lines, numberAttrValue(item, createdColumn), m.updated.acc.getI64(ptr), changelogOf(item))
}
