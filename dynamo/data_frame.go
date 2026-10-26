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
//   - the frames, compiled from Schema.DataFrames (compileDataFrames);
//   - every item's crt (created, client.go) and cgl (changelog): the item attributes
//     beside d and ver that say what a record held at any moment of the last ~36 h.
//     putIfVersion writes the next cgl from the stored record (putChangelog);
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
		specs[i] = dataframe.Spec{Name: declared.Name, Count: declared.Count}
		for _, column := range declared.Keys {
			specs[i].Keys = append(specs[i].Keys, frameColumnOf(column, declared.Name))
		}
		for _, column := range declared.Rows {
			specs[i].Rows = append(specs[i].Rows, frameColumnOf(column, declared.Name))
		}
		for _, column := range declared.Sums {
			specs[i].Sums = append(specs[i].Sums, frameColumnOf(column, declared.Name))
		}
	}
	var status *dataframe.Column
	if meta.status != nil {
		columnID, _ := strconv.Atoi(cbColumnID(recordType, statusFieldName))
		status = &dataframe.Column{FieldName: statusFieldName, ColumnID: columnID}
	}
	frames, err := dataframe.Compile(recordName, uint64(resolveTableID(schema)), specs, status)
	if err != nil {
		panic(err.Error())
	}
	meta.frames = frames
	for _, column := range frames.Tracked {
		meta.trackedAccessors = append(meta.trackedAccessors, accessors[column.FieldName])
	}
}

// trackedValuesOf reads the frames' tracked columns of the record at ptr, floats scaled by
// dataframe.FloatScale.
func (m *tableMeta) trackedValuesOf(ptr unsafe.Pointer) ([]int64, error) {
	values := make([]int64, len(m.frames.Tracked))
	for i, column := range m.frames.Tracked {
		if !column.IsFloat {
			values[i] = m.trackedAccessors[i].getI64(ptr)
			continue
		}
		var isKept bool
		if values[i], isKept = dataframe.ScaledFloat(m.trackedAccessors[i].getF64(ptr)); !isKept {
			return nil, fmt.Errorf("db: %s DataFrame field %s holds %v, outside what a frame keeps (±1.1e12, 6 decimals)",
				m.recordType.Name(), column.FieldName, m.trackedAccessors[i].getF64(ptr))
		}
	}
	return values, nil
}

// checkFrameValues refuses, before the record is written, what its frames can't keep (marshalItem).
func (m *tableMeta) checkFrameValues(ptr unsafe.Pointer) error {
	if m.frames == nil {
		return nil
	}
	trackedValues, err := m.trackedValuesOf(ptr)
	if err != nil {
		return err
	}
	return m.frames.CheckValues(trackedValues)
}

// putChangelog sets the cgl of item, the record at writtenPtr written over stored (no ptr: a new
// record, which has none).
func (m *tableMeta) putChangelog(item map[string]types.AttributeValue, stored storedRecord, writtenPtr unsafe.Pointer) error {
	if m.frames == nil || stored.ptr == nil {
		return nil
	}
	storedValues, err := m.trackedValuesOf(stored.ptr)
	if err != nil {
		return err
	}
	writtenValues, err := m.trackedValuesOf(writtenPtr)
	if err != nil {
		return err
	}
	changelog, err := m.frames.NextChangelog(stored.changelog, m.updated.acc.getI64(stored.ptr), m.updated.acc.getI64(writtenPtr), storedValues, writtenValues)
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
	trackedValues, err := m.trackedValuesOf(ptr)
	if err != nil {
		return dataframe.History{}, err
	}
	return dataframe.NewHistory(framePartition, m.recordKey(ptr), trackedValues, numberAttrValue(item, createdColumn), m.updated.acc.getI64(ptr), changelogOf(item))
}
