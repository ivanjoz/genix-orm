package dynamo

import (
	"context"
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrames (../DATA_FRAMES.md): group-by aggregates of a table, kept as files in
// a dataframe.Store (an S3 bucket) and brought up to date by MaterializeDataFrames.
// Everything that does not depend on DynamoDB is the dataframe module's; this side
// resolves the schema's declarations, feeds the write path, and implements
// dataframe.Table (data_frame_run.go).
//
// Inserts reach the frames from DynamoDB: a run reads the records written since its
// snapshot through the whole-entity delta index. Updates and deletes also need the
// values the record held before, which only the write sees, so the write appends
// them to the frame's log, before the base write, when the frame's values change.
// CreatedVersion (the Updated of the inserting write) tells a run whether its
// snapshot already holds a record. A frame's versions are Updated values.
// ─────────────────────────────────────────────────────────────────────────────

const (
	createdVersionFieldName = "CreatedVersion"
	frameStateSKPrefix      = "f"
)

// frameWriteWindowFrom opens the window of a write to the table (dataframe.WriteWindow): at the stored
// read the write diffs against, or when it starts if it reads nothing. A table without frames has no
// bound.
func (m *tableMeta) frameWriteWindowFrom(start time.Time) dataframe.WriteWindow {
	if len(m.dataFrames) == 0 {
		return dataframe.WriteWindow{}
	}
	return dataframe.OpenWriteWindow(start)
}

// compileDataFrames resolves the schema's DataFrames and compiles them (dataframe.Compile). It runs
// after the indexes are compiled: a frame needs the whole-entity delta index the run reads inserts
// through.
func compileDataFrames(schema Schema, recordType reflect.Type, accessors map[string]*colAccessor, meta *tableMeta) []dataframe.Frame {
	if len(schema.DataFrames) == 0 {
		return nil
	}
	recordName := recordType.Name()
	if len(schema.Partition) > 0 {
		panic(fmt.Sprintf("db: %s declares DataFrames and a Partition: frames need an entity without one", recordName))
	}
	hasWholeEntityDelta := slices.ContainsFunc(meta.arrayIndexes, func(index arrayIndexMeta) bool {
		return index.isDelta && index.elementPosition < 0 && len(index.keys) == 1
	})
	if !hasWholeEntityDelta {
		panic(fmt.Sprintf("db: %s declares DataFrames and needs a TypeDelta index without pinned Keys, such as {Type: TypeDelta, Keys: Cols(table.Status)}: the run reads the changed records through it", recordName))
	}

	declarations := make([]dataframe.Declaration, len(schema.DataFrames))
	for i, declared := range schema.DataFrames {
		// resolveColumn makes a Col a frame column; nil is a column the declaration left out, which
		// dataframe.Compile rejects. The id is the cb tag (cbColumnID panics without one), so renaming a
		// Go field keeps the files.
		resolveColumn := func(column Coln) dataframe.Column {
			if column == nil {
				return dataframe.Column{}
			}
			fieldName := column.col().fieldName
			accessor := accessors[fieldName]
			if _, isSlice := column.(SliceColn); isSlice || accessor == nil || !accessor.kind.isInteger() {
				panic(fmt.Sprintf("db: %s DataFrame %q column %q must be an integer Col", recordName, declared.Name, fieldName))
			}
			return dataframe.Column{FieldName: fieldName, ID: cbColumnID(recordType, fieldName), Get: accessor.getI64, Set: accessor.setI64}
		}
		declarations[i] = dataframe.Declaration{
			Name: declared.Name, Rows: resolveColumn(declared.Rows), AllowNegativeSums: declared.AllowNegativeSums, Count: declared.Count,
		}
		for _, column := range declared.Keys {
			declarations[i].Keys = append(declarations[i].Keys, resolveColumn(column))
		}
		for _, column := range declared.Sums {
			declarations[i].Sums = append(declarations[i].Sums, resolveColumn(column))
		}
	}
	tableID := resolveTableID(schema)
	frames := dataframe.Compile(recordName, declarations, func(frameName string) string { return frameFolder(tableID, frameName) })

	// Keys[0] is the rebuild's range: something must sort the records by it.
	leadingKeyFields := []string{schema.Keys[0].col().fieldName}
	for _, index := range schema.Indexes {
		if len(index.Keys) > 0 && (index.Slot.index != "" || index.Type == TypeLocal) {
			leadingKeyFields = append(leadingKeyFields, index.Keys[0].col().fieldName)
		}
	}
	for _, frame := range frames {
		if !slices.Contains(leadingKeyFields, frame.Keys[0].FieldName) {
			panic(fmt.Sprintf("db: %s DataFrame %q Keys[0] %q must lead the entity's Keys, a GSI or a local index: a rebuild reads a range of it",
				recordName, frame.Name, frame.Keys[0].FieldName))
		}
	}
	return frames
}

// frameKeyLeadsBaseKeys reports whether the frame's Keys[0] leads the entity's Keys: a rebuild then
// reads its range from the base table, consistently; otherwise it goes through a GSI, which cannot.
func (m *tableMeta) frameKeyLeadsBaseKeys(frame *dataframe.Frame) bool {
	return m.keys[0].fieldName == frame.Keys[0].FieldName
}

// frameFolder names a frame's folder in the store by its entity's TableID and a 32-bit hash of its name,
// in the order-preserving base64 of the keys (5 and 6 characters): every object key stays short, and an
// entity renamed with its TableID pinned keeps its files as it keeps its items.
func frameFolder(tableID int32, frameName string) string {
	nameHasher := fnv.New32a()
	nameHasher.Write([]byte(frameName))
	return FrameTableFolder(tableID) + EncodeOrderedUint(uint64(nameHasher.Sum32()), 6) + "/"
}

// FrameTableFolder is the folder holding every frame of one entity, relative to the store's root:
// whoever guards the store by entity (a proxy authorizing writes) matches object keys against it.
func FrameTableFolder(tableID int32) string { return EncodeOrderedInt(int64(tableID), 5) + "/" }

// resolveCreatedVersion validates the managed CreatedVersion field a table with frames needs.
func resolveCreatedVersion(recordType reflect.Type, accessors map[string]*colAccessor) *colAccessor {
	field, ok := recordType.FieldByName(createdVersionFieldName)
	if !ok || field.Type.Kind() != reflect.Int64 {
		panic(fmt.Sprintf("db: %s declares DataFrames and needs an int64 field %q (json \"crv\")", recordType.Name(), createdVersionFieldName))
	}
	return accessors[createdVersionFieldName]
}

// countedRecord is the record as the frames take it: nil for no record, or for a soft-deleted one
// (Status 0), which counts nowhere.
func (m *tableMeta) countedRecord(ptr unsafe.Pointer) unsafe.Pointer {
	if ptr == nil || (m.status != nil && m.status.getI64(ptr) == 0) {
		return nil
	}
	return ptr
}

// frameValuesOf reads what the record at ptr contributes to the frame.
func (m *tableMeta) frameValuesOf(frame *dataframe.Frame, ptr unsafe.Pointer) *dataframe.Values {
	return frame.ValuesOf(m.countedRecord(ptr))
}

// checkFrameValues fails a write holding a value its frames can't store (dataframe.CheckValues).
func (m *tableMeta) checkFrameValues(ptrs []unsafe.Pointer) error {
	return dataframe.CheckValues(m.dataFrames, ptrs)
}

// stampCreatedVersions sets the managed CreatedVersion of the records about to be written: the
// stored version's, or this write's Updated for a record with none stored (an insert).
func (m *tableMeta) stampCreatedVersions(ptrs []unsafe.Pointer, storedByKey map[string]unsafe.Pointer) {
	if m.createdVersion == nil {
		return
	}
	for _, ptr := range ptrs {
		createdVersion := m.updated.acc.getI64(ptr)
		if storedPtr := storedByKey[m.recordKey(ptr)]; storedPtr != nil {
			createdVersion = m.createdVersion.getI64(storedPtr)
		}
		m.createdVersion.setI64(ptr, createdVersion)
	}
}

// loggedWrite is the write of writtenPtr (nil for a delete) over storedPtr, with the version newVersion.
func (m *tableMeta) loggedWrite(storedPtr, writtenPtr unsafe.Pointer, newVersion int64) dataframe.LoggedWrite {
	return dataframe.LoggedWrite{
		SK: m.skValue(storedPtr), CreatedVersion: m.createdVersion.getI64(storedPtr), NewVersion: newVersion,
		Stored: m.countedRecord(storedPtr), Written: m.countedRecord(writtenPtr),
	}
}

// frameWritesOf pairs the records about to be written with their stored versions. A record with none
// stored is an insert, which the run reads from DynamoDB: it logs nothing, so it is left out.
func (m *tableMeta) frameWritesOf(ptrs []unsafe.Pointer, storedByKey map[string]unsafe.Pointer) []dataframe.LoggedWrite {
	if len(m.dataFrames) == 0 {
		return nil
	}
	var writes []dataframe.LoggedWrite
	for _, ptr := range ptrs {
		if storedPtr := storedByKey[m.recordKey(ptr)]; storedPtr != nil {
			writes = append(writes, m.loggedWrite(storedPtr, ptr, m.updated.acc.getI64(ptr)))
		}
	}
	return writes
}

// appendFrameDeleteEntries logs the values a record about to be deleted held in the frames it counts
// in, and returns the write it logged. Its version is the Updated Delete stamped on keyPtr, above the
// stored one.
func (m *tableMeta) appendFrameDeleteEntries(ctx context.Context, storedPtr, keyPtr unsafe.Pointer) ([]dataframe.LoggedWrite, error) {
	if len(m.dataFrames) == 0 || m.countedRecord(storedPtr) == nil {
		return nil, nil
	}
	writes := []dataframe.LoggedWrite{m.loggedWrite(storedPtr, nil, m.updated.acc.getI64(keyPtr))}
	return writes, dataframe.AppendLogEntries(ctx, m.dataFrames, writes, false)
}
