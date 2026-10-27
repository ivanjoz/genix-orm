package scylla

import (
	"fmt"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrames: group-by reports of a table kept as columnar files in the file store.
// The logic lives in the dataframe module (../dataframe, DATAFRAMES.md); scylla gives it:
//
//   - the frames, compiled from TableSchema.DataFrames (configureDataFrames), each
//     partition of the table with its own files, and the lines a record puts in them:
//     from the frame's columns, or from its ToFrames method;
//   - a change log table per table, <table>__dtf_log, that every write appends to
//     before its rows land (data_frame_log.go);
//   - the frames' state in a file of the store and their lock in AcquireFrameLock,
//     and the records as Histories: their lines now, from the whole stored row, undone
//     by their logged changes (data_frame_backend.go).
// ─────────────────────────────────────────────────────────────────────────────

// AcquireFrameLock takes the lock of a DataFrame partition, queuing up to wait behind its holder:
// release frees it, false when the holder kept it past wait. lockName names the frame partition
// across tables; lease is how long the compaction may hold it. The application installs it at boot
// (genix: fareward) before the first read of a frame. The lock spares duplicate work and lets a read
// wait for the compaction under way: the frames' state and files are written conditioned on their
// ETags.
var AcquireFrameLock func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)

// dataFrameInfo is a table's compiled DataFrames.
type dataFrameInfo struct {
	frames *dataframe.Table
	// recordType is the record struct: a stored row is decoded into one to take its lines.
	recordType reflect.Type
	// storedColumns are every persisted column. A ToFrames method may read any field, so the write
	// path and the Histories read whole rows.
	storedColumns []IColInfo
	logTableName  string
}

// configureDataFrames validates a table's DataFrames and compiles them. Schema mistakes panic when
// the table compiles.
func configureDataFrames(scyllaTable *ScyllaTable, schema TableSchema, schemaStruct reflect.Value) {
	if len(schema.DataFrames) == 0 {
		return
	}
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": DataFrames `, scyllaTable.Name) + fmt.Sprintf(format, args...))
	}
	if scyllaTable.PartKey == nil || scyllaTable.PartKey.IsNil() {
		fail("require a partition column: each partition has its own files")
	}
	if fieldType := scyllaTable.PartKey.GetType().FieldType; fieldType != "int32" && fieldType != "int" {
		fail(`partition column "%v" must be int32. Found: %v`, scyllaTable.PartKey.GetName(), fieldType)
	}
	if len(scyllaTable.Keys) != 1 || !slices.Contains([]string{"int16", "int32", "int64", "int"}, scyllaTable.Keys[0].GetType().FieldType) {
		fail("require exactly one integer key column, the record ID of the change log")
	}
	if scyllaTable.ID <= 0 {
		fail("require the table's ID: it names the frames' folder")
	}
	recordTyped, isTyped := schemaStruct.Addr().Interface().(interface{ BaseStructType() reflect.Type })
	if !isTyped || recordTyped.BaseStructType() == nil {
		fail("need the table's record type")
	}

	isIntegerType := func(fieldType string) bool {
		return slices.Contains([]string{"int8", "int16", "int32", "int64", "int"}, fieldType)
	}
	var statusColumn IColInfo
	if column := scyllaTable.ColumnsMap["status"]; column != nil && !column.IsNil() {
		if !isIntegerType(column.GetType().FieldType) {
			fail(`status column must be an integer. Found: %v`, column.GetType().FieldType)
		}
		statusColumn = column
	}
	specs := make([]dataframe.Spec, len(schema.DataFrames))
	for i, declared := range schema.DataFrames {
		specs[i] = dataframe.Spec{Name: declared.Name, Count: declared.Count, Version: declared.Version,
			UseZeroSumsMask: declared.UseZeroSumsMask, CompactOn: declared.CompactOn, Projection: declared.ToFrames}
		if declared.ToFrames != nil {
			continue
		}
		var lineColumns []IColInfo
		frameColumnOf := func(declaredColumn db.Coln) dataframe.Column {
			column := scyllaTable.ColumnsMap[declaredColumn.GetName()]
			if column == nil || column.GetInfo().IsVirtual {
				fail(`frame %q column "%v" must be a persisted column of the table`, declared.Name, declaredColumn.GetName())
			}
			fieldType := column.GetType().FieldType
			isFloat := fieldType == "float32" || fieldType == "float64"
			if !isFloat && !isIntegerType(fieldType) {
				fail(`frame %q column "%v" must be an integer or a float. Found: %v`, declared.Name, column.GetName(), fieldType)
			}
			lineColumns = append(lineColumns, column)
			// The column's position names it in the frames' shape: moving the field rebuilds them.
			return dataframe.Column{FieldName: column.GetName(), ColumnID: int(column.GetInfo().Idx), IsFloat: isFloat}
		}
		for _, column := range declared.Keys {
			specs[i].Keys = append(specs[i].Keys, frameColumnOf(column))
		}
		for _, column := range declared.Rows {
			specs[i].Rows = append(specs[i].Rows, frameColumnOf(column))
		}
		for _, column := range declared.Sums {
			specs[i].Sums = append(specs[i].Sums, frameColumnOf(column))
		}
		specs[i].LinesOf = columnLinesOf(lineColumns, statusColumn)
	}
	frames, err := dataframe.Compile(scyllaTable.Name, uint64(scyllaTable.ID), specs)
	if err != nil {
		fail("%v", err)
	}

	info := &dataFrameInfo{frames: frames, recordType: recordTyped.BaseStructType(), logTableName: scyllaTable.Name + "__dtf_log"}
	// ColumnsMap: Columns is filled after the schema's features compile.
	for _, column := range scyllaTable.ColumnsMap {
		if !column.GetInfo().IsVirtual && column.GetInfo().Field != nil {
			info.storedColumns = append(info.storedColumns, column)
		}
	}
	slices.SortFunc(info.storedColumns, func(a, b IColInfo) int { return int(a.GetInfo().Idx) - int(b.GetInfo().Idx) })
	scyllaTable.dataFrames = info
}

// columnLinesOf is the LinesOf of a frame declared by columns: one line of the columns' values
// (integers as they are, floats × dataframe.FloatScale, null as 0), none when status is 0.
func columnLinesOf(lineColumns []IColInfo, statusColumn IColInfo) func(record unsafe.Pointer) ([]dataframe.Line, error) {
	return func(record unsafe.Pointer) ([]dataframe.Line, error) {
		if statusColumn != nil && convertToInt64(statusColumn.GetRawValue(record)) == 0 {
			return nil, nil
		}
		line := make(dataframe.Line, len(lineColumns))
		for i, column := range lineColumns {
			var err error
			if line[i], err = frameValueOf(column, column.GetRawValue(record)); err != nil {
				return nil, err
			}
		}
		return []dataframe.Line{line}, nil
	}
}

// frameValueOf is a column's value as frames keep it: integers as they are, floats ×
// dataframe.FloatScale, null as 0.
func frameValueOf(column IColInfo, rawValue any) (int64, error) {
	if plainValue, isNamed := db.NormalizeNamedNumeric(rawValue); isNamed {
		rawValue = plainValue
	}
	floatValue, isFloat := 0.0, false
	switch value := rawValue.(type) {
	case nil:
		return 0, nil
	case float32:
		floatValue, isFloat = float64(value), true
	case float64:
		floatValue, isFloat = value, true
	}
	if !isFloat {
		return convertToInt64(rawValue), nil
	}
	scaled, isKept := dataframe.ScaledFloat(floatValue)
	if !isKept {
		return 0, fmt.Errorf(`DataFrame column "%v" holds %v, outside what a frame keeps (±1.1e12, 6 decimals)`, column.GetName(), floatValue)
	}
	return scaled, nil
}

// newRecord is a zero record of the table, its fields set from a stored row (nil: none).
func (info *dataFrameInfo) newRecord(storedRow map[string]any) unsafe.Pointer {
	record := reflect.New(info.recordType).UnsafePointer()
	for _, column := range info.storedColumns {
		if storedValue, isSet := storedRow[column.GetName()]; isSet {
			column.SetValue(record, storedValue)
		}
	}
	return record
}

// linesOfRow is what a stored row puts in every frame.
func (info *dataFrameInfo) linesOfRow(storedRow map[string]any) (dataframe.RecordLines, error) {
	return info.frames.LinesOf(info.newRecord(storedRow))
}

/* Controller */

// CompactDataFrames brings one partition's files of every DataFrame of the table up to date: the
// manual compaction (the reads compact a frame with CompactOn on their own).
func (e *ScyllaController[T, E]) CompactDataFrames(partValue int32) error {
	return withFrameBackend(&e.Table, func(backend *frameBackend) error {
		return dataframe.Compact(backend.Table, backend, cache.FileStore(), int64(partValue), backend.startedAt)
	})
}

// RebuildDataFramesPartition computes one partition's files of every frame again from its records.
func (e *ScyllaController[T, E]) RebuildDataFramesPartition(partValue int32) error {
	return withFrameBackend(&e.Table, func(backend *frameBackend) error {
		return dataframe.Rebuild(backend.Table, backend, cache.FileStore(), int64(partValue), "", backend.startedAt)
	})
}

// withFrameBackend runs one frames step with a new backend, or nothing for a table without frames.
func withFrameBackend(scyllaTable *ScyllaTable, run func(backend *frameBackend) error) error {
	if scyllaTable.dataFrames == nil {
		return nil
	}
	backend, err := newFrameBackend(scyllaTable)
	if err != nil {
		return err
	}
	return run(backend)
}

/* Reads */

func (Exec[TableT, RecordT]) QueryFrame(request db.FrameRequest) ([]db.FrameRow[RecordT], error) {
	scyllaTable := MakeScyllaTable[RecordT, TableT]()
	rows, frame, err := readFrameRows(&scyllaTable, request)
	if err != nil {
		return nil, err
	}
	frameRows := make([]db.FrameRow[RecordT], len(rows))
	for i, row := range rows {
		frameRows[i] = db.FrameRow[RecordT]{Sums: row.Sums, Frame: frame}
		setFrameRowKey(&scyllaTable, frame, unsafe.Pointer(&frameRows[i].Key), row)
	}
	return frameRows, nil
}

func (Exec[TableT, RecordT]) QueryFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	scyllaTable := MakeScyllaTable[RecordT, TableT]()
	return readFrameChanged(&scyllaTable, request, heldGeneration, heldFiles)
}

// frameOfRequest resolves the frame and the selection a request names.
func frameOfRequest(scyllaTable *ScyllaTable, request db.FrameRequest) (*dataframe.Frame, dataframe.Selection, error) {
	if scyllaTable.dataFrames == nil || scyllaTable.dataFrames.frames.FrameNamed(request.Frame) == nil {
		return nil, dataframe.Selection{}, fmt.Errorf("table %q has no DataFrame %q", scyllaTable.Name, request.Frame)
	}
	frame := scyllaTable.dataFrames.frames.FrameNamed(request.Frame)
	filters := request.Filters
	if frame.IsProjected {
		// A ToFrames frame's Keys are its frame struct's field names: the filter on a record column
		// selects the key of the same field name (as dynamo names them).
		filters = slices.Clone(filters)
		for i, filter := range filters {
			if column := scyllaTable.ColumnsMap[filter.Column]; column != nil && !column.IsNil() {
				filters[i].Column = column.GetInfo().FieldName
			}
		}
	}
	selection, err := db.FrameSelection(frame, filters)
	return frame, selection, err
}

func readFrameRows(scyllaTable *ScyllaTable, request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	frame, selection, err := frameOfRequest(scyllaTable, request)
	if err != nil {
		return nil, nil, err
	}
	backend, err := newFrameBackend(scyllaTable)
	if err != nil {
		return nil, nil, err
	}
	rows, err := dataframe.Read(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, request.IsFresh, backend.startedAt)
	return rows, frame, err
}

func readFrameChanged(scyllaTable *ScyllaTable, request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	frame, selection, err := frameOfRequest(scyllaTable, request)
	if err != nil {
		return nil, err
	}
	held, err := dataframe.ParseHeldFiles(heldFiles)
	if err != nil {
		return nil, err
	}
	backend, err := newFrameBackend(scyllaTable)
	if err != nil {
		return nil, err
	}
	answer, err := dataframe.ReadChanged(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, heldGeneration, held, backend.startedAt)
	if err != nil {
		return nil, err
	}
	return dataframe.AppendChangedFiles(nil, answer), nil
}

// setFrameRowKey sets the frame's Keys and Rows columns of the key record at keyPointer. A frame
// declared by ToFrames names a frame struct's fields, not the record's: its rows leave the key empty.
func setFrameRowKey(scyllaTable *ScyllaTable, frame *dataframe.Frame, keyPointer unsafe.Pointer, row dataframe.Row) {
	if frame.IsProjected {
		return
	}
	for k, key := range frame.Keys {
		column := scyllaTable.ColumnsMap[key.FieldName]
		column.SetValue(keyPointer, coerceManagedIntegerValue(column, row.Keys[k]))
	}
	for c, rowColumn := range frame.Rows {
		column := scyllaTable.ColumnsMap[rowColumn.FieldName]
		column.SetValue(keyPointer, coerceManagedIntegerValue(column, row.RowValues[c]))
	}
}
