package scylla

import (
	"fmt"
	"hash/crc32"
	"slices"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// ─────────────────────────────────────────────────────────────────────────────
// DataFrames: group-by reports of a table kept as columnar files in the file store.
// The logic lives in the dataframe module (../dataframe, DATAFRAMES.md); scylla gives it:
//
//   - the frames, compiled from TableSchema.DataFrames (configureDataFrames), each
//     partition of the table with its own files;
//   - a change log table per table, <table>__dtf_log, that every write appends to
//     before its rows land (data_frame_log.go);
//   - the frames' state in a file of the store and their lock in AcquireFrameLock,
//     and the records as Histories: their values now, read from the table, undone by
//     their logged changes (data_frame_backend.go).
// ─────────────────────────────────────────────────────────────────────────────

// AcquireFrameLock takes a DataFrame's lock without waiting: release frees it, false when another
// holder has it. lockName names the frame across tables; lease is how long the run may hold it. The
// application installs it at boot (genix: fareward) before the first MaterializeDataFrames. The lock
// only spares duplicate work: the frames' state and files are written conditioned on their ETags.
var AcquireFrameLock func(lockName string, lease time.Duration) (release func(), isTaken bool, err error)

// dataFrameInfo is a table's compiled DataFrames.
type dataFrameInfo struct {
	frames *dataframe.Table
	// trackedColumns are the columns of frames.Tracked, in its order.
	trackedColumns []IColInfo
	// columnHashes name the tracked columns in the log (data_frame_log.go): 16 bits of each column
	// name's CRC32, so a log row keeps its meaning when the struct's fields move.
	columnHashes       []uint16
	trackedIndexByHash map[uint16]int
	logTableName       string
}

// configureDataFrames validates a table's DataFrames and compiles them. Schema mistakes panic when
// the table compiles.
func configureDataFrames(scyllaTable *ScyllaTable, schema TableSchema) {
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

	isIntegerType := func(fieldType string) bool {
		return slices.Contains([]string{"int8", "int16", "int32", "int64", "int"}, fieldType)
	}
	frameColumnOf := func(declared Coln, frameName string) dataframe.Column {
		column := scyllaTable.ColumnsMap[declared.GetName()]
		if column == nil || column.GetInfo().IsVirtual {
			fail(`frame %q column "%v" must be a persisted column of the table`, frameName, declared.GetName())
		}
		fieldType := column.GetType().FieldType
		isFloat := fieldType == "float32" || fieldType == "float64"
		if !isFloat && !isIntegerType(fieldType) {
			fail(`frame %q column "%v" must be an integer or a float. Found: %v`, frameName, column.GetName(), fieldType)
		}
		// The column's position names it in the frames' shape: moving the field rebuilds them.
		return dataframe.Column{FieldName: column.GetName(), ColumnID: int(column.GetInfo().Idx), IsFloat: isFloat}
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
	if statusColumn := scyllaTable.ColumnsMap["status"]; statusColumn != nil && !statusColumn.IsNil() {
		if !isIntegerType(statusColumn.GetType().FieldType) {
			fail(`status column must be an integer. Found: %v`, statusColumn.GetType().FieldType)
		}
		status = &dataframe.Column{FieldName: statusColumn.GetName(), ColumnID: int(statusColumn.GetInfo().Idx)}
	}
	frames, err := dataframe.Compile(scyllaTable.Name, uint64(scyllaTable.ID), specs, status)
	if err != nil {
		fail("%v", err)
	}

	info := &dataFrameInfo{frames: frames, trackedIndexByHash: map[uint16]int{}, logTableName: scyllaTable.Name + "__dtf_log"}
	for trackedIndex, tracked := range frames.Tracked {
		info.trackedColumns = append(info.trackedColumns, scyllaTable.ColumnsMap[tracked.FieldName])
		columnHash := uint16(crc32.ChecksumIEEE([]byte(tracked.FieldName)))
		if _, isTaken := info.trackedIndexByHash[columnHash]; isTaken {
			fail(`column "%v" has the same log hash as another tracked column: rename one of them`, tracked.FieldName)
		}
		info.columnHashes = append(info.columnHashes, columnHash)
		info.trackedIndexByHash[columnHash] = trackedIndex
	}
	scyllaTable.dataFrames = info
}

// trackedValueOf is a tracked column's value as frames keep it: integers as they are, floats ×
// dataframe.FloatScale, null as 0.
func (info *dataFrameInfo) trackedValueOf(column IColInfo, rawValue any) (int64, error) {
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

// trackedValuesOfRow reads the tracked values of a stored row.
func (info *dataFrameInfo) trackedValuesOfRow(storedRow map[string]any) ([]int64, error) {
	values := make([]int64, len(info.trackedColumns))
	for i, column := range info.trackedColumns {
		var err error
		if values[i], err = info.trackedValueOf(column, storedRow[column.GetName()]); err != nil {
			return nil, err
		}
	}
	return values, nil
}

/* Controller */

// MaterializeDataFrames brings every DataFrame of the table up to date, every partition at once.
// Run it every few minutes (genix: fn-materialize-frames).
func (e *ScyllaController[T, E]) MaterializeDataFrames() error {
	return withFrameBackend(&e.Table, func(backend *frameBackend) error {
		return dataframe.Materialize(backend.Table, backend, cache.FileStore(), backend.startedAt)
	})
}

// RebuildDataFrames computes the files of the frame frameName ("" every frame) again from the
// records, every partition.
func (e *ScyllaController[T, E]) RebuildDataFrames(frameName string) error {
	return withFrameBackend(&e.Table, func(backend *frameBackend) error {
		return dataframe.Rebuild(backend.Table, backend, cache.FileStore(), frameName, backend.startedAt)
	})
}

// RebuildDataFramesPartition computes one partition's files of every frame again from its records.
func (e *ScyllaController[T, E]) RebuildDataFramesPartition(partValue int32) error {
	return rebuildDataFramesPartition(&e.Table, partValue)
}

func rebuildDataFramesPartition(scyllaTable *ScyllaTable, partValue int32) error {
	return withFrameBackend(scyllaTable, func(backend *frameBackend) error {
		return dataframe.RebuildPartition(backend.Table, backend, cache.FileStore(), int64(partValue), backend.startedAt)
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
	selection, err := db.FrameSelection(frame, request.Filters)
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
	rows, err := dataframe.Read(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, request.IsFresh)
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
	answer, err := dataframe.ReadChanged(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, heldGeneration, held, utils.SUnixTimeOf(backend.startedAt))
	if err != nil {
		return nil, err
	}
	return dataframe.AppendChangedFiles(nil, answer), nil
}

// setFrameRowKey sets the frame's Keys and Rows columns of the key record at keyPointer.
func setFrameRowKey(scyllaTable *ScyllaTable, frame *dataframe.Frame, keyPointer unsafe.Pointer, row dataframe.Row) {
	trackedColumns := scyllaTable.dataFrames.trackedColumns
	for k, key := range frame.Keys {
		trackedColumns[key.TrackedIndex].SetValue(keyPointer, coerceManagedIntegerValue(trackedColumns[key.TrackedIndex], row.Keys[k]))
	}
	for c, rowColumn := range frame.Rows {
		trackedColumns[rowColumn.TrackedIndex].SetValue(keyPointer, coerceManagedIntegerValue(trackedColumns[rowColumn.TrackedIndex], row.RowValues[c]))
	}
}
