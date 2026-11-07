package obdriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// DataFrames (plan §9 step 6, §8.4), as Scylla's driver keeps them (scylla/data_frame.go,
// data_frame_log.go, data_frame_backend.go). The frames, their files and their state are the
// dataframe module's; this driver gives it:
//
//   - the frames compiled from TableSchema.DataFrames, and the lines a record puts in them;
//   - the change log <table>__dtf_log, written in the chunk's transaction from the stored records the
//     chunk locks first, so a committed write always has its log rows and a failed one has none
//     (Scylla writes the log before the rows instead);
//   - the records as Histories: their lines now, from the stored row, undone by their logged changes.
//     The log is read AFTER the rows: a write landing between the two is in both, and its change undoes
//     the new value for any moment before it.

// AcquireFrameLock takes the lock of a DataFrame partition (scylla.AcquireFrameLock): installed at
// boot through db.SetDataFrameLock.
var AcquireFrameLock func(lockName string, wait, lease time.Duration) (release func(), isTaken bool, err error)

const (
	dataFrameLogBucketSeconds = 1800
	dataFrameLogFormat        = 2
	dataFrameLogInsertFlag    = 1
	// frameScanPageSize is how many records a rebuild reads before it reads the log for them.
	frameScanPageSize = 5000
	// frameRebuildLogMargin: a rebuild needs every change logged after its target; the log is read
	// from 5 minutes (150 ticks) earlier.
	frameRebuildLogMargin = 150
)

// dataFrameInfo is a table's compiled DataFrames.
type dataFrameInfo struct {
	frames     *dataframe.Table
	recordType reflect.Type
	// storedColumns are every persisted column with a field: a ToFrames method may read any of them,
	// so the log and the Histories read whole records.
	storedColumns []db.IColInfo
	logTable      tableSpec
}

// configureDataFrames validates a table's DataFrames and compiles them (scylla/data_frame.go
// configureDataFrames). Schema mistakes panic when the table compiles.
func configureDataFrames(table *ObTable, schema db.TableSchema, schemaStruct reflect.Value) {
	if len(schema.DataFrames) == 0 {
		return
	}
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": DataFrames `, table.Name) + fmt.Sprintf(format, args...))
	}
	partitionColumn := table.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		fail("require a partition column: each partition has its own files")
	}
	if fieldType := partitionColumn.GetType().FieldType; fieldType != "int32" && fieldType != "int" {
		fail(`partition column "%v" must be int32. Found: %v`, partitionColumn.GetName(), fieldType)
	}
	if len(table.Keys) != 1 || !slices.Contains([]string{"int16", "int32", "int64", "int"}, table.Keys[0].GetType().FieldType) {
		fail("require exactly one integer key column, the record ID of the change log")
	}
	if table.ID <= 0 {
		fail("require the table's ID: it names the frames' folder")
	}
	recordTyped, isTyped := schemaStruct.Addr().Interface().(interface{ BaseStructType() reflect.Type })
	if !isTyped || recordTyped.BaseStructType() == nil {
		fail("need the table's record type")
	}

	isIntegerType := func(fieldType string) bool {
		return slices.Contains([]string{"int8", "int16", "int32", "int64", "int"}, fieldType)
	}
	var statusColumn db.IColInfo
	if column := table.ColumnsMap["status"]; column != nil && !column.IsNil() {
		if !isIntegerType(column.GetType().FieldType) {
			fail(`status column must be an integer. Found: %v`, column.GetType().FieldType)
		}
		statusColumn = column
	}
	specs := make([]dataframe.Spec, len(schema.DataFrames))
	for specIndex, declared := range schema.DataFrames {
		specs[specIndex] = dataframe.Spec{Name: declared.Name, Count: declared.Count, Version: declared.Version,
			UseZeroSumsMask: declared.UseZeroSumsMask, CompactOn: declared.CompactOn, Projection: declared.ToFrames}
		if declared.ToFrames != nil {
			continue
		}
		var lineColumns []db.IColInfo
		frameColumnOf := func(declaredColumn db.Coln) dataframe.Column {
			column := table.ColumnsMap[declaredColumn.GetName()]
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
			specs[specIndex].Keys = append(specs[specIndex].Keys, frameColumnOf(column))
		}
		for _, column := range declared.Rows {
			specs[specIndex].Rows = append(specs[specIndex].Rows, frameColumnOf(column))
		}
		for _, column := range declared.Sums {
			specs[specIndex].Sums = append(specs[specIndex].Sums, frameColumnOf(column))
		}
		specs[specIndex].LinesOf = columnLinesOf(lineColumns, statusColumn)
	}
	frames, err := dataframe.Compile(table.Name, uint64(table.ID), specs)
	if err != nil {
		fail("%v", err)
	}
	info := &dataFrameInfo{frames: frames, recordType: recordTyped.BaseStructType(),
		logTable: dataFrameLogTableSpec(table.Namespace, table.Name, "", 0)}
	for _, column := range table.ColumnsMap {
		if !column.GetInfo().IsVirtual && column.GetInfo().Field != nil {
			info.storedColumns = append(info.storedColumns, column)
		}
	}
	slices.SortFunc(info.storedColumns, func(left, right db.IColInfo) int { return int(left.GetInfo().Idx) - int(right.GetInfo().Idx) })
	table.dataFrames = info
}

// columnLinesOf is the LinesOf of a frame declared by columns: one line of the columns' values, none
// when status is 0.
func columnLinesOf(lineColumns []db.IColInfo, statusColumn db.IColInfo) func(record unsafe.Pointer) ([]dataframe.Line, error) {
	return func(record unsafe.Pointer) ([]dataframe.Line, error) {
		if statusColumn != nil && db.ConvertToInt64(statusColumn.GetRawValue(record)) == 0 {
			return nil, nil
		}
		line := make(dataframe.Line, len(lineColumns))
		for position, column := range lineColumns {
			var err error
			if line[position], err = frameValueOf(column, column.GetRawValue(record)); err != nil {
				return nil, err
			}
		}
		return []dataframe.Line{line}, nil
	}
}

// frameValueOf is a column's value as frames keep it: integers as they are, floats ×
// dataframe.FloatScale, null as 0.
func frameValueOf(column db.IColInfo, rawValue any) (int64, error) {
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
		return db.ConvertToInt64(rawValue), nil
	}
	scaled, isKept := dataframe.ScaledFloat(floatValue)
	if !isKept {
		return 0, fmt.Errorf(`DataFrame column "%v" holds %v, outside what a frame keeps (±1.1e12, 6 decimals)`, column.GetName(), floatValue)
	}
	return scaled, nil
}

// cloneRecord is a new record holding a copy of source; nil source: a zero record.
func (info *dataFrameInfo) cloneRecord(source unsafe.Pointer) unsafe.Pointer {
	record := reflect.New(info.recordType)
	if source != nil {
		record.Elem().Set(reflect.NewAt(info.recordType, source).Elem())
	}
	return record.UnsafePointer()
}

/* The log */

// dataFrameLogPosition is where a write at moment goes in the log: its half hour and its offset in it.
func dataFrameLogPosition(moment time.Time) (halfHour int32, offsetMicros int32) {
	unixMicros := moment.UnixMicro()
	bucketMicros := int64(dataFrameLogBucketSeconds) * 1e6
	return int32(unixMicros / bucketMicros), int32(unixMicros % bucketMicros)
}

// dataFrameLogMoment is the SUnixTime of a logged write.
func dataFrameLogMoment(halfHour int32, offsetMicros int32) int64 {
	return utils.SUnixTimeOf(time.UnixMicro(int64(halfHour)*dataFrameLogBucketSeconds*1e6 + int64(offsetMicros)))
}

// writeDataFrameLog logs, in the chunk's transaction, every record the chunk inserts or whose lines
// it changes. The record the write leaves is the stored one with the written columns over it, so a
// partial update still yields the record's whole lines. storedByKey are the chunk's locked records.
func (table *ObTable) writeDataFrameLog(transaction *sql.Tx, ctx context.Context, phase writePhase, chunkStart, chunkEnd int, storedByKey map[string]unsafe.Pointer) error {
	info := table.dataFrames
	partitionColumn, keyColumn := table.GetPartKey(), table.Keys[0]
	halfHour, offsetMicros := dataFrameLogPosition(db.Now())
	writtenAt := db.Now().UTC()
	args := []any{}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := phase.records.At(recordIndex)
		partition := db.ConvertToInt64(partitionColumn.GetRawValue(recordPointer))
		if partition < 0 {
			return fmt.Errorf("table %q: a table with DataFrames takes no negative partition (%d): it names the frames' folders", table.Name, partition)
		}
		storedRecord, isStored := storedByKey[table.primaryKeyToken(recordPointer)]
		writtenRecord := info.cloneRecord(storedRecord)
		for _, stored := range phase.writtenColumns {
			copyColumnValue(stored.column, writtenRecord, recordPointer)
		}
		// LinesOf also refuses what a frame can't keep, before anything commits.
		writtenLines, err := info.frames.LinesOf(writtenRecord)
		if err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		var changes []byte
		if !isStored {
			changes = []byte{dataFrameLogFormat, dataFrameLogInsertFlag}
		} else {
			storedLines, err := info.frames.LinesOf(storedRecord)
			if err != nil {
				return fmt.Errorf("table %q: %w", table.Name, err)
			}
			body := info.frames.EncodeChangedLines(storedLines, writtenLines)
			if body == nil {
				continue
			}
			changes = append([]byte{dataFrameLogFormat, 0}, body...)
		}
		// Every row of one write shares its moment; the record ID tells them apart.
		args = append(args, halfHour, partition, offsetMicros, keyColumn.GetRawValue(recordPointer), changes, writtenAt)
	}
	if len(args) == 0 {
		return nil
	}
	statement := fmt.Sprintf("INSERT INTO %s (`half_hour`, `partition_id`, `ts`, `record_id`, `changes`, `written_at`) VALUES %s ON DUPLICATE KEY UPDATE `changes` = VALUES(`changes`)",
		info.logTable.qualifiedName(), strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?, ?, ?), ", len(args)/6), ", "))
	if _, err := transaction.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("table %q: writing the DataFrame log: %w", table.Name, err)
	}
	return nil
}

var errDataFrameLogCorrupt = errors.New("the DataFrame log row is corrupt")

// readDataFrameLog reads the changes of one partition logged after the SUnixTime afterMoment, by
// (partition, record ID), newest first per record.
func (table *ObTable) readDataFrameLog(partition int64, afterMoment int64, now time.Time) (map[[2]int64][]dataframe.HistoryChange, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	firstHalfHour, _ := dataFrameLogPosition(time.Unix(afterMoment*2+1e9, 0))
	lastHalfHour, _ := dataFrameLogPosition(now)
	rows, err := pool.Query(fmt.Sprintf("SELECT `half_hour`, `ts`, `record_id`, `changes` FROM %s WHERE `half_hour` BETWEEN ? AND ? AND `partition_id` = ? ORDER BY `half_hour`, `ts`",
		table.dataFrames.logTable.qualifiedName()), firstHalfHour, lastHalfHour, partition)
	if err != nil {
		return nil, fmt.Errorf("table %q: reading the DataFrame log: %w", table.Name, err)
	}
	defer rows.Close()
	changesByRecord := map[[2]int64][]dataframe.HistoryChange{}
	for rows.Next() {
		var halfHour, offsetMicros int32
		var recordID int64
		var changes []byte
		if err := rows.Scan(&halfHour, &offsetMicros, &recordID, &changes); err != nil {
			return nil, err
		}
		moment := dataFrameLogMoment(halfHour, offsetMicros)
		if moment <= afterMoment {
			continue
		}
		if len(changes) < 2 {
			return nil, fmt.Errorf("table %q: %w", table.Name, errDataFrameLogCorrupt)
		}
		if changes[0] != dataFrameLogFormat {
			continue
		}
		recordKey := [2]int64{partition, recordID}
		changesByRecord[recordKey] = append(changesByRecord[recordKey], dataframe.HistoryChange{
			Moment: moment, IsInsert: changes[1]&dataFrameLogInsertFlag != 0, Body: changes[2:],
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, changes := range changesByRecord {
		slices.Reverse(changes)
	}
	return changesByRecord, nil
}

/* The backend */

type frameBackend struct {
	*dataframe.FileState
	table *ObTable
	// startedAt is the call's now: the reads, Compact and Rebuild take it as theirs.
	startedAt time.Time
}

func newFrameBackend(table *ObTable) (*frameBackend, error) {
	if cache.FileStore() == nil {
		return nil, fmt.Errorf("table %q has DataFrames and no file store holds their files: call db.SetFileStore at boot", table.Name)
	}
	if AcquireFrameLock == nil {
		return nil, fmt.Errorf("table %q has DataFrames and no lock takes them: install obdriver.AcquireFrameLock at boot", table.Name)
	}
	return &frameBackend{
		FileState: &dataframe.FileState{Table: table.dataFrames.frames, Store: cache.FileStore(), AcquireLock: AcquireFrameLock},
		table:     table, startedAt: db.Now(),
	}, nil
}

// storedFrameRecords are decoded stored records by (partition, record ID).
type storedFrameRecords map[[2]int64]unsafe.Pointer

// frameRecordSink decodes whole records of the table into stored.
func (table *ObTable) frameRecordSink(stored storedFrameRecords) recordSink {
	recordType := table.dataFrames.recordType
	return recordSink{
		recordType: recordType,
		newRecord:  func() unsafe.Pointer { return reflect.New(recordType).UnsafePointer() },
		appendRecord: func(record unsafe.Pointer) {
			stored[[2]int64{table.GetPartValue(record), db.ConvertToInt64(table.Keys[0].GetRawValue(record))}] = record
		},
	}
}

// frameColumns are the stored columns with a field: what a frame record is decoded from.
func (table *ObTable) frameColumns() ([]storedColumn, string) {
	columns := []storedColumn{}
	names := []string{}
	for _, stored := range table.storedColumns {
		if stored.hasField() {
			columns, names = append(columns, stored), append(names, stored.name)
		}
	}
	return columns, quotedNames(names)
}

// PartitionHistoriesWrittenAfter is the History of every record of the partition with a change logged
// after moment. The log is read once to know the records, then again after their values. A record only
// the second read logs changed after the first read began: it is left to the next run.
func (backend *frameBackend) PartitionHistoriesWrittenAfter(partition int64, moment int64) ([]dataframe.History, error) {
	table := backend.table
	loggedFirst, err := table.readDataFrameLog(partition, moment, db.Now())
	if err != nil || len(loggedFirst) == 0 {
		return nil, err
	}
	recordIDs := make([]int64, 0, len(loggedFirst))
	for recordKey := range loggedFirst {
		recordIDs = append(recordIDs, recordKey[1])
	}
	stored := storedFrameRecords{}
	columns, columnList := table.frameColumns()
	for batchStart := 0; batchStart < len(recordIDs); batchStart += writeChunkRecords {
		batch := recordIDs[batchStart:min(batchStart+writeChunkRecords, len(recordIDs))]
		args := []any{partition}
		for _, recordID := range batch {
			args = append(args, recordID)
		}
		statement := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? AND %s IN (%s)", columnList, table.qualifiedName(),
			quoteName(table.GetPartKey().GetName()), quoteName(table.Keys[0].GetName()), strings.TrimSuffix(strings.Repeat("?, ", len(batch)), ", "))
		if err := table.queryInto(statement, args, columns, table.frameRecordSink(stored)); err != nil {
			return nil, err
		}
	}
	logged, err := table.readDataFrameLog(partition, moment, db.Now())
	if err != nil {
		return nil, err
	}
	var histories []dataframe.History
	for recordKey := range loggedFirst {
		// A record the table does not hold had its write fail after its log row: nothing to undo.
		if storedRecord, isStored := stored[recordKey]; isStored {
			history, err := backend.historyOf(recordKey, storedRecord, logged[recordKey])
			if err != nil {
				return nil, err
			}
			histories = append(histories, history)
		}
	}
	return histories, nil
}

// historyOf builds a record's History from its stored record and its changes, newest first.
func (backend *frameBackend) historyOf(recordKey [2]int64, storedRecord unsafe.Pointer, changes []dataframe.HistoryChange) (dataframe.History, error) {
	frames := backend.table.dataFrames.frames
	lines, err := frames.LinesOf(storedRecord)
	if err != nil {
		return dataframe.History{}, fmt.Errorf("table %q: %w", backend.table.Name, err)
	}
	history, err := frames.NewHistoryOfChanges(recordKey[0], fmt.Sprint(recordKey[1]), lines, changes)
	if err != nil {
		return dataframe.History{}, fmt.Errorf("table %q: %w", backend.table.Name, err)
	}
	return history, nil
}

// ScanPartitionHistories reads every record of one partition, a page at a time, and the log after each
// page: the changes since a little before the rebuild's target.
func (backend *frameBackend) ScanPartitionHistories(partition int64, onPage func(histories []dataframe.History) error) error {
	return backend.scanPartition(partition, func(page storedFrameRecords, logged map[[2]int64][]dataframe.HistoryChange) error {
		histories := make([]dataframe.History, 0, len(page))
		for recordKey, storedRecord := range page {
			history, err := backend.historyOf(recordKey, storedRecord, logged[recordKey])
			if err != nil {
				return err
			}
			histories = append(histories, history)
		}
		return onPage(histories)
	})
}

// ScanPartitionRecords hands a rebuild the records themselves: a History only for the few the log
// names since a little before the rebuild's target.
func (backend *frameBackend) ScanPartitionRecords(partition int64, onPage func(records []dataframe.ScannedRecord) error) error {
	return backend.scanPartition(partition, func(page storedFrameRecords, logged map[[2]int64][]dataframe.HistoryChange) error {
		records := make([]dataframe.ScannedRecord, 0, len(page))
		for recordKey, storedRecord := range page {
			scanned := dataframe.ScannedRecord{Record: storedRecord}
			if changes := logged[recordKey]; len(changes) > 0 {
				history, err := backend.historyOf(recordKey, storedRecord, changes)
				if err != nil {
					return err
				}
				scanned.History = &history
			}
			records = append(records, scanned)
		}
		return onPage(records)
	})
}

// scanPartition reads every record of one partition in key order, frameScanPageSize at a time, and
// the log after each page.
func (backend *frameBackend) scanPartition(partition int64, onPage func(page storedFrameRecords, logged map[[2]int64][]dataframe.HistoryChange) error) error {
	table := backend.table
	columns, columnList := table.frameColumns()
	keyName := quoteName(table.Keys[0].GetName())
	logSince := utils.SUnixTimeOf(backend.startedAt) - dataframe.TargetLag - frameRebuildLogMargin
	var lastKey any
	for {
		statement := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", columnList, table.qualifiedName(), quoteName(table.GetPartKey().GetName()))
		args := []any{partition}
		if lastKey != nil {
			statement += fmt.Sprintf(" AND %s > ?", keyName)
			args = append(args, lastKey)
		}
		statement += fmt.Sprintf(" ORDER BY %s LIMIT %d", keyName, frameScanPageSize)
		page := storedFrameRecords{}
		maxKey := int64(0)
		sink := table.frameRecordSink(page)
		storeRecord := sink.appendRecord
		sink.appendRecord = func(record unsafe.Pointer) {
			storeRecord(record)
			maxKey = db.ConvertToInt64(table.Keys[0].GetRawValue(record))
		}
		if err := table.queryInto(statement, args, columns, sink); err != nil {
			return fmt.Errorf("table %q: reading the records for a DataFrame rebuild: %w", table.Name, err)
		}
		if len(page) == 0 {
			return nil
		}
		logged, err := table.readDataFrameLog(partition, logSince, db.Now())
		if err != nil {
			return err
		}
		if err := onPage(page, logged); err != nil {
			return err
		}
		if len(page) < frameScanPageSize {
			return nil
		}
		lastKey = maxKey
	}
}

/* Reads */

// frameOfRequest resolves the frame and the selection a request names.
func (table *ObTable) frameOfRequest(request db.FrameRequest) (*dataframe.Frame, dataframe.Selection, error) {
	if table.dataFrames == nil || table.dataFrames.frames.FrameNamed(request.Frame) == nil {
		return nil, dataframe.Selection{}, fmt.Errorf("table %q has no DataFrame %q", table.Name, request.Frame)
	}
	frame := table.dataFrames.frames.FrameNamed(request.Frame)
	if request.Selection != nil {
		return frame, *request.Selection, nil
	}
	filters := request.Filters
	if frame.IsProjected {
		// A ToFrames frame's Keys are its frame struct's field names: a filter on a record column
		// selects the key of the same field name.
		filters = slices.Clone(filters)
		for position, filter := range filters {
			if column := table.ColumnsMap[filter.Column]; column != nil && !column.IsNil() {
				filters[position].Column = column.GetInfo().FieldName
			}
		}
	}
	selection, err := db.FrameSelection(frame, filters)
	return frame, selection, err
}

func (table *ObTable) readFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	frame, selection, err := table.frameOfRequest(request)
	if err != nil {
		return nil, nil, err
	}
	backend, err := newFrameBackend(table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := dataframe.Read(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, request.IsFresh, backend.startedAt)
	return rows, frame, err
}

func (table *ObTable) readFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	frame, selection, err := table.frameOfRequest(request)
	if err != nil {
		return nil, err
	}
	held, err := dataframe.ParseHeldFiles(heldFiles)
	if err != nil {
		return nil, err
	}
	backend, err := newFrameBackend(table)
	if err != nil {
		return nil, err
	}
	answer, err := dataframe.ReadChanged(backend.Table, frame, backend, cache.FileStore(), int64(request.Partition), selection, heldGeneration, held, backend.startedAt)
	if err != nil {
		return nil, err
	}
	return dataframe.AppendChangedFiles(nil, answer), nil
}

// setFrameRowKey sets the frame's Keys and Rows columns of the key record. A ToFrames frame names a
// frame struct's fields, not the record's: its rows leave the key empty.
func (table *ObTable) setFrameRowKey(frame *dataframe.Frame, keyPointer unsafe.Pointer, row dataframe.Row) {
	if frame.IsProjected {
		return
	}
	for keyIndex, key := range frame.Keys {
		column := table.ColumnsMap[key.FieldName]
		column.SetValue(keyPointer, db.CoerceManagedInteger(column, row.Keys[keyIndex]))
	}
	for rowIndex, rowColumn := range frame.Rows {
		column := table.ColumnsMap[rowColumn.FieldName]
		column.SetValue(keyPointer, db.CoerceManagedInteger(column, row.RowValues[rowIndex]))
	}
}

// withFrameBackend runs one frames step with a new backend, or nothing for a table without frames.
func (table *ObTable) withFrameBackend(run func(backend *frameBackend) error) error {
	if table.dataFrames == nil {
		return nil
	}
	backend, err := newFrameBackend(table)
	if err != nil {
		return err
	}
	return run(backend)
}
