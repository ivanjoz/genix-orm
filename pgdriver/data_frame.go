package pgdriver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/utils"
)

// DataFrames as SQL aggregate tables (dataframe/aggregate_table.go). Each frame of a table is one table
// `<table>__agg_<frame>`:
//
//	partition_id INTEGER, packed_key BIGINT, one BIGINT per summed column, updated INTEGER
//	PRIMARY KEY (partition_id, packed_key)
//
// The write path keeps it exact in the record's transaction (writeFrameAggregates): the lines the
// stored record gave go out, the lines the written one gives come in, one upsert per touched row, in
// packed-key order. A read is a range scan of it. Deploy rebuilds a table whose frame changed shape
// (deployFrameAggregates), from the records, and records the shape in `_frame_shapes`.

// dataFrameInfo is a table's compiled DataFrames.
type dataFrameInfo struct {
	frames     *dataframe.Table
	recordType reflect.Type
	// storedColumns are every persisted column with a field: a ToFrames method may read any of them,
	// so a write reads whole stored records.
	storedColumns []db.IColInfo
	// aggregateTables are the frames' aggregate tables, by frame index.
	aggregateTables []tableSpec
}

// configureDataFrames validates a table's DataFrames and compiles them. Schema mistakes panic when
// the table compiles.
func configureDataFrames(table *PgTable, schema db.TableSchema, schemaStruct reflect.Value) {
	if len(schema.DataFrames) == 0 {
		return
	}
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": DataFrames `, table.Name) + fmt.Sprintf(format, args...))
	}
	partitionColumn := table.GetPartKey()
	if partitionColumn == nil || partitionColumn.IsNil() {
		fail("require a partition column: the aggregate rows are kept per partition")
	}
	if fieldType := partitionColumn.GetType().FieldType; fieldType != "int32" && fieldType != "int" {
		fail(`partition column "%v" must be int32. Found: %v`, partitionColumn.GetName(), fieldType)
	}
	recordTyped, isTyped := schemaStruct.Addr().Interface().(interface{ BaseStructType() reflect.Type })
	if !isTyped || recordTyped.BaseStructType() == nil {
		fail("need the table's record type")
	}

	var statusColumn db.IColInfo
	if column := table.ColumnsMap["status"]; column != nil && !column.IsNil() {
		if !isIntegerFieldType(column.GetType().FieldType) {
			fail(`status column must be an integer. Found: %v`, column.GetType().FieldType)
		}
		statusColumn = column
	}
	specs := make([]dataframe.Spec, len(schema.DataFrames))
	for specIndex, declared := range schema.DataFrames {
		specs[specIndex] = dataframe.Spec{Name: declared.Name, Count: declared.Count, Version: declared.Version,
			UseZeroSumsMask: declared.UseZeroSumsMask, Projection: declared.ToFrames}
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
			if !isFloat && !isIntegerFieldType(fieldType) {
				fail(`frame %q column "%v" must be an integer or a float. Found: %v`, declared.Name, column.GetName(), fieldType)
			}
			lineColumns = append(lineColumns, column)
			// The column's position names it in the frames' shape; its integer type sizes its packed-key width.
			return dataframe.Column{FieldName: column.GetName(), ColumnID: int(column.GetInfo().Idx), IsFloat: isFloat,
				Bits: dataframe.IntegerKindBits(column.GetInfo().Field.Type.Kind())}
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
	info := &dataFrameInfo{frames: frames, recordType: recordTyped.BaseStructType()}
	for frameIndex := range frames.Frames {
		frame := &frames.Frames[frameIndex]
		if err := frame.CheckPackedKey(table.Name); err != nil {
			fail("%v", err)
		}
		info.aggregateTables = append(info.aggregateTables, aggregateTableSpec(table.Namespace, table.Name, frame))
	}
	for _, column := range table.ColumnsMap {
		if !column.GetInfo().IsVirtual && column.GetInfo().Field != nil {
			info.storedColumns = append(info.storedColumns, column)
		}
	}
	slices.SortFunc(info.storedColumns, func(left, right db.IColInfo) int { return int(left.GetInfo().Idx) - int(right.GetInfo().Idx) })
	table.dataFrames = info
}

func isIntegerFieldType(fieldType string) bool {
	return slices.Contains([]string{"int8", "int16", "int32", "int64", "int"}, fieldType)
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

/* The aggregate tables */

// aggregateTableSpec is a frame's aggregate table: a column per summed value, named after its frame
// struct field (record_count for the count).
func aggregateTableSpec(database, tableName string, frame *dataframe.Frame) tableSpec {
	columns := []columnSpec{{"partition_id", "INTEGER", true}, {"packed_key", "BIGINT", true}}
	for _, sumColumnName := range aggregateSumColumns(frame) {
		columns = append(columns, columnSpec{sumColumnName, "BIGINT", true})
	}
	columns = append(columns, columnSpec{"updated", "INTEGER", true})
	return tableSpec{database: database, name: boundedIdentifier(tableName + "__agg_" + strings.ReplaceAll(frame.Name, "-", "_")),
		columns: columns, primaryKey: []string{"partition_id", "packed_key"}}
}

// aggregateSumColumns names the summed columns of a frame's aggregate table, in its sums' order.
func aggregateSumColumns(frame *dataframe.Frame) []string {
	names := make([]string, 0, frame.SumsCount)
	for _, sumColumn := range frame.Sums {
		names = append(names, db.ToSnakeCase(sumColumn.FieldName))
	}
	if frame.CountsRecords {
		names = append(names, "record_count")
	}
	return names
}

// writeFrameAggregates moves the aggregate rows a chunk changes, in its transaction: for every record,
// the lines its stored version gave go out and the lines it gives once written come in. The written
// record is the stored one with the written columns over it, so a partial update still yields its
// whole lines. A record written twice in the chunk counts its last write only, as its row does.
func (table *PgTable) writeFrameAggregates(transaction *writeTransaction, phase writePhase, chunkStart, chunkEnd int, storedByKey map[string]unsafe.Pointer) error {
	info := table.dataFrames
	lastIndexByKey := map[string]int{}
	recordIndexes := []int{}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		keyToken := table.primaryKeyToken(phase.records.At(recordIndex))
		if position, isSeen := lastIndexByKey[keyToken]; isSeen {
			recordIndexes[position] = recordIndex
			continue
		}
		lastIndexByKey[keyToken] = len(recordIndexes)
		recordIndexes = append(recordIndexes, recordIndex)
	}
	// deltas[frame][partition][packed key] are the sums each touched row moves by.
	deltas := make([]map[int64]map[int64][]int64, len(info.frames.Frames))
	for frameIndex := range deltas {
		deltas[frameIndex] = map[int64]map[int64][]int64{}
	}
	for _, recordIndex := range recordIndexes {
		recordPointer := phase.records.At(recordIndex)
		partition := db.ConvertToInt64(table.GetPartKey().GetRawValue(recordPointer))
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
		var storedLines dataframe.RecordLines
		if isStored {
			if storedLines, err = info.frames.LinesOf(storedRecord); err != nil {
				return fmt.Errorf("table %q: %w", table.Name, err)
			}
		}
		for frameIndex := range info.frames.Frames {
			frame := &info.frames.Frames[frameIndex]
			if deltas[frameIndex][partition] == nil {
				deltas[frameIndex][partition] = map[int64][]int64{}
			}
			if isStored {
				if err := frame.AddLines(deltas[frameIndex][partition], storedLines[frameIndex], -1); err != nil {
					return fmt.Errorf("table %q: %w", table.Name, err)
				}
			}
			if err := frame.AddLines(deltas[frameIndex][partition], writtenLines[frameIndex], 1); err != nil {
				return fmt.Errorf("table %q: %w", table.Name, err)
			}
		}
	}
	updated := db.ManagedUnixTime()
	for frameIndex := range info.frames.Frames {
		if err := table.upsertAggregateRows(transaction, frameIndex, deltas[frameIndex], updated); err != nil {
			return err
		}
	}
	return nil
}

// upsertAggregateRows adds sums to a frame's aggregate rows, by partition then packed key: two writes
// of one partition lock their shared rows in the same order and never deadlock on them.
func (table *PgTable) upsertAggregateRows(transaction *writeTransaction, frameIndex int, deltasByPartition map[int64]map[int64][]int64, updated int32) error {
	frame := &table.dataFrames.frames.Frames[frameIndex]
	sumColumns := aggregateSumColumns(frame)
	partitions := make([]int64, 0, len(deltasByPartition))
	for partition := range deltasByPartition {
		partitions = append(partitions, partition)
	}
	slices.Sort(partitions)
	args, rowCount := []any{}, 0
	for _, partition := range partitions {
		for _, delta := range dataframe.SortedDeltas(deltasByPartition[partition]) {
			args = append(args, partition, delta.PackedKey)
			for _, sum := range delta.Sums {
				args = append(args, sum)
			}
			args = append(args, updated)
			rowCount++
		}
	}
	if rowCount == 0 {
		return nil
	}
	assignments := make([]string, 0, len(sumColumns)+1)
	for _, sumColumn := range sumColumns {
		assignments = append(assignments, fmt.Sprintf(`%s = "stored".%s + EXCLUDED.%s`, quoteName(sumColumn), quoteName(sumColumn), quoteName(sumColumn)))
	}
	assignments = append(assignments, `"updated" = EXCLUDED."updated"`)
	rowPlaceholders := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(sumColumns)+3), ", ") + ")"
	statement := fmt.Sprintf(`INSERT INTO %s AS "stored" ("partition_id", "packed_key", %s, "updated") VALUES %s ON CONFLICT ("partition_id", "packed_key") DO UPDATE SET %s`,
		table.dataFrames.aggregateTables[frameIndex].qualifiedName(), quotedNames(sumColumns),
		strings.TrimSuffix(strings.Repeat(rowPlaceholders+", ", rowCount), ", "), strings.Join(assignments, ", "))
	if err := transaction.queue(statement, args...); err != nil {
		return fmt.Errorf("table %q frame %q: writing its aggregate rows: %w", table.Name, frame.Name, err)
	}
	return nil
}

/* Reads */

// frameOfRequest resolves the frame and the selection a request names.
func (table *PgTable) frameOfRequest(request db.FrameRequest) (*dataframe.Frame, dataframe.Selection, error) {
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

// readAggregateRows reads the aggregate rows of a selection's packed-key range, ascending.
func (table *PgTable) readAggregateRows(frame *dataframe.Frame, partition int64, selection dataframe.Selection) ([]dataframe.AggregateRow, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	sumColumns := aggregateSumColumns(frame)
	fromKey, toKey := frame.PackedKeyRange(selection)
	rows, err := queryStatement(pool, context.Background(), fmt.Sprintf(`SELECT "packed_key", %s, "updated" FROM %s WHERE "partition_id" = ? AND "packed_key" BETWEEN ? AND ? ORDER BY "packed_key"`,
		quotedNames(sumColumns), table.dataFrames.aggregateTables[frame.Index].qualifiedName()), partition, fromKey, toKey)
	if err != nil {
		return nil, fmt.Errorf("table %q frame %q: reading its aggregate rows: %w", table.Name, frame.Name, err)
	}
	defer rows.Close()
	aggregateRows := []dataframe.AggregateRow{}
	for rows.Next() {
		row := dataframe.AggregateRow{Sums: make([]int64, len(sumColumns))}
		targets := []any{&row.PackedKey}
		for position := range row.Sums {
			targets = append(targets, &row.Sums[position])
		}
		if err := rows.Scan(append(targets, &row.Updated)...); err != nil {
			return nil, err
		}
		aggregateRows = append(aggregateRows, row)
	}
	return aggregateRows, rows.Err()
}

func (table *PgTable) readFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	frame, selection, err := table.frameOfRequest(request)
	if err != nil {
		return nil, nil, err
	}
	aggregateRows, err := table.readAggregateRows(frame, int64(request.Partition), selection)
	if err != nil {
		return nil, nil, err
	}
	return frame.RowsOfAggregate(aggregateRows, selection), frame, nil
}

func (table *PgTable) readFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	frame, selection, err := table.frameOfRequest(request)
	if err != nil {
		return nil, err
	}
	held, err := dataframe.ParseHeldFiles(heldFiles)
	if err != nil {
		return nil, err
	}
	aggregateRows, err := table.readAggregateRows(frame, int64(request.Partition), selection)
	if err != nil {
		return nil, err
	}
	answer := frame.ChangedFilesOfAggregate(aggregateRows, selection, heldGeneration, held, utils.SUnixTimeOf(db.Now()))
	return dataframe.AppendChangedFiles(nil, answer), nil
}

// setFrameRowKey sets the frame's Keys and Rows columns of the key record. A ToFrames frame names a
// frame struct's fields, not the record's: its rows leave the key empty.
func (table *PgTable) setFrameRowKey(frame *dataframe.Frame, keyPointer unsafe.Pointer, row dataframe.Row) {
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

/* Rebuilds */

// frameShapesTableSpec is `<database>._frame_shapes`: the shape each aggregate table was built with.
func frameShapesTableSpec(database string) tableSpec {
	return tableSpec{database: database, name: "_frame_shapes",
		columns:    []columnSpec{{"aggregate_table", keyedStringType(), true}, {"shape", keyedStringType(), true}},
		primaryKey: []string{"aggregate_table"}}
}

// deployFrameAggregates rebuilds, after the tables are homologated, every aggregate table of the
// table whose shape is not the one `_frame_shapes` records (a new frame, a bumped Version, a new sum,
// other widths): the table is recreated, its rows recomputed from the records, and the shape recorded.
func (table *PgTable) deployFrameAggregates(pool *sql.DB) error {
	shapesTable := frameShapesTableSpec(table.Namespace).qualifiedName()
	for frameIndex := range table.dataFrames.frames.Frames {
		frame := &table.dataFrames.frames.Frames[frameIndex]
		aggregateTable := table.dataFrames.aggregateTables[frameIndex].name
		var storedShape string
		err := queryRowStatement(pool, context.Background(), `SELECT "shape" FROM `+shapesTable+` WHERE "aggregate_table" = ?`, aggregateTable).Scan(&storedShape)
		if err == nil && storedShape == frame.AggregateShape() {
			continue
		}
		// Recreated, not altered: a removed or renamed sum column would stay behind as NOT NULL.
		fmt.Printf("pgdriver deploy: rebuilding %q (frame %q)\n", aggregateTable, frame.Name)
		aggregateSpec := table.dataFrames.aggregateTables[frameIndex]
		if err := executeDeployStatements(pool, []string{"DROP TABLE IF EXISTS " + aggregateSpec.qualifiedName()}); err != nil {
			return err
		}
		if _, _, err := homologateTable(pool, aggregateSpec, nil); err != nil {
			return err
		}
		if err := table.rebuildFrameAggregates(nil, []int{frameIndex}); err != nil {
			return err
		}
		if _, err := execStatement(pool, context.Background(), `INSERT INTO `+shapesTable+` ("aggregate_table", "shape") VALUES (?, ?)
			ON CONFLICT ("aggregate_table") DO UPDATE SET "shape" = EXCLUDED."shape"`, aggregateTable, frame.AggregateShape()); err != nil {
			return fmt.Errorf("table %q frame %q: recording its shape: %w", table.Name, frame.Name, err)
		}
	}
	return nil
}

// rebuildFrameAggregates recomputes the aggregate rows of the given frames from the records: of one
// partition, or of every one when partition is nil. Its transaction takes the aggregate tables in
// EXCLUSIVE mode first (plain reads go on), then reads the committed records. A writer upserts its
// aggregate rows after it locked its records (or inserted new ones), so no write is counted twice or lost:
//   - one that already upserted holds the tables: the rebuild waits until it commits, then sees it;
//   - one that didn't waits for the rebuild, then moves the rebuilt rows from what it locked.
//
// The main table is not locked: a writer holds its aggregate rows while it waits for the main table,
// so locking it too could deadlock.
func (table *PgTable) rebuildFrameAggregates(partition *int64, frameIndexes []int) error {
	info := table.dataFrames
	partitionName := quoteName(table.GetPartKey().GetName())
	return runTransaction(func(transaction *writeTransaction) error {
		lockedTables := []string{}
		for _, frameIndex := range frameIndexes {
			lockedTables = append(lockedTables, info.aggregateTables[frameIndex].qualifiedName())
		}
		if err := transaction.queue("LOCK TABLE " + strings.Join(lockedTables, ", ") + " IN EXCLUSIVE MODE"); err != nil {
			return err
		}
		for _, frameIndex := range frameIndexes {
			statement, args := "DELETE FROM "+info.aggregateTables[frameIndex].qualifiedName(), []any{}
			if partition != nil {
				statement, args = statement+` WHERE "partition_id" = ?`, []any{*partition}
			}
			if err := transaction.queue(statement, args...); err != nil {
				return err
			}
		}
		deltas := make([]map[int64]map[int64][]int64, len(info.frames.Frames))
		for _, frameIndex := range frameIndexes {
			deltas[frameIndex] = map[int64]map[int64][]int64{}
		}
		storedColumns := []storedColumn{}
		storedNames := []string{}
		for _, stored := range table.storedColumns {
			if stored.hasField() {
				storedColumns, storedNames = append(storedColumns, stored), append(storedNames, stored.name)
			}
		}
		statement, args := fmt.Sprintf("SELECT %s FROM %s", quotedNames(storedNames), table.qualifiedName()), []any{}
		if partition != nil {
			statement, args = statement+" WHERE "+partitionName+" = ?", []any{*partition}
		}
		sink := recordSink{
			recordType: info.recordType,
			newRecord:  func() unsafe.Pointer { return reflect.New(info.recordType).UnsafePointer() },
			appendRecord: func(record unsafe.Pointer) {
				recordPartition := db.ConvertToInt64(table.GetPartKey().GetRawValue(record))
				recordLines, err := info.frames.LinesOf(record)
				for _, frameIndex := range frameIndexes {
					frame := &info.frames.Frames[frameIndex]
					if deltas[frameIndex][recordPartition] == nil {
						deltas[frameIndex][recordPartition] = map[int64][]int64{}
					}
					if err == nil {
						err = frame.AddLines(deltas[frameIndex][recordPartition], recordLines[frameIndex], 1)
					}
					if err != nil {
						panic(fmt.Errorf("table %q frame %q: rebuilding its aggregate rows: %w", table.Name, frame.Name, err))
					}
				}
			},
		}
		if err := table.queryIntoWith(transaction, transaction.ctx, statement, args, storedColumns, sink); err != nil {
			return err
		}
		updated := db.ManagedUnixTime()
		for _, frameIndex := range frameIndexes {
			if err := table.upsertAggregateRows(transaction, frameIndex, deltas[frameIndex], updated); err != nil {
				return err
			}
		}
		return nil
	})
}
