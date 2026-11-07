package obdriver

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db/cache"

	"github.com/ivanjoz/genix-orm/db"
)

// ObController is the db.Controller of one table (docs/OCEANBASE_PLAN.md §12). GetTable compiles it;
// every other method answers errNotImplemented until its step lands.
type ObController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]] struct {
	Schema db.TableSchema
}

func (controller *ObController[RecordT, TableT]) GetTable() db.Table {
	return compiledTableOf[TableT, RecordT]()
}

func (controller *ObController[RecordT, TableT]) GetTableName() string {
	return controller.Schema.Name
}

// GetRecords reads one partition (every record when partValue is 0), from lastKey on when set.
func (controller *ObController[RecordT, TableT]) GetRecords(partValue, limit int32, lastKey any) []any {
	recordsPointer, err := compiledTableOf[TableT, RecordT]().readPartitionRecords(reflect.TypeFor[RecordT](), partValue, lastKey)
	if err != nil {
		fmt.Println("obdriver: GetRecords of", controller.Schema.Name, "failed:", err)
		return nil
	}
	records := recordsPointer.Elem()
	recordsAny := make([]any, records.Len())
	for index := range recordsAny {
		recordsAny[index] = records.Index(index).Interface()
	}
	return recordsAny
}

func (controller *ObController[RecordT, TableT]) GetRecordsGob(partValue, limit int32, lastKey any) ([]byte, error) {
	gob.Register(*new(RecordT))
	records := controller.GetRecords(partValue, limit, lastKey)
	if len(records) == 0 {
		return nil, nil
	}
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(records); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// ExportRecordsColbin streams one partition out in colbin batches of batchSize records; the restore
// sets the partition back on every record.
func (controller *ObController[RecordT, TableT]) ExportRecordsColbin(partValue int32, batchSize int, emitBatch func(encoded []byte, rowsCount int32) error) (int32, error) {
	table := compiledTableOf[TableT, RecordT]()
	tableInfo := &db.TableInfo{}
	if partitionColumn := table.GetPartKey(); partitionColumn != nil && !partitionColumn.IsNil() {
		tableInfo.SetWhere(partitionColumn.GetName(), "=", partValue)
	}
	batch := make([]RecordT, 0, batchSize)
	rowsTotal := int32(0)
	var emitErr error
	emitCurrentBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		encoded, err := colbin.Marshal(&batch)
		if err != nil {
			return fmt.Errorf("obdriver: encoding a colbin batch of %q: %w", table.Name, err)
		}
		if err := emitBatch(encoded, int32(len(batch))); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	err := selectRecords(table, tableInfo, makeRecordSink(&batch, func(record *RecordT) bool {
		if emitErr != nil {
			return true
		}
		batch = append(batch, *record)
		rowsTotal++
		if len(batch) >= batchSize {
			emitErr = emitCurrentBatch()
		}
		return true
	}))
	if err != nil {
		return rowsTotal, err
	}
	if emitErr != nil {
		return rowsTotal, emitErr
	}
	return rowsTotal, emitCurrentBatch()
}

// RestoreRecordsColbin inserts one exported batch. deletePartitionFirst wipes the partition (and its
// text index) first: it belongs on the first batch of a table only.
func (controller *ObController[RecordT, TableT]) RestoreRecordsColbin(partValue int32, encoded []byte, deletePartitionFirst bool) (int, error) {
	table := compiledTableOf[TableT, RecordT]()
	records := []RecordT{}
	if err := colbin.Unmarshal(encoded, &records); err != nil {
		return 0, fmt.Errorf("obdriver: decoding a colbin batch of %q: %w", table.Name, err)
	}
	partitionColumn := table.GetPartKey()
	hasPartition := partitionColumn != nil && !partitionColumn.IsNil()
	if hasPartition {
		for index := range records {
			partitionColumn.SetValue(unsafe.Pointer(&records[index]), partValue)
		}
	}
	if deletePartitionFirst && partValue > 0 && hasPartition {
		pool, err := sharedConnection()
		if err != nil {
			return 0, err
		}
		statement := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", table.qualifiedName(), quoteName(partitionColumn.GetName()))
		if _, err := pool.Exec(statement, partValue); err != nil {
			return 0, fmt.Errorf("obdriver: wiping partition %v of %q: %w", partValue, table.Name, err)
		}
		if err := controller.DropTextSearch(partValue); err != nil {
			return 0, err
		}
	}
	if err := writeRecords(table, []writePhase{insertPhase(db.MakeRecordSlice(&records), nil)}); err != nil {
		return 0, err
	}
	return len(records), nil
}

// ReloadRecords reads a partition and writes it back through Insert, which recomputes every derived
// value: generated columns, indexes, caches.
func (controller *ObController[RecordT, TableT]) ReloadRecords(partValue int32) error {
	table := compiledTableOf[TableT, RecordT]()
	recordsPointer, err := table.readPartitionRecords(reflect.TypeFor[RecordT](), partValue, nil)
	if err != nil {
		return err
	}
	return writeRecords(table, []writePhase{insertPhase(db.MakeReflectRecordSlice(recordsPointer), nil)})
}

func (controller *ObController[RecordT, TableT]) RecalcVirtualColumns(partValue int32) error {
	return errNotImplemented("Controller.RecalcVirtualColumns")
}

func (controller *ObController[RecordT, TableT]) RecalcGroupIndexHashes(partValue int32) error {
	return errNotImplemented("Controller.RecalcGroupIndexHashes")
}

func (controller *ObController[RecordT, TableT]) ResetCounter(partValue any) error {
	return errNotImplemented("Controller.ResetCounter")
}

func (controller *ObController[RecordT, TableT]) DeleteViewsAndIndexes() error {
	return errNotImplemented("Controller.DeleteViewsAndIndexes")
}

// ReindexTextSearch rebuilds one partition's text index from the table's active records.
func (controller *ObController[RecordT, TableT]) ReindexTextSearch(partValue int32) error {
	return compiledTableOf[TableT, RecordT]().reindexTextSearch(partValue)
}

// ReindexTextSearchAll rebuilds the text index of every partition that has records.
func (controller *ObController[RecordT, TableT]) ReindexTextSearchAll() error {
	table := compiledTableOf[TableT, RecordT]()
	if table.TextSearchIndex == nil {
		return nil
	}
	partitions, err := table.listPartitions()
	if err != nil {
		return err
	}
	for _, partition := range partitions {
		if err := table.reindexTextSearch(partition); err != nil {
			return err
		}
	}
	return nil
}

func (controller *ObController[RecordT, TableT]) CompactTextSearch(partValue int32) error {
	table := compiledTableOf[TableT, RecordT]()
	if table.TextSearchIndex == nil {
		return nil
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	return backend.Compact(context.Background(), table.Name, db.TextSearchPartition(int64(partValue)))
}

func (controller *ObController[RecordT, TableT]) DropTextSearch(partValue int32) error {
	table := compiledTableOf[TableT, RecordT]()
	if table.TextSearchIndex == nil {
		return nil
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	return backend.Drop(context.Background(), table.Name, db.TextSearchPartition(int64(partValue)))
}

// CompactDataFrames brings one partition's files of every DataFrame of the table up to date.
func (controller *ObController[RecordT, TableT]) CompactDataFrames(partValue int32) error {
	return compiledTableOf[TableT, RecordT]().withFrameBackend(func(backend *frameBackend) error {
		return dataframe.Compact(backend.Table, backend, cache.FileStore(), int64(partValue), backend.startedAt)
	})
}

// RebuildDataFramesPartition computes one partition's files of every frame again from its records.
func (controller *ObController[RecordT, TableT]) RebuildDataFramesPartition(partValue int32) error {
	return compiledTableOf[TableT, RecordT]().withFrameBackend(func(backend *frameBackend) error {
		return dataframe.Rebuild(backend.Table, backend, cache.FileStore(), int64(partValue), "", backend.startedAt)
	})
}

// DescribeTable publishes the table's schema in serializable form, with its query shapes.
func (controller *ObController[RecordT, TableT]) DescribeTable() db.TableDescription {
	table := compiledTableOf[TableT, RecordT]()
	keyNames := []string{}
	for _, keyColumn := range table.Keys {
		if keyColumn != nil && !keyColumn.IsNil() {
			keyNames = append(keyNames, keyColumn.GetName())
		}
	}
	queryShapes := []string{}
	for _, capability := range table.capabilities {
		if !slices.Contains(queryShapes, capability.Signature) {
			queryShapes = append(queryShapes, capability.Signature)
		}
	}
	slices.Sort(queryShapes)
	partitionColumn := table.GetPartKey()
	partitionName := ""
	if partitionColumn != nil && !partitionColumn.IsNil() {
		partitionName = partitionColumn.GetName()
	}
	return db.TableDescription{
		Name: table.Name, Namespace: table.Namespace, ID: table.ID, Partition: partitionName, Keys: keyNames,
		Columns: db.DescribeColumns(table.Columns, table.Keys, partitionColumn,
			[]db.IColInfo{table.CreatedCol, table.UpdatedCol, table.VersionCol}, reflect.TypeFor[RecordT]()),
		QueryShapes: queryShapes,
		CacheByIDs:  table.CacheByIDs,
	}
}

// QueryRecordsJSON runs a read described with strings and renders the rows with the record's json
// tags, the shape InsertRecordsJSON and UpdateRecordsJSON accept back.
func (controller *ObController[RecordT, TableT]) QueryRecordsJSON(spec db.QuerySpec) ([]byte, int, error) {
	table := compiledTableOf[TableT, RecordT]()
	tableInfo := &db.TableInfo{Limit: spec.Limit, AllowFilter: spec.AllowFilter}
	if err := db.ApplyFilters(tableInfo, table, spec.Filters); err != nil {
		return nil, 0, err
	}
	if len(spec.Columns) > 0 {
		projection, err := db.ColnsByName(table, spec.Columns)
		if err != nil {
			return nil, 0, err
		}
		for _, column := range projection {
			tableInfo.ColumnsInclude = append(tableInfo.ColumnsInclude, column.GetInfo())
		}
	}
	if spec.OrderDesc {
		tableInfo.SetOrderDesc()
	}
	records := []RecordT{}
	if err := selectRecords(table, tableInfo, makeRecordSink(&records, nil)); err != nil {
		return nil, 0, err
	}
	payload, err := json.Marshal(records)
	if err != nil {
		return nil, 0, fmt.Errorf("obdriver: rendering the records of %q: %w", table.Name, err)
	}
	return payload, len(records), nil
}

// InsertRecordsJSON inserts a JSON array of records through the managed write.
func (controller *ObController[RecordT, TableT]) InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error) {
	table := compiledTableOf[TableT, RecordT]()
	records, err := parseRecordsJSON[RecordT](table, payload)
	if err != nil {
		return 0, err
	}
	excluded, err := db.ColnsByName(table, columnsToExclude)
	if err != nil {
		return 0, err
	}
	if err := writeRecords(table, []writePhase{insertPhase(db.MakeRecordSlice(&records), excluded)}); err != nil {
		return 0, err
	}
	return len(records), nil
}

// UpdateRecordsJSON updates only columnsToInclude on every record of the JSON array. The list is
// required: defaulting it to every column would let a partial payload blank out fields.
func (controller *ObController[RecordT, TableT]) UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error) {
	table := compiledTableOf[TableT, RecordT]()
	if len(columnsToInclude) == 0 {
		return 0, fmt.Errorf("obdriver: an update of %q needs the columns it writes", table.Name)
	}
	records, err := parseRecordsJSON[RecordT](table, payload)
	if err != nil {
		return 0, err
	}
	included, err := db.ColnsByName(table, columnsToInclude)
	if err != nil {
		return 0, err
	}
	if err := writeRecords(table, []writePhase{updateIncludePhase(db.MakeRecordSlice(&records), included)}); err != nil {
		return 0, err
	}
	return len(records), nil
}

// parseRecordsJSON decodes an HTTP payload into the table's records; an empty array is refused.
func parseRecordsJSON[RecordT any](table *ObTable, payload []byte) ([]RecordT, error) {
	records := []RecordT{}
	if err := json.Unmarshal(payload, &records); err != nil {
		return nil, fmt.Errorf("obdriver: parsing the records of %q: %w", table.Name, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("obdriver: no records sent for %q", table.Name)
	}
	return records, nil
}

// readPartitionRecords reads one partition (every record when partValue is 0) into a reflect-built
// []recordType, from lastKey on when it is set.
func (table *ObTable) readPartitionRecords(recordType reflect.Type, partValue int32, lastKey any) (reflect.Value, error) {
	tableInfo := &db.TableInfo{}
	if partitionColumn := table.GetPartKey(); partValue > 0 && partitionColumn != nil && !partitionColumn.IsNil() {
		tableInfo.SetWhere(partitionColumn.GetName(), "=", partValue)
	}
	if lastKey != nil && len(table.Keys) > 0 {
		tableInfo.SetWhere(table.Keys[0].GetName(), ">=", lastKey)
	}
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	recordsPointer.Elem().Set(reflect.MakeSlice(recordsPointer.Elem().Type(), 0, 0))
	return recordsPointer, selectRecords(table, tableInfo, makeReflectRecordSink(recordsPointer))
}
