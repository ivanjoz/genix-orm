package fdbdriver

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/fdb"
)

// Sequences (decision D3): FoundationDB owns them. A reservation is one serializable read+write in
// the daemon (fdbgate ReserveRange), so two writers can never be handed the same range — the race
// that makes the Scylla build route every reservation through fareward does not exist here.

func reserveCounter(namespace, counterName string, count int) (int64, error) {
	gate, err := gateClient()
	if err != nil {
		return 0, err
	}
	ctx, cancel := requestContext()
	defer cancel()
	return gate.ReserveRange(ctx, sequenceKey(namespace, counterName), uint32(max(count, 1)))
}

// GetAutoincrementID reserves recordsSize consecutive values of an arbitrary counter and returns the
// first one (db.GetAutoincrementID).
func GetAutoincrementID(counterName string, recordsSize int) (int64, error) {
	return reserveCounter(connParams.Keyspace, counterName, recordsSize)
}

// SetCounter moves a counter to an absolute value and returns what it held.
func SetCounter(counterName string, value int64) (int64, error) {
	gate, err := gateClient()
	if err != nil {
		return 0, err
	}
	ctx, cancel := requestContext()
	defer cancel()
	return gate.SetCounter(ctx, sequenceKey(connParams.Keyspace, counterName), value)
}

// ListCounters returns the name of every counter of the default namespace.
func ListCounters() ([]string, error) {
	gate, err := gateClient()
	if err != nil {
		return nil, err
	}
	prefix := sequenceKey(connParams.Keyspace, "")
	readRange := fdb.Range{Begin: prefix, End: prefixEnd(prefix)}
	var counterNames []string
	for {
		readRange.Limit = readPageRecords
		ctx, cancel := requestContext()
		results, err := gate.GetRanges(ctx, []fdb.Range{readRange})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, counterKey := range results[0].Keys.Slices() {
			counterNames = append(counterNames, string(counterKey[len(prefix):]))
		}
		if !results[0].More {
			return counterNames, nil
		}
		readRange.Begin = results[0].NextBegin
	}
}

// WipePartition deletes one partition of a table: its records, index entries and delta moves, which
// all share the partition's key prefix.
func WipePartition(namespace string, tableID int16, partition int64) error {
	gate, err := gateClient()
	if err != nil {
		return err
	}
	if namespace == "" {
		namespace = connParams.Keyspace
	}
	table := &FdbTable{TableCore: db.TableCore{Namespace: namespace, ID: tableID}}
	ctx, cancel := requestContext()
	defer cancel()
	return gate.ClearPrefix(ctx, table.partitionPrefix(partition))
}

// Ping checks the daemon answers.
func Ping() error {
	gate, err := gateClient()
	if err != nil {
		return err
	}
	ctx, cancel := requestContext()
	defer cancel()
	_, err = gate.Ping(ctx)
	return err
}

// FdbController is the db.Controller of one table. It serves the name-addressed data surface (the
// fn-db console, the database-records skill) and counters; backup, rebuilds, text search and
// DataFrame maintenance are not implemented yet and say so.
type FdbController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]] struct {
	Schema db.TableSchema
}

func (controller *FdbController[RecordT, TableT]) table() *FdbTable {
	return compiledTableOf[TableT, RecordT]()
}

func (controller *FdbController[RecordT, TableT]) GetTable() db.Table { return controller.table() }

func (controller *FdbController[RecordT, TableT]) GetTableName() string {
	return controller.table().GetFullName()
}

func (controller *FdbController[RecordT, TableT]) GetRecords(partValue, limit int32, lastKey any) []any {
	panic(errNotImplemented("Controller.GetRecords"))
}

func (controller *FdbController[RecordT, TableT]) GetRecordsGob(partValue, limit int32, lastKey any) ([]byte, error) {
	return nil, errNotImplemented("Controller.GetRecordsGob")
}

func (controller *FdbController[RecordT, TableT]) ExportRecordsColbin(partValue int32, batchSize int, emitBatch func(encoded []byte, rowsCount int32) error) (int32, error) {
	return 0, errNotImplemented("Controller.ExportRecordsColbin")
}

func (controller *FdbController[RecordT, TableT]) RestoreRecordsColbin(partValue int32, encoded []byte, deletePartitionFirst bool) (int, error) {
	return 0, errNotImplemented("Controller.RestoreRecordsColbin")
}

func (controller *FdbController[RecordT, TableT]) ReloadRecords(partValue int32) error {
	return errNotImplemented("Controller.ReloadRecords")
}

func (controller *FdbController[RecordT, TableT]) RecalcVirtualColumns(partValue int32) error {
	return errNotImplemented("Controller.RecalcVirtualColumns")
}

func (controller *FdbController[RecordT, TableT]) RecalcGroupIndexHashes(partValue int32) error {
	return errNotImplemented("Controller.RecalcGroupIndexHashes")
}

func (controller *FdbController[RecordT, TableT]) ResetCounter(partValue any) error {
	return errNotImplemented("Controller.ResetCounter")
}

// DeleteViewsAndIndexes has nothing to drop: there are no server-side views or indexes.
func (controller *FdbController[RecordT, TableT]) DeleteViewsAndIndexes() error { return nil }

func (controller *FdbController[RecordT, TableT]) ReindexTextSearch(partValue int32) error {
	return errNotImplemented("Controller.ReindexTextSearch")
}

func (controller *FdbController[RecordT, TableT]) ReindexTextSearchAll() error {
	return errNotImplemented("Controller.ReindexTextSearchAll")
}

func (controller *FdbController[RecordT, TableT]) CompactTextSearch(partValue int32) error {
	return errNotImplemented("Controller.CompactTextSearch")
}

func (controller *FdbController[RecordT, TableT]) DropTextSearch(partValue int32) error {
	return errNotImplemented("Controller.DropTextSearch")
}

func (controller *FdbController[RecordT, TableT]) CompactDataFrames(partValue int32) error {
	return errNotImplemented("Controller.CompactDataFrames")
}

func (controller *FdbController[RecordT, TableT]) RebuildDataFramesPartition(partValue int32) error {
	return errNotImplemented("Controller.RebuildDataFramesPartition")
}

func (controller *FdbController[RecordT, TableT]) DescribeTable() db.TableDescription {
	table := controller.table()
	keyNames := []string{}
	for _, keyColumn := range table.Keys {
		keyNames = append(keyNames, keyColumn.GetName())
	}
	partitionName := ""
	if table.PartKey != nil && !table.PartKey.IsNil() {
		partitionName = table.PartKey.GetName()
	}
	return db.TableDescription{
		Name: table.Name, Namespace: table.Namespace, ID: table.ID, Partition: partitionName, Keys: keyNames,
		Columns: db.DescribeColumns(table.Columns, table.Keys, table.PartKey,
			[]db.IColInfo{table.CreatedCol, table.UpdatedCol, table.VersionCol}, reflect.TypeFor[RecordT]()),
		// Any predicate runs: a read without a usable index scans the partition and filters exactly.
		QueryShapes: []string{partitionName + " + any predicate"},
		CacheByIDs:  table.CacheByIDs,
	}
}

func (controller *FdbController[RecordT, TableT]) QueryRecordsJSON(spec db.QuerySpec) ([]byte, int, error) {
	table := controller.table()
	tableInfo := &db.TableInfo{Limit: spec.Limit, AllowFilter: spec.AllowFilter}
	if err := db.ApplyFilters(tableInfo, table, spec.Filters); err != nil {
		return nil, 0, err
	}
	for _, columnName := range spec.Columns {
		column, err := db.ResolveColumn(table, columnName)
		if err != nil {
			return nil, 0, err
		}
		tableInfo.ColumnsInclude = append(tableInfo.ColumnsInclude, *column.(*db.ColumnInfo))
	}
	records := []RecordT{}
	if err := selectRecords(table, tableInfo, makeRecordSink(&records, nil)); err != nil {
		return nil, 0, err
	}
	payload, err := json.Marshal(records)
	return payload, len(records), err
}

func (controller *FdbController[RecordT, TableT]) InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error) {
	records := []RecordT{}
	if err := json.Unmarshal(payload, &records); err != nil {
		return 0, fmt.Errorf("table %q: %w", controller.Schema.Name, err)
	}
	excluded, err := db.ColnsByName(controller.table(), columnsToExclude)
	if err != nil {
		return 0, err
	}
	return len(records), Exec[TableT, RecordT]{}.Insert(&records, excluded...)
}

func (controller *FdbController[RecordT, TableT]) UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error) {
	if len(columnsToInclude) == 0 {
		return 0, fmt.Errorf("table %q: an update names the columns it writes", controller.Schema.Name)
	}
	records := []RecordT{}
	if err := json.Unmarshal(payload, &records); err != nil {
		return 0, fmt.Errorf("table %q: %w", controller.Schema.Name, err)
	}
	included, err := db.ColnsByName(controller.table(), columnsToInclude)
	if err != nil {
		return 0, err
	}
	return len(records), Exec[TableT, RecordT]{}.Update(&records, included...)
}
