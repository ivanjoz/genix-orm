package ydbdriver

import "github.com/ivanjoz/genix-orm/db"

// YdbController is the db.Controller of one table (docs/YDB_PLAN.md §12). GetTable compiles it (Step
// 2); every other method answers errNotImplemented until its step lands.
type YdbController[RecordT db.TableBaseInterface[TableT, RecordT], TableT db.TableSchemaInterface[TableT]] struct {
	Schema db.TableSchema
}

func (controller *YdbController[RecordT, TableT]) GetTable() db.Table {
	return compiledTableOf[TableT, RecordT]()
}

func (controller *YdbController[RecordT, TableT]) GetTableName() string {
	return controller.Schema.Name
}

func (controller *YdbController[RecordT, TableT]) GetRecords(partValue, limit int32, lastKey any) []any {
	panic(errNotImplemented("Controller.GetRecords"))
}

func (controller *YdbController[RecordT, TableT]) GetRecordsGob(partValue, limit int32, lastKey any) ([]byte, error) {
	return nil, errNotImplemented("Controller.GetRecordsGob")
}

func (controller *YdbController[RecordT, TableT]) ExportRecordsColbin(partValue int32, batchSize int, emitBatch func(encoded []byte, rowsCount int32) error) (int32, error) {
	return 0, errNotImplemented("Controller.ExportRecordsColbin")
}

func (controller *YdbController[RecordT, TableT]) RestoreRecordsColbin(partValue int32, encoded []byte, deletePartitionFirst bool) (int, error) {
	return 0, errNotImplemented("Controller.RestoreRecordsColbin")
}

func (controller *YdbController[RecordT, TableT]) ReloadRecords(partValue int32) error {
	return errNotImplemented("Controller.ReloadRecords")
}

func (controller *YdbController[RecordT, TableT]) RecalcVirtualColumns(partValue int32) error {
	return errNotImplemented("Controller.RecalcVirtualColumns")
}

func (controller *YdbController[RecordT, TableT]) RecalcGroupIndexHashes(partValue int32) error {
	return errNotImplemented("Controller.RecalcGroupIndexHashes")
}

func (controller *YdbController[RecordT, TableT]) ResetCounter(partValue any) error {
	return errNotImplemented("Controller.ResetCounter")
}

func (controller *YdbController[RecordT, TableT]) DeleteViewsAndIndexes() error {
	return errNotImplemented("Controller.DeleteViewsAndIndexes")
}

func (controller *YdbController[RecordT, TableT]) ReindexTextSearch(partValue int32) error {
	return errNotImplemented("Controller.ReindexTextSearch")
}

func (controller *YdbController[RecordT, TableT]) ReindexTextSearchAll() error {
	return errNotImplemented("Controller.ReindexTextSearchAll")
}

func (controller *YdbController[RecordT, TableT]) CompactTextSearch(partValue int32) error {
	return errNotImplemented("Controller.CompactTextSearch")
}

func (controller *YdbController[RecordT, TableT]) DropTextSearch(partValue int32) error {
	return errNotImplemented("Controller.DropTextSearch")
}

func (controller *YdbController[RecordT, TableT]) CompactDataFrames(partValue int32) error {
	return errNotImplemented("Controller.CompactDataFrames")
}

func (controller *YdbController[RecordT, TableT]) RebuildDataFramesPartition(partValue int32) error {
	return errNotImplemented("Controller.RebuildDataFramesPartition")
}

func (controller *YdbController[RecordT, TableT]) DescribeTable() db.TableDescription {
	panic(errNotImplemented("Controller.DescribeTable"))
}

func (controller *YdbController[RecordT, TableT]) QueryRecordsJSON(spec db.QuerySpec) ([]byte, int, error) {
	return nil, 0, errNotImplemented("Controller.QueryRecordsJSON")
}

func (controller *YdbController[RecordT, TableT]) InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error) {
	return 0, errNotImplemented("Controller.InsertRecordsJSON")
}

func (controller *YdbController[RecordT, TableT]) UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error) {
	return 0, errNotImplemented("Controller.UpdateRecordsJSON")
}
