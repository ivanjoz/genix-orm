package ydbdriver

import (
	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// Exec is the YDB driver. Zero-size, like scylla.Exec: per-table state lives in the compiled-table
// cache. Step 1 of docs/YDB_PLAN.md: every operation answers errNotImplemented until its step lands.
type Exec[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] struct{}

func (Exec[TableT, RecordT]) Name() string { return "ydb" }

func (Exec[TableT, RecordT]) Select(schema *TableT, tableInfo *db.TableInfo, dst *[]RecordT, scan func(record *RecordT) bool) error {
	return errNotImplemented("Select")
}

func (Exec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *db.TableInfo) error {
	return errNotImplemented("SelectGrouped")
}

func (Exec[TableT, RecordT]) Insert(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return errNotImplemented("Insert")
}

func (Exec[TableT, RecordT]) Update(records *[]RecordT, columnsToInclude ...db.Coln) error {
	return errNotImplemented("Update")
}

func (Exec[TableT, RecordT]) UpdateExclude(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return errNotImplemented("UpdateExclude")
}

func (Exec[TableT, RecordT]) InsertUpdate(recordsForInsert, recordsForUpdate *[]RecordT, columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	return errNotImplemented("InsertUpdate")
}

func (Exec[TableT, RecordT]) InsertUpdateInclude(records *[]RecordT, isInsert func(record *RecordT) bool, columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	return errNotImplemented("InsertUpdateInclude")
}

func (Exec[TableT, RecordT]) InsertUpdateExclude(records *[]RecordT, isInsert func(record *RecordT) bool, columnsToExcludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	return errNotImplemented("InsertUpdateExclude")
}

func (Exec[TableT, RecordT]) Merge(records *[]RecordT, columnsToExcludeUpdate []db.Coln, onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT)) error {
	return errNotImplemented("Merge")
}

func (Exec[TableT, RecordT]) QueryCachedIDs(request cache.CachedIDsRequest) ([]cache.CachedPartition[RecordT], error) {
	return nil, errNotImplemented("QueryCachedIDs")
}

func (Exec[TableT, RecordT]) SearchTextIDs(partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	return nil, errNotImplemented("SearchTextIDs")
}

func (Exec[TableT, RecordT]) SearchText(refSlice *[]RecordT, partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	return nil, errNotImplemented("SearchText")
}

func (Exec[TableT, RecordT]) QueryFrame(request db.FrameRequest) ([]db.FrameRow[RecordT], error) {
	return nil, errNotImplemented("QueryFrame")
}

func (Exec[TableT, RecordT]) QueryFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	return nil, nil, errNotImplemented("QueryFrameRows")
}

func (Exec[TableT, RecordT]) QueryFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	return nil, errNotImplemented("QueryFrameChanged")
}

func (Exec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	return getOrCompileYdbTable(schema)
}

// TableStruct is what tables embed to default to this driver: the same shape as scylla.TableStruct,
// so a project switches drivers by changing one alias (backend/db/driver_ydb.go).
type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]

func init() {
	db.DBTypeResolver = func(typeID int8) string { return ydbTypeNames[typeID] }
	db.SetCodec(ydbValueCodec{})
	db.GetAutoincrementID = GetAutoincrementID
	db.QueryCachedGenericByIDs = func(tableName string, request cache.CachedIDsRequest) ([]cache.CachedPartition[db.GenericRecord], error) {
		return nil, errNotImplemented("QueryCachedGenericByIDs")
	}
	db.SetDebugLogging = func(level int) { db.DebugLevel = level }
}
