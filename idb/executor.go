// Package idb is the IndexedDB implementation of db.Executor, for a backend compiled to
// GOOS=js GOARCH=wasm and run inside the browser.
//
// It is a skeleton: every operation reports errNotImplemented. It exists so a build can name
// this driver in place of scylla and link without gocql; the storage itself comes next
// (see genix docs/WASM_LOCAL_MODE_PLAN.md, phase 2).
package idb

import (
	"errors"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

var errNotImplemented = errors.New("idb: the IndexedDB driver is not implemented yet")

// Exec is the IndexedDB driver. Zero-size, like scylla.Exec: per-table state will live in a
// compiled-table cache, never in the executor value.
type Exec[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] struct{}

func (Exec[TableT, RecordT]) Name() string { return "idb" }

func (Exec[TableT, RecordT]) Select(
	schema *TableT, tableInfo *db.TableInfo, dst *[]RecordT, scan func(record *RecordT) bool,
) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *db.TableInfo) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) Insert(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) Update(records *[]RecordT, columnsToInclude ...db.Coln) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) UpdateExclude(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) InsertUpdate(
	recordsForInsert, recordsForUpdate *[]RecordT,
	columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) InsertUpdateInclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) InsertUpdateExclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToExcludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) Merge(
	records *[]RecordT, columnsToExcludeUpdate []db.Coln,
	onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT),
) error {
	return errNotImplemented
}

func (Exec[TableT, RecordT]) QueryCachedIDs(request cache.CachedIDsRequest) ([]cache.CachedPartition[RecordT], error) {
	return nil, errNotImplemented
}

func (Exec[TableT, RecordT]) SearchTextIDs(partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	return nil, errNotImplemented
}

func (Exec[TableT, RecordT]) SearchText(
	refSlice *[]RecordT, partition int32, query string, limit, offset int,
) ([]db.TextHit, error) {
	return nil, errNotImplemented
}

func (Exec[TableT, RecordT]) QueryFrame(request db.FrameRequest) ([]db.FrameRow[RecordT], error) {
	return nil, errNotImplemented
}

func (Exec[TableT, RecordT]) QueryFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	return nil, nil, errNotImplemented
}

func (Exec[TableT, RecordT]) QueryFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	return nil, errNotImplemented
}

// CompileTable has no error return in the contract, and a nil Table would only fail later at a
// distance, so the skeleton stops right here instead.
func (Exec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	panic(errNotImplemented)
}

// TableStruct is what tables embed to default to this driver — the same shape as
// scylla.TableStruct, so a project switches drivers by changing one alias.
type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]

// The hooks keyed by name instead of table types; scylla installs its own in the same place.
func init() {
	db.GetAutoincrementID = func(key string, recordsSize int) (int64, error) { return 0, errNotImplemented }
	db.GetBlockAutoincrementID = db.GetAutoincrementID
	db.QueryCachedGenericByIDs = func(tableName string, request cache.CachedIDsRequest) ([]cache.CachedPartition[db.GenericRecord], error) {
		return nil, errNotImplemented
	}
	db.SetDebugLogging = func(level int) {}
}
