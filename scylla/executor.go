package scylla

import (
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// Exec is the ScyllaDB implementation of db.Executor. It is a zero-size type: all
// per-table state lives in the compiled ScyllaTable cache, so an Exec value costs
// nothing to create or copy.
//
// Every method keeps its type parameters, which is why these are thin wrappers over
// the existing generic implementations rather than a rewrite — no record type is
// erased anywhere on the path from Query() to the row scan.
type Exec[TableT TableSchemaInterface[TableT], RecordT TableBaseInterface[TableT, RecordT]] struct{}

func (Exec[TableT, RecordT]) Name() string { return "scylla" }

func (Exec[TableT, RecordT]) Select(
	schema *TableT, tableInfo *TableInfo, dst *[]RecordT, scan func(record *RecordT) bool,
) error {
	selectStartTime := time.Now()
	return selectExec(dst, tableInfo, getOrCompileScyllaTable(schema), scan, selectStartTime)
}

func (Exec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *TableInfo) error {
	return execIndexGroupQuery[TableT, RecordT](schema, tableInfo)
}

func (Exec[TableT, RecordT]) Insert(records *[]RecordT, columnsToExclude ...Coln) error {
	return Insert[RecordT, TableT](records, columnsToExclude...)
}

func (Exec[TableT, RecordT]) Update(records *[]RecordT, columnsToInclude ...Coln) error {
	return Update[RecordT, TableT](records, columnsToInclude...)
}

func (Exec[TableT, RecordT]) UpdateExclude(records *[]RecordT, columnsToExclude ...Coln) error {
	return UpdateExclude[RecordT, TableT](records, columnsToExclude...)
}

func (Exec[TableT, RecordT]) InsertUpdate(
	recordsForInsert, recordsForUpdate *[]RecordT,
	columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return InsertUpdate[RecordT, TableT](
		recordsForInsert, recordsForUpdate, columnsToIncludeUpdate, columnsToExcludeInsert...)
}

func (Exec[TableT, RecordT]) InsertUpdateInclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return InsertUpdateInclude[RecordT, TableT](
		records, isInsert, columnsToIncludeUpdate, columnsToExcludeInsert...)
}

func (Exec[TableT, RecordT]) InsertUpdateExclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToExcludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return InsertUpdateExclude[RecordT, TableT](
		records, isInsert, columnsToExcludeUpdate, columnsToExcludeInsert...)
}

func (Exec[TableT, RecordT]) Merge(
	records *[]RecordT, columnsToExcludeUpdate []Coln,
	onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT),
) error {
	return Merge[RecordT, TableT](records, columnsToExcludeUpdate, onUpdate, onInsert)
}

func (Exec[TableT, RecordT]) QueryCachedIDs(refSlice *[]RecordT, cachedIDs []IDCacheVersion) error {
	return QueryCachedIDs[RecordT, TableT](refSlice, cachedIDs)
}

func (Exec[TableT, RecordT]) SearchTextIDs(
	partition int32, query string, statusGroup int8, limit int,
) ([]db.IDWeight, error) {
	return SearchTextIDs[RecordT, TableT](partition, query, statusGroup, limit)
}

func (Exec[TableT, RecordT]) SearchText(
	refSlice *[]RecordT, partition int32, query string, statusGroup int8, limit int,
) ([]db.IDWeight, error) {
	return SearchText[RecordT, TableT](refSlice, partition, query, statusGroup, limit)
}

func (Exec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	return getOrCompileScyllaTable(schema)
}

// TableStruct is what tables embed. It supplies Exec as the table's default driver
// so declarations stay two-parameter — `TableStruct[ProductTable, Product]` — while
// every operation stays statically typed. Swapping a table to another database means
// embedding that driver's TableStruct instead.
type TableStruct[TableT TableSchemaInterface[TableT], RecordT TableBaseInterface[TableT, RecordT]] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]
