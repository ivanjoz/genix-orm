package pgdriver

import (
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// Exec is the PostgreSQL driver. Zero-size, like scylla.Exec: per-table state lives in the
// compiled-table cache.
type Exec[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] struct{}

func (Exec[TableT, RecordT]) Name() string { return "postgres" }

func (Exec[TableT, RecordT]) Select(schema *TableT, tableInfo *db.TableInfo, dst *[]RecordT, scan func(record *RecordT) bool) error {
	return selectRecords(getOrCompilePgTable(schema), tableInfo, makeRecordSink(dst, scan))
}

// SelectGrouped is a cache-group read (grouped.go): the groups land in tableInfo.RefSlice.
func (Exec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *db.TableInfo) error {
	refGroups := tableInfo.RefSlice.(*[]db.RecordGroup[RecordT])
	*refGroups = (*refGroups)[:0]
	var unusedRecords []RecordT
	erasedGroups, err := getOrCompilePgTable(schema).queryIndexGroups(tableInfo, makeRecordSink(&unusedRecords, nil))
	for _, erasedGroup := range erasedGroups {
		records := make([]RecordT, len(erasedGroup.Records))
		for recordIndex, recordPointer := range erasedGroup.Records {
			records[recordIndex] = *(*RecordT)(recordPointer)
		}
		*refGroups = append(*refGroups, db.RecordGroup[RecordT]{
			IndexID: erasedGroup.IndexID, GroupHash: erasedGroup.GroupHash, IndexGroupValues: erasedGroup.IndexGroupValues,
			Records: records, LastWrite: erasedGroup.LastWrite,
		})
	}
	return err
}

func (Exec[TableT, RecordT]) Insert(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{insertPhase(db.MakeRecordSlice(records), columnsToExclude)})
}

// InsertNew writes records whose keys the caller promises are new (db.Insert): no read before
// writing, and a key already stored fails the write.
func (Exec[TableT, RecordT]) InsertNew(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{insertNewPhase(db.MakeRecordSlice(records), columnsToExclude)})
}

// LockByKeys replaces records with their stored versions, read by primary key and locked (FOR
// UPDATE) inside the transaction of db.RunTransaction. A record with nothing stored is dropped.
func (Exec[TableT, RecordT]) LockByKeys(driverTransaction any, records *[]RecordT) error {
	table := compiledTableOf[TableT, RecordT]()
	recordSlice := db.MakeRecordSlice(records)
	if recordSlice.Len() == 0 {
		return nil
	}
	readColumns := make([]db.IColInfo, 0, len(table.storedColumns))
	for _, stored := range table.storedColumns {
		readColumns = append(readColumns, stored.column)
	}
	storedByKey, err := table.lockStoredRecords(driverTransaction.(*writeTransaction), writePhase{records: recordSlice}, 0, recordSlice.Len(), readColumns)
	if err != nil {
		return err
	}
	driverTransaction.(*writeTransaction).rememberLocked(table, storedByKey)
	lockedRecords := make([]RecordT, 0, len(storedByKey))
	for recordIndex := range *records {
		if storedRecord, isStored := storedByKey[table.primaryKeyToken(unsafe.Pointer(&(*records)[recordIndex]))]; isStored {
			lockedRecords = append(lockedRecords, *(*RecordT)(storedRecord))
		}
	}
	*records = lockedRecords
	return nil
}

// InsertIn is db.Insert inside the transaction of db.RunTransaction.
func (Exec[TableT, RecordT]) InsertIn(driverTransaction any, records *[]RecordT, columnsToExclude ...db.Coln) (func() error, error) {
	return writeRecordsIn(driverTransaction.(*writeTransaction), compiledTableOf[TableT, RecordT](), []writePhase{insertNewPhase(db.MakeRecordSlice(records), columnsToExclude)})
}

// UpdateIn is db.Update inside the transaction of db.RunTransaction.
func (Exec[TableT, RecordT]) UpdateIn(driverTransaction any, records *[]RecordT, columnsToInclude ...db.Coln) (func() error, error) {
	return writeRecordsIn(driverTransaction.(*writeTransaction), compiledTableOf[TableT, RecordT](), []writePhase{updateIncludePhase(db.MakeRecordSlice(records), columnsToInclude)})
}

// UpsertIn is db.Upsert inside the transaction of db.RunTransaction.
func (Exec[TableT, RecordT]) UpsertIn(driverTransaction any, records *[]RecordT, columnsToExclude ...db.Coln) (func() error, error) {
	return writeRecordsIn(driverTransaction.(*writeTransaction), compiledTableOf[TableT, RecordT](), []writePhase{insertPhase(db.MakeRecordSlice(records), columnsToExclude)})
}

// InsertUpdateIncludeIn is db.InsertUpdateInclude inside the transaction of db.RunTransaction.
func (Exec[TableT, RecordT]) InsertUpdateIncludeIn(driverTransaction any, records *[]RecordT, isInsert func(record *RecordT) bool, columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) (func() error, error) {
	toInsert, toUpdate := db.SplitInsertAndUpdateRecords(db.MakeRecordSlice(records), erasedIsInsert(isInsert))
	return writeRecordsIn(driverTransaction.(*writeTransaction), compiledTableOf[TableT, RecordT](), []writePhase{
		insertPhase(toInsert, columnsToExcludeInsert),
		updateIncludePhase(toUpdate, columnsToIncludeUpdate),
	})
}

func (Exec[TableT, RecordT]) Update(records *[]RecordT, columnsToInclude ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{updateIncludePhase(db.MakeRecordSlice(records), columnsToInclude)})
}

func (Exec[TableT, RecordT]) UpdateExclude(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{updateExcludePhase(db.MakeRecordSlice(records), columnsToExclude)})
}

func (Exec[TableT, RecordT]) InsertUpdate(recordsForInsert, recordsForUpdate *[]RecordT, columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{
		insertPhase(db.MakeRecordSlice(recordsForInsert), columnsToExcludeInsert),
		updateIncludePhase(db.MakeRecordSlice(recordsForUpdate), columnsToIncludeUpdate),
	})
}

func (Exec[TableT, RecordT]) InsertUpdateInclude(records *[]RecordT, isInsert func(record *RecordT) bool, columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	toInsert, toUpdate := db.SplitInsertAndUpdateRecords(db.MakeRecordSlice(records), erasedIsInsert(isInsert))
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{
		insertPhase(toInsert, columnsToExcludeInsert),
		updateIncludePhase(toUpdate, columnsToIncludeUpdate),
	})
}

func (Exec[TableT, RecordT]) InsertUpdateExclude(records *[]RecordT, isInsert func(record *RecordT) bool, columnsToExcludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln) error {
	toInsert, toUpdate := db.SplitInsertAndUpdateRecords(db.MakeRecordSlice(records), erasedIsInsert(isInsert))
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{
		insertPhase(toInsert, columnsToExcludeInsert),
		updateExcludePhase(toUpdate, columnsToExcludeUpdate),
	})
}

// erasedIsInsert adapts the caller's typed classifier to records by pointer.
func erasedIsInsert[RecordT any](isInsert func(record *RecordT) bool) func(unsafe.Pointer) bool {
	return func(record unsafe.Pointer) bool { return isInsert((*RecordT)(record)) }
}

func (Exec[TableT, RecordT]) Merge(records *[]RecordT, columnsToExcludeUpdate []db.Coln, onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT)) error {
	if onUpdate == nil || onInsert == nil {
		panic("Merge requires non-nil onUpdateHandler and onInsertHandler")
	}
	if records == nil || len(*records) == 0 {
		return nil
	}
	return mergeRecords(compiledTableOf[TableT, RecordT](), db.MakeRecordSlice(records), columnsToExcludeUpdate,
		func(previous, current unsafe.Pointer) bool {
			return onUpdate((*RecordT)(previous), (*RecordT)(current))
		},
		func(record unsafe.Pointer) { onInsert((*RecordT)(record)) })
}

func (Exec[TableT, RecordT]) QueryCachedIDs(request cache.CachedIDsRequest) ([]cache.CachedPartition[RecordT], error) {
	var unusedRecords []RecordT
	partitionsRead, err := compiledTableOf[TableT, RecordT]().queryCachedIDs(request, makeRecordSink(&unusedRecords, nil))
	if err != nil || partitionsRead == nil {
		return nil, err
	}
	partitions := make([]cache.CachedPartition[RecordT], len(partitionsRead))
	for partitionIndex, partitionRead := range partitionsRead {
		partitions[partitionIndex].Partition = partitionRead.partition
		partitions[partitionIndex].Records = make([]RecordT, len(partitionRead.records))
		for recordIndex, record := range partitionRead.records {
			partitions[partitionIndex].Records[recordIndex] = *(*RecordT)(record)
		}
	}
	return partitions, nil
}

func (Exec[TableT, RecordT]) SearchTextIDs(partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	return compiledTableOf[TableT, RecordT]().searchTextIDs(partition, query, limit, offset)
}

// SearchText reads the records of the hits into refSlice, best first.
func (Exec[TableT, RecordT]) SearchText(refSlice *[]RecordT, partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	table := compiledTableOf[TableT, RecordT]()
	hits, err := table.searchTextIDs(partition, query, limit, offset)
	if err != nil {
		return nil, err
	}
	return hits, table.appendTextSearchRecords(makeRecordSink(refSlice, nil), partition, hits)
}

func (Exec[TableT, RecordT]) QueryFrame(request db.FrameRequest) ([]db.FrameRow[RecordT], error) {
	table := compiledTableOf[TableT, RecordT]()
	rows, frame, err := table.readFrameRows(request)
	if err != nil {
		return nil, err
	}
	frameRows := make([]db.FrameRow[RecordT], len(rows))
	for rowIndex, row := range rows {
		frameRows[rowIndex] = db.FrameRow[RecordT]{Sums: row.Sums, Frame: frame}
		table.setFrameRowKey(frame, unsafe.Pointer(&frameRows[rowIndex].Key), row)
	}
	return frameRows, nil
}

func (Exec[TableT, RecordT]) QueryFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	return compiledTableOf[TableT, RecordT]().readFrameRows(request)
}

func (Exec[TableT, RecordT]) QueryFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	return compiledTableOf[TableT, RecordT]().readFrameChanged(request, heldGeneration, heldFiles)
}

func (Exec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	return getOrCompilePgTable(schema)
}

// TableStruct is what tables embed to default to this driver: the same shape as scylla.TableStruct,
// so a project switches drivers by changing one alias (backend/db/driver_oceanbase.go).
type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]

func init() {
	db.DBTypeResolver = func(typeID int8) string { return pgTypeNames[typeID] }
	db.SetCodec(pgValueCodec{})
	db.GetAutoincrementID = GetAutoincrementID
	db.QueryCachedGenericByIDs = func(tableName string, request cache.CachedIDsRequest) ([]cache.CachedPartition[db.GenericRecord], error) {
		return nil, errNotImplemented("QueryCachedGenericByIDs")
	}
	db.SetDebugLogging = func(level int) { db.DebugLevel = level }
}
