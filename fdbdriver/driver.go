// Package fdbdriver is the FoundationDB implementation of db.Executor. It never links libfdb_c: every
// operation is a frame to the fdbgate daemon (genix-orm/fdb holds the wire client and the daemon).
//
// Storage model (docs/FOUNDATIONDB_PLAN.md §6): one key per record holding every stored column (the
// column codec, rowcodec.go), and index entries written in the same commit as the row, so there is
// no view lag. A TypeDelta entry holds a copy of the row, so a sync is one range read; the other index
// kinds keep no entries yet (reads scan the partition and filter exactly).
//
// It lives in the root genix-orm module, beside the scylla driver, and not in the fdb module: that
// module also builds the cgo daemon from its own folder, which must not need the ORM's sources.
package fdbdriver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ivanjoz/genix-orm/dataframe"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/fdb"
)

// errNotImplemented marks the db.Executor operations this driver does not serve yet. Each one fails
// loudly instead of answering wrong: docs/FOUNDATIONDB_PLAN.md §10 lists what parity still needs.
func errNotImplemented(operation string) error {
	return fmt.Errorf("fdbdriver: %s is not implemented on FoundationDB yet", operation)
}

// ConnParams is what Connect needs: the daemon's socket and the namespace tables default to.
type ConnParams struct {
	SocketPath string
	// Keyspace is the default namespace, the first component of every key.
	Keyspace string
	PoolSize int
}

var (
	connParams     ConnParams
	clientMutex    sync.Mutex
	connectedGate  *fdb.Client
	requestTimeout = 30 * time.Second
)

// SetConnection records the parameters; the socket is dialed on the first operation.
func SetConnection(params ConnParams) {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	connParams = params
	if connectedGate != nil {
		connectedGate.Close()
		connectedGate = nil
	}
}

// Keyspace is the default namespace Connect received.
func Keyspace() string { return connParams.Keyspace }

// gateClient dials the daemon once and returns the shared client.
func gateClient() (*fdb.Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if connectedGate != nil {
		return connectedGate, nil
	}
	if connParams.SocketPath == "" {
		return nil, errors.New("fdbdriver: no fdbgate socket configured (SetConnection)")
	}
	client, err := fdb.Dial(connParams.SocketPath, connParams.PoolSize)
	if err != nil {
		return nil, fmt.Errorf("fdbdriver: dialing fdbgate at %s: %w", connParams.SocketPath, err)
	}
	connectedGate = client
	return client, nil
}

func requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), requestTimeout)
}

// Exec is the FoundationDB driver. Zero-size, like scylla.Exec: per-table state lives in the
// compiled-table cache.
type Exec[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] struct{}

func (Exec[TableT, RecordT]) Name() string { return "fdb" }

func (Exec[TableT, RecordT]) Select(
	schema *TableT, tableInfo *db.TableInfo, dst *[]RecordT, scan func(record *RecordT) bool,
) error {
	return selectRecords(getOrCompileFdbTable(schema), tableInfo, makeRecordSink(dst, scan))
}

func (Exec[TableT, RecordT]) SelectGrouped(schema *TableT, tableInfo *db.TableInfo) error {
	return errNotImplemented("SelectGrouped (cache groups)")
}

func (Exec[TableT, RecordT]) Insert(records *[]RecordT, columnsToExclude ...db.Coln) error {
	return writeRecords(compiledTableOf[TableT, RecordT](), []writePhase{insertPhase(records, columnsToExclude)})
}

func (Exec[TableT, RecordT]) Update(records *[]RecordT, columnsToInclude ...db.Coln) error {
	table := compiledTableOf[TableT, RecordT]()
	return writeRecords(table, []writePhase{updateIncludePhase(table, records, columnsToInclude)})
}

func (Exec[TableT, RecordT]) UpdateExclude(records *[]RecordT, columnsToExclude ...db.Coln) error {
	table := compiledTableOf[TableT, RecordT]()
	return writeRecords(table, []writePhase{updateExcludePhase(table, records, columnsToExclude)})
}

func (Exec[TableT, RecordT]) InsertUpdate(
	recordsForInsert, recordsForUpdate *[]RecordT,
	columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	table := compiledTableOf[TableT, RecordT]()
	return writeRecords(table, []writePhase{
		insertPhase(recordsForInsert, columnsToExcludeInsert),
		updateIncludePhase(table, recordsForUpdate, columnsToIncludeUpdate),
	})
}

func (Exec[TableT, RecordT]) InsertUpdateInclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToIncludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	table := compiledTableOf[TableT, RecordT]()
	toInsert, toUpdate := splitByIsInsert(records, isInsert)
	return writeRecords(table, []writePhase{
		insertPhase(&toInsert, columnsToExcludeInsert),
		updateIncludePhase(table, &toUpdate, columnsToIncludeUpdate),
	})
}

func (Exec[TableT, RecordT]) InsertUpdateExclude(
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToExcludeUpdate []db.Coln, columnsToExcludeInsert ...db.Coln,
) error {
	table := compiledTableOf[TableT, RecordT]()
	toInsert, toUpdate := splitByIsInsert(records, isInsert)
	return writeRecords(table, []writePhase{
		insertPhase(&toInsert, columnsToExcludeInsert),
		updateExcludePhase(table, &toUpdate, columnsToExcludeUpdate),
	})
}

func (Exec[TableT, RecordT]) Merge(
	records *[]RecordT, columnsToExcludeUpdate []db.Coln,
	onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT),
) error {
	return errNotImplemented("Merge")
}

func (Exec[TableT, RecordT]) QueryCachedIDs(request cache.CachedIDsRequest) ([]cache.CachedPartition[RecordT], error) {
	return nil, errNotImplemented("QueryCachedIDs (by-IDs cache)")
}

func (Exec[TableT, RecordT]) SearchTextIDs(partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	return nil, errNotImplemented("SearchTextIDs")
}

func (Exec[TableT, RecordT]) SearchText(
	refSlice *[]RecordT, partition int32, query string, limit, offset int,
) ([]db.TextHit, error) {
	return nil, errNotImplemented("SearchText")
}

func (Exec[TableT, RecordT]) QueryFrame(request db.FrameRequest) ([]db.FrameRow[RecordT], error) {
	return nil, errNotImplemented("QueryFrame (DataFrames)")
}

func (Exec[TableT, RecordT]) QueryFrameRows(request db.FrameRequest) ([]dataframe.Row, *dataframe.Frame, error) {
	return nil, nil, errNotImplemented("QueryFrameRows (DataFrames)")
}

func (Exec[TableT, RecordT]) QueryFrameChanged(request db.FrameRequest, heldGeneration, heldFiles string) ([]byte, error) {
	return nil, errNotImplemented("QueryFrameChanged (DataFrames)")
}

func (Exec[TableT, RecordT]) CompileTable(schema *TableT) db.Table {
	return getOrCompileFdbTable(schema)
}

// TableStruct is what tables embed to default to this driver — the same shape as scylla.TableStruct,
// so a project switches drivers by changing one alias.
type TableStruct[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]] = db.TableStruct[Exec[TableT, RecordT], TableT, RecordT]

func init() {
	db.DBTypeResolver = func(typeID int8) string { return fdbTypeNames[typeID] }
	db.SetCodec(fdbValueCodec{})
	db.GetAutoincrementID = GetAutoincrementID
	db.QueryCachedGenericByIDs = func(tableName string, request cache.CachedIDsRequest) ([]cache.CachedPartition[db.GenericRecord], error) {
		return nil, errNotImplemented("QueryCachedGenericByIDs")
	}
	db.SetDebugLogging = func(level int) { db.DebugLevel = level }
}
