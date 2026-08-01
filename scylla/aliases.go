package scylla

import "github.com/ivanjoz/genix-orm/db"

// The schema, column and compiled-table vocabulary is shared with every other
// driver, so it lives in db. These aliases let this package's own code use the
// short names, and expose the handful of spellings that startup, deploy and the
// admin controller still reach for.
type (
	Coln            = db.Coln
	ColumnStatement = db.ColumnStatement
	TableInfo       = db.TableInfo
	TableSchema     = db.TableSchema
	Index           = db.Index
	FixedValues     = db.FixedValues

	GenericRecordSchema = db.GenericRecordSchema
	GenericRecord       = db.GenericRecord
	IDUpdatedVersion    = db.IDUpdatedVersion

	Table     = db.Table
	CSVResult = db.CSVResult
)

// RegisterTableFactory records how to compile one table by name, for endpoints that
// receive a table name at runtime.
var RegisterTableFactory = db.RegisterTableFactory

// ResolveTableByName compiles and returns the named table.
var ResolveTableByName = db.ResolveTableByName

type (
	TableSchemaInterface[T any]      = db.TableSchemaInterface[T]
	TableBaseInterface[T any, E any] = db.TableBaseInterface[T, E]
	TableInterface[T any]            = db.TableInterface[T]
	TableQueryInterface[T any]       = db.TableQueryInterface[T]
	RecordGroup[T any]               = db.RecordGroup[T]
)

type (
	Col[T TableInterface[T], E any]      = db.Col[T, E]
	ColSlice[T TableInterface[T], E any] = db.ColSlice[T, E]
)

// RecordOf constrains a record type to this driver. It is what generic helpers use
// when they need to build a Scylla controller or query for an arbitrary table.
type RecordOf[TableT TableSchemaInterface[TableT], RecordT TableBaseInterface[TableT, RecordT]] interface {
	db.RecordWithExecutor[TableT, RecordT, Exec[TableT, RecordT]]
}

const (
	// Bounds of the packed by-IDs cache key, re-exported so the driver's own packing code reads
	// against the same declaration the schema layer validates with.
	MaxTableID          = db.MaxTableID
	MaxCachePartitionID = db.MaxCachePartitionID

	TypeGlobalIndex    = db.TypeGlobalIndex
	TypeLocalIndex     = db.TypeLocalIndex
	TypeInheritFromKey = db.TypeInheritFromKey
	TypeView           = db.TypeView
	TypeViewTable      = db.TypeViewTable
	TypeDelta          = db.TypeDelta
)

// Cols returns columns as the slice required by schema declarations.
var Cols = db.Cols

// Key encoding and the text-search/grouped-read value types are part of the shared
// storage contract, so they live in db and are re-exported here.
type (
	IDWeight = db.IDWeight
)

var (
	MakeKeyConcat = db.MakeKeyConcat
)

// Generic functions cannot be aliased, so the shared query constructors are
// re-exported as thin wrappers until callers move to db directly.

// These pass straight through to db, forwarding the driver type parameter so it is
// still inferred from the record type at the call site.

// Query starts a read into refSlice and returns the table struct to build predicates on.
func Query[T db.RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D db.Executor[E, T]](refSlice *[]T) *E {
	return db.Query[T, E, D](refSlice)
}

// QueryIndexGroup starts a grouped read, bucketed by index-group hash.
func QueryIndexGroup[T db.RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D db.Executor[E, T]](refSlice *[]RecordGroup[T]) *E {
	return db.QueryIndexGroup[T, E, D](refSlice)
}

// TableOf returns a bound table struct for write operations, with no read destination.
func TableOf[T db.RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D db.Executor[E, T]]() *E {
	return db.TableOf[T, E, D]()
}
