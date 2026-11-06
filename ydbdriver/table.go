package ydbdriver

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/ivanjoz/genix-orm/db"
)

// YdbTable is a table compiled for YDB: the shared TableCore plus the YDB objects it materializes as
// (docs/YDB_PLAN.md §8): the main row table with its native async indexes, and the side tables the
// ORM maintains next to it.
type YdbTable struct {
	db.TableCore
	// path is `<keyspace>/<table>`, relative to the database.
	path string
	// mainTable is the main table and its GLOBAL ASYNC indexes (§8.1–§8.3).
	mainTable tableSpec
	// sideTables are the key-only tables written in the record's transaction (§8.4): slice indexes,
	// delta moves, cache-group marks and the DataFrame log.
	sideTables []tableSpec
}

type ydbTableCacheEntry struct {
	once  sync.Once
	table *YdbTable
}

var ydbTableCache sync.Map

// getOrCompileYdbTable compiles a bound table struct once per type. It takes `any` so one compiled
// body serves every table type, as the other drivers do.
func getOrCompileYdbTable(schemaStruct any) *YdbTable {
	schemaType := reflect.TypeOf(schemaStruct).Elem()
	cacheEntryAny, _ := ydbTableCache.LoadOrStore(schemaType, &ydbTableCacheEntry{})
	cacheEntry := cacheEntryAny.(*ydbTableCacheEntry)
	cacheEntry.once.Do(func() {
		cacheEntry.table = compileTable(schemaStruct.(db.TableHandle).GetSchema(), reflect.ValueOf(schemaStruct).Elem())
	})
	return cacheEntry.table
}

// compiledTableOf compiles (or returns) the table of a record type, for the paths that get no bound
// table struct.
func compiledTableOf[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]]() *YdbTable {
	return getOrCompileYdbTable(db.InitStructTable[TableT, RecordT](new(TableT)))
}

// compileTable turns a schema into a YdbTable. The table-level compile and the index descriptions
// are the shared ones (db), so every driver agrees on what a record and an index are; this driver
// only decides how each described index is stored (index_mapping.go) and checks YDB's limits.
func compileTable(schema db.TableSchema, structRefValue reflect.Value) *YdbTable {
	if schema.SequenceColumn != nil || schema.CounterColumn != nil {
		panic(fmt.Sprintf(`Table "%v": SequenceColumn / CounterColumn are not implemented on YDB yet`, schema.Name))
	}
	table := &YdbTable{}
	table.CompileFromSchema(schema, structRefValue)
	if table.Namespace == "" {
		table.Namespace = connParams.Keyspace
	}
	indexCount := int8(1)
	table.DescribeIndexes(&indexCount, schema.Indexes)
	table.FinalizeColumns()

	table.path = table.Namespace + "/" + table.Name
	table.mainTable, table.sideTables = mapTableObjects(table, len(schema.DataFrames) > 0)
	// D5: more than 20 native indexes is an error; there is no spill to side tables.
	assertNativeIndexLimit(table.path, table.mainTable.indexes)
	return table
}
