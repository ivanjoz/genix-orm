package pgdriver

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/ivanjoz/genix-orm/db"
)

// PgTable is a table compiled for PostgreSQL: the shared TableCore plus the PostgreSQL objects it
// materializes as: the main table with its native indexes, and the side tables the ORM writes next
// to it (delta moves, cache-group marks, DataFrame log).
type PgTable struct {
	db.TableCore
	mainTable  tableSpec
	sideTables []tableSpec
	// storedColumns are the main table's columns in mainTable.columns order, with the ORM column each
	// one is written from and read into.
	storedColumns []storedColumn
	// primaryKeyColumns are the partition column (when the table has one), then the keys.
	primaryKeyColumns []db.IColInfo
	// dataFrames is nil for a table without DataFrames (data_frame.go).
	dataFrames *dataFrameInfo
	// capabilities feed the shared planner; routeByShape caches its route per query shape.
	capabilities []db.QueryCapability
	routeByShape sync.Map
}

// storedColumn is one column of the main table. isJSON marks a slice stored as a JSONB array for its
// GIN index; every other slice or complex value is colbin bytes.
type storedColumn struct {
	name   string
	column db.IColInfo
	isJSON bool
}

// hasField: a column the record struct exposes. Managed DB-only and generated columns have none.
func (stored storedColumn) hasField() bool {
	columnInfo, isColumnInfo := stored.column.(*db.ColumnInfo)
	return isColumnInfo && columnInfo.Field != nil && !columnInfo.IsVirtual
}

type pgTableCacheEntry struct {
	once  sync.Once
	table *PgTable
}

var pgTableCache sync.Map

// getOrCompilePgTable compiles a bound table struct once per type. It takes `any` so one compiled
// body serves every table type, as the other drivers do.
func getOrCompilePgTable(schemaStruct any) *PgTable {
	schemaType := reflect.TypeOf(schemaStruct).Elem()
	cacheEntryAny, _ := pgTableCache.LoadOrStore(schemaType, &pgTableCacheEntry{})
	cacheEntry := cacheEntryAny.(*pgTableCacheEntry)
	cacheEntry.once.Do(func() {
		cacheEntry.table = compileTable(schemaStruct.(db.TableHandle).GetSchema(), reflect.ValueOf(schemaStruct).Elem())
	})
	return cacheEntry.table
}

// compiledTableOf compiles (or returns) the table of a record type, for the paths that get no bound
// table struct.
func compiledTableOf[TableT db.TableSchemaInterface[TableT], RecordT db.TableBaseInterface[TableT, RecordT]]() *PgTable {
	return getOrCompilePgTable(db.InitStructTable[TableT, RecordT](new(TableT)))
}

// compileTable turns a schema into an PgTable. The table-level compile and the index descriptions
// are the shared ones (db), so every driver agrees on what a record and an index are; this driver
// only decides how each described index is stored (index_mapping.go).
func compileTable(schema db.TableSchema, structRefValue reflect.Value) *PgTable {
	if schema.SequenceColumn != nil || schema.CounterColumn != nil {
		panic(fmt.Sprintf(`Table "%v": SequenceColumn / CounterColumn are not implemented on PostgreSQL yet`, schema.Name))
	}
	table := &PgTable{}
	table.CompileFromSchema(schema, structRefValue)
	if table.Namespace == "" {
		table.Namespace = connParams.Database
	}
	indexCount := int8(1)
	table.DescribeIndexes(&indexCount, schema.Indexes)
	table.FinalizeColumns()
	configureDataFrames(table, schema, structRefValue)
	table.mainTable, table.sideTables = mapTableObjects(table)

	// Every stored column resolves to a declared column or to the generated column an index keys on.
	generatedByName := map[string]db.IColInfo{}
	for _, source := range table.IndexSources {
		if isGeneratedColumn(source.Column) {
			generatedByName[source.Column.GetName()] = source.Column
		}
	}
	for _, spec := range table.mainTable.columns {
		column := table.ColumnsMap[spec.name]
		if generatedColumn := generatedByName[spec.name]; generatedColumn != nil {
			column = generatedColumn
		}
		if column == nil || column.IsNil() {
			panic(fmt.Sprintf(`pgdriver: table "%v": stored column "%v" has no ORM column`, table.Name, spec.name))
		}
		table.storedColumns = append(table.storedColumns, storedColumn{name: spec.name, column: column, isJSON: spec.sqlType == "JSONB"})
	}
	if partitionColumn := table.GetPartKey(); partitionColumn != nil && !partitionColumn.IsNil() {
		table.primaryKeyColumns = append(table.primaryKeyColumns, partitionColumn)
	}
	table.primaryKeyColumns = append(table.primaryKeyColumns, table.Keys...)
	table.capabilities = table.ComputeCapabilities()
	return table
}

func (table *PgTable) qualifiedName() string { return table.mainTable.qualifiedName() }
