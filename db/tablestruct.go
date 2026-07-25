package db

import (
	"fmt"
	"reflect"
	"strings"
)

// TableStruct is embedded in both the record struct and the table struct of every
// table. It carries the whole shared surface — schema declaration, query building
// and execution — and reaches the storage engine through a generic Executor, so no
// operation is type-erased.
//
// D is the table's *default* driver, named once in the declaration (drivers export
// an alias that supplies it, so declarations stay two-parameter). Via() overrides it
// per query, which is how one record type can be read from two databases at once.
type TableStruct[D Executor[T, E], T TableSchemaInterface[T], E TableBaseInterface[T, E]] struct {
	// exec is nil until a query picks a driver; nil means "use D".
	exec                   Executor[T, E]
	schemaStruct           *T
	tableInfo              *TableInfo
	baseStructType         reflect.Type
	cacheVersionFieldIndex []int
	// field just for encoding purposes
	I__ bool `gob:"-" json:"-"`
}

// GetExecutor exposes the declared default driver. It exists so Go can infer D
// from a record type, keeping explicit type arguments off every call site.
func (e TableStruct[D, T, E]) GetExecutor() D { return *new(D) }

// Executor returns the driver this query will run on: the override set by Via(),
// or the default declared on the table.
func (e *TableStruct[D, T, E]) Executor() Executor[T, E] {
	if e.exec == nil {
		var declaredDefault D
		e.exec = declaredDefault
	}
	return e.exec
}

// Via runs this query through an explicitly chosen driver instead of the declared
// default. The argument is typed to this table's record, so passing another
// table's driver is a compile error.
func (e *TableStruct[D, T, E]) Via(exec Executor[T, E]) *T {
	e.exec = exec
	return e.schemaStruct
}

// Exec runs the read that Query built.
func (e *TableStruct[D, T, E]) Exec() error {
	if e.tableInfo.UseIndexGroupSelect {
		return e.Executor().SelectGrouped(e.schemaStruct, e.tableInfo)
	}
	dst, ok := e.tableInfo.RefSlice.(*[]E)
	if !ok {
		return Err("Exec: query was not bound to a destination slice")
	}
	return e.Executor().Select(e.schemaStruct, e.tableInfo, dst, nil)
}

// ExecScan hands each decoded record to scanHandler instead of collecting them all,
// so a large result set never has to be held in memory. Returning true from the
// handler drops the record.
func (e *TableStruct[D, T, E]) ExecScan(scanHandler func(record *E) bool) error {
	dst, ok := e.tableInfo.RefSlice.(*[]E)
	if !ok {
		return Err("ExecScan: query was not bound to a destination slice")
	}
	return e.Executor().Select(e.schemaStruct, e.tableInfo, dst, scanHandler)
}

func (e *TableStruct[D, T, E]) Insert(records *[]E, columnsToExclude ...Coln) error {
	return e.Executor().Insert(records, columnsToExclude...)
}

func (e *TableStruct[D, T, E]) InsertOne(record E, columnsToExclude ...Coln) error {
	return e.Executor().Insert(&[]E{record}, columnsToExclude...)
}

func (e *TableStruct[D, T, E]) Update(records *[]E, columnsToInclude ...Coln) error {
	return e.Executor().Update(records, columnsToInclude...)
}

func (e *TableStruct[D, T, E]) UpdateOne(record E, columnsToInclude ...Coln) error {
	return e.Executor().Update(&[]E{record}, columnsToInclude...)
}

func (e *TableStruct[D, T, E]) UpdateExclude(records *[]E, columnsToExclude ...Coln) error {
	return e.Executor().UpdateExclude(records, columnsToExclude...)
}

// MakeTable compiles this table with the active driver.
func (e *TableStruct[D, T, E]) MakeTable() Table {
	return e.Executor().CompileTable(e.schemaStruct)
}

func (e *TableStruct[D, T, E]) setBaseStructType(baseType reflect.Type) {
	e.baseStructType = baseType
}

func (e *TableStruct[D, T, E]) setCacheVersionFieldIndex(fieldIndex []int) {
	e.cacheVersionFieldIndex = fieldIndex
}

// CacheVersionFieldIndex is the record field path of the cache-version column, or
// nil when the record declares none. Drivers read it to stamp versions after a read.
func (e *TableStruct[D, T, E]) CacheVersionFieldIndex() []int {
	return e.cacheVersionFieldIndex
}

// BaseStructType is the record struct type this table was compiled against.
func (e *TableStruct[D, T, E]) BaseStructType() reflect.Type {
	return e.baseStructType
}

// GetSchema is the default a record struct inherits; the table struct overrides it.
func (e TableStruct[D, T, E]) GetSchema() TableSchema {
	return TableSchema{}
}

func (e *TableStruct[D, T, E]) SetWhere(colname string, operator string, value any) {
	cs := ColumnStatement{Col: colname, Operator: operator, Value: value}
	e.tableInfo.Statements = append(e.tableInfo.Statements, cs)
}

func (e *TableStruct[D, T, E]) SetTableInfo(t *TableInfo) {
	e.tableInfo = t
}
func (e *TableStruct[D, T, E]) SetRefSlice(refSlice *[]E) {
	e.tableInfo.RefSlice = refSlice
}
func (e *TableStruct[D, T, E]) GetTableInfo() *TableInfo {
	return e.tableInfo
}
func (e *TableStruct[D, T, E]) GetInfoPointer() *ColumnInfo { // Para compatibilidad
	return &ColumnInfo{}
}
func (e *TableStruct[D, T, E]) SetSchemaStruct(schemaStruct any) {
	if schema, ok := schemaStruct.(*T); ok {
		e.schemaStruct = schema
	}
}
func (e TableStruct[D, T, E]) GetBaseStruct() E {
	return *new(E)
}
func (e TableStruct[D, T, E]) GetTableStruct() T {
	return *new(T)
}

func (e *TableStruct[D, T, E]) Select(columns ...Coln) *T {
	for _, col := range columns {
		e.tableInfo.ColumnsInclude = append(e.tableInfo.ColumnsInclude, col.GetInfo())
	}
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) GroupBy(columns ...Coln) *T {
	for _, col := range columns {
		e.tableInfo.GroupByColumns = append(e.tableInfo.GroupByColumns, col.GetInfo())
	}
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) Exclude(columns ...Coln) *T {
	for _, col := range columns {
		e.tableInfo.ColumnsExclude = append(e.tableInfo.ColumnsExclude, col.GetInfo())
	}
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) GetRefSchema() *T {
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) AllowFilter() *T {
	e.tableInfo.AllowFilter = true
	return e.schemaStruct
}

// Autoincrement declares the table's key as generated. randDecimalSize is a random
// value appended to the counter to avoid taking the same value under high
// concurrency: if 3, an ID of 100 becomes something like 100567.
func (e *TableStruct[D, T, E]) Autoincrement(randDecimalSize int8) Col[T, E] {
	if randDecimalSize > 8 {
		panic("randDecimalSize TOO BIG.")
	}
	return Col[T, E]{info: ColumnInfo{AutoincrementRandDigits: randDecimalSize}}
}

func (e *TableStruct[D, T, E]) Limit(limit int32) *T {
	e.tableInfo.Limit = limit
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) OrderDesc() *T {
	e.tableInfo.OrderBy = "ORDER BY %v DESC"
	return e.schemaStruct
}

func (e *TableStruct[D, T, E]) IncludeCachedGroup(indexGroupHash int32, updateCountValue int32) *T {
	if e.tableInfo.CachedIndexGroups == nil {
		e.tableInfo.CachedIndexGroups = map[int32]int32{}
	}
	e.tableInfo.CachedIndexGroups[indexGroupHash] = updateCountValue
	return e.schemaStruct
}

// Query starts a read into refSlice and returns the table struct to build predicates
// on. D is inferred from the record type's declared driver, so callers pass no
// explicit type arguments.
func Query[T RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D Executor[E, T]](refSlice *[]T) *E {
	refTable := InitStructTable[E, T](new(E))
	any(refTable).(TableStructInterfaceQuery[E, T]).SetRefSlice(refSlice)
	return refTable
}

// QueryIndexGroup starts a grouped read, where records arrive bucketed by their
// index-group hash instead of as a flat slice.
func QueryIndexGroup[T RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D Executor[E, T]](refSlice *[]RecordGroup[T]) *E {
	refTable := InitStructTable[E, T](new(E))
	tableInfo := any(refTable).(interface{ GetTableInfo() *TableInfo }).GetTableInfo()
	tableInfo.RefSlice = refSlice
	tableInfo.UseIndexGroupSelect = true
	return refTable
}

// TableOf returns a bound table struct for write operations, with no read destination.
func TableOf[T RecordWithExecutor[E, T, D], E TableSchemaInterface[E], D Executor[E, T]]() *E {
	return InitStructTable[E, T](new(E))
}

// MakeSchema returns a table's declared schema without compiling it.
func MakeSchema[T TableBaseInterface[E, T], E TableSchemaInterface[E]]() TableSchema {
	refTable := InitStructTable[E, T](new(E))
	return (*refTable).GetSchema()
}

type tableStructCacheMetaSetter interface {
	setBaseStructType(reflect.Type)
	setCacheVersionFieldIndex([]int)
}

// getCollectionFrozenDefaultForTableField reports whether the declaring handle
// implies a frozen default, and what it is. A Col holding a slice means the whole
// value is one opaque unit (frozen); a ColSlice means an addressable collection.
func getCollectionFrozenDefaultForTableField(tableFieldType reflect.Type) (bool, bool) {
	tableFieldTypeName := tableFieldType.String()
	if strings.Contains(tableFieldTypeName, "ColSlice[") {
		return true, false
	}
	if strings.Contains(tableFieldTypeName, "Col[") {
		return true, true
	}
	return false, false
}

// InitStructTable binds one query's state onto a table struct: it resolves every
// column handle's name and type from the cached record metadata, then points all
// handles at a fresh TableInfo. Immutable record metadata is built once per record
// type; only the per-call query state is allocated here.
func InitStructTable[T TableInterface[T], E any](schemaStruct *T) *T {
	structRefType := reflect.TypeOf(*new(E))
	recordMetadata := getOrBuildStructFieldMetadata(structRefType)
	refTableInfo := &TableInfo{}

	structValue := reflect.ValueOf(schemaStruct).Elem()
	structType := structValue.Type()

	// Read schema-level flags once so the slice-default swap can be applied per column below.
	useListAsDefault := (*schemaStruct).GetSchema().UseListAsDefault

	for i := 0; i < structValue.NumField(); i++ {
		field := structValue.Field(i)
		fieldType := structType.Field(i)

		// Check if field can be addressed and if it implements ColGetInfoPointer interface
		if !field.CanAddr() || !field.Addr().CanInterface() {
			fmt.Println("no es::", fieldType.Name)
			continue
		}

		fieldAddr := field.Addr()
		column, ok := fieldAddr.Interface().(ColGetInfoPointer)
		if !ok {
			fmt.Println("El field", fieldType.Name, "no implementa ColGetInfoPointer")
			continue
		}

		// Extract column name from db tag or convert field name to snake_case
		columnName := ParseDBTag(fieldType.Tag.Get("db")).ColumnName

		if colBase, ok := recordMetadata.fieldMetadataByName[fieldType.Name]; ok {
			if columnName == "" {
				columnName = colBase.Name
			}
			if columnName == "" {
				columnName = ToSnakeCase(fieldType.Name)
			}

			colInfo := column.GetInfoPointer()
			// Copy cached metadata to keep immutable cache entries untouched.
			*colInfo = colBase
			colInfo.Name = columnName
			if colInfo.IsSlice && !colInfo.HasCollectionTagOptions && codec != nil {
				// Honor schema-level UseListAsDefault and the Col-vs-ColSlice frozen default
				// only when record tags did not already force the collection options.
				applyFrozenDefault, frozen := getCollectionFrozenDefaultForTableField(fieldType.Type)
				colInfo.ColType = codec.ApplyCollectionDefaults(
					colInfo.ColType, useListAsDefault, applyFrozenDefault, frozen)
			}

			if column1, ok1 := fieldAddr.Interface().(Coln); ok1 {
				// Transfer properties from Col if they were set
				if c, ok := any(column1).(*Col[T, E]); ok {
					colInfo.DecimalDigits = c.info.DecimalDigits
					colInfo.AutoincrementRandDigits = c.info.AutoincrementRandDigits
					colInfo.UseInt32Packing = c.info.UseInt32Packing
				}

				if column1.GetInfo().Name == "" {
					panic("No se seteo el nombre: " + columnName)
				}
			}
		} else if fieldType.Name != "TableStruct" {
			err := fmt.Sprintf(`No se encontró el field "%v" en el struct "%v"`, fieldType.Name, structRefType.Name())
			panic(err)
		}

		// Set the column name using the interface method
		column.SetSchemaStruct(schemaStruct)
		column.SetTableInfo(refTableInfo)
	}

	if tableStructMeta, ok := any(schemaStruct).(tableStructCacheMetaSetter); ok {
		tableStructMeta.setBaseStructType(recordMetadata.recordType)
		tableStructMeta.setCacheVersionFieldIndex(append([]int(nil), recordMetadata.cacheVersionFieldIndex...))
	}
	return schemaStruct
}
