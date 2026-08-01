package db

// The operations below are the package-level API application code calls. Each one
// resolves the driver from the record type's declared default (D is inferred, so
// callers pass no type arguments) and dispatches through the generic Executor —
// which is why none of them erase the record type.

func executorFor[RecordT any, TableT any, D Executor[TableT, RecordT]]() Executor[TableT, RecordT] {
	var declaredDefault D
	return declaredDefault
}

func Insert[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, columnsToExclude ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().Insert(records, columnsToExclude...)
}

func InsertOne[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	record RecordT, columnsToExclude ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().Insert(&[]RecordT{record}, columnsToExclude...)
}

func Update[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, columnsToInclude ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().Update(records, columnsToInclude...)
}

func UpdateOne[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	record RecordT, columnsToInclude ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().Update(&[]RecordT{record}, columnsToInclude...)
}

func UpdateExclude[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, columnsToExclude ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().UpdateExclude(records, columnsToExclude...)
}

func InsertUpdate[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	recordsForInsert *[]RecordT, recordsForUpdate *[]RecordT,
	columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().InsertUpdate(
		recordsForInsert, recordsForUpdate, columnsToIncludeUpdate, columnsToExcludeInsert...)
}

func InsertUpdateInclude[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().InsertUpdateInclude(
		records, isInsert, columnsToIncludeUpdate, columnsToExcludeInsert...)
}

func InsertUpdateExclude[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, isInsert func(record *RecordT) bool,
	columnsToExcludeUpdate []Coln, columnsToExcludeInsert ...Coln,
) error {
	return executorFor[RecordT, TableT, D]().InsertUpdateExclude(
		records, isInsert, columnsToExcludeUpdate, columnsToExcludeInsert...)
}

func Merge[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	records *[]RecordT, columnsToExcludeUpdate []Coln,
	onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT),
) error {
	return executorFor[RecordT, TableT, D]().Merge(records, columnsToExcludeUpdate, onUpdate, onInsert)
}

func QueryCachedIDs[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	refSlice *[]RecordT, cachedIDs []IDUpdatedVersion,
) error {
	return executorFor[RecordT, TableT, D]().QueryCachedIDs(refSlice, cachedIDs)
}

func SearchTextIDs[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	partition int32, query string, statusGroup int8, limit int,
) ([]IDWeight, error) {
	return executorFor[RecordT, TableT, D]().SearchTextIDs(partition, query, statusGroup, limit)
}

func SearchText[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]](
	refSlice *[]RecordT, partition int32, query string, statusGroup int8, limit int,
) ([]IDWeight, error) {
	return executorFor[RecordT, TableT, D]().SearchText(refSlice, partition, query, statusGroup, limit)
}

// MakeTable compiles a table's metadata, panicking on an invalid schema. Generated
// registry code and deploy tooling use it to force validation early.
func MakeTable[RecordT RecordWithExecutor[TableT, RecordT, D], TableT TableSchemaInterface[TableT], D Executor[TableT, RecordT]]() Table {
	return executorFor[RecordT, TableT, D]().CompileTable(InitStructTable[TableT, RecordT](new(TableT)))
}

// Driver-bound entry points that take a table *name* rather than its types cannot be
// generic, so the active driver installs them at registration time.
var (
	// GetAutoincrementID reserves recordsSize consecutive IDs for an arbitrary counter key.
	GetAutoincrementID func(key string, recordsSize int) (int64, error)
	// QueryCachedGenericByIDs resolves IDs to the flat GenericRecord shape for any table
	// that opted in through TableSchema.GenericRecord.
	QueryCachedGenericByIDs func(tableName string, cachedIDs []IDUpdatedVersion) ([]GenericRecord, error)
	// SetDebugLogging raises the ORM's log verbosity: 0 off, 1 statements, 2 full.
	SetDebugLogging func(level int)
)
