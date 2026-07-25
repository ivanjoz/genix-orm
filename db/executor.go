package db

// Executor is one storage engine's implementation of the operations that need a
// live connection. It is deliberately *generic*: an Executor[ProductTable, Product]
// can only ever be handed a *[]Product, so the record type survives all the way
// into the driver and nothing here is type-erased.
//
// Being an interface *value* rather than a type parameter is what allows a driver
// to be chosen at runtime, and two drivers to be live at once for the same record
// type — declare one Executor variable per driver and pass it to Via().
type Executor[TableT any, RecordT any] interface {
	// Name identifies the driver in logs and errors ("scylla", "dynamo").
	Name() string
	// Select runs the read described by ti and appends the rows to dst. When scan is
	// non-nil it is called per decoded record, which lets callers stream a large
	// result set instead of holding it in memory.
	Select(schema *TableT, ti *TableInfo, dst *[]RecordT, scan func(record *RecordT) bool) error
	// SelectGrouped runs a read whose results arrive bucketed by index-group hash.
	// The destination lives in ti.RefSlice because its element type is
	// RecordGroup[RecordT], not RecordT.
	SelectGrouped(schema *TableT, ti *TableInfo) error
	Insert(records *[]RecordT, columnsToExclude ...Coln) error
	Update(records *[]RecordT, columnsToInclude ...Coln) error
	UpdateExclude(records *[]RecordT, columnsToExclude ...Coln) error
	// InsertUpdate writes two already-split groups in one batch.
	InsertUpdate(recordsForInsert, recordsForUpdate *[]RecordT,
		columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
	// InsertUpdateInclude splits records with isInsert, listing updated columns explicitly.
	InsertUpdateInclude(records *[]RecordT, isInsert func(record *RecordT) bool,
		columnsToIncludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
	// InsertUpdateExclude splits records with isInsert, listing updated columns by exclusion.
	InsertUpdateExclude(records *[]RecordT, isInsert func(record *RecordT) bool,
		columnsToExcludeUpdate []Coln, columnsToExcludeInsert ...Coln) error
	// Merge reads the existing rows first so callers can decide per record whether an
	// update is needed, and mutate new records before insert.
	Merge(records *[]RecordT, columnsToExcludeUpdate []Coln,
		onUpdate func(previous, current *RecordT) bool, onInsert func(record *RecordT)) error
	// QueryCachedIDs resolves records by ID, skipping any whose client cache version
	// still matches the server.
	QueryCachedIDs(refSlice *[]RecordT, cachedIDs []IDCacheVersion) error
	// SearchTextIDs returns matching record IDs ranked by weight.
	SearchTextIDs(partition int32, query string, statusGroup int8, limit int) ([]IDWeight, error)
	// SearchText fills refSlice with the matching records and returns their weights.
	SearchText(refSlice *[]RecordT, partition int32, query string, statusGroup int8, limit int) ([]IDWeight, error)
	// CompileTable turns the declared schema into this driver's compiled form.
	CompileTable(schema *TableT) Table
}

// IDWeight is one text-search hit: the record ID and how well it matched.
type IDWeight struct {
	ID     int32   `json:"id"`
	Weight float32 `json:"w"`
}

// GroupIndexCache is a client's freshness claim for one index group, so a grouped
// read can skip groups the caller already holds.
type GroupIndexCache struct {
	IndexID       int16
	GroupHash     int32
	UpdateCounter int32
}

// RecordWithExecutor is the constraint on a record type. It extends
// TableBaseInterface with the driver accessor, which is what lets Go infer the
// driver from the record alone — the same mechanism that already infers the table
// struct — so call sites need no explicit type arguments.
type RecordWithExecutor[TableT any, RecordT any, D any] interface {
	TableBaseInterface[TableT, RecordT]
	GetExecutor() D
}
