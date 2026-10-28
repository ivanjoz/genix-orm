package db

import (
	"reflect"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// Coln is the type-erased handle for a column, as referenced from a schema
// declaration or an index. Both Col and ColSlice satisfy it.
type Coln interface {
	GetInfo() ColumnInfo
	GetName() string
}

// Cols returns columns as the slice required by schema declarations.
func Cols(columns ...Coln) []Coln {
	return columns
}

// ColGetInfoPointer is implemented by every column handle so the schema
// compiler can stamp resolved metadata into it in place.
type ColGetInfoPointer interface {
	GetInfoPointer() *ColumnInfo
	SetSchemaStruct(any)
	SetTableInfo(*TableInfo)
}

type ColumnSetInfo interface {
	SetName(string)
	SetTableInfo(*TableInfo)
	SetSchemaStruct(any)
}

// Index kinds. The names describe intent, not a physical structure: a driver
// maps each onto whatever it actually has (a Scylla materialized view, a
// DynamoDB GSI) and rejects the ones it cannot express.
const (
	TypeGlobalIndex    int8 = 1
	TypeLocalIndex     int8 = 2
	TypeInheritFromKey int8 = 4
	TypeView           int8 = 6
	TypeViewTable      int8 = 9
	// TypeDelta is a change log (db/cache): every write appends (log ID, Version, deleted) to the
	// file of the record's partition, pinned values and 10-minute bucket, and Delta() reads it past
	// the client's cursor. The last Key is the sync filter column, which a first sync filters on;
	// the other Keys are pinned, each value with its own log. The Keys that are not pinned make the
	// log ID. A keyless TypeDelta logs the whole partition with no sync filter.
	TypeDelta int8 = 10
)

// IndexTypeNames labels index kinds for logs and deploy output.
var IndexTypeNames = map[int8]string{
	TypeGlobalIndex:    "Global Index",
	TypeLocalIndex:     "Local Index",
	3:                  "Hash Index",
	TypeInheritFromKey: "Inherit From Key",
	TypeView:           "View",
	TypeViewTable:      "View Table",
	TypeDelta:          "Delta",
}

// ColumnNameUpdated is the column the ORM maintains as every record's last-write timestamp. It is
// resolved by name rather than declared, so the query side and the drivers share one spelling.
const ColumnNameUpdated = "updated"

// ColumnNameVersion is the column the ORM maintains as every record's write count on a table with a
// TypeDelta index: 1 on insert, one more on every write, 1 again after cache.MaxVersion. The delta
// log carries it, and the client skips a logged write whose version it already holds. The record
// declares it as `Version int16 json:"ver,omitempty"`.
const ColumnNameVersion = "version"

// MaxTableID is the largest value TableSchema.ID can hold.
const MaxTableID = int16(1<<14 - 1)

type Index struct {
	Type int8
	Keys []Coln
	// Cols declares the non-key payload columns to keep in the index.
	// When empty, the ORM keeps the previous SELECT * behavior.
	Cols []Coln
	// Partition overrides the partition column of the generated index.
	// When empty the index keeps the base table partition, which is what
	// almost every schema wants: key = (part_col) new_col, pk_col
	Partition Coln
	// Create a hash for use with IN operators
	UseHash       bool
	UseIndexGroup bool
}

// FixedValues pins down the set of values a column can hold. Declaring it lets the schema
// compiler size the column's bit slot inside a packed key (see TypeDelta) and lets Delta()
// enumerate every value of a sync-filter column, neither of which is derivable from the Go type.
type FixedValues struct {
	Col          Coln
	Values       []int64
	ValuesString []string // Not used, meaby in the future
	Min          int64
	Max          int64
}

// Bounds returns the inclusive range this declaration pins down, preferring an explicit Values
// list over Min/Max. The third result is false when neither was declared, leaving the column's
// width unknown to whatever wanted to size a packed slot from it.
func (f FixedValues) Bounds() (int64, int64, bool) {
	if len(f.Values) > 0 {
		minValue, maxValue := f.Values[0], f.Values[0]
		for _, value := range f.Values[1:] {
			if value < minValue {
				minValue = value
			}
			if value > maxValue {
				maxValue = value
			}
		}
		return minValue, maxValue, true
	}
	// A Max of zero cannot describe a useful range, so it reads as "not declared" rather than
	// as a column pinned to the single value 0.
	if f.Max > 0 {
		return f.Min, f.Max, true
	}
	return 0, 0, false
}

// TableSchema is the single, driver-independent declaration of a table. Each
// driver reads the subset it can honour and fails loudly at compile time on the
// rest, so a table never silently loses a key or an index under a driver that
// cannot express it.
type TableSchema struct {
	// ID is the table's stable, hand-assigned identity, unique across the whole project and never
	// derived from the name. It names the table's cache files (db/cache), so changing it silently
	// repoints them. Range: 1..MaxTableID.
	ID int16
	// Namespace is the logical grouping a table lives in — a keyspace on Scylla.
	Namespace string
	Name      string
	Keys      []Coln
	Partition Coln
	// TextSearch declares the table's text index, kept by the backend set with
	// SetTextSearchBackend. It requires a single numeric key that fits int32.
	TextSearch        *TextSearch
	Indexes           []Index
	SequenceColumn    Coln
	CounterColumn     Coln
	UseSequences      bool
	SequencePartCol   Coln
	KeyConcatenated   []Coln
	KeyIntPacking     []Coln
	AutoincrementPart Coln
	// DisableDefaultColumns prevents the ORM from adding its DB-only "created" and "updated"
	// timestamp columns. Explicitly declared columns with those names remain regular user columns.
	DisableDefaultColumns bool
	// CacheByIDs opts the table into the by-IDs cache (db/cache): writes raise the record's slot in
	// its partition's slot file, and QueryCachedIDs skips reading rows whose slot did not move. The
	// table needs one integer key and the record an `Updated int32 json:"upd"` field.
	CacheByIDs bool
	// GenericRecord maps this table's columns onto the flat shape returned by
	// QueryCachedGenericByIDs, so a single endpoint can resolve labels for any table by name.
	GenericRecord GenericRecordSchema
	// UseListAsDefault makes slice columns map to an ordered collection instead of a
	// set when no explicit ",list" / ",set" db tag is set. Per-field tags still
	// override this default.
	UseListAsDefault bool
	FixedValues      []FixedValues
	// DataFrames declares group-by reports of the table kept as columnar files in the file store
	// (genix-orm/dataframe, DATAFRAMES.md), each partition with its own files. The table needs a
	// partition column and one integer key; the reads compact them (DataFrame.CompactOn), or
	// CompactDataFrames.
	DataFrames []DataFrame
	// Computed declares the table-struct columns that have no record field: their value is a method of
	// the record, so a value derived from another column (the product IDs of the sale lines) can feed
	// an index without being stored twice. See ComputedCol.
	Computed []ComputedCol
}

// ComputedCol binds a table-struct column with no record field to a record method:
//
//	{Col: e.DetailProductIDs, From: db.Cols(e.Lines), Value: db.Compute((*SaleOrder).DetailProductIDs)}
//
// The column is never written, read or deployed. Only a multi-column UseIndexGroup can name it, since
// that index stores hashes computed from the record in memory; every other use is rejected when the
// table compiles. A query on it must be served by one of those index groups.
type ComputedCol struct {
	Col Coln
	// From are the stored columns the value is derived from: an update that writes any of them
	// recomputes the index groups of the computed column.
	From  []Coln
	Value *ComputedValue
}

// ComputedValue is a record method compiled for ComputedCol.Value.
type ComputedValue struct {
	RecordType reflect.Type
	ValueType  reflect.Type
	Read       func(recordPointer unsafe.Pointer) any
}

// Compute compiles a record method for ComputedCol.Value: db.Compute((*SaleOrder).DetailProductIDs).
// It must be pure, it is called on every write of the record.
func Compute[RecordT any, ValueT any](method func(*RecordT) ValueT) *ComputedValue {
	return &ComputedValue{
		RecordType: reflect.TypeFor[RecordT](),
		ValueType:  reflect.TypeFor[ValueT](),
		Read:       func(recordPointer unsafe.Pointer) any { return method((*RecordT)(recordPointer)) },
	}
}

// DataFrame is one group-by report of a table: one file per value of Keys, one row per value of Rows
// in it, holding the Sums of the records with those values and, with Count, how many they are. It is
// declared either by the record's columns (one row per record; a soft-deleted record, status 0, is in
// no file) or by a ToFrames method (0..N rows per record), never both.
type DataFrame struct {
	// Name: a-z, 0-9 and '-'. It names the frame's folder.
	Name string
	// Keys are 1 to 3 integer columns, non-negative: which file a record goes to.
	Keys []Coln
	// Rows are 1 to 3 integer columns, non-negative, the widest group first: which row of its file.
	Rows []Coln
	// Sums are the integer or float columns each row adds up.
	Sums  []Coln
	Count bool
	// ToFrames declares the frame by a method of the record instead of its columns:
	// db.ToFrames((*SaleOrder).ToSaleSummaryFrames). See dataframe/projection.go.
	ToFrames *dataframe.Projection
	// Version is part of the frame's shape: bump it when the logic of ToFrames changes, which rebuilds
	// the frame.
	Version int
	// UseZeroSumsMask writes a byte per file saying which summed columns hold anything, so a column
	// empty in a whole file costs nothing. At most 8 summed columns (the Sums, then the count).
	UseZeroSumsMask bool
	// CompactOn makes a read compact the partition it reads, before answering, once Records records
	// changed since the last compaction or Seconds passed since it. Zero: only CompactDataFrames does.
	CompactOn CompactOn
}

// CompactOn is when a read compacts the DataFrame partition it reads (dataframe/compact.go).
type CompactOn = dataframe.CompactOn

// ToFrames compiles a record's ToFrames method for DataFrame.ToFrames: it appends the record's lines
// to the slice it is given and returns it, as structs whose fields are tagged frame:"key",
// frame:"row" or frame:"sum". It must be pure: it is called on the record before and after each
// write. A rebuild hands it one reused buffer, so it must not keep the slice.
func ToFrames[R any, F any](method func(*R, []F) []F) *dataframe.Projection {
	return dataframe.ProjectionOf(method)
}

// GenericRecordSchema declares which columns fill the generic by-IDs shape. Name is required; the
// rest are optional. ID and Status are not listed: they are always the table's single key column
// and its "status" column, resolved automatically.
type GenericRecordSchema struct {
	Name Coln // string  — the display label
	S1   Coln // string  — optional secondary text (e.g. SKU, document number)
	S2   Coln // string  — optional
	N1   Coln // integer — optional numeric (e.g. price, foreign key)
	N2   Coln // integer — optional
}

func (schema GenericRecordSchema) IsEmpty() bool {
	return schema.Name == nil && schema.S1 == nil && schema.S2 == nil && schema.N1 == nil && schema.N2 == nil
}

// GenericRecord is the flat, table-agnostic row returned to the client. Short json tags keep the
// payload small, which is the whole point of this endpoint. ID keeps its capitalised name and upd/ss
// are required by the frontend by-ID cache contract (IMinimalRecord).
type GenericRecord struct {
	ID     int64  `json:"ID"`
	Name   string `json:"nm,omitempty"`
	S1     string `json:"s1,omitempty"`
	S2     string `json:"s2,omitempty"`
	N1     int64  `json:"n1,omitempty"`
	N2     int64  `json:"n2,omitempty"`
	Status int8   `json:"ss,omitempty"`
	// Updated carries the record's slot tick as the client holds it (db/cache), not its own write
	// time: by-IDs reads are the one path where "upd" means "the tick this record was validated at".
	Updated int32 `json:"upd,omitempty"`
}

// RecordGroup is a set of records sharing one index-group hash, plus the counter
// the client uses to decide whether its cached copy is still fresh.
type RecordGroup[T any] struct {
	IndexID          int16   `json:"ig"`
	GroupHash        int32   `json:"id"`
	IndexGroupValues []int64 `json:"igVal"`
	Records          []T     `json:"records"`
	UpdateCounter    int32   `json:"upc"`
}
