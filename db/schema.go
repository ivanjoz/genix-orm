package db

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
	// TypeDelta is a TypeView packed range view with the table's "updated_version" column appended
	// as its last key, so one index serves both halves of a delta-cache sync. The digit width of
	// every declared key comes from its FixedValues instead of a .DecimalSize() decorator, and the
	// packed column is int32 whenever the resulting maximum fits. Keys[0] is also the column Delta()
	// filters on; see TableStruct.Delta for the query side.
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
	TypeDelta:          "Delta View",
}

// ColumnNameUpdated is the column the ORM maintains as every record's last-write timestamp. It is
// resolved by name rather than declared, so the query side and the drivers share one spelling.
const ColumnNameUpdated = "updated"

// ColumnNameUpdatedVersion is the column the ORM maintains as every record's write sequence number:
// one value per write call per partition, taken from the same counter that hands out autoincrement
// IDs. Unlike a timestamp it is strictly increasing and never collides, which is what lets a delta
// sync ask for "> my watermark" and lets the by-IDs cache compare slot versions for equality.
//
// It costs one counter read per write, so it is maintained only for tables that consume it: those
// declaring a TypeDelta index or SaveUpdatedVersion. Both require the record and table structs to
// declare the field, since the value has to reach the client to be sent back as a watermark.
const ColumnNameUpdatedVersion = "updated_version"

// MaxTableID is the largest value TableSchema.ID can hold: it occupies the low 14 bits of the
// packed partition_table_id key of cache_updated_version.
const MaxTableID = int16(1<<14 - 1)

// MaxCachePartitionID is the largest partition value the same packed key can carry, in its high
// 18 bits.
const MaxCachePartitionID = int32(1<<18 - 1)

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
// compiler size the column's digit slot inside a packed key (see TypeDelta) and lets Delta()
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
	// derived from the name. It is packed into cache_updated_version's partition key, so changing
	// it silently repoints a table's cached slot versions. Range: 1..MaxTableID.
	ID int16
	// Namespace is the logical grouping a table lives in — a keyspace on Scylla.
	Namespace         string
	Name              string
	Keys              []Coln
	Partition         Coln
	TextSearchColumn  Coln
	Indexes           []Index
	SequenceColumn    Coln
	CounterColumn     Coln
	UseSequences      bool
	SequencePartCol   Coln
	KeyConcatenated   []Coln
	KeyIntPacking     []Coln
	AutoincrementPart Coln
	// SaveUpdatedVersion opts the table into the by-IDs cache: writes bump the record's slot in
	// cache_updated_version, and QueryCachedIDs skips reading rows whose slot did not move.
	SaveUpdatedVersion bool
	// GenericRecord maps this table's columns onto the flat shape returned by
	// QueryCachedGenericByIDs, so a single endpoint can resolve labels for any table by name.
	GenericRecord GenericRecordSchema
	// UseListAsDefault makes slice columns map to an ordered collection instead of a
	// set when no explicit ",list" / ",set" db tag is set. Per-field tags still
	// override this default.
	UseListAsDefault bool
	FixedValues      []FixedValues
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
// payload small, which is the whole point of this endpoint. ID keeps its capitalised name and upv/ss
// are required by the frontend by-ID cache contract (IMinimalRecord).
type GenericRecord struct {
	ID     int64  `json:"ID"`
	Name   string `json:"nm,omitempty"`
	S1     string `json:"s1,omitempty"`
	S2     string `json:"s2,omitempty"`
	N1     int64  `json:"n1,omitempty"`
	N2     int64  `json:"n2,omitempty"`
	Status int8   `json:"ss,omitempty"`
	// UpdatedVersion carries the record's *slot* version here, not its own write version: by-IDs
	// reads are the one path where "upv" means "the version this record was validated against".
	UpdatedVersion uint16 `json:"upv,omitempty"`
}

// IDUpdatedVersion is one entry of a client's by-ID cache: which record it holds
// and which slot version it was last validated against, so the server can reply
// with only what changed.
type IDUpdatedVersion struct {
	ID          int64
	PartitionID int32
	// UpdatedVersion is 0 when the client holds no version yet, which never matches a stored slot
	// version and therefore always forces a read.
	UpdatedVersion uint16
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
