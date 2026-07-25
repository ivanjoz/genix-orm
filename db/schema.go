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
)

// IndexTypeNames labels index kinds for logs and deploy output.
var IndexTypeNames = map[int8]string{
	TypeGlobalIndex:    "Global Index",
	TypeLocalIndex:     "Local Index",
	3:                  "Hash Index",
	TypeInheritFromKey: "Inherit From Key",
	TypeView:           "View",
	TypeViewTable:      "View Table",
}

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

// TableSchema is the single, driver-independent declaration of a table. Each
// driver reads the subset it can honour and fails loudly at compile time on the
// rest, so a table never silently loses a key or an index under a driver that
// cannot express it.
type TableSchema struct {
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
	SaveCacheVersion  bool
	// GenericRecord maps this table's columns onto the flat shape returned by
	// QueryCachedGenericByIDs, so a single endpoint can resolve labels for any table by name.
	GenericRecord        GenericRecordSchema
	UseUpdateCounter     Coln
	DisableUpdateCounter bool
	// UseListAsDefault makes slice columns map to an ordered collection instead of a
	// set when no explicit ",list" / ",set" db tag is set. Per-field tags still
	// override this default.
	UseListAsDefault bool
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
// payload small, which is the whole point of this endpoint. ID keeps its capitalised name and ccv/ss
// are required by the frontend by-ID cache contract (IMinimalRecord).
type GenericRecord struct {
	ID           int64  `json:"ID"`
	Name         string `json:"nm,omitempty"`
	S1           string `json:"s1,omitempty"`
	S2           string `json:"s2,omitempty"`
	N1           int64  `json:"n1,omitempty"`
	N2           int64  `json:"n2,omitempty"`
	Status       int8   `json:"ss,omitempty"`
	CacheVersion uint8  `json:"ccv,omitempty"`
}

// IDCacheVersion is one entry of a client's by-ID cache: which record it holds
// and which version of it, so the server can reply with only what changed.
type IDCacheVersion struct {
	ID           int64
	PartitionID  int32
	CacheVersion uint8
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
