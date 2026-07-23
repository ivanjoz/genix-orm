package dynamo

import "reflect"

// ─────────────────────────────────────────────────────────────────────────────
// Statically-typed schema declaration
//
// Mirrors genix's `TableStruct[T,E]` / `Col[T,E]` / `GetSchema()` shape, trimmed
// to what a single-table DynamoDB store needs. You declare two structs:
//
//	type Product struct {              // the record (plain data)
//	    db.Model[ProductTable, Product]
//	    ID       string
//	    Category string
//	    Price    int64
//	    Created  int64
//	}
//
//	type ProductTable struct {         // the schema (typed column handles)
//	    db.Model[ProductTable, Product]
//	    ID       db.Col[ProductTable, string]
//	    Category db.Col[ProductTable, string]
//	    Price    db.Col[ProductTable, int64]
//	    Created  db.Col[ProductTable, int64]
//	}
//
//	func (t ProductTable) GetSchema() db.Schema {
//	    return db.Schema{
//	        Entity:    "prod",
//	        Partition: db.Keys(t.Category),                 // -> pk
//	        Sort:      db.Keys(t.Created.Base(8), t.ID),    // -> sk (order-preserving)
//	        Indexes: []db.Index{
//	            {Slot: db.N1, Keys: db.Keys(t.Price)},      // numeric GSI
//	            {Slot: db.S1, Keys: db.Keys(t.Category)},   // string GSI
//	        },
//	    }
//	}
//
// The record and table structs must list the same fields in the same order.
// ─────────────────────────────────────────────────────────────────────────────

// valueKind classifies a column's Go value type for key encoding and marshaling.
type valueKind int8

const (
	kindUnset valueKind = iota
	kindString
	kindInt   // signed integer
	kindUint  // unsigned integer
	kindFloat // float32/float64
	kindBool
	kindOther // structs, slices, etc. (stored, but not usable as a key)
)

// colMeta is the resolved metadata for one column. Name is filled in by the
// compiler via reflection over the table struct (see compile.go).
type colMeta struct {
	fieldName string    // Go struct field name, e.g. "Category"
	attrName  string    // DynamoDB attribute name; defaults to fieldName
	kind      valueKind // resolved from the column's value type
	base      int       // order-preserving base64 width for numeric key columns
}

// Coln is the type-erased column handle used inside schema, index and query
// declarations — the analogue of genix's `Coln`.
type Coln interface {
	col() colMeta
}

// Keys is sugar for a []Coln literal.
func Keys(cols ...Coln) []Coln { return cols }

// Col is a statically-typed column handle. T is the table struct type, E is the
// column's Go value type (string, int64, ...), exactly like genix's Col[T,E].
type Col[T any, E any] struct {
	info colMeta
}

// col resolves the column metadata, lazily classifying the value kind from E.
func (c Col[T, E]) col() colMeta {
	m := c.info
	if m.kind == kindUnset {
		m.kind = classifyKind(reflect.TypeOf((*E)(nil)).Elem())
	}
	if m.attrName == "" {
		m.attrName = m.fieldName
	}
	return m
}

// infoPtr exposes the metadata for in-place mutation during compilation. It is
// unexported and only reachable from within this package, matching genix's
// GetInfoPointer trick for assigning field names via reflection.
func (c *Col[T, E]) infoPtr() *colMeta { return &c.info }

// Base sets the order-preserving Base64 width (number of base64 characters, 6
// bits each) reserved for this numeric column when it is packed into a composite
// key. It is the DynamoDB analogue of genix's DecimalSize: it fixes the column's
// slot width so concatenated keys stay sortable. Widths are 1..11 (11 covers a
// full uint64). Only valid on integer columns.
func (c Col[T, E]) Base(width int) Col[T, E] {
	c.info.base = width
	return c
}

// colPointer is the internal interface used by the compiler to set field names.
type colPointer interface{ infoPtr() *colMeta }

// classifyKind maps a reflect.Type to a valueKind.
func classifyKind(t reflect.Type) valueKind {
	switch t.Kind() {
	case reflect.String:
		return kindString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return kindInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return kindUint
	case reflect.Float32, reflect.Float64:
		return kindFloat
	case reflect.Bool:
		return kindBool
	default:
		return kindOther
	}
}

func (k valueKind) isInteger() bool { return k == kindInt || k == kindUint }

// String is the stable, frontend-facing name of a column's value type. It is the
// wire form used by GetSchema (see introspect.go); keep these tokens stable.
func (k valueKind) String() string {
	switch k {
	case kindString:
		return "string"
	case kindInt:
		return "int"
	case kindUint:
		return "uint"
	case kindFloat:
		return "float"
	case kindBool:
		return "bool"
	case kindOther:
		return "other"
	default:
		return "unset"
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Model: embedded in both the record and the table struct (like genix's
// TableStruct). It supplies a default GetSchema so the record type satisfies the
// interface; the table struct overrides it with the real schema.
// ─────────────────────────────────────────────────────────────────────────────

type Model[T any, E any] struct{}

func (Model[T, E]) GetSchema() Schema { return Schema{} }

// Schema declares how an entity maps onto the shared single table.
type Schema struct {
	// Name is an optional, human-readable label for the schema (e.g. "Products",
	// "Customer Orders"). It is purely informational — carried through to
	// introspection (GetSchema) for display in tooling — and never affects keys,
	// queries or storage.
	Name string
	// Entity is a short discriminator prefixed onto pk (and string index slots)
	// so multiple entity types can share the physical table without colliding.
	Entity string
	// Partition columns build the base table pk (equality lookups).
	Partition []Coln
	// Sort columns build the base table sk. Because the physical table shares
	// one sk across the base table and every GSI, this is also the range/order
	// dimension for index queries. Numeric sort columns must declare .Base(n).
	Sort []Coln
	// Indexes map onto the physical GSI slots (N1, N2, S1, S2, S3).
	Indexes []Index

	// UseAutoincrement makes the ORM assign the record's integer "ID" field
	// automatically on Put/PutMany when it is still zero. IDs come from a
	// per-entity sequence kept in the shared table and reserved atomically, so
	// they are unique across concurrent writers without a read-modify-write race.
	// The record and table structs must declare an integer field named "ID".
	UseAutoincrement bool
	// AutoincrementRandomPadding is how many low decimal digits of a generated ID
	// are filled with a random value, so IDs are non-consecutive (harder to guess
	// / enumerate) and carry an extra collision margin under concurrency. The ID
	// is `sequence * 10^padding + random(0, 10^padding)`. 0 means plain sequential
	// IDs (1, 2, 3, …); 3 turns sequence 42 into an ID like 42_837. Range 0..9.
	// Ignored unless UseAutoincrement is true.
	AutoincrementRandomPadding int
}

// Slot identifies one of the five physical GSI attributes.
type Slot struct {
	attr     string // "n1".."s3"
	index    string // GSI name, e.g. "gsi-n1"
	isNumber bool
}

var (
	// N1, N2 are the numeric GSI slots (a single integer column, stored as a
	// native DynamoDB number — natively range-ordered).
	N1 = Slot{attr: "n1", index: "gsi-n1", isNumber: true}
	N2 = Slot{attr: "n2", index: "gsi-n2", isNumber: true}
	// S1, S2, S3 are the string GSI slots (one column or a composite of several;
	// numeric components are order-preserving Base64 via .Base(n)).
	S1 = Slot{attr: "s1", index: "gsi-s1"}
	S2 = Slot{attr: "s2", index: "gsi-s2"}
	S3 = Slot{attr: "s3", index: "gsi-s3"}
)

// Index maps a set of key columns onto one physical GSI slot.
type Index struct {
	Slot Slot
	Keys []Coln
}
