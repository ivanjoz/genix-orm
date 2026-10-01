package dynamo

import (
	"fmt"
	"reflect"
)

// ─────────────────────────────────────────────────────────────────────────────
// Statically-typed schema declaration
//
// Mirrors genix's `TableStruct[T,E]` / `Col[T,E]` / `GetSchema()` shape, trimmed
// to what a single-table DynamoDB store needs. You declare two structs:
//
//	type Product struct {              // the record (plain data)
//	    ID         string
//	    CategoryID int32
//	    Brand      string
//	    Price      int64
//	    Created    int64
//	    TagIDs     []int32 `cb:"6"`
//	}
//
//	type ProductTable struct {         // the schema (typed column handles)
//	    db.Model[ProductTable, Product]
//	    ID         db.Col[ProductTable, string]
//	    CategoryID db.Col[ProductTable, int32]
//	    Brand      db.Col[ProductTable, string]
//	    Price      db.Col[ProductTable, int64]
//	    Created    db.Col[ProductTable, int64]
//	    TagIDs     db.ColSlice[ProductTable, int32]
//	}
//
//	func (t ProductTable) GetSchema() db.Schema {
//	    return db.Schema{
//	        Entity: "prod",                                  // pk = TableID (no Partition)
//	        Keys:   db.Keys(t.ID),                           // -> sk: the record's key
//	        Indexes: []db.Index{
//	            {Slot: db.N1, Keys: db.Keys(t.Price.Size(40))},          // numeric GSI
//	            {Slot: db.S1, Keys: db.Keys(t.Brand)},                   // string GSI
//	            {Keys: db.Keys(t.TagIDs.Size(32), t.Created.Size(48))}, // fan-out: Contains(TagIDs, ...)
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
	bits      int8      // declared bit size (1..64) for numeric key columns
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

// Size declares how many bits (1..64) this numeric column needs when it is
// packed into a composite key, the same unit as genix-orm/db's Size(bits). The
// key stores ceil(bits/6) order-preserving Base64 characters, so the width is
// fixed and concatenated keys stay sortable. The declared bits are the real cap:
// Size(32) stores 6 characters (room for 36 bits) yet rejects values >= 2^32.
// Only valid on integer columns.
func (c Col[T, E]) Size(bits int8) Col[T, E] {
	c.info.bits = checkedBits(bits, c.info.fieldName)
	return c
}

func checkedBits(bits int8, fieldName string) int8 {
	if bits < 1 || bits > 64 {
		panic(fmt.Sprintf("db: Size(%d) on %q: bits must be 1..64", bits, fieldName))
	}
	return bits
}

// ColSlice is the handle for a slice column: the one that makes an Index fan out,
// and the only kind QueryBuilder.Contains accepts. E is the *element* type, as in
// genix-orm/db, so a []int32 field is declared ColSlice[XTable, int32].
type ColSlice[T any, E any] struct {
	info colMeta
}

// col reports the column itself, which is a slice (kindOther): a fan-out Index
// resolves the element kind from the record field.
func (c ColSlice[T, E]) col() colMeta {
	m := c.info
	m.kind = kindOther
	if m.attrName == "" {
		m.attrName = m.fieldName
	}
	return m
}

func (c *ColSlice[T, E]) infoPtr() *colMeta { return &c.info }

func (c ColSlice[T, E]) elementType() reflect.Type { return reflect.TypeFor[E]() }

// Size declares how many bits (1..64) each integer element needs: the fan-out
// rows pack the element into their sk, like an integer Keys column.
func (c ColSlice[T, E]) Size(bits int8) ColSlice[T, E] {
	c.info.bits = checkedBits(bits, c.info.fieldName)
	return c
}

// SliceColn is the type-erased ColSlice, the analogue of Coln for slice columns.
type SliceColn interface {
	Coln
	elementType() reflect.Type
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
	// Entity is the entity's name. It names the autoincrement sequence in
	// introspection and, when TableID is left at 0, is hashed into the TableID.
	Entity string
	// TableID is the 8-digit number (10_000_000..99_999_999) every key of this
	// entity starts with: pk, the string/numeric GSI slots and the array index
	// rows. Leave it 0 to use HashTableID(Entity); set it explicitly to pin it
	// (renaming Entity then no longer moves the data) or to resolve a collision.
	TableID int32
	// Partition columns are optional and build the base table pk after the
	// TableID; without them the pk is the TableID alone. The pk is a DynamoDB
	// number, so they must be integers declaring .Size(bits).
	Partition []Coln
	// Keys are the record's key inside its partition: they build the base table
	// sk, so pk + sk identify the record (Get/Delete take them, and a Put with
	// other values writes another item). Because the physical table shares one
	// sk across the base table and every GSI, Keys are also the only range/order
	// dimension, for base and index queries alike. Numeric Keys columns must
	// declare .Size(bits).
	Keys []Coln
	// Indexes are the secondary access paths: GSI slots (N1..N5, S1..S5) and
	// fan-out indexes over a slice field (see Index).
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

	// SaveUpdatedVersion enables the by-IDs cache (cache_updated_version.go): every
	// write bumps the version of the record's slot, and Repo.QueryCachedIDs returns
	// only the requested records whose slot moved since the client's version. It
	// needs exactly one integer Keys column (the ID) and the managed int32
	// "UpdatedVersion" field (see delta.go), which a by-IDs read overwrites with
	// the slot version.
	SaveUpdatedVersion bool

	// VersionedWrites enables Repo.Modify (modify.go) on a table that has neither
	// SaveUpdatedVersion nor a TypeDelta index: every write stamps the managed int32
	// "UpdatedVersion" field (delta.go) and stores it as the item attribute "upv".
	// Those two already imply it.
	VersionedWrites bool
}

// TypeDelta marks an Index as a delta index (delta.go): the ORM appends the
// managed UpdatedVersion to its Keys, so Delta() reads "changed since the
// client's watermark" as one exact sk range.
const TypeDelta int8 = 10

// Slot identifies one of the ten physical GSI attributes.
type Slot struct {
	attr     string // "n1".."n5", "s1".."s5"
	index    string // GSI name, e.g. "gsi-n1"
	isNumber bool
}

var (
	// N1..N5 are the numeric GSI slots (a single integer column, stored as a
	// native DynamoDB number — natively range-ordered).
	N1 = Slot{attr: "n1", index: "gsi-n1", isNumber: true}
	N2 = Slot{attr: "n2", index: "gsi-n2", isNumber: true}
	N3 = Slot{attr: "n3", index: "gsi-n3", isNumber: true}
	N4 = Slot{attr: "n4", index: "gsi-n4", isNumber: true}
	N5 = Slot{attr: "n5", index: "gsi-n5", isNumber: true}
	// S1..S5 are the string GSI slots (one column or a composite of several;
	// numeric components are order-preserving Base64 via .Size(bits)).
	S1 = Slot{attr: "s1", index: "gsi-s1"}
	S2 = Slot{attr: "s2", index: "gsi-s2"}
	S3 = Slot{attr: "s3", index: "gsi-s3"}
	S4 = Slot{attr: "s4", index: "gsi-s4"}
	S5 = Slot{attr: "s5", index: "gsi-s5"}
)

// Index is one secondary access path, of one of two shapes:
//
//   - A GSI: Slot is set and Keys are scalar columns mapped onto that slot.
//   - A fan-out index: exactly one of the Keys is a ColSlice (of integers
//     declaring .Size(bits), or of strings), anywhere in the list, and Slot is
//     left empty. DynamoDB cannot index inside a list, so the record is fanned
//     out into one hidden base-table row per distinct element, whose sk is the
//     Keys with the slice replaced by that element. Contains (or Eq) on the slice
//     queries them; every Keys column before the slice needs an Eq, and the ones
//     after it take ranges: Keys(ProductIDs.Size(32), Created.Size(32)) serves
//     Contains(ProductIDs, 5).Gt(Created, 1000). The slice field must carry a
//     `cb:"N"` tag (1..999): that stable id, not the Go name, locates its rows,
//     so a slice field takes one fan-out index. The rows are kept in sync by
//     Put/PutMany/PutIfAbsent/Delete (see array_index.go); a scalar column in
//     Keys that changes on a write rewrites every element row.
//   - A delta index: Type is TypeDelta (see delta.go). It lives in hidden rows
//     like a fan-out index, with or without a ColSlice among its Keys.
type Index struct {
	// Type is 0, or TypeDelta for a delta index.
	Type int8
	Slot Slot
	Keys []Coln
	// FullCopy (fan-out indexes only) stores the whole record blob on every
	// element row, so Contains reads it in one Query. Without it a row holds only
	// keys, and Contains reads the base records in a second BatchGetItem.
	FullCopy bool
}

// holdsSliceColumn reports whether the index is a fan-out index: one of its Keys is a ColSlice.
func holdsSliceColumn(index Index) bool {
	for _, column := range index.Keys {
		if _, isSlice := column.(SliceColn); isSlice {
			return true
		}
	}
	return false
}
