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
//	        Keys:   db.Cols(t.ID),                           // -> sk: the record's key
//	        Indexes: []db.Index{
//	            {Slot: db.G1, Keys: db.Cols(t.Brand, t.Price.Size(40))}, // GSI sorted by Brand, Price
//	            {Slot: db.G2, Keys: db.Cols(t.Created.Size(48))},       // GSI sorted by Created
//	            {Keys: db.Cols(t.TagIDs.Size(32), t.Created.Size(48))}, // fan-out: Contains(TagIDs, ...)
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

// Cols is sugar for a []Coln literal, for any schema field that takes columns: Partition, Keys,
// Index.Keys. The field, not the helper, says what the columns are for.
func Cols(cols ...Coln) []Coln { return cols }

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
	// other values writes another item). They are also the base table's range
	// and order dimension; a GSI ranges on its own Keys first. Numeric Keys
	// columns must declare .Size(bits).
	Keys []Coln
	// Indexes are the secondary access paths: GSI slots (G1..G10), fan-out
	// indexes over a slice field, delta indexes and GroupBy counters (see Index).
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

	// CacheByIDs enables the by-IDs cache (cache_by_ids.go): every write sets the
	// record's slot to its Updated, and Repo.QueryCachedIDs returns only the
	// requested records whose slot moved since the value the client holds. It needs
	// exactly one integer Keys column (the ID) and the managed int64 "Updated" field
	// (see delta.go), which a by-IDs read overwrites with the slot's.
	CacheByIDs bool

	// VersionedWrites enables Repo.Modify (modify.go) on a table that has neither
	// CacheByIDs, GroupDelta, DataFrames nor a TypeDelta index (those already imply
	// it): the item carries the managed int64 "Updated" field (delta.go) as the
	// attribute "upd" too, which the conditional write compares.
	VersionedWrites bool

	// DataFrames are aggregates of the records kept as files in the frame store
	// (data_frame.go), brought up to date by MaterializeDataFrames.
	DataFrames []DataFrame
}

// DataFrame is an aggregate of the records kept as files in the frame store: one
// file per distinct value of Keys, holding one row per distinct value of Rows,
// ascending, with the sum of each Sums column. Keys[0] is the reprocessing unit (a
// day): RebuildDataFrames recomputes a range of it from the records.
//
// A table with frames needs the managed int64 fields "Updated" (json "upd") and
// "CreatedVersion" (json "crv"), a TypeDelta index without pinned Keys (the run
// reads the changed records through it), and no Partition.
type DataFrame struct {
	// Name is the frame's folder: lowercase letters, digits and '-', unique in the
	// entity. Renaming it orphans the files.
	Name string
	// Keys name the file, 1 to 3 integer Cols with a cb tag. With 1, Keys[0] is the
	// file; with 2–3, Keys[0] is a folder and the rest name the file inside it.
	// Keys[0] must lead the entity's Keys, a GSI or a local index.
	Keys []Coln
	// Rows is the integer Col each file lists, ascending.
	Rows Coln
	// Sums are the integer Cols summed per row, one column each in the file. A write
	// with a negative value fails, unless AllowNegativeSums.
	Sums []Coln
	// AllowNegativeSums accepts negative Sums values. It only changes that
	// validation: the file format stores any int64.
	AllowNegativeSums bool
	// Count adds one more summed column after the Sums, which every record (Status
	// ≠ 0) adds 1 to: how many records a row sums, read with FrameRow.Count. With it
	// the Sums may be empty.
	Count bool
}

// TypeDelta marks an Index as a delta index (delta.go): the ORM appends the
// managed Updated to its Keys, so Delta() reads "changed since the client's
// watermark" as one exact sk range.
const TypeDelta int8 = 10

// TypeLocal marks an Index as a local index (local_index.go): one hidden
// base-table row per record, under the record's own partition, whose sk is the
// index Keys then the base Keys. The planner treats it like a GSI (an Eq on a
// leading run of its Keys, then one range), but it lives in the base table, so
// Consistent() reads it: a lookup right after a write sees that write.
const TypeLocal int8 = 20

// Slot identifies one of the ten physical GSIs. Each has its own hash attribute
// hN (a number: TableID ‖ the index Partition columns) and range attribute rN (a
// string: composite of the index Keys, then the base Keys not among them).
type Slot struct {
	hashAttr  string // "h1".."h10"
	rangeAttr string // "r1".."r10"
	index     string // GSI name, "gsi-1".."gsi-10"
}

// Index is the GSI name; HashAttr and RangeAttr its key attributes. The table
// definition (which the ORM never creates) builds the GSIs from them.
func (s Slot) Index() string     { return s.index }
func (s Slot) HashAttr() string  { return s.hashAttr }
func (s Slot) RangeAttr() string { return s.rangeAttr }

var (
	G1  = Slot{hashAttr: "h1", rangeAttr: "r1", index: "gsi-1"}
	G2  = Slot{hashAttr: "h2", rangeAttr: "r2", index: "gsi-2"}
	G3  = Slot{hashAttr: "h3", rangeAttr: "r3", index: "gsi-3"}
	G4  = Slot{hashAttr: "h4", rangeAttr: "r4", index: "gsi-4"}
	G5  = Slot{hashAttr: "h5", rangeAttr: "r5", index: "gsi-5"}
	G6  = Slot{hashAttr: "h6", rangeAttr: "r6", index: "gsi-6"}
	G7  = Slot{hashAttr: "h7", rangeAttr: "r7", index: "gsi-7"}
	G8  = Slot{hashAttr: "h8", rangeAttr: "r8", index: "gsi-8"}
	G9  = Slot{hashAttr: "h9", rangeAttr: "r9", index: "gsi-9"}
	G10 = Slot{hashAttr: "h10", rangeAttr: "r10", index: "gsi-10"}
	// Slots lists the ten GSIs of the physical table.
	Slots = []Slot{G1, G2, G3, G4, G5, G6, G7, G8, G9, G10}
)

// Index is one secondary access path, of one of two shapes:
//
//   - A GSI: Slot is set. Keys are its sort key, ranged like the base Keys: an
//     Eq on a leading run of them, then one range. The base Keys not among them
//     follow, so the range is unique, ordered and still ranges on the base Keys
//     after the index Keys are pinned. Partition (integers, optional) is its hash
//     key and needs an Eq; it defaults to the entity's Partition.
//   - A fan-out index: exactly one of the Keys is a ColSlice (of integers
//     declaring .Size(bits), or of strings), anywhere in the list, and Slot is
//     left empty. DynamoDB cannot index inside a list, so the record is fanned
//     out into one hidden base-table row per distinct element, whose sk is the
//     Keys with the slice replaced by that element. Contains (or Eq) on the slice
//     queries them; every Keys column before the slice needs an Eq, and the ones
//     after it take ranges: Cols(ProductIDs.Size(32), Created.Size(32)) serves
//     Contains(ProductIDs, 5).Gt(Created, 1000). The slice field must carry a
//     `cb:"N"` tag (1..999): that stable id, not the Go name, locates its rows,
//     so a slice field takes one fan-out index. The rows are kept in sync by
//     Put/PutMany/PutIfAbsent/Delete (see array_index.go); a scalar column in
//     Keys that changes on a write rewrites every element row.
//   - A delta index: Type is TypeDelta (see delta.go). It lives in hidden rows
//     like a fan-out index, with or without a ColSlice among its Keys.
//   - A local index: Type is TypeLocal (see local_index.go). Scalar Keys, no
//     Slot: one hidden row per record, read like a GSI but consistently.
type Index struct {
	// Type is 0, TypeDelta for a delta index or TypeLocal for a local index.
	Type int8
	Slot Slot
	// Partition (GSIs only) overrides the entity's Partition as the GSI hash key:
	// spread a hot index, or partition it unlike the base table.
	Partition []Coln
	Keys      []Coln
	// FullCopy (fan-out indexes only) stores the whole record blob on every
	// element row, so Contains reads it in one Query. Without it a row holds only
	// keys, and Contains reads the base records in a second BatchGetItem.
	FullCopy bool
	// GroupBy lists integer or float columns: the ORM keeps, per base partition and
	// per distinct value of Keys, a counter with the record count and the sum of
	// each column, read with Repo.QueryGroups (group_by.go). Not on a TypeDelta
	// index. An Index with a GroupBy may go without a Slot: counters only.
	GroupBy []Coln
	// GroupDelta (with GroupBy) also stamps each counter with the Updated of the
	// last write that touched it, so QueryGroups().Since() reads only changed
	// groups. It needs the managed Updated field (delta.go).
	GroupDelta bool
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
