package db

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/viant/xunsafe"
)

// ColInfo is the identity of one column: where it lives in the record struct and
// what role it plays in the table. It carries no storage-engine knowledge.
type ColInfo struct {
	Name         string
	FieldName    string
	NameAlias    string
	IsPrimaryKey int8
	FieldIdx     int
	IsVirtual    bool
	HasView      bool
	ViewIdx      int8
	Idx          int16
	RefType      reflect.Type
	Field        *xunsafe.Field
}

// ColumnInfo is the fully resolved, driver-independent descriptor for a column:
// its identity, its type, the packing modifiers declared in the schema, and the
// precompiled accessor closures that read and write the field.
//
// The *Fn closures are exported because drivers live in other packages and need
// to overlay their own storage-specific paths (see Codec). A nil closure
// means "fall back to the method below", which routes to the active driver.
type ColumnInfo struct {
	ColInfo
	ColType
	// HasCollectionTagOptions records that a db tag pinned the collection kind, so
	// schema-level and per-field-type defaults must not override it.
	HasCollectionTagOptions bool
	GetValueFn              func(ptr unsafe.Pointer) any
	GetRawValueFn           func(ptr unsafe.Pointer) any
	GetStatementValueFn     func(ptr unsafe.Pointer) any
	SetValueFn              func(ptr unsafe.Pointer, v any)
	// GetValueStringFn stringifies the typed field directly (no interface boxing, no
	// fmt reflection); used for stable lookup tokens. FieldsEqualFn compares the same
	// field across two records without allocating a string. Both are precomputed per
	// column type in CompileFastAccessors.
	GetValueStringFn func(ptr unsafe.Pointer) string
	FieldsEqualFn    func(a, b unsafe.Pointer) bool
	// DecimalDigits is the digit width this column occupies inside a packed key. It
	// is named for the field rather than the Col.DecimalSize(n) setter because Go
	// forbids a field and a method sharing one name.
	DecimalDigits int8
	// AutoincrementRandDigits is the number of trailing digits of a generated ID
	// filled with randomness, so IDs are non-consecutive and collide less often.
	AutoincrementRandDigits int8
	CompositeBucketSizes    []int8
	IsWeek                  bool
	UseInt32Packing         bool
	AggregateFn             string
}

// Codec supplies everything about a column that depends on the storage engine
// rather than on the Go type: how a value renders into a statement, how types
// with no native representation are encoded, how a value coming back from the
// driver is coerced into the field, which accessors only the driver can build
// (collection literals, for instance), and how a slice column's collection kind
// is settled.
//
// The active driver installs one via SetCodec.
type Codec interface {
	RenderValue(c *ColumnInfo, ptr unsafe.Pointer) any
	EncodeStatementValue(c *ColumnInfo, ptr unsafe.Pointer) any
	AssignValue(c *ColumnInfo, ptr unsafe.Pointer, v any)
	CompileDriverAccessors(c *ColumnInfo)
	// ApplyCollectionOptions settles a slice column's storage type from an explicit
	// ",list" / ",set" / ",frozen" db tag on the record field.
	ApplyCollectionOptions(recordTypeName, fieldName string, ct ColType, tag DBTag) ColType
	// ApplyCollectionDefaults settles it from the schema-level UseListAsDefault flag
	// and the Col-vs-ColSlice frozen default. Only consulted when no tag pinned the kind.
	ApplyCollectionDefaults(ct ColType, useListAsDefault, applyFrozenDefault, frozen bool) ColType
}

var codec Codec

// SetCodec installs the active driver's codec. Called from the driver's Register
// function, before any table is compiled.
func SetCodec(c Codec) { codec = c }

// IColInfo is the type-erased view of a column used by compiled tables, views
// and query planners.
type IColInfo interface {
	GetName() string
	GetValue(ptr unsafe.Pointer) any
	GetRawValue(ptr unsafe.Pointer) any
	GetStatementValue(ptr unsafe.Pointer) any
	// GetValueString returns the field value as a stable string token (no boxing/fmt for scalars).
	GetValueString(ptr unsafe.Pointer) string
	// FieldsEqual reports whether the column holds the same value in two records (zero-alloc for scalars).
	FieldsEqual(a, b unsafe.Pointer) bool
	SetValue(ptr unsafe.Pointer, v any)
	GetInfo() *ColInfo
	GetType() *ColType
	IsNil() bool
	// SetAutoincrementRandSize sets the random suffix size for autoincrement columns
	SetAutoincrementRandSize(size int8)
	// SetDecimalSize sets the decimal size for KeyIntPacking columns
	SetDecimalSize(size int8)
}

func (c *ColumnInfo) GetValue(ptr unsafe.Pointer) any {
	if c.GetValueFn != nil {
		return c.GetValueFn(ptr)
	}
	if codec == nil {
		return nil
	}
	return codec.RenderValue(c, ptr)
}

func (c *ColumnInfo) GetRawValue(ptr unsafe.Pointer) any {
	if c.GetRawValueFn != nil {
		return c.GetRawValueFn(ptr)
	}
	if c.Field == nil {
		return nil
	}
	if c.IsPointer && c.Field.IsNil(ptr) {
		return nil
	}
	return c.Field.Interface(ptr)
}

func (c *ColumnInfo) GetStatementValue(ptr unsafe.Pointer) any {
	if c.GetStatementValueFn != nil {
		return c.GetStatementValueFn(ptr)
	}
	if c.GetRawValueFn != nil {
		return c.GetRawValueFn(ptr)
	}
	if c.GetValueFn != nil {
		return c.GetValueFn(ptr)
	}
	if c.Field == nil {
		return nil
	}
	if c.IsPointer {
		if c.Field.IsNil(ptr) {
			return nil
		}
		return c.Field.Interface(ptr)
	}
	// Values with no native driver type (blobs, complex types) are encoded by the driver.
	if codec == nil {
		return c.Field.Interface(ptr)
	}
	return codec.EncodeStatementValue(c, ptr)
}

// GetValueString returns the field value as a string for use as a lookup/equality token.
// Scalar columns hit precompiled type-specific formatters; everything else falls back to fmt.
func (c *ColumnInfo) GetValueString(ptr unsafe.Pointer) string {
	if c.GetValueStringFn != nil {
		return c.GetValueStringFn(ptr)
	}
	return fmt.Sprintf("%v", c.GetRawValue(ptr))
}

// FieldsEqual reports whether this column holds the same value in records a and b.
// Scalar columns compare typed values with zero allocation; other types fall back to
// comparing their string tokens.
func (c *ColumnInfo) FieldsEqual(a, b unsafe.Pointer) bool {
	if c.FieldsEqualFn != nil {
		return c.FieldsEqualFn(a, b)
	}
	return c.GetValueString(a) == c.GetValueString(b)
}

func (c *ColumnInfo) SetValue(ptr unsafe.Pointer, v any) {
	if c.SetValueFn != nil {
		c.SetValueFn(ptr, v)
		return
	}
	if c.Field == nil || codec == nil {
		return
	}
	codec.AssignValue(c, ptr, v)
}

func (c *ColumnInfo) GetType() *ColType {
	return &c.ColType
}

func (c *ColumnInfo) GetName() string {
	return c.Name
}

func (c *ColumnInfo) GetInfo() *ColInfo {
	return &c.ColInfo
}

func (c *ColumnInfo) IsNil() bool {
	return c == nil
}

func (c *ColumnInfo) SetAutoincrementRandSize(size int8) {
	c.AutoincrementRandDigits = size
}

func (c *ColumnInfo) SetDecimalSize(size int8) {
	c.DecimalDigits = size
}
