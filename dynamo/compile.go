package dynamo

import (
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/viant/xunsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// Schema compilation
//
// A schema is compiled once per (table, record) type pair into an immutable
// tableMeta, then cached — the same separation of "immutable metadata" from
// "per-query state" that the genix ORM uses. Compilation:
//
//  1. Allocates a *T, walks its fields with reflection and stamps each Col's
//     field name into its metadata (the genix GetInfoPointer trick).
//  2. Calls GetSchema() on the now-named table struct.
//  3. Resolves every Coln into a keyCol and validates key rules.
//  4. Indexes the record struct's fields for marshaling.
// ─────────────────────────────────────────────────────────────────────────────

// colAccessor holds precompiled, type-specialized field readers built once per
// record type with xunsafe — the DynamoDB analogue of genix's
// compileFastAccessors. Only the closures relevant to the field's kind are set;
// each reads straight from the struct pointer with no per-call reflection or
// interface boxing.
type colAccessor struct {
	kind    valueKind
	getStr  func(unsafe.Pointer) string  // string fields
	getI64  func(unsafe.Pointer) int64   // integer fields (sign preserved)
	getU64  func(unsafe.Pointer) uint64  // integer fields (>= 0, for order-encoding)
	getF64  func(unsafe.Pointer) float64 // numeric fields (post-filter compares)
	getBool func(unsafe.Pointer) bool    // bool fields
	setI64  func(unsafe.Pointer, int64)  // integer fields (autoincrement write-back)
}

// keyCol is a resolved key component with its precompiled accessor.
type keyCol struct {
	fieldName string
	kind      valueKind
	base      int // base64 width for numeric components of composite string keys
	acc       *colAccessor
}

// indexMeta is a resolved GSI mapping.
type indexMeta struct {
	slot Slot
	keys []keyCol
}

// tableMeta is the compiled, immutable descriptor for an entity. The whole
// record is serialized by colbin; alongside that we cache one precompiled
// xunsafe accessor per record field, used to read key columns and to evaluate
// in-memory post-filters without runtime reflection.
type tableMeta struct {
	entity     string
	recordType reflect.Type
	partition  []keyCol
	sort       []keyCol
	indexes    []indexMeta
	accessors  map[string]*colAccessor // record field name -> precompiled accessor
	autoinc    *autoincConfig          // nil unless the schema sets UseAutoincrement
}

var metaCache sync.Map // reflect.Type (record) -> *tableMeta

// schemaProvider is satisfied by the table struct.
type schemaProvider interface{ GetSchema() Schema }

// getOrCompile returns the cached tableMeta and the name-populated table struct.
func getOrCompile[T any, E any]() (*tableMeta, T) {
	tablePtr := new(T)
	populateColumnNames(tablePtr)

	recordType := reflect.TypeOf((*E)(nil)).Elem()
	if cached, ok := metaCache.Load(recordType); ok {
		return cached.(*tableMeta), *tablePtr
	}

	sp, ok := any(*tablePtr).(schemaProvider)
	if !ok {
		panic(fmt.Sprintf("db: %s does not implement GetSchema()", recordType.Name()))
	}
	schema := sp.GetSchema()

	meta := buildTableMeta(schema, recordType)
	metaCache.Store(recordType, meta)
	return meta, *tablePtr
}

// populateColumnNames stamps each Col field's Go name into its metadata, so that
// GetSchema() can reference columns by identity.
func populateColumnNames[T any](tablePtr *T) {
	rv := reflect.ValueOf(tablePtr).Elem()
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		if !field.CanAddr() {
			continue
		}
		cp, ok := field.Addr().Interface().(colPointer)
		if !ok {
			continue // embedded Model, etc.
		}
		info := cp.infoPtr()
		info.fieldName = rt.Field(i).Name
		if info.attrName == "" {
			info.attrName = info.fieldName
		}
	}
}

func buildTableMeta(schema Schema, recordType reflect.Type) *tableMeta {
	if schema.Entity == "" {
		panic(fmt.Sprintf("db: %s schema is missing Entity", recordType.Name()))
	}
	if len(schema.Sort) == 0 {
		panic(fmt.Sprintf("db: %s schema must declare at least one Sort column (the sk)", recordType.Name()))
	}

	accessors := buildAccessors(recordType)

	meta := &tableMeta{
		entity:     schema.Entity,
		recordType: recordType,
		partition:  resolveKeyCols(recordType, accessors, schema.Partition, false),
		sort:       resolveKeyCols(recordType, accessors, schema.Sort, true),
		accessors:  accessors,
	}

	usedSlots := map[string]bool{}
	for _, idx := range schema.Indexes {
		if idx.Slot.attr == "" {
			panic(fmt.Sprintf("db: %s has an index with no Slot", recordType.Name()))
		}
		if usedSlots[idx.Slot.attr] {
			panic(fmt.Sprintf("db: %s reuses slot %s", recordType.Name(), idx.Slot.attr))
		}
		usedSlots[idx.Slot.attr] = true

		keys := resolveKeyCols(recordType, accessors, idx.Keys, !idx.Slot.isNumber)
		if idx.Slot.isNumber {
			// Numeric GSI slots store one native DynamoDB number.
			if len(keys) != 1 || !keys[0].kind.isInteger() {
				panic(fmt.Sprintf("db: %s numeric slot %s requires exactly one integer key column",
					recordType.Name(), idx.Slot.attr))
			}
		}
		meta.indexes = append(meta.indexes, indexMeta{slot: idx.Slot, keys: keys})
	}

	if schema.UseAutoincrement {
		meta.autoinc = resolveAutoincrement(schema, recordType, accessors)
	}

	return meta
}

// autoincFieldName is the record field the ORM fills for UseAutoincrement.
const autoincFieldName = "ID"

// resolveAutoincrement validates the autoincrement declaration and precompiles
// the read/write accessors for the ID field. The record must declare an integer
// field named "ID"; the padding must be 0..9 so seq*10^padding stays well within
// int64.
func resolveAutoincrement(schema Schema, recordType reflect.Type, accessors map[string]*colAccessor) *autoincConfig {
	padding := schema.AutoincrementRandomPadding
	if padding < 0 || padding > 9 {
		panic(fmt.Sprintf("db: %s AutoincrementRandomPadding must be 0..9, got %d", recordType.Name(), padding))
	}
	acc, ok := accessors[autoincFieldName]
	if !ok {
		panic(fmt.Sprintf("db: %s uses UseAutoincrement but has no exported field %q", recordType.Name(), autoincFieldName))
	}
	if !acc.kind.isInteger() || acc.setI64 == nil {
		panic(fmt.Sprintf("db: %s field %q must be an integer type to use UseAutoincrement", recordType.Name(), autoincFieldName))
	}
	return &autoincConfig{
		seqName: schema.Entity,
		padding: padding,
		factor:  pow10(padding),
		get:     acc.getI64,
		set:     acc.setI64,
	}
}

// assignAutoIDs reserves and assigns IDs for the records whose ID is still zero.
// It reserves the whole batch in one atomic sequence bump, then lays each
// reserved value (+ random low digits) into its record. A no-op when the entity
// has no autoincrement or every record already carries an ID.
func (m *tableMeta) assignAutoIDs(ptrs []unsafe.Pointer) error {
	if m.autoinc == nil {
		return nil
	}
	var need []unsafe.Pointer
	for _, p := range ptrs {
		if m.autoinc.get(p) == 0 {
			need = append(need, p)
		}
	}
	if len(need) == 0 {
		return nil
	}
	base, err := reserveSequence(m.autoinc.seqName, len(need))
	if err != nil {
		return err
	}
	for i, p := range need {
		m.autoinc.set(p, m.autoinc.composeID(base+int64(i), m.autoinc.randDigits()))
	}
	return nil
}

// buildAccessors precompiles one xunsafe accessor per exported record field.
func buildAccessors(recordType reflect.Type) map[string]*colAccessor {
	accessors := map[string]*colAccessor{}
	for i := 0; i < recordType.NumField(); i++ {
		f := recordType.Field(i)
		if f.Anonymous || f.PkgPath != "" {
			continue // embedded / unexported
		}
		accessors[f.Name] = buildAccessor(f)
	}
	return accessors
}

// buildAccessor specializes the closures for one field's exact type.
func buildAccessor(f reflect.StructField) *colAccessor {
	xf := xunsafe.NewField(f)
	kind := classifyKind(f.Type)
	a := &colAccessor{kind: kind}
	switch kind {
	case kindString:
		a.getStr = func(p unsafe.Pointer) string { return xf.String(p) }
	case kindBool:
		a.getBool = func(p unsafe.Pointer) bool { return xf.Bool(p) }
	case kindFloat:
		if f.Type.Kind() == reflect.Float32 {
			a.getF64 = func(p unsafe.Pointer) float64 { return float64(xf.Float32(p)) }
		} else {
			a.getF64 = func(p unsafe.Pointer) float64 { return xf.Float64(p) }
		}
	case kindInt, kindUint:
		i64 := intReader(xf, f.Type.Kind())
		name := f.Name
		a.getI64 = i64
		a.setI64 = intWriter(xf, f.Type.Kind())
		a.getF64 = func(p unsafe.Pointer) float64 { return float64(i64(p)) }
		a.getU64 = func(p unsafe.Pointer) uint64 {
			v := i64(p)
			if v < 0 {
				panic(fmt.Sprintf("db: key column %q is negative (%d); key numbers must be >= 0", name, v))
			}
			return uint64(v)
		}
	}
	return a
}

// intWriter returns a closure writing an int64 into the field via the
// width-correct xunsafe setter (SetInt32 writes 4 bytes, SetInt64 writes 8, …).
// It is the write counterpart of intReader, used for autoincrement ID assignment.
func intWriter(xf *xunsafe.Field, k reflect.Kind) func(unsafe.Pointer, int64) {
	switch k {
	case reflect.Int:
		return func(p unsafe.Pointer, v int64) { xf.SetInt(p, int(v)) }
	case reflect.Int8:
		return func(p unsafe.Pointer, v int64) { xf.SetInt8(p, int8(v)) }
	case reflect.Int16:
		return func(p unsafe.Pointer, v int64) { xf.SetInt16(p, int16(v)) }
	case reflect.Int32:
		return func(p unsafe.Pointer, v int64) { xf.SetInt32(p, int32(v)) }
	case reflect.Int64:
		return func(p unsafe.Pointer, v int64) { xf.SetInt64(p, v) }
	case reflect.Uint:
		return func(p unsafe.Pointer, v int64) { xf.SetUint(p, uint(v)) }
	case reflect.Uint8:
		return func(p unsafe.Pointer, v int64) { xf.SetUint8(p, uint8(v)) }
	case reflect.Uint16:
		return func(p unsafe.Pointer, v int64) { xf.SetUint16(p, uint16(v)) }
	case reflect.Uint32:
		return func(p unsafe.Pointer, v int64) { xf.SetUint32(p, uint32(v)) }
	case reflect.Uint64:
		return func(p unsafe.Pointer, v int64) { xf.SetUint64(p, uint64(v)) }
	default:
		panic(fmt.Sprintf("db: intWriter on non-integer kind %v", k))
	}
}

// intReader returns a closure reading the field as int64 via the width-correct
// xunsafe getter (Int32 reads 4 bytes, Int64 reads 8, etc.).
func intReader(xf *xunsafe.Field, k reflect.Kind) func(unsafe.Pointer) int64 {
	switch k {
	case reflect.Int:
		return func(p unsafe.Pointer) int64 { return int64(xf.Int(p)) }
	case reflect.Int8:
		return func(p unsafe.Pointer) int64 { return int64(xf.Int8(p)) }
	case reflect.Int16:
		return func(p unsafe.Pointer) int64 { return int64(xf.Int16(p)) }
	case reflect.Int32:
		return func(p unsafe.Pointer) int64 { return int64(xf.Int32(p)) }
	case reflect.Int64:
		return func(p unsafe.Pointer) int64 { return xf.Int64(p) }
	case reflect.Uint:
		return func(p unsafe.Pointer) int64 { return int64(xf.Uint(p)) }
	case reflect.Uint8:
		return func(p unsafe.Pointer) int64 { return int64(xf.Uint8(p)) }
	case reflect.Uint16:
		return func(p unsafe.Pointer) int64 { return int64(xf.Uint16(p)) }
	case reflect.Uint32:
		return func(p unsafe.Pointer) int64 { return int64(xf.Uint32(p)) }
	case reflect.Uint64:
		return func(p unsafe.Pointer) int64 { return int64(xf.Uint64(p)) }
	default:
		panic(fmt.Sprintf("db: intReader on non-integer kind %v", k))
	}
}

// resolveKeyCols turns Colns into keyCols, attaching each column's precompiled
// accessor. When requireBaseForNumbers is true (composite string keys: sort key,
// string GSI slots), every integer component must declare .Base(n) so its slot
// width is fixed and the key stays sortable.
func resolveKeyCols(recordType reflect.Type, accessors map[string]*colAccessor, cols []Coln, requireBaseForNumbers bool) []keyCol {
	out := make([]keyCol, 0, len(cols))
	for _, c := range cols {
		m := c.col()
		acc, ok := accessors[m.fieldName]
		if !ok {
			panic(fmt.Sprintf("db: key column %q is not an exported field of %s", m.fieldName, recordType.Name()))
		}
		if m.kind.isInteger() && requireBaseForNumbers && m.base <= 0 {
			panic(fmt.Sprintf("db: numeric key column %q used in a composite/sort key must declare .Base(n)", m.fieldName))
		}
		out = append(out, keyCol{fieldName: m.fieldName, kind: m.kind, base: m.base, acc: acc})
	}
	return out
}
