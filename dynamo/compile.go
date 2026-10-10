package dynamo

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"unsafe"

	"github.com/viant/xunsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
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
	bits      int8 // declared bit size of numeric components of composite string keys
	acc       *colAccessor
}

// indexMeta is a resolved GSI: its hash is TableID ‖ partition, its range the
// composite of sortColumns (the index Keys, then the base Keys not among them).
type indexMeta struct {
	slot        Slot
	partition   []keyCol
	sortColumns []keyCol
}

// tableMeta is the compiled, immutable descriptor for an entity. The whole
// record is serialized by colbin; alongside that we cache one precompiled
// xunsafe accessor per record field, used to read key columns and to evaluate
// in-memory post-filters without runtime reflection.
type tableMeta struct {
	entity          string
	tableID         string // the 8-digit TableID, as the decimal prefix of every key
	recordType      reflect.Type
	partition       []keyCol
	partitionDigits int // total decimal width of the partition columns in pk
	keys            []keyCol
	indexes         []indexMeta
	arrayIndexes    []arrayIndexMeta
	groupIndexes    []groupIndexMeta        // the Indexes declaring GroupBy (group_by.go)
	accessors       map[string]*colAccessor // record field name -> precompiled accessor
	autoinc         *autoincConfig          // nil unless the schema sets UseAutoincrement
	cacheByIDs      *cacheByIDsConfig       // nil unless the schema sets CacheByIDs
	// The managed "Updated" field (delta.go), nil without one. isVersioned: a TypeDelta
	// index, GroupDelta, CacheByIDs, VersionedWrites or DataFrames consume it, and the
	// item also carries it as the attribute "upd", which Modify's conditional write compares.
	updated     *keyCol
	isVersioned bool
	// status is the record's integer "Status" field (nil without one): 0 marks a
	// soft-deleted record, which counts in no GroupBy group or DataFrame.
	status *colAccessor
	// dataFrames are the schema's DataFrames (data_frame.go), and createdVersion the
	// managed int64 "CreatedVersion" they need (nil without frames).
	dataFrames     []dataframe.Frame
	createdVersion *colAccessor
}

// readsStoredVersion reports whether a write needs the record as stored first: to
// diff its hidden rows or its GroupBy counters, or to log its frame values.
func (m *tableMeta) readsStoredVersion() bool {
	return len(m.arrayIndexes) > 0 || len(m.groupIndexes) > 0 || len(m.dataFrames) > 0
}

var metaCache sync.Map // reflect.Type (record) -> *tableMeta

// entityByTableID guards against two entities landing on the same TableID (a hash
// collision, or a copy-pasted explicit ID): they would read and overwrite each
// other's rows, so the second one to compile panics at boot.
var entityByTableID sync.Map // int32 -> entity name

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
	if len(schema.Keys) == 0 {
		panic(fmt.Sprintf("db: %s schema must declare at least one Keys column (the sk)", recordType.Name()))
	}

	accessors := buildAccessors(recordType)
	tableID := resolveTableID(schema)
	if previousEntity, loaded := entityByTableID.LoadOrStore(tableID, schema.Entity); loaded && previousEntity != schema.Entity {
		panic(fmt.Sprintf("db: entities %q and %q share TableID %d: set an explicit TableID on one of them",
			previousEntity, schema.Entity, tableID))
	}

	meta := &tableMeta{
		entity:     schema.Entity,
		tableID:    strconv.Itoa(int(tableID)),
		recordType: recordType,
		partition:  resolveKeyCols(recordType, accessors, schema.Partition),
		keys:       resolveKeyCols(recordType, accessors, schema.Keys),
		accessors:  accessors,
	}

	// The pk is a DynamoDB number, so only integers can be packed after the TableID.
	for _, partitionCol := range meta.partition {
		if !partitionCol.kind.isInteger() {
			panic(fmt.Sprintf("db: %s partition column %q must be an integer: the pk is a number", recordType.Name(), partitionCol.fieldName))
		}
		meta.partitionDigits += decimalWidth(partitionCol.bits)
	}

	if statusAccessor := accessors[statusFieldName]; statusAccessor != nil && statusAccessor.kind.isInteger() {
		meta.status = statusAccessor
	}
	meta.isVersioned = schema.CacheByIDs || schema.VersionedWrites || len(schema.DataFrames) > 0 ||
		slices.ContainsFunc(schema.Indexes, func(idx Index) bool { return idx.Type == TypeDelta || idx.GroupDelta })
	meta.updated = resolveUpdated(recordType, accessors, meta.isVersioned)
	if len(schema.DataFrames) > 0 {
		meta.createdVersion = resolveCreatedVersion(recordType, accessors)
	}

	usedSlots := map[string]bool{}
	usedRowColumnIDs := map[string]bool{}
	usedGroupTags := map[string]bool{}
	for _, idx := range schema.Indexes {
		if idx.Type != 0 && idx.Type != TypeDelta && idx.Type != TypeLocal {
			panic(fmt.Sprintf("db: %s has an index of unknown Type %d", recordType.Name(), idx.Type))
		}
		if len(idx.Partition) > 0 && idx.Slot.index == "" {
			panic(fmt.Sprintf("db: %s declares a Partition on an index with no Slot: only a GSI takes one", recordType.Name()))
		}
		if idx.GroupDelta && len(idx.GroupBy) == 0 {
			panic(fmt.Sprintf("db: %s sets GroupDelta on an index without GroupBy columns", recordType.Name()))
		}
		if len(idx.GroupBy) > 0 && idx.Type != TypeLocal {
			groupIndex := compileGroupIndex(recordType, accessors, idx)
			if usedGroupTags[groupIndex.tag] {
				panic(fmt.Sprintf("db: %s declares two GroupBy on the same Keys", recordType.Name()))
			}
			usedGroupTags[groupIndex.tag] = true
			meta.groupIndexes = append(meta.groupIndexes, groupIndex)
		}
		// A delta, local or fan-out index lives in hidden base-table rows, with no GSI slot.
		if idx.Type == TypeDelta || idx.Type == TypeLocal || holdsSliceColumn(idx) {
			var resolved arrayIndexMeta
			switch {
			case idx.Type == TypeDelta:
				resolved = compileDeltaIndex(recordType, accessors, idx, *meta.updated)
			case idx.Type == TypeLocal:
				resolved = compileLocalIndex(recordType, accessors, idx)
			default:
				resolved = resolveArrayIndex(recordType, accessors, idx)
			}
			if usedRowColumnIDs[resolved.columnID] {
				panic(fmt.Sprintf("db: %s declares two hidden-rows indexes named by cb id %s: a slice field takes one, and so does the first key of a delta or local index",
					recordType.Name(), resolved.columnID))
			}
			usedRowColumnIDs[resolved.columnID] = true
			meta.arrayIndexes = append(meta.arrayIndexes, resolved)
			continue
		}
		if idx.Slot.index == "" {
			if len(idx.GroupBy) > 0 {
				continue // a GroupBy alone: counters, no GSI
			}
			panic(fmt.Sprintf("db: %s has an index with no Slot (only an Index holding a ColSlice or a GroupBy goes without one)", recordType.Name()))
		}
		if idx.FullCopy {
			panic(fmt.Sprintf("db: %s index %s sets FullCopy, which only applies to an Index holding a ColSlice", recordType.Name(), idx.Slot.index))
		}
		if usedSlots[idx.Slot.index] {
			panic(fmt.Sprintf("db: %s reuses slot %s", recordType.Name(), idx.Slot.index))
		}
		usedSlots[idx.Slot.index] = true
		meta.indexes = append(meta.indexes, compileGSI(recordType, accessors, idx, meta))
	}
	meta.dataFrames = compileDataFrames(schema, recordType, accessors, meta)

	pkDigits := len(meta.tableID) + meta.partitionDigits
	// Fan-out rows, the by-IDs slots item, the GroupBy counters and the frame states append 3 digits to the base pk.
	if len(meta.arrayIndexes) > 0 || len(meta.groupIndexes) > 0 || len(meta.dataFrames) > 0 || schema.CacheByIDs {
		pkDigits += arrayIndexColumnIDDigits
	}
	if pkDigits > maxNumericKeyDigits {
		panic(fmt.Sprintf("db: %s pk takes %d digits, over DynamoDB's %d: shrink the partition Size(bits)",
			recordType.Name(), pkDigits, maxNumericKeyDigits))
	}

	if schema.UseAutoincrement {
		meta.autoinc = resolveAutoincrement(schema, recordType, accessors, meta.tableID)
	}
	if schema.CacheByIDs {
		meta.cacheByIDs = resolveCacheByIDs(recordType, meta.keys)
	}

	return meta
}

// compileGSI resolves a slot Index. Its hash is the index Partition, or the
// entity's when it declares none. Its range is the index Keys followed by the
// base Keys they leave out: that keeps every range value unique and ordered, and
// a GSI on (ClientID) still ranges on the base Keys once ClientID is pinned.
func compileGSI(recordType reflect.Type, accessors map[string]*colAccessor, idx Index, meta *tableMeta) indexMeta {
	if len(idx.Keys) == 0 {
		panic(fmt.Sprintf("db: %s index %s declares no Keys", recordType.Name(), idx.Slot.index))
	}
	gsi := indexMeta{slot: idx.Slot, partition: meta.partition}
	if len(idx.Partition) > 0 {
		gsi.partition = resolveKeyCols(recordType, accessors, idx.Partition)
		hashDigits := len(meta.tableID)
		for _, partitionCol := range gsi.partition {
			if !partitionCol.kind.isInteger() {
				panic(fmt.Sprintf("db: %s index %s partition column %q must be an integer: the GSI hash is a number",
					recordType.Name(), idx.Slot.index, partitionCol.fieldName))
			}
			hashDigits += decimalWidth(partitionCol.bits)
		}
		if hashDigits > maxNumericKeyDigits {
			panic(fmt.Sprintf("db: %s index %s hash takes %d digits, over DynamoDB's %d: shrink the partition Size(bits)",
				recordType.Name(), idx.Slot.index, hashDigits, maxNumericKeyDigits))
		}
	}
	gsi.sortColumns = resolveKeyCols(recordType, accessors, idx.Keys)
	for _, baseKey := range meta.keys {
		if !slices.ContainsFunc(gsi.sortColumns, func(column keyCol) bool { return column.fieldName == baseKey.fieldName }) {
			gsi.sortColumns = append(gsi.sortColumns, baseKey)
		}
	}
	return gsi
}

// resolveTableID returns the schema's explicit TableID, or HashTableID(Entity) when it is 0.
func resolveTableID(schema Schema) int32 {
	if schema.TableID == 0 {
		return HashTableID(schema.Entity)
	}
	if schema.TableID < 10_000_000 || schema.TableID > 99_999_999 {
		panic(fmt.Sprintf("db: %q TableID %d must have exactly 8 digits (10000000..99999999)", schema.Entity, schema.TableID))
	}
	return schema.TableID
}

// autoincFieldName is the record field the ORM fills for UseAutoincrement.
const autoincFieldName = "ID"

// resolveAutoincrement validates the autoincrement declaration and precompiles
// the read/write accessors for the ID field. The record must declare an integer
// field named "ID"; the padding must be 0..9 so seq*10^padding stays well within
// int64.
func resolveAutoincrement(schema Schema, recordType reflect.Type, accessors map[string]*colAccessor, tableID string) *autoincConfig {
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
		seqName: tableID, // keyed by TableID, so renaming the Entity keeps the counter
		padding: padding,
		factor:  pow10(padding),
		get:     acc.getI64,
		set:     acc.setI64,
	}
}

// assignAutoIDs reserves and assigns IDs for the records whose ID is still zero,
// and returns those records. It reserves the whole batch in one atomic sequence
// bump, then lays each reserved value (+ random low digits) into its record. A
// no-op when the entity has no autoincrement or every record already carries an ID.
func (m *tableMeta) assignAutoIDs(ptrs []unsafe.Pointer) ([]unsafe.Pointer, error) {
	if m.autoinc == nil {
		return nil, nil
	}
	var need []unsafe.Pointer
	for _, p := range ptrs {
		if m.autoinc.get(p) == 0 {
			need = append(need, p)
		}
	}
	if len(need) == 0 {
		return nil, nil
	}
	base, err := reserveSequence(m.autoinc.seqName, len(need))
	if err != nil {
		return nil, err
	}
	for i, p := range need {
		m.autoinc.set(p, m.autoinc.composeID(base+int64(i), m.autoinc.randDigits()))
	}
	return need, nil
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
// accessor. Every integer key column must declare .Size(bits): it fixes the
// column's width in every key shape (Base64 in sk and string slots, decimal after
// the TableID in pk and numeric slots), which keeps keys sortable and tables apart.
func resolveKeyCols(recordType reflect.Type, accessors map[string]*colAccessor, cols []Coln) []keyCol {
	out := make([]keyCol, 0, len(cols))
	for _, c := range cols {
		m := c.col()
		acc, ok := accessors[m.fieldName]
		if !ok {
			panic(fmt.Sprintf("db: key column %q is not an exported field of %s", m.fieldName, recordType.Name()))
		}
		if m.kind.isInteger() && m.bits <= 0 {
			panic(fmt.Sprintf("db: numeric key column %q must declare .Size(bits)", m.fieldName))
		}
		out = append(out, keyCol{fieldName: m.fieldName, kind: m.kind, bits: m.bits, acc: acc})
	}
	return out
}
