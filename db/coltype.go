package db

import "sync"

// ColType describes how one record field maps onto storage.
//
// Type is a stable ORM type ID. It is switched on by the accessor engine and it
// leaks into persisted data (packed keys, blob encodings), so IDs must never be
// renumbered. FieldType is the Go type name the ID was resolved from. DBType is
// that same column named in the *active driver's* type system ("bigint" on
// Scylla, "N" on Dynamo) and is filled from DBTypeResolver, which is what keeps
// the ID table itself backend-agnostic.
type ColType struct {
	Type          int8
	FieldType     string
	DBType        string
	IsSlice       bool
	IsPointer     bool
	IsComplexType bool
}

// TypeBlob is the catch-all ID for values with no native driver representation
// (structs, maps, unsigned integers). They are stored as an opaque blob.
const TypeBlob int8 = 9

// goTypes maps each ORM type ID onto its Go type name and shape flags. Slice
// entries carry no collection kind here — whether a slice becomes a set, a list
// or a frozen collection is the driver's decision, applied to DBType later.
var goTypes = []ColType{
	{Type: 1, FieldType: "string"},
	{Type: 2, FieldType: "int64"},
	{Type: 3, FieldType: "int32"},
	{Type: 4, FieldType: "int16"},
	{Type: 5, FieldType: "int8"},
	{Type: 6, FieldType: "float32"},
	{Type: 7, FieldType: "float64"},
	{Type: 8, FieldType: "bool"},
	{Type: TypeBlob, FieldType: "[]byte", IsComplexType: true},
	{Type: 11, FieldType: "[]string", IsSlice: true},
	{Type: 12, FieldType: "[]int64", IsSlice: true},
	{Type: 13, FieldType: "[]int32", IsSlice: true},
	{Type: 14, FieldType: "[]int16", IsSlice: true},
	{Type: 15, FieldType: "[]int8", IsSlice: true},
	{Type: 16, FieldType: "[]float32", IsSlice: true},
	{Type: 17, FieldType: "[]float64", IsSlice: true},
	{Type: 21, FieldType: "*string", IsPointer: true},
	{Type: 22, FieldType: "*int64", IsPointer: true},
	{Type: 23, FieldType: "*int32", IsPointer: true},
	{Type: 24, FieldType: "*int16", IsPointer: true},
	{Type: 25, FieldType: "*int8", IsPointer: true},
	{Type: 26, FieldType: "*float32", IsPointer: true},
	{Type: 27, FieldType: "*float64", IsPointer: true},
	{Type: 31, FieldType: "*[]string", IsSlice: true, IsPointer: true},
	{Type: 32, FieldType: "*[]int64", IsSlice: true, IsPointer: true},
	{Type: 33, FieldType: "*[]int32", IsSlice: true, IsPointer: true},
	{Type: 34, FieldType: "*[]int16", IsSlice: true, IsPointer: true},
	{Type: 35, FieldType: "*[]int8", IsSlice: true, IsPointer: true},
	{Type: 36, FieldType: "*[]float32", IsSlice: true, IsPointer: true},
	{Type: 37, FieldType: "*[]float64", IsSlice: true, IsPointer: true},
}

// DBTypeResolver is installed by the active driver to name an ORM type ID in the
// driver's own type system. It is consulted on every resolve rather than baked
// into the ID table, so swapping drivers cannot leave stale type names behind.
var DBTypeResolver func(typeID int8) string

var (
	colTypesByID        = map[int8]ColType{}
	colTypesByFieldType = map[string]ColType{}
	initColTypesOnce    sync.Once
)

func initColTypes() {
	initColTypesOnce.Do(func() {
		for _, columnType := range goTypes {
			colTypesByID[columnType.Type] = columnType
			colTypesByFieldType[columnType.FieldType] = columnType
			colTypesByFieldType["*"+columnType.FieldType] = columnType
		}
	})
}

// GetColTypeByID resolves an ORM type ID, naming it in the active driver's type
// system. An unknown ID yields the zero ColType (Type == 0), which callers treat
// as "not a recognised column type".
func GetColTypeByID(typeID int8) ColType {
	initColTypes()
	return withDBType(colTypesByID[typeID])
}

// GetColTypeByName resolves a Go type name (as printed by reflect.Type.String())
// to its ORM type. Pointer forms resolve to the same entry as their base type.
func GetColTypeByName(goTypeName string) ColType {
	initColTypes()
	if goTypeName == "" {
		return ColType{}
	}
	return withDBType(colTypesByFieldType[goTypeName])
}

func withDBType(columnType ColType) ColType {
	if columnType.Type != 0 && DBTypeResolver != nil {
		columnType.DBType = DBTypeResolver(columnType.Type)
	}
	return columnType
}
