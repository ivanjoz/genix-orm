package fdbdriver

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// fdbTypeNames names each ORM type ID. FoundationDB has no column types (a row is bytes, see
// rowcodec.go), so the name only shows in diagnostics.
var fdbTypeNames = map[int8]string{
	1: "string", 2: "int64", 3: "int32", 4: "int16", 5: "int8", 6: "float32", 7: "float64", 8: "bool",
	db.TypeBlob: "blob", 10: "int",
	11: "[]string", 12: "[]int64", 13: "[]int32", 14: "[]int16", 15: "[]int8", 16: "[]float32", 17: "[]float64",
	21: "string", 22: "int64", 23: "int32", 24: "int16", 25: "int8", 26: "float32", 27: "float64",
	31: "[]string", 32: "[]int64", 33: "[]int32", 34: "[]int16", 35: "[]int8", 36: "[]float32", 37: "[]float64",
}

// fdbValueCodec is the db.Codec of this driver. The row codec reads and writes record fields
// directly, so this only serves the shared accessor engine's fallbacks: a statement value is the Go
// value itself, and a value assigned to a field is converted by reflection.
type fdbValueCodec struct{}

func (fdbValueCodec) RenderValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	return c.GetRawValue(ptr)
}

func (fdbValueCodec) EncodeStatementValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	if c.IsComplexType && !isByteSliceField(c) {
		encoded, err := colbin.Marshal(c.Field.Interface(ptr))
		if err != nil {
			panic(fmt.Sprintf("fdbdriver: column %q cannot be colbin-encoded: %v", c.Name, err))
		}
		return encoded
	}
	return c.Field.Interface(ptr)
}

func (fdbValueCodec) AssignValue(c *db.ColumnInfo, ptr unsafe.Pointer, value any) {
	if c.Field == nil || value == nil {
		return
	}
	destination := reflect.NewAt(c.Field.Type, c.Field.Pointer(ptr)).Elem()
	assigned := reflect.ValueOf(value)
	if destination.Kind() == reflect.Pointer && assigned.Kind() != reflect.Pointer {
		boxed := reflect.New(destination.Type().Elem())
		if assigned.Type().ConvertibleTo(boxed.Elem().Type()) {
			boxed.Elem().Set(assigned.Convert(boxed.Elem().Type()))
			destination.Set(boxed)
		}
		return
	}
	if assigned.Type().AssignableTo(destination.Type()) {
		destination.Set(assigned)
	} else if assigned.Type().ConvertibleTo(destination.Type()) {
		destination.Set(assigned.Convert(destination.Type()))
	}
}

func (fdbValueCodec) CompileDriverAccessors(c *db.ColumnInfo) {}

func (fdbValueCodec) ApplyCollectionOptions(recordTypeName, fieldName string, ct db.ColType, tag db.DBTag) db.ColType {
	return ct
}

func (fdbValueCodec) ApplyCollectionDefaults(ct db.ColType, useListAsDefault, applyFrozenDefault, frozen bool) db.ColType {
	return ct
}

func isByteSliceField(c *db.ColumnInfo) bool {
	return c.Field != nil && c.Field.Type.Kind() == reflect.Slice && c.Field.Type.Elem().Kind() == reflect.Uint8
}
