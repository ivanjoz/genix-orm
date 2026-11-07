package mariadbdriver

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// mariaTypeNames maps each ORM type ID (db/coltype.go) to its MariaDB column type
// (docs/OCEANBASE_PLAN.md §7.1). Pointers are the same column type, nullable. These are the
// defaults; index_mapping.go overrides two cases per column:
//   - a string in the primary key or an index key is VARCHAR(512), since LONGTEXT cannot be keyed (D1);
//   - a slice an index keys on is JSON, for its multi-value index (O11).
//
// Every other slice, and every blob, is colbin bytes in a LONGBLOB (the Scylla codec's bytes).
var mariaTypeNames = map[int8]string{
	1: "LONGTEXT", 2: "BIGINT", 3: "INT", 4: "SMALLINT", 5: "TINYINT", 6: "FLOAT", 7: "DOUBLE", 8: "BOOL",
	db.TypeBlob: "LONGBLOB", 10: "BIGINT",
	11: "LONGBLOB", 12: "LONGBLOB", 13: "LONGBLOB", 14: "LONGBLOB", 15: "LONGBLOB", 16: "LONGBLOB", 17: "LONGBLOB",
	21: "LONGTEXT", 22: "BIGINT", 23: "INT", 24: "SMALLINT", 25: "TINYINT", 26: "FLOAT", 27: "DOUBLE",
	31: "LONGBLOB", 32: "LONGBLOB", 33: "LONGBLOB", 34: "LONGBLOB", 35: "LONGBLOB", 36: "LONGBLOB", 37: "LONGBLOB",
}

// keyedStringType is D1: a string in a key, long enough for codes and names, short enough for an
// index key. A longer value is refused by strict SQL mode, never truncated.
const keyedStringType = "VARCHAR(512)"

// mariaValueCodec is the db.Codec of this driver (§7.2): a statement value is the Go value itself, a
// slice or complex value its colbin bytes; a value assigned to a field is converted by reflection.
// The write and read paths bind and scan rows themselves (values.go), since only the compiled table
// knows which slices are JSON.
type mariaValueCodec struct{}

func (mariaValueCodec) RenderValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	return c.GetRawValue(ptr)
}

func (mariaValueCodec) EncodeStatementValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	if c.IsComplexType && !isByteSliceField(c) {
		encoded, err := colbin.Marshal(c.Field.Interface(ptr))
		if err != nil {
			panic(fmt.Sprintf("mariadbdriver: column %q cannot be colbin-encoded: %v", c.Name, err))
		}
		return encoded
	}
	return c.Field.Interface(ptr)
}

func (mariaValueCodec) AssignValue(c *db.ColumnInfo, ptr unsafe.Pointer, value any) {
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

// CompileDriverAccessors: MariaDB has no collection literals, so there is nothing to compile.
func (mariaValueCodec) CompileDriverAccessors(c *db.ColumnInfo) {}

// The collection hints (list / set / frozen) mean nothing on MariaDB.
func (mariaValueCodec) ApplyCollectionOptions(recordTypeName, fieldName string, ct db.ColType, tag db.DBTag) db.ColType {
	return ct
}

func (mariaValueCodec) ApplyCollectionDefaults(ct db.ColType, useListAsDefault, applyFrozenDefault, frozen bool) db.ColType {
	return ct
}

func isByteSliceField(c *db.ColumnInfo) bool {
	return c.Field != nil && c.Field.Type.Kind() == reflect.Slice && c.Field.Type.Elem().Kind() == reflect.Uint8
}
