package pgdriver

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// pgTypeNames maps each ORM type ID (db/coltype.go) to its PostgreSQL column type. Pointers are the
// same column type, nullable. index_mapping.go overrides two cases per column:
//   - a string in the primary key or an index key compares bytes (COLLATE "C"), as Scylla does;
//   - a slice an index keys on is JSONB, for its GIN index.
//
// Every other slice, and every blob, is colbin bytes in a BYTEA (the Scylla codec's bytes).
var pgTypeNames = map[int8]string{
	1: "TEXT", 2: "BIGINT", 3: "INTEGER", 4: "SMALLINT", 5: "SMALLINT", 6: "REAL", 7: "DOUBLE PRECISION", 8: "BOOLEAN",
	db.TypeBlob: "BYTEA", 10: "BIGINT",
	11: "BYTEA", 12: "BYTEA", 13: "BYTEA", 14: "BYTEA", 15: "BYTEA", 16: "BYTEA", 17: "BYTEA",
	21: "TEXT", 22: "BIGINT", 23: "INTEGER", 24: "SMALLINT", 25: "SMALLINT", 26: "REAL", 27: "DOUBLE PRECISION",
	31: "BYTEA", 32: "BYTEA", 33: "BYTEA", 34: "BYTEA", 35: "BYTEA", 36: "BYTEA", 37: "BYTEA",
}

// keyedStringType is a string in a key: compared byte by byte like Scylla, not by the locale.
// CockroachDB's STRING already compares bytes and has no "C" collation.
func keyedStringType() string {
	if isCockroachDB() {
		return "TEXT"
	}
	return `TEXT COLLATE "C"`
}

// pgValueCodec is the db.Codec of this driver (§7.2): a statement value is the Go value itself, a
// slice or complex value its colbin bytes; a value assigned to a field is converted by reflection.
// The write and read paths bind and scan rows themselves (values.go), since only the compiled table
// knows which slices are JSON.
type pgValueCodec struct{}

func (pgValueCodec) RenderValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	return c.GetRawValue(ptr)
}

func (pgValueCodec) EncodeStatementValue(c *db.ColumnInfo, ptr unsafe.Pointer) any {
	if c.IsComplexType && !isByteSliceField(c) {
		encoded, err := colbin.Marshal(c.Field.Interface(ptr))
		if err != nil {
			panic(fmt.Sprintf("pgdriver: column %q cannot be colbin-encoded: %v", c.Name, err))
		}
		return encoded
	}
	return c.Field.Interface(ptr)
}

func (pgValueCodec) AssignValue(c *db.ColumnInfo, ptr unsafe.Pointer, value any) {
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

// CompileDriverAccessors: statements bind values, never collection literals, so there is nothing to compile.
func (pgValueCodec) CompileDriverAccessors(c *db.ColumnInfo) {}

// The collection hints (list / set / frozen) mean nothing on PostgreSQL (O2).
func (pgValueCodec) ApplyCollectionOptions(recordTypeName, fieldName string, ct db.ColType, tag db.DBTag) db.ColType {
	return ct
}

func (pgValueCodec) ApplyCollectionDefaults(ct db.ColType, useListAsDefault, applyFrozenDefault, frozen bool) db.ColType {
	return ct
}

func isByteSliceField(c *db.ColumnInfo) bool {
	return c.Field != nil && c.Field.Type.Kind() == reflect.Slice && c.Field.Type.Elem().Kind() == reflect.Uint8
}
