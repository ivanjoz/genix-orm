package scylla

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Guards the seam introduced when the ORM type table moved to db: db owns the Go
// type names, this driver names each ID in CQL. A mismatch here would silently
// change generated DDL.
func TestCQLTypeNamesMatchEveryORMTypeID(t *testing.T) {
	want := map[string]string{
		"string": "text", "int64": "bigint", "int32": "int", "int16": "smallint",
		"int8": "tinyint", "float32": "float", "float64": "double", "bool": "boolean",
		"[]byte": "blob", "[]string": "set<text>", "[]int64": "set<bigint>",
		"[]int32": "set<int>", "[]int16": "set<smallint>", "[]int8": "set<tinyint>",
		"[]float32": "set<float>", "[]float64": "set<double>",
		"*[]string": "set<text>", "*[]int32": "set<int>",
	}
	for goType, cqlType := range want {
		resolved := db.GetColTypeByName(goType)
		if resolved.Type == 0 {
			t.Fatalf("%s did not resolve to an ORM type ID", goType)
		}
		if resolved.DBType != cqlType {
			t.Errorf("%s: DBType = %q, want %q", goType, resolved.DBType, cqlType)
		}
	}
	// *string must resolve to the pointer entry (21), not the value entry (1).
	if pointerString := db.GetColTypeByName("*string"); pointerString.Type != 21 || !pointerString.IsPointer {
		t.Errorf(`"*string" resolved to Type %d (IsPointer=%v), want 21/true`, pointerString.Type, pointerString.IsPointer)
	}
}
