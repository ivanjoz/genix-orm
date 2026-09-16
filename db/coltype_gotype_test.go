package db

import (
	"reflect"
	"testing"
)

type movementKind int8

type documentCode string

type ratioFactor float64

// A declared scalar has to store as the scalar it is. It used to resolve by name
// only, miss the table, and reach the caller's TypeBlob catch-all -- so the
// column was created as a blob and handed to the serializer on every write,
// which refuses a bare int8 at the root and left the column empty. The failure
// was silent, which is why this is pinned.
func TestNamedScalarResolvesByUnderlyingKind(t *testing.T) {
	for _, want := range []struct {
		goType    reflect.Type
		fieldType string
	}{
		{reflect.TypeFor[movementKind](), "int8"},
		{reflect.TypeFor[documentCode](), "string"},
		{reflect.TypeFor[ratioFactor](), "float64"},
		{reflect.TypeFor[*movementKind](), "*int8"},
		// An unnamed scalar keeps resolving by name, as it always did.
		{reflect.TypeFor[int32](), "int32"},
		{reflect.TypeFor[[]string](), "[]string"},
	} {
		got := GetColTypeByGoType(want.goType)
		if got.FieldType != want.fieldType {
			t.Errorf("%s resolved to %q, want %q", want.goType, got.FieldType, want.fieldType)
		}
		if got.IsComplexType {
			t.Errorf("%s resolved to a complex type, so it would be serialized rather than stored", want.goType)
		}
	}
}

// What genuinely has no native form still resolves to nothing, leaving the
// caller's blob catch-all to apply. Only the kind fallback is new.
func TestNonScalarStillResolvesToNothing(t *testing.T) {
	type payload struct{ A int32 }
	for _, goType := range []reflect.Type{
		reflect.TypeFor[payload](),
		reflect.TypeFor[[]payload](),
		reflect.TypeFor[map[string]int32](),
		// A bare `int` has no entry, so its width stays the declaration's to state.
		reflect.TypeFor[int](),
	} {
		if got := GetColTypeByGoType(goType); got.Type != 0 {
			t.Errorf("%s resolved to type %d (%q), want no native mapping",
				goType, got.Type, got.FieldType)
		}
	}
}
