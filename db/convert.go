package db

import (
	"reflect"
	"unsafe"

	"github.com/viant/xunsafe"
)

// SetField writes a typed value straight into a record field through its
// precomputed offset — no reflection, no interface boxing.
func SetField[T any](field *xunsafe.Field, ptr unsafe.Pointer, value T) {
	*(*T)(field.Pointer(ptr)) = value
}

// NormalizeNamedNumeric converts a value whose dynamic type is a *named* numeric type
// — `type CashMovementType int8` — into its plain underlying Go type.
//
// It exists because a type switch tests the exact type, not the kind: `case int8:` never
// fires for a named int8, so every numeric type switch in the ORM silently misses named
// types. `db.Col[T, E any]` has no constraint, so such a column compiles fine and then
// misbehaves at runtime. Normalizing lets each switch keep its cheap exact-type cases and
// handle named types on the fallback path only — no reflection on the hot path.
//
// ok is false when there is nothing to normalize (already a predeclared type, or not
// numeric at all). That is what makes recursion safe at the call sites: the normalized
// value is a predeclared type, so a second call always returns false.
func NormalizeNamedNumeric(value any) (any, bool) {
	reflected := reflect.ValueOf(value)
	for reflected.Kind() == reflect.Pointer {
		if reflected.IsNil() {
			return nil, false
		}
		reflected = reflected.Elem()
	}
	// A predeclared type has no package path, and the switches already match it directly.
	if !reflected.IsValid() || reflected.Type().PkgPath() == "" {
		return nil, false
	}

	switch reflected.Kind() {
	case reflect.Int:
		return int(reflected.Int()), true
	case reflect.Int8:
		return int8(reflected.Int()), true
	case reflect.Int16:
		return int16(reflected.Int()), true
	case reflect.Int32:
		return int32(reflected.Int()), true
	case reflect.Int64:
		return reflected.Int(), true
	case reflect.Uint:
		return uint(reflected.Uint()), true
	case reflect.Uint8:
		return uint8(reflected.Uint()), true
	case reflect.Uint16:
		return uint16(reflected.Uint()), true
	case reflect.Uint32:
		return uint32(reflected.Uint()), true
	case reflect.Uint64:
		return reflected.Uint(), true
	case reflect.Float32:
		return float32(reflected.Float()), true
	case reflect.Float64:
		return reflected.Float(), true
	}
	return nil, false
}

// ToInt64 widens any signed-integer value (or pointer to one) to int64. It
// returns 0 for anything else, which callers treat as "absent".
func ToInt64(value any) int64 {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue)
	case *int:
		return int64(*typedValue)
	case int64:
		return typedValue
	case *int64:
		return *typedValue
	case int32:
		return int64(typedValue)
	case *int32:
		return int64(*typedValue)
	case int16:
		return int64(typedValue)
	case *int16:
		return int64(*typedValue)
	case int8:
		return int64(typedValue)
	case *int8:
		return int64(*typedValue)
	case uint:
		return int64(typedValue)
	case uint8:
		return int64(typedValue)
	case uint16:
		return int64(typedValue)
	case uint32:
		return int64(typedValue)
	case uint64:
		return int64(typedValue)
	}
	if plainValue, isNamed := NormalizeNamedNumeric(value); isNamed {
		return ToInt64(plainValue)
	}
	return 0
}

// ToFloat64 converts any float value (or pointer to one) to float64.
func ToFloat64(value any) float64 {
	switch typedValue := value.(type) {
	case float64:
		return typedValue
	case *float64:
		return *typedValue
	case float32:
		return float64(typedValue)
	case *float32:
		return float64(*typedValue)
	}
	if plainValue, isNamed := NormalizeNamedNumeric(value); isNamed {
		return ToFloat64(plainValue)
	}
	return 0
}

// ToFloat32 converts any float value (or pointer to one) to float32.
func ToFloat32(value any) float32 {
	switch typedValue := value.(type) {
	case float32:
		return typedValue
	case *float32:
		return *typedValue
	case float64:
		return float32(typedValue)
	case *float64:
		return float32(*typedValue)
	}
	if plainValue, isNamed := NormalizeNamedNumeric(value); isNamed {
		return ToFloat32(plainValue)
	}
	return 0
}

func coerceString(value any) string {
	switch typedValue := value.(type) {
	case string:
		return typedValue
	case *string:
		if typedValue != nil {
			return *typedValue
		}
	}
	return ""
}

func coerceBool(value any) bool {
	switch typedValue := value.(type) {
	case bool:
		return typedValue
	case *bool:
		if typedValue != nil {
			return *typedValue
		}
	}
	return false
}
