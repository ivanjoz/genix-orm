package db

import (
	"unsafe"

	"github.com/viant/xunsafe"
)

// SetField writes a typed value straight into a record field through its
// precomputed offset — no reflection, no interface boxing.
func SetField[T any](field *xunsafe.Field, ptr unsafe.Pointer, value T) {
	*(*T)(field.Pointer(ptr)) = value
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
