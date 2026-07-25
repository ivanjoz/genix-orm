package db

import (
	"slices"
	"strconv"
	"unsafe"
)

// CompileFastAccessors builds type-specialized read/write closures for a column
// once, so hot scan and write loops never pay for reflection or interface
// boxing. Everything here is derived from the Go type alone; the driver overlays
// its own storage-specific accessors at the end through
// Codec.CompileDriverAccessors.
func (c *ColumnInfo) CompileFastAccessors() {
	if c.Field == nil {
		return
	}
	// Do not override custom virtual-key/view accessors that already encode business-specific logic.
	if c.GetRawValueFn != nil || c.GetValueFn != nil || c.GetStatementValueFn != nil || c.SetValueFn != nil {
		return
	}

	c.compileValueAccessors()
	c.compileTokenAccessors()

	if codec != nil {
		codec.CompileDriverAccessors(c)
	}
}

// assignFallback routes a value the fast setters could not place onto the
// driver's generic conversion path.
func (c *ColumnInfo) assignFallback(ptr unsafe.Pointer, v any) {
	if codec != nil {
		codec.AssignValue(c, ptr, v)
	}
}

func (c *ColumnInfo) compileValueAccessors() {
	switch c.Type {
	// Scalar fast paths use direct xunsafe typed accessors to avoid interface boxing
	// and repeated type-switch work in hot scan/write loops.
	case 1: // string
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.String(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetString(ptr, coerceString(v)) }
	case 2: // int64
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Int64(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetInt64(ptr, ToInt64(v)) }
	case 3: // int32
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Int32(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetInt32(ptr, int32(ToInt64(v))) }
	case 4: // int16
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Int16(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetInt16(ptr, int16(ToInt64(v))) }
	case 5: // int8
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Int8(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetInt8(ptr, int8(ToInt64(v))) }
	case 6: // float32
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Float32(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetFloat32(ptr, ToFloat32(v)) }
	case 7: // float64
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Float64(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetFloat64(ptr, ToFloat64(v)) }
	case 8: // bool
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Bool(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetBool(ptr, coerceBool(v)) }
	case 10: // int
		c.GetRawValueFn = func(ptr unsafe.Pointer) any { return c.Field.Int(ptr) }
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) { c.Field.SetInt(ptr, int(ToInt64(v))) }
	// Hot slice types get exact-type fast setters. The statement literal for a
	// collection is driver-specific, so GetValueFn is left to the codec.
	// Any non-exact input type intentionally falls back to generic conversion.
	case 11: // []string
		c.GetRawValueFn = sliceReader[string](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSliceExact[string](c)
	case 12: // []int64
		c.GetRawValueFn = sliceReader[int64](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSliceExact[int64](c)
	case 13: // []int32
		c.GetRawValueFn = sliceReader[int32](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSliceExact[int32](c)
	case 14: // []int16
		c.GetRawValueFn = sliceReader[int16](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSliceExact[int16](c)
	case 15: // []int8
		c.GetRawValueFn = sliceReader[int8](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSliceExact[int8](c)
	// Pointer scalar paths preserve nil semantics and avoid reflection.Interface calls.
	// Fast setters cover exact common assignments while keeping fallback compatibility.
	case 21: // *string
		c.GetRawValueFn = pointerReader[string](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			switch typedValue := v.(type) {
			case string:
				stringValue := typedValue
				SetField(c.Field, ptr, &stringValue)
				return
			case *string:
				SetField(c.Field, ptr, typedValue)
				return
			}
			c.assignFallback(ptr, v)
		}
	case 22: // *int64
		c.GetRawValueFn = pointerReader[int64](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			int64Value := ToInt64(v)
			SetField(c.Field, ptr, &int64Value)
		}
	case 23: // *int32
		c.GetRawValueFn = pointerReader[int32](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			int32Value := int32(ToInt64(v))
			SetField(c.Field, ptr, &int32Value)
		}
	case 24: // *int16
		c.GetRawValueFn = pointerReader[int16](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			int16Value := int16(ToInt64(v))
			SetField(c.Field, ptr, &int16Value)
		}
	case 25: // *int8
		c.GetRawValueFn = pointerReader[int8](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			int8Value := int8(ToInt64(v))
			SetField(c.Field, ptr, &int8Value)
		}
	case 26: // *float32
		c.GetRawValueFn = pointerReader[float32](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			float32Value := ToFloat32(v)
			SetField(c.Field, ptr, &float32Value)
		}
	case 27: // *float64
		c.GetRawValueFn = pointerReader[float64](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			float64Value := ToFloat64(v)
			SetField(c.Field, ptr, &float64Value)
		}
	case 28: // *int
		c.GetRawValueFn = pointerReader[int](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = func(ptr unsafe.Pointer, v any) {
			intValue := int(ToInt64(v))
			SetField(c.Field, ptr, &intValue)
		}
	// Pointer-to-slice paths mirror slice optimizations but retain nil pointer identity.
	// We clone on write to avoid aliasing caller-owned backing arrays.
	case 31: // *[]string
		c.GetRawValueFn = pointerReader[[]string](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSlicePointerExact[string](c)
	case 32: // *[]int64
		c.GetRawValueFn = pointerReader[[]int64](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSlicePointerExact[int64](c)
	case 33: // *[]int32
		c.GetRawValueFn = pointerReader[[]int32](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSlicePointerExact[int32](c)
	case 34: // *[]int16
		c.GetRawValueFn = pointerReader[[]int16](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSlicePointerExact[int16](c)
	case 35: // *[]int8
		c.GetRawValueFn = pointerReader[[]int8](c)
		c.GetStatementValueFn = c.GetRawValueFn
		c.SetValueFn = setSlicePointerExact[int8](c)
	case 36, 37:
		// Less-used float pointer-slice types stay generic to keep fast-path code size controlled.
		// Pointer/slice pointer columns keep generic conversion to preserve legacy nil/value semantics.
		c.GetRawValueFn = func(ptr unsafe.Pointer) any {
			if c.Field.IsNil(ptr) {
				return nil
			}
			return c.Field.Interface(ptr)
		}
		c.GetStatementValueFn = c.GetRawValueFn
	}
}

// compileTokenAccessors precomputes string-token and equality accessors for scalar
// columns so hot merge/index token paths avoid interface boxing and fmt reflection.
// Non-scalar and pointer types fall back to the GetValueString/FieldsEqual methods,
// preserving their existing semantics.
func (c *ColumnInfo) compileTokenAccessors() {
	switch c.Type {
	case 1: // string
		c.GetValueStringFn = func(ptr unsafe.Pointer) string { return c.Field.String(ptr) }
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.String(a) == c.Field.String(b) }
	case 2: // int64
		c.GetValueStringFn = func(ptr unsafe.Pointer) string { return strconv.FormatInt(c.Field.Int64(ptr), 10) }
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Int64(a) == c.Field.Int64(b) }
	case 3: // int32
		c.GetValueStringFn = func(ptr unsafe.Pointer) string {
			return strconv.FormatInt(int64(c.Field.Int32(ptr)), 10)
		}
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Int32(a) == c.Field.Int32(b) }
	case 4: // int16
		c.GetValueStringFn = func(ptr unsafe.Pointer) string {
			return strconv.FormatInt(int64(c.Field.Int16(ptr)), 10)
		}
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Int16(a) == c.Field.Int16(b) }
	case 5: // int8
		c.GetValueStringFn = func(ptr unsafe.Pointer) string {
			return strconv.FormatInt(int64(c.Field.Int8(ptr)), 10)
		}
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Int8(a) == c.Field.Int8(b) }
	case 8: // bool
		c.GetValueStringFn = func(ptr unsafe.Pointer) string { return strconv.FormatBool(c.Field.Bool(ptr)) }
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Bool(a) == c.Field.Bool(b) }
	case 10: // int
		c.GetValueStringFn = func(ptr unsafe.Pointer) string {
			return strconv.FormatInt(int64(c.Field.Int(ptr)), 10)
		}
		c.FieldsEqualFn = func(a, b unsafe.Pointer) bool { return c.Field.Int(a) == c.Field.Int(b) }
	}
}

// pointerReader reads a *T field, returning an untyped nil (not a typed nil
// pointer) when the field is unset so callers can test it with `== nil`.
func pointerReader[T any](c *ColumnInfo) func(unsafe.Pointer) any {
	return func(ptr unsafe.Pointer) any {
		valuePointer := *(**T)(c.Field.Pointer(ptr))
		if valuePointer == nil {
			return nil
		}
		return valuePointer
	}
}

// sliceReader reads a []T field without boxing the elements.
func sliceReader[T any](c *ColumnInfo) func(unsafe.Pointer) any {
	return func(ptr unsafe.Pointer) any { return *(*[]T)(c.Field.Pointer(ptr)) }
}

// setSliceExact stores a []T (or *[]T) into a []T field, cloning so the record
// never aliases the caller's backing array. The element type is pinned to the
// column's own type: any other input falls back to the driver's generic
// conversion rather than being written straight into the field's memory.
func setSliceExact[T any](c *ColumnInfo) func(unsafe.Pointer, any) {
	return func(ptr unsafe.Pointer, v any) {
		switch typedValue := v.(type) {
		case []T:
			SetField(c.Field, ptr, slices.Clone(typedValue))
		case *[]T:
			if typedValue == nil {
				SetField(c.Field, ptr, []T(nil))
				return
			}
			SetField(c.Field, ptr, slices.Clone(*typedValue))
		default:
			c.assignFallback(ptr, v)
		}
	}
}

// setSlicePointerExact mirrors setSliceExact for *[]T fields, keeping nil-pointer identity.
func setSlicePointerExact[T any](c *ColumnInfo) func(unsafe.Pointer, any) {
	return func(ptr unsafe.Pointer, v any) {
		switch typedValue := v.(type) {
		case []T:
			clonedSlice := slices.Clone(typedValue)
			SetField(c.Field, ptr, &clonedSlice)
		case *[]T:
			if typedValue == nil {
				SetField(c.Field, ptr, (*[]T)(nil))
				return
			}
			clonedSlice := slices.Clone(*typedValue)
			SetField(c.Field, ptr, &clonedSlice)
		default:
			c.assignFallback(ptr, v)
		}
	}
}
