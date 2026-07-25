package scylla

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/viant/xunsafe"
)

// cqlTypeNames names each ORM type ID in Cassandra's type system. It is installed
// as db.DBTypeResolver at Register time, which is what lets the shared type table
// in db stay free of CQL syntax. Slices default to set semantics; record-level db
// tags can override to list/set/frozen variants.
var cqlTypeNames = map[int8]string{
	1: "text", 2: "bigint", 3: "int", 4: "smallint", 5: "tinyint",
	6: "float", 7: "double", 8: "boolean", 9: "blob",
	11: "set<text>", 12: "set<bigint>", 13: "set<int>", 14: "set<smallint>",
	15: "set<tinyint>", 16: "set<float>", 17: "set<double>",
	21: "text", 22: "bigint", 23: "int", 24: "smallint", 25: "tinyint",
	26: "float", 27: "double",
	31: "set<text>", 32: "set<bigint>", 33: "set<int>", 34: "set<smallint>",
	35: "set<tinyint>", 36: "set<float>", 37: "set<double>",
}

var assignFallbackCountByType [64]uint64
var assignFallbackLoggedByType [64]uint32

type number1 interface {
	int | int32 | int8 | uint8 | int16 | uint16 | int64 | float32 | float64
}

func setReflectIntSlice[T number1, E number1](f *xunsafe.Field, ptr unsafe.Pointer, vl *[]E) {
	newSlice := make([]T, len(*vl))
	for i, v := range *vl {
		newSlice[i] = T(v)
	}
	db.SetField(f, ptr, newSlice)
}

func setReflectIntSlicePointer[T number1, E number1](f *xunsafe.Field, ptr unsafe.Pointer, vl *[]E) {
	newSlice := make([]T, len(*vl))
	for i, v := range *vl {
		newSlice[i] = T(v)
	}
	db.SetField(f, ptr, &newSlice)
}

func printError(valType string, v any) {
	fmt.Printf("Error: El valor %v no fue mapeado = %v | %T\n", valType, v, v)
	panic("!")
}

// Number constraint to cover all numeric types requested
type Number interface {
	~int | ~int64 | ~int32 | ~int16 | ~int8 | ~float64 | ~float32
}

// Generic function to append any numeric slice in {val1, val2} format
func makeNumericSlice[T Number](slice []T) []byte {
	dst := []byte{}

	for i, v := range slice {
		if i > 0 {
			dst = append(dst, ',')
		}
		// Since we need to call specific strconv functions,
		// we switch on the type one more time inside the generic.
		switch val := any(v).(type) {
		case int:
			dst = append(dst, Int64ToBase64Bytes(int64(val))...)
		case int64:
			dst = append(dst, Int64ToBase64Bytes(val)...)
		case int32:
			dst = append(dst, Int64ToBase64Bytes(int64(val))...)
		case int16:
			dst = append(dst, Int64ToBase64Bytes(int64(val))...)
		case int8:
			dst = append(dst, Int64ToBase64Bytes(int64(val))...)
		case float64:
			dst = strconv.AppendFloat(dst, val, 'f', -1, 64)
		case float32:
			dst = strconv.AppendFloat(dst, float64(val), 'f', -1, 32)
		}
	}
	return dst
}

// Helper to convert any integer constraint to int64 for strconv
func supportsUnsignedBlobEncoding(targetType reflect.Type) bool {
	if targetType == nil {
		return false
	}
	for targetType.Kind() == reflect.Pointer {
		targetType = targetType.Elem()
	}

	switch targetType.Kind() {
	case reflect.Uint8, reflect.Uint16:
		return true
	case reflect.Slice:
		elementKind := targetType.Elem().Kind()
		return elementKind == reflect.Uint8 || elementKind == reflect.Uint16
	default:
		return false
	}
}

func encodeUnsignedValueToBlob(value any, targetType reflect.Type) ([]byte, bool, error) {
	if !supportsUnsignedBlobEncoding(targetType) {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}

	valueRef := reflect.ValueOf(value)
	for targetType.Kind() == reflect.Pointer {
		if valueRef.Kind() != reflect.Pointer {
			return nil, true, fmt.Errorf("expected pointer value for type %v, got %T", targetType, value)
		}
		if valueRef.IsNil() {
			return nil, true, nil
		}
		valueRef = valueRef.Elem()
		targetType = targetType.Elem()
	}

	switch targetType.Kind() {
	case reflect.Uint8:
		return []byte{byte(valueRef.Uint())}, true, nil
	case reflect.Uint16:
		blob := make([]byte, 2)
		binary.LittleEndian.PutUint16(blob, uint16(valueRef.Uint()))
		return blob, true, nil
	case reflect.Slice:
		if valueRef.Kind() != reflect.Slice {
			return nil, true, fmt.Errorf("expected slice value for type %v, got %T", targetType, value)
		}

		elementKind := targetType.Elem().Kind()
		if elementKind == reflect.Uint8 {
			blob := make([]byte, valueRef.Len())
			reflect.Copy(reflect.ValueOf(blob), valueRef)
			return blob, true, nil
		}

		blob := make([]byte, valueRef.Len()*2)
		for index := 0; index < valueRef.Len(); index++ {
			offset := index * 2
			binary.LittleEndian.PutUint16(blob[offset:offset+2], uint16(valueRef.Index(index).Uint()))
		}
		return blob, true, nil
	}

	return nil, true, fmt.Errorf("unsupported unsigned blob type %v", targetType)
}

func readBlobBytes(rawValue any) ([]byte, bool) {
	switch blobValue := rawValue.(type) {
	case []byte:
		return blobValue, true
	case *[]byte:
		if blobValue == nil {
			return nil, true
		}
		return *blobValue, true
	default:
		return nil, false
	}
}

func decodeUnsignedValueFromBlob(rawValue any, targetType reflect.Type) (any, bool, error) {
	if !supportsUnsignedBlobEncoding(targetType) {
		return nil, false, nil
	}

	blob, isBlob := readBlobBytes(rawValue)
	if !isBlob {
		return nil, true, fmt.Errorf("expected []byte value for unsigned blob decode, got %T", rawValue)
	}

	// Decode base type first, then wrap it as pointer recursively if needed.
	if targetType.Kind() == reflect.Pointer {
		if blob == nil {
			return reflect.Zero(targetType).Interface(), true, nil
		}
		decodedElement, _, err := decodeUnsignedValueFromBlob(blob, targetType.Elem())
		if err != nil {
			return nil, true, err
		}
		decodedValue := reflect.New(targetType.Elem())
		decodedValue.Elem().Set(reflect.ValueOf(decodedElement))
		return decodedValue.Interface(), true, nil
	}

	switch targetType.Kind() {
	case reflect.Uint8:
		if len(blob) == 0 {
			return uint8(0), true, nil
		}
		return uint8(blob[0]), true, nil
	case reflect.Uint16:
		if len(blob) == 0 {
			return uint16(0), true, nil
		}
		if len(blob) < 2 {
			return nil, true, fmt.Errorf("invalid blob length %d for uint16", len(blob))
		}
		return binary.LittleEndian.Uint16(blob[:2]), true, nil
	case reflect.Slice:
		elementKind := targetType.Elem().Kind()
		if elementKind == reflect.Uint8 {
			decodedBytes := make([]byte, len(blob))
			copy(decodedBytes, blob)
			return decodedBytes, true, nil
		}
		if len(blob)%2 != 0 {
			return nil, true, fmt.Errorf("invalid blob length %d for []uint16", len(blob))
		}

		decodedSlice := make([]uint16, len(blob)/2)
		for index := 0; index < len(decodedSlice); index++ {
			offset := index * 2
			decodedSlice[index] = binary.LittleEndian.Uint16(blob[offset : offset+2])
		}
		return decodedSlice, true, nil
	}

	return nil, true, fmt.Errorf("unsupported unsigned blob target type %v", targetType)
}

const pipeByte byte = '|'
const specialByte byte = '`'
const backslashByte byte = '\\'

// | is replaced by '`'
// \ is replaced by '```'
// '``' is used to contatenate 2 strings

func sanitizeString(value string) []byte {
	dst := []byte{}

	for _, b := range []byte(value) {
		switch b {
		case pipeByte:
			dst = append(dst, specialByte)
		case backslashByte:
			dst = append(dst, specialByte, specialByte, specialByte)
		default:
			dst = append(dst, b)
		}
	}
	return dst
}

func unSanitizeString(value string) string {
	dst := []byte{}
	src := []byte(value)
	for i := 0; i < len(src); i++ {
		if src[i] == specialByte {
			if i+2 < len(src) && src[i+1] == specialByte && src[i+2] == specialByte {
				dst = append(dst, backslashByte)
				i += 2
			} else {
				dst = append(dst, pipeByte)
			}
		} else {
			dst = append(dst, src[i])
		}
	}
	return string(dst)
}

func valueToCSVBase64(val any) []byte {

	if val == nil {
		return []byte{}
	}

	switch v := val.(type) {
	// Individual Numbers
	case int:
		return Int64ToBase64Bytes(int64(v))
	case *int:
		return Int64ToBase64Bytes(int64(*v))
	case int64:
		return Int64ToBase64Bytes(v)
	case *int64:
		return Int64ToBase64Bytes(*v)
	case int32:
		return Int64ToBase64Bytes(int64(v))
	case *int32:
		return Int64ToBase64Bytes(int64(*v))
	case int16:
		return Int64ToBase64Bytes(int64(v))
	case *int16:
		return Int64ToBase64Bytes(int64(*v))
	case int8:
		return Int64ToBase64Bytes(int64(v))
	case *int8:
		return Int64ToBase64Bytes(int64(*v))
	case float64:
		return strconv.AppendFloat([]byte{}, v, 'f', -1, 64)
	case *float64:
		return strconv.AppendFloat([]byte{}, *v, 'f', -1, 64)
	case float32:
		return strconv.AppendFloat([]byte{}, float64(v), 'f', -1, 32)
	case *float32:
		return strconv.AppendFloat([]byte{}, float64(*v), 'f', -1, 32)

	// Slices using our Generic Function
	case []int:
		return makeNumericSlice(v)
	case *[]int:
		return makeNumericSlice(*v)
	case []int64:
		return makeNumericSlice(v)
	case *[]int64:
		return makeNumericSlice(*v)
	case []int32:
		return makeNumericSlice(v)
	case *[]int32:
		return makeNumericSlice(*v)
	case []float64:
		return makeNumericSlice(v)
	case *[]float64:
		return makeNumericSlice(*v)
	case []float32:
		return makeNumericSlice(v)
	case *[]float32:
		return makeNumericSlice(*v)

	// String & String Slices
	case string:
		return sanitizeString(v)
	case *string:
		return sanitizeString(*v)
	case []string:
		dst := []byte{}
		for i, s := range v {
			if i > 0 {
				dst = append(dst, '`', '`')
			}
			dst = append(dst, sanitizeString(s)...)
		}
		return dst
	case *[]string:
		dst := []byte{}
		for i, s := range *v {
			if i > 0 {
				dst = append(dst, '`', '`')
			}
			dst = append(dst, sanitizeString(s)...)
		}
		return dst
	case []byte:
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
		base64.StdEncoding.Encode(dst, v)
		for len(dst) > 0 && dst[len(dst)-1] == '=' {
			dst = dst[:len(dst)-1]
		}
		return dst
	case *[]byte:
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(*v)))
		base64.StdEncoding.Encode(dst, *v)
		for len(dst) > 0 && dst[len(dst)-1] == '=' {
			dst = dst[:len(dst)-1]
		}
		return dst
	case bool:
		if v {
			return []byte{'1'}
		}
		return []byte{'0'}
	case *bool:
		if *v {
			return []byte{'1'}
		}
		return []byte{'0'}
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

func base64CSVStringToValue(val string, valType int8) any {
	if val == "" {
		return nil
	}

	switch valType {
	case 1: // string
		return unSanitizeString(val)
	case 2, 3, 4, 5: // int64, int32, int16, int8
		return Base64BytesToInt64([]byte(val))
	case 6, 7: // float32, float64
		f, _ := strconv.ParseFloat(val, 64)
		if valType == 6 {
			return float32(f)
		}
		return f
	case 8: // bool
		return val == "1"
	case 9: // []byte
		for len(val)%4 != 0 {
			val += "="
		}
		recordBytes, _ := base64.StdEncoding.DecodeString(val)
		return recordBytes
	case 11: // []string
		parts := strings.Split(val, "``")
		res := make([]string, len(parts))
		for i, p := range parts {
			res[i] = unSanitizeString(p)
		}
		return res
	case 12, 13, 14, 15: // []int64, []int32, []int16, []int8
		// fmt.Println("slice value:", val)
		parts := strings.Split(val, ",")
		switch valType {
		case 12:
			res := make([]int64, len(parts))
			for i, p := range parts {
				res[i] = Base64BytesToInt64([]byte(p))
			}
			return res
		case 13:
			res := make([]int32, len(parts))
			for i, p := range parts {
				res[i] = int32(Base64BytesToInt64([]byte(p)))
			}
			return res
		case 14:
			res := make([]int16, len(parts))
			for i, p := range parts {
				res[i] = int16(Base64BytesToInt64([]byte(p)))
			}
			return res
		default:
			res := make([]int8, len(parts))
			for i, p := range parts {
				res[i] = int8(Base64BytesToInt64([]byte(p)))
			}
			return res
		}
	case 16, 17: // []float32, []float64
		parts := strings.Split(val, ",")
		if valType == 16 {
			res := make([]float32, len(parts))
			for i, p := range parts {
				f, _ := strconv.ParseFloat(p, 32)
				res[i] = float32(f)
			}
			return res
		} else {
			res := make([]float64, len(parts))
			for i, p := range parts {
				f, _ := strconv.ParseFloat(p, 64)
				res[i] = f
			}
			return res
		}
	}

	return nil
}

func trySetNumberSlice[T number1](f *xunsafe.Field, ptr unsafe.Pointer, colType int8, value any) {
	switch vl := value.(type) {
	case []int:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]int:
		setReflectIntSlice[T](f, ptr, vl)
	case []int64:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]int64:
		setReflectIntSlice[T](f, ptr, vl)
	case []int32:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]int32:
		setReflectIntSlice[T](f, ptr, vl)
	case []int16:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]int16:
		setReflectIntSlice[T](f, ptr, vl)
	case []int8:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]int8:
		setReflectIntSlice[T](f, ptr, vl)
	case []float64:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]float64:
		setReflectIntSlice[T](f, ptr, vl)
	case []float32:
		setReflectIntSlice[T](f, ptr, &vl)
	case *[]float32:
		setReflectIntSlice[T](f, ptr, vl)
	default:
		printError(db.GetColTypeByID(colType).FieldType, value)
	}
}

func trySetNumberSlicePointer[T number1](f *xunsafe.Field, ptr unsafe.Pointer, colType int8, value any) {
	switch vl := value.(type) {
	case []int:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]int:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []int64:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]int64:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []int32:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]int32:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []int16:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]int16:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []int8:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]int8:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []float64:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]float64:
		setReflectIntSlicePointer[T](f, ptr, vl)
	case []float32:
		setReflectIntSlicePointer[T](f, ptr, &vl)
	case *[]float32:
		setReflectIntSlicePointer[T](f, ptr, vl)
	default:
		printError(db.GetColTypeByID(colType).FieldType, value)
	}
}

func incrementAssignFallbackStats(colType int8) {
	// Fallback stats show where fast accessors are still missing or bypassed.
	// This keeps optimization work data-driven instead of guess-based.
	if colType <= 0 || int(colType) >= len(assignFallbackCountByType) {
		return
	}

	atomic.AddUint64(&assignFallbackCountByType[colType], 1)
	// Log only first hit per type to keep diagnostics visible without flooding logs.
	if ShouldLogFull() && atomic.CompareAndSwapUint32(&assignFallbackLoggedByType[colType], 0, 1) {
		fmt.Printf("assingValue fallback engaged for colType=%d (%s)\n", colType, db.GetColTypeByID(colType).FieldType)
	}
}

// GetAssignFallbackUsageByType returns fallback usage counters keyed by colType id.
func GetAssignFallbackUsageByType() map[int8]uint64 {
	// Return only non-zero counters to keep debug output compact.
	fallbackUsageByType := map[int8]uint64{}
	for typeID := range len(assignFallbackCountByType) {
		currentCount := atomic.LoadUint64(&assignFallbackCountByType[typeID])
		if currentCount == 0 {
			continue
		}
		fallbackUsageByType[int8(typeID)] = currentCount
	}
	return fallbackUsageByType
}

// ResetAssignFallbackUsageByType clears fallback counters to measure a fresh runtime window.
func ResetAssignFallbackUsageByType() {
	// Reset both counters and one-shot log guards for fresh profiling windows.
	for typeID := range len(assignFallbackCountByType) {
		atomic.StoreUint64(&assignFallbackCountByType[typeID], 0)
		atomic.StoreUint32(&assignFallbackLoggedByType[typeID], 0)
	}
}

func assignScalarFallback(f *xunsafe.Field, ptr unsafe.Pointer, colType int8, value any) {
	// Scalar fallback is centralized to keep assingValue focused on uncommon branches.
	// This path preserves legacy behavior when fast accessors are not used.
	switch colType {
	case 1: // string
		if vl, ok := value.(string); ok {
			f.SetString(ptr, vl)
		} else if vl, ok := value.(*string); ok {
			f.SetString(ptr, *vl)
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 2: // int64
		f.SetInt64(ptr, db.ToInt64(value))
	case 3: // int32
		f.SetInt32(ptr, int32(db.ToInt64(value)))
	case 4: // int16
		f.SetInt16(ptr, int16(db.ToInt64(value)))
	case 5: // int8
		f.SetInt8(ptr, int8(db.ToInt64(value)))
	case 6: // float32
		f.SetFloat32(ptr, db.ToFloat32(value))
	case 7: // float64
		f.SetFloat64(ptr, db.ToFloat64(value))
	case 8: // bool
		if vl, ok := value.(bool); ok {
			f.SetBool(ptr, vl)
		} else if vl, ok := value.(*bool); ok {
			f.SetBool(ptr, *vl)
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 10: // int
		f.SetInt(ptr, int(db.ToInt64(value)))
	case 21: // *string
		if vl, ok := value.(string); ok {
			f.Set(ptr, &vl)
		} else if vl, ok := value.(*string); ok {
			f.Set(ptr, vl)
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 22: // *int64
		val := db.ToInt64(value)
		f.Set(ptr, &val)
	case 23: // *int32
		val := int32(db.ToInt64(value))
		f.Set(ptr, &val)
	case 24: // *int16
		val := int16(db.ToInt64(value))
		f.Set(ptr, &val)
	case 25: // *int8
		val := int8(db.ToInt64(value))
		f.Set(ptr, &val)
	case 26: // *float32
		val := db.ToFloat32(value)
		f.Set(ptr, &val)
	case 27: // *float64
		val := db.ToFloat64(value)
		f.Set(ptr, &val)
	case 28: // *int
		val := int(db.ToInt64(value))
		f.Set(ptr, &val)
	default:
		printError(db.GetColTypeByID(colType).FieldType, value)
	}
}

func assingValue(f *xunsafe.Field, ptr unsafe.Pointer, colType int8, value any) {
	if value == nil {
		return
	}
	incrementAssignFallbackStats(colType)

	// Keep fallback switch focused on uncommon/collection types now that common cases have fast accessors.
	// Any type not handled here routes to scalar fallback to preserve compatibility.
	switch colType {
	case 9: // IsComplexType = true | []byte as cbor
		if vl, ok := value.([]byte); ok {
			f.Set(ptr, vl)
		} else if vl, ok := value.(*[]byte); ok {
			f.Set(ptr, *vl)
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 11: // []string
		// Clone slices on assignment to avoid sharing mutable backing arrays with caller buffers.
		if vl, ok := value.([]string); ok {
			f.Set(ptr, slices.Clone(vl))
		} else if vl, ok := value.(*[]string); ok {
			f.Set(ptr, slices.Clone(*vl))
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 12: // []int64
		// Numeric slice helpers keep support for multiple incoming numeric widths in fallback mode.
		trySetNumberSlice[int64](f, ptr, colType, value)
	case 13: // []int32
		trySetNumberSlice[int32](f, ptr, colType, value)
	case 14: // []int16
		trySetNumberSlice[int16](f, ptr, colType, value)
	case 15: // []int8
		trySetNumberSlice[int8](f, ptr, colType, value)
	case 16: // []float32
		trySetNumberSlice[float32](f, ptr, colType, value)
	case 17: // []float64
		trySetNumberSlice[float64](f, ptr, colType, value)
	case 31: // *[]string
		// Pointer-to-slice fallback keeps pointer semantics while still cloning payload data.
		if vl, ok := value.([]string); ok {
			val := slices.Clone(vl)
			f.Set(ptr, &val)
		} else if vl, ok := value.(*[]string); ok {
			val := slices.Clone(*vl)
			f.Set(ptr, &val)
		} else {
			printError(db.GetColTypeByID(colType).FieldType, value)
		}
	case 32: // *[]int64
		trySetNumberSlicePointer[int64](f, ptr, colType, value)
	case 33: // *[]int32
		trySetNumberSlicePointer[int32](f, ptr, colType, value)
	case 34: // *[]int16
		trySetNumberSlicePointer[int16](f, ptr, colType, value)
	case 35: // *[]int8
		trySetNumberSlicePointer[int8](f, ptr, colType, value)
	case 36: // *[]float32
		trySetNumberSlicePointer[float32](f, ptr, colType, value)
	case 37: // *[]float64
		trySetNumberSlicePointer[float64](f, ptr, colType, value)
	default:
		assignScalarFallback(f, ptr, colType, value)
	}
}

func makeScyllaValue(f *xunsafe.Field, ptr unsafe.Pointer, colType int8, colTypeName string) any {
	if f == nil || ptr == nil {
		return nil
	}

	// Handle pointer types first
	if (colType >= 21 && colType <= 28) || (colType >= 31 && colType <= 37) {
		if f.IsNil(ptr) {
			return nil
		}
	}

	switch colType {
	case 1, 21: // string, *string
		return "'" + strings.ReplaceAll(f.String(ptr), "'", "''") + "'"
	case 11, 31: // []string, *[]string
		var values []string
		if colType == 11 {
			values = f.Interface(ptr).([]string)
		} else {
			pValues := f.Interface(ptr).(*[]string)
			if pValues == nil {
				return nil
			}
			values = *pValues
		}
		strValues := make([]string, len(values))
		for i, v := range values {
			strValues[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}
		openBracket, closeBracket := getCollectionLiteralBrackets(colTypeName)
		return openBracket + strings.Join(strValues, ",") + closeBracket
	case 12, 13, 14, 15, 16, 17, 32, 33, 34, 35, 36, 37: // other slices/pointers to slices
		concatenatedValues := Concatx(",", reflectToSlicePtr(f, ptr))
		openBracket, closeBracket := getCollectionLiteralBrackets(colTypeName)
		return openBracket + concatenatedValues + closeBracket
	case 9: // []byte / blob (could be complex type)
		// UPDATE statements need blob literals (0x...) instead of raw []byte formatting.
		if encodedBlob, encoded, err := encodeUnsignedValueToBlob(f.Interface(ptr), f.Type); encoded {
			if err != nil {
				fmt.Println("Error encoding unsigned blob:", f.Name, err)
				return ""
			}
			return "0x" + hex.EncodeToString(encodedBlob)
		}
		// Check if it's a complex type or just []byte
		if f.Type.Kind() != reflect.Slice || f.Type.Elem().Kind() != reflect.Uint8 {
			// Complex type
			fieldValue := f.Interface(ptr)
			recordBytes, err := colbin.Marshal(fieldValue)
			if err != nil {
				fmt.Println("Error al encodeding .colbin:: ", f.Name, err)
				return ""
			}
			hexString := hex.EncodeToString(recordBytes)
			return "0x" + hexString
		}
		// Plain []byte
		blobValue := f.Interface(ptr).([]byte)
		return "0x" + hex.EncodeToString(blobValue)
	default:
		return f.Interface(ptr)
	}
}
