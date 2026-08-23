package scylla

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"reflect"
	"strings"
	"unsafe"

	"github.com/fatih/color"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/kr/pretty"
	"github.com/viant/xunsafe"
)

var (
	DebugFull   bool
	DebugNormal bool
)

// SetDebugLogging maps a verbosity level to the DB debug flags. Level
// semantics: 0 = silent, 1 = DebugNormal only (per-query elapsed,
// table-write summaries), 2 = DebugNormal + DebugFull (adds verbose
// internal traces like TextSearchIndex sync lines). Called from the
// runtime bootstrap so the db package doesn't need to import core.
func SetDebugLogging(level int) {
	DebugNormal = level >= 1
	DebugFull = level >= 2
}

func ShouldLogFull() bool {
	return DebugFull
}

func ShouldLog() bool {
	return DebugNormal
}

func BasicHashInt(s string) int32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	hashValue := int32(h.Sum32())
	// Keep hash deterministic while reserving 0 as an invalid/sentinel ID.
	if hashValue == 0 {
		return 1
	}
	return hashValue
}

const base64Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+/"

func Int64ToBase64Bytes(n int64) []byte {
	if n == 0 {
		return []byte{base64Chars[0]}
	}

	// 1. Determine if negative and use absolute value
	isNegative := false
	un := uint64(n)
	if n < 0 {
		isNegative = true
		// Handle math.MinInt64 edge case by converting to uint64 first
		un = uint64(-n)
	}

	// 2. Max length: 11 chars for magnitude + 1 for sign = 12
	var buf [12]byte
	i := 12

	// 3. Mathematical Base64 conversion
	for un > 0 {
		i--
		buf[i] = base64Chars[un%64]
		un /= 64
	}

	// 4. Add the minus sign if necessary
	if isNegative {
		i--
		buf[i] = '-'
	}

	// Return only the occupied part of the buffer
	// We create a copy to avoid the buffer escaping to heap if needed
	result := make([]byte, 12-i)
	copy(result, buf[i:])
	return result
}

func Base64BytesToInt64(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	isNegative := false
	if b[0] == '-' {
		isNegative = true
		b = b[1:]
	}
	var res uint64
	for _, char := range b {
		idx := strings.IndexByte(base64Chars, char)
		if idx == -1 {
			continue
		}
		res = res*64 + uint64(idx)
	}
	if isNegative {
		return -int64(res)
	}
	return int64(res)
}

func HashInt(values ...any) int32 {
	buf := new(bytes.Buffer)

	for _, anyVal := range values {
		appendHashValue(buf, anyVal)
		buf.WriteByte(0)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return int32(h.Sum32())
}

// appendHashValue writes one value into the hash buffer. A named numeric type is
// normalized to its underlying plain type and re-dispatched, so it hashes to the SAME
// bytes as the plain type it wraps — retyping a column from int8 to a named int8 must not
// silently invalidate every hash already persisted from it. Without this it would fall to
// the %v branch and hash the decimal text instead of the binary value.
func appendHashValue(buf *bytes.Buffer, anyVal any) {
	switch val := anyVal.(type) {
	case int:
		binary.Write(buf, binary.LittleEndian, int64(val))
	case int32:
		binary.Write(buf, binary.LittleEndian, val)
	case int64:
		binary.Write(buf, binary.LittleEndian, val)
	case int16:
		binary.Write(buf, binary.LittleEndian, val)
	case int8:
		binary.Write(buf, binary.LittleEndian, val)
	case float32:
		binary.Write(buf, binary.LittleEndian, val)
	case float64:
		binary.Write(buf, binary.LittleEndian, val)
	case string:
		buf.WriteString(val)
	default:
		if plainValue, isNamed := db.NormalizeNamedNumeric(anyVal); isNamed {
			appendHashValue(buf, plainValue)
			return
		}
		buf.WriteString(fmt.Sprintf("%v", val))
	}
}

func HashInt64(values ...int64) int32 {
	buf := new(bytes.Buffer)

	for _, value := range values {
		binary.Write(buf, binary.LittleEndian, value)
		buf.WriteByte(0)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return int32(h.Sum32())
}

func makeWeekIndexFromWeekCode(weekCode int64) (int64, bool) {
	// Use the Monday unix-day as the real timeline, then collapse it into contiguous week units.
	mondayUnixDay := makeUnixDayFromWeekCode(int16(weekCode))
	if mondayUnixDay == 0 {
		return 0, false
	}
	return int64(mondayUnixDay) / 7, true
}

func normalizeCompositeRange(from, to int64, isWeek bool) (int64, int64) {
	// Range planning needs contiguous numbers; week-coded values use Monday-date conversion.
	if isWeek {
		fromWeekIndex, fromOk := makeWeekIndexFromWeekCode(from)
		toWeekIndex, toOk := makeWeekIndexFromWeekCode(to)
		if fromOk && toOk {
			if toWeekIndex < fromWeekIndex {
				return toWeekIndex, fromWeekIndex
			}
			return fromWeekIndex, toWeekIndex
		}
	}
	if to < from {
		return to, from
	}
	return from, to
}

func makeCompositeBucketID(value int64, bucketSize int8, isWeek bool) int64 {
	// Bucket IDs are computed on a contiguous domain using date-derived week indexes when needed.
	basisValue := value
	if isWeek {
		if weekIndex, ok := makeWeekIndexFromWeekCode(value); ok {
			basisValue = weekIndex
		}
	}
	return basisValue / int64(bucketSize)
}

func Logx(style int8, messageInColor string, params ...any) {
	var c *color.Color

	switch style {
	case 1:
		c = color.New(color.FgCyan, color.Bold)
	case 2:
		c = color.New(color.FgGreen, color.Bold)
	case 3:
		c = color.New(color.FgYellow, color.Bold)
	case 4:
		c = color.New(color.FgBlue, color.Bold)
	case 5:
		c = color.New(color.FgRed, color.Bold)
	case 6:
		c = color.New(color.FgMagenta, color.Bold)
	}

	c.Print(messageInColor)
	if len(params) > 0 {
		fmt.Print(" | ")
		for _, e := range params {
			fmt.Print(e)
			fmt.Print(" ")
		}
		fmt.Println("")
	}
}

func convertToInt64(val any) int64 {
	// Use a type assertion to check if it's an int or other integer types
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		if plainValue, isNamed := db.NormalizeNamedNumeric(val); isNamed {
			return convertToInt64(plainValue)
		}
		// The value is not an integer
		fmt.Println("Error: Value is not an integer:", v)
		return 0
	}
}

func convertToInt32(val any) int32 {
	// Use a type assertion to check if it's an int or other integer types
	switch v := val.(type) {
	case int:
		return int32(v)
	case int8:
		return int32(v)
	case int16:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	default:
		if plainValue, isNamed := db.NormalizeNamedNumeric(val); isNamed {
			return convertToInt32(plainValue)
		}
		// The value is not an integer
		fmt.Println("Error: Value is not an integer:", v)
		return 0
	}
}

func Pow10Int64(m int64) int64 {
	if m == 0 {
		return 1
	}

	if m == 1 {
		return 10
	}

	number := int64(10)
	for i := int64(2); i <= m; i++ {
		number *= 10
	}
	return number
}

func Concatx[T any](sep string, slice1 []T) string {
	sliceOfStrings := []string{}
	for _, value := range slice1 {
		sliceOfStrings = append(sliceOfStrings, fmt.Sprintf("%v", value))
	}
	return strings.Join(sliceOfStrings, sep)
}

func Err(content ...any) error {
	errMessage := Concatx(" ", content)
	return errors.New(errMessage)
}

func sliceToAny[T any](valuesGeneric *[]T) []any {
	values := []any{}
	for _, v := range *valuesGeneric {
		values = append(values, any(v))
	}
	return values
}

func reflectToSlicePtr(field *xunsafe.Field, ptr unsafe.Pointer) []any {
	return reflectToSliceValue(field.Interface(ptr))
}

func reflectToSliceValue(value any) []any {
	var values []any

	switch sl := value.(type) {
	case []int:
		values = sliceToAny(&sl)
	case []int8:
		values = sliceToAny(&sl)
	case []int16:
		values = sliceToAny(&sl)
	case []int32:
		values = sliceToAny(&sl)
	case []int64:
		values = sliceToAny(&sl)
	case []float32:
		values = sliceToAny(&sl)
	case []float64:
		values = sliceToAny(&sl)
	case []string:
		values = sliceToAny(&sl)
	default:
		// The value is not an integer
		panic("Value was not recognised of a slice: " + reflect.TypeOf(value).String())
	}
	return values
}

func Print(Struct any) {
	pretty.Println(Struct)
}

func GetRandomInt64(digits int8) int64 {
	if digits <= 0 {
		return 0
	}
	max := Pow10Int64(int64(digits))
	return rand.Int64N(max)
}
