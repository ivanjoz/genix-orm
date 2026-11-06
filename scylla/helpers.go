package scylla

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/fatih/color"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/utils"
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
	// The db package builds the query before this driver executes it; it logs under the same flag.
	db.DebugLevel = level
}

func ShouldLogFull() bool {
	return DebugFull
}

func ShouldLog() bool {
	return DebugNormal
}

// negativeSign marks a negative Int64ToBase64Bytes value: "-" is a digit of utils.Base64Alphabet.
const negativeSign = '~'

// Int64ToBase64Bytes writes n's magnitude in utils.EncodeUintBase64, after negativeSign when n < 0.
func Int64ToBase64Bytes(n int64) []byte {
	if n < 0 {
		// uint64(-n) is also right for math.MinInt64: its negation wraps to 2^63.
		return utils.AppendUintBase64([]byte{negativeSign}, uint64(-n))
	}
	return utils.AppendUintBase64(nil, uint64(n))
}

// Base64BytesToInt64 reverses Int64ToBase64Bytes; 0 for an empty or malformed value.
func Base64BytesToInt64(b []byte) int64 {
	magnitudeText, isNegative := strings.CutPrefix(string(b), string(negativeSign))
	magnitude, _ := utils.DecodeUintBase64(magnitudeText)
	if isNegative {
		return -int64(magnitude)
	}
	return int64(magnitude)
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
