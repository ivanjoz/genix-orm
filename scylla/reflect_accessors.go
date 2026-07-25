package scylla

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// The column metadata types and the generic accessor engine live in db, shared
// with every other driver. Only the CQL-specific value paths stay here, plugged
// in through db.ValueCodec.
type (
	colInfo    = db.ColInfo
	colType    = db.ColType
	columnInfo = db.ColumnInfo
	IColInfo   = db.IColInfo
)

// scyllaValueCodec supplies the parts of value handling that depend on
// Cassandra's type system: how a collection renders as a CQL literal, how values
// with no native CQL type are encoded, and how a scanned value is coerced back
// into a record field.
type scyllaValueCodec struct{}

// RenderValue builds the CQL statement form of a column whose type has no
// precompiled fast path.
func (scyllaValueCodec) RenderValue(c *columnInfo, ptr unsafe.Pointer) any {
	return makeScyllaValue(c.Field, ptr, c.Type, c.DBType)
}

// EncodeStatementValue handles the types Cassandra cannot store natively:
// unsigned integers and complex Go values both become blobs.
func (scyllaValueCodec) EncodeStatementValue(c *columnInfo, ptr unsafe.Pointer) any {
	// Unsupported unsigned types are stored as raw blob bytes instead of numeric CQL types.
	if c.Type == db.TypeBlob {
		if encodedBlob, encoded, err := encodeUnsignedValueToBlob(c.Field.Interface(ptr), c.RefType); encoded {
			if err != nil {
				fmt.Println("Error encoding unsigned blob:", c.FieldName, err)
				return nil
			}
			return encodedBlob
		}
	}
	if c.IsComplexType {
		fieldValue := c.Field.Interface(ptr)
		recordBytes, err := colbin.Marshal(fieldValue)
		if err != nil {
			fmt.Println("Error al encodeding .colbin:: ", c.FieldName, err)
			return ""
		}
		return recordBytes
	}
	return c.Field.Interface(ptr)
}

// AssignValue writes a value returned by gocql into a record field. It is the
// fallback for columns with no precompiled fast setter, and for fast setters
// handed a value whose type does not match the column exactly.
func (scyllaValueCodec) AssignValue(c *columnInfo, ptr unsafe.Pointer, v any) {
	if c.Type != db.TypeBlob {
		assingValue(c.Field, ptr, c.Type, v)
		return
	}

	if decodedValue, decoded, err := decodeUnsignedValueFromBlob(v, c.RefType); decoded {
		if err != nil {
			// Keep backward compatibility with legacy CBOR blobs when binary decode is not possible.
			fmt.Printf("Error decoding unsigned blob for Col %s, trying legacy CBOR: %v\n", c.Name, err)
		} else {
			// xunsafe generic Set does not reliably assign []uint16 slices; use direct typed memory assignment.
			destination := reflect.NewAt(c.RefType, c.Field.Pointer(ptr)).Elem()
			decodedReflectValue := reflect.ValueOf(decodedValue)
			if decodedReflectValue.IsValid() {
				if decodedReflectValue.Type().AssignableTo(c.RefType) {
					destination.Set(decodedReflectValue)
					return
				}
				if decodedReflectValue.Type().ConvertibleTo(c.RefType) {
					destination.Set(decodedReflectValue.Convert(c.RefType))
					return
				}
			}
		}
	}

	var vl []byte
	if b, ok := v.(*[]byte); ok {
		vl = *b
	} else if b, ok := v.([]byte); ok {
		vl = b
	}

	if len(vl) > 3 && c.Field != nil {
		// Direct unmarshal into the field memory using xunsafe pointer. colbin's
		// any decode yields map[string]any for nested objects (what the old
		// cborDecMode was configured for), so JSON re-serialization keeps working.
		dest := reflect.NewAt(c.RefType, c.Field.Pointer(ptr)).Interface()
		err := colbin.Unmarshal(vl, dest)
		if err != nil {
			fmt.Printf("Error al convertir ComplexType for Col %s: %v\n", c.Name, err)
		}
	} else if ShouldLogFull() {
		fmt.Printf("Complex Type could not be parsed or empty: %s (Type: %T)\n", c.Name, v)
	}
}

// ApplyCollectionOptions turns an explicit ",list" / ",set" / ",frozen" db tag into
// the CQL collection type the column is stored as.
func (scyllaValueCodec) ApplyCollectionOptions(recordTypeName, fieldName string, ct colType, tag db.DBTag) colType {
	return applyCollectionTagOptions(recordTypeName, fieldName, ct, tag)
}

// ApplyCollectionDefaults settles a slice column's CQL collection type when no db
// tag pinned it: UseListAsDefault swaps set<> for list<>, and a slice declared
// through Col (rather than ColSlice) is frozen, because Cassandra can only treat
// it as one opaque value.
func (scyllaValueCodec) ApplyCollectionDefaults(ct colType, useListAsDefault, applyFrozenDefault, frozen bool) colType {
	if useListAsDefault {
		ct.DBType = swapCollectionKind(unwrapFrozenCollectionType(ct.DBType), "list")
	}
	if applyFrozenDefault {
		ct.DBType = applyFrozenCollectionDefault(ct.DBType, frozen)
	}
	return ct
}

// CompileDriverAccessors adds the accessors only this driver can build. Writing a
// collection into a CQL statement needs the literal rendered with the column's
// own collection syntax, so it cannot be derived from the Go type alone.
func (scyllaValueCodec) CompileDriverAccessors(c *columnInfo) {
	switch c.Type {
	case 11: // []string
		c.GetValueFn = func(ptr unsafe.Pointer) any {
			return makeStringCollectionLiteral(c.DBType, *(*[]string)(c.Field.Pointer(ptr)))
		}
	case 12: // []int64
		c.GetValueFn = intCollectionLiteralAccessor[int64](c)
	case 13: // []int32
		c.GetValueFn = intCollectionLiteralAccessor[int32](c)
	case 14: // []int16
		c.GetValueFn = intCollectionLiteralAccessor[int16](c)
	case 15: // []int8
		c.GetValueFn = intCollectionLiteralAccessor[int8](c)
	case 31: // *[]string
		c.GetValueFn = func(ptr unsafe.Pointer) any {
			stringSlicePointer := *(**[]string)(c.Field.Pointer(ptr))
			if stringSlicePointer == nil {
				return nil
			}
			return makeStringCollectionLiteral(c.DBType, *stringSlicePointer)
		}
	case 32: // *[]int64
		c.GetValueFn = intCollectionPointerLiteralAccessor[int64](c)
	case 33: // *[]int32
		c.GetValueFn = intCollectionPointerLiteralAccessor[int32](c)
	case 34: // *[]int16
		c.GetValueFn = intCollectionPointerLiteralAccessor[int16](c)
	case 35: // *[]int8
		c.GetValueFn = intCollectionPointerLiteralAccessor[int8](c)
	}
}

func intCollectionLiteralAccessor[T ~int64 | ~int32 | ~int16 | ~int8](c *columnInfo) func(unsafe.Pointer) any {
	return func(ptr unsafe.Pointer) any {
		return makeSignedIntCollectionLiteral(c.DBType, *(*[]T)(c.Field.Pointer(ptr)))
	}
}

func intCollectionPointerLiteralAccessor[T ~int64 | ~int32 | ~int16 | ~int8](c *columnInfo) func(unsafe.Pointer) any {
	return func(ptr unsafe.Pointer) any {
		slicePointer := *(**[]T)(c.Field.Pointer(ptr))
		if slicePointer == nil {
			return nil
		}
		return makeSignedIntCollectionLiteral(c.DBType, *slicePointer)
	}
}

func makeStringCollectionLiteral(collectionColType string, values []string) string {
	openBracket, closeBracket := getCollectionLiteralBrackets(collectionColType)
	stringValuesQuoted := make([]string, len(values))
	for valueIndex, currentValue := range values {
		stringValuesQuoted[valueIndex] = "'" + strings.ReplaceAll(currentValue, "'", "''") + "'"
	}
	return openBracket + strings.Join(stringValuesQuoted, ",") + closeBracket
}

func makeSignedIntCollectionLiteral[T ~int64 | ~int32 | ~int16 | ~int8](collectionColType string, values []T) string {
	openBracket, closeBracket := getCollectionLiteralBrackets(collectionColType)
	if len(values) == 0 {
		return openBracket + closeBracket
	}
	var statementBuilder strings.Builder
	statementBuilder.Grow(len(values) * 4)
	statementBuilder.WriteString(openBracket)
	for valueIndex, currentValue := range values {
		if valueIndex > 0 {
			statementBuilder.WriteByte(',')
		}
		statementBuilder.WriteString(strconv.FormatInt(int64(currentValue), 10))
	}
	statementBuilder.WriteString(closeBracket)
	return statementBuilder.String()
}

func getCollectionLiteralBrackets(collectionColType string) (string, string) {
	normalizedCollectionType := strings.ToLower(unwrapFrozenCollectionType(collectionColType))
	if strings.HasPrefix(normalizedCollectionType, "list<") {
		return "[", "]"
	}
	return "{", "}"
}
