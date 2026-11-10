package mariadbdriver

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// How a record's fields become statement arguments and how scanned columns become fields again
// (docs/OCEANBASE_PLAN.md §7). Scalars bind as themselves; an indexed slice is a JSON array, never
// NULL (an empty one is `[]`, which a Scylla list also reads back as); a slice of fixed-width numbers
// is colbin.EncodeSlice bytes; any other slice or complex value is a colbin message. Both are NULL
// when empty (a Scylla collection reads back nil then).

// bindStoredValue is the statement argument of one stored column of a record.
func bindStoredValue(table *MariaTable, stored storedColumn, recordPointer unsafe.Pointer, stamp db.ManagedStamp) (any, error) {
	if !stored.hasField() {
		switch {
		case stored.column == table.CreatedCol:
			return db.CoerceManagedInteger(stored.column, stamp.Created), nil
		case stored.column == table.UpdatedCol:
			return db.CoerceManagedInteger(stored.column, stamp.Updated), nil
		}
		// A generated column: db computes it, in the unsigned layout (packed, hash, delta).
		generatedValue := stored.column.GetValue(recordPointer)
		if stored.isJSON {
			return jsonArrayArgument(generatedValue)
		}
		return generatedValue, nil
	}

	columnInfo := stored.column.(*db.ColumnInfo)
	fieldValue := reflect.NewAt(columnInfo.Field.Type, columnInfo.Field.Pointer(recordPointer)).Elem()
	if stored.isJSON {
		return jsonArrayArgument(fieldValue.Interface())
	}
	if fieldValue.Kind() == reflect.Pointer {
		if fieldValue.IsNil() {
			return nil, nil
		}
		fieldValue = fieldValue.Elem()
	}
	switch fieldValue.Kind() {
	case reflect.Slice, reflect.Map, reflect.Array, reflect.Struct:
		if fieldValue.Kind() == reflect.Slice && fieldValue.Type().Elem().Kind() == reflect.Uint8 {
			return fieldValue.Bytes(), nil
		}
		if fieldValue.IsZero() || (fieldValue.Kind() != reflect.Struct && fieldValue.Len() == 0) {
			return nil, nil
		}
		if encoded, isNumberSlice := encodeNumberSlice(fieldValue); isNumberSlice {
			return encoded, nil
		}
		encoded, err := colbin.Marshal(fieldValue.Interface())
		if err != nil {
			return nil, fmt.Errorf("column %q cannot be colbin-encoded: %w", stored.name, err)
		}
		return encoded, nil
	}
	return fieldValue.Interface(), nil
}

// jsonArrayArgument renders a slice as the JSON array text of its elements; nil is `[]`.
func jsonArrayArgument(sliceValue any) (any, error) {
	elements := reflect.ValueOf(sliceValue)
	for elements.Kind() == reflect.Pointer && !elements.IsNil() {
		elements = elements.Elem()
	}
	if !elements.IsValid() || elements.Kind() != reflect.Slice || elements.Len() == 0 {
		return "[]", nil
	}
	encoded, err := json.Marshal(elements.Interface())
	if err != nil {
		return nil, err
	}
	return string(encoded), nil
}

// scanTarget is the database/sql destination of one stored column with a record field, typed by the
// field's kind so the text and binary protocols both scan into it.
func scanTarget(stored storedColumn) any {
	fieldType := stored.column.(*db.ColumnInfo).Field.Type
	if fieldType.Kind() == reflect.Pointer {
		fieldType = fieldType.Elem()
	}
	if stored.isJSON {
		return new([]byte)
	}
	switch fieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return new(sql.NullInt64)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(sql.Null[uint64])
	case reflect.Float32, reflect.Float64:
		return new(sql.NullFloat64)
	case reflect.Bool:
		return new(sql.NullBool)
	case reflect.String:
		return new(sql.NullString)
	}
	return new([]byte)
}

// assignScanned writes a scanned column into its field; NULL leaves the zero value.
func assignScanned(stored storedColumn, recordPointer unsafe.Pointer, target any) error {
	columnInfo := stored.column.(*db.ColumnInfo)
	fieldValue := reflect.NewAt(columnInfo.Field.Type, columnInfo.Field.Pointer(recordPointer)).Elem()
	if stored.isJSON {
		encoded := *target.(*[]byte)
		if encoded == nil {
			return nil
		}
		return json.Unmarshal(encoded, fieldValue.Addr().Interface())
	}

	// A pointer field is allocated only for a non-NULL value.
	valueOf := func() reflect.Value {
		if fieldValue.Kind() != reflect.Pointer {
			return fieldValue
		}
		fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
		return fieldValue.Elem()
	}
	switch scanned := target.(type) {
	case *sql.NullInt64:
		if scanned.Valid {
			valueOf().SetInt(scanned.Int64)
		}
	case *sql.Null[uint64]:
		if scanned.Valid {
			valueOf().SetUint(scanned.V)
		}
	case *sql.NullFloat64:
		if scanned.Valid {
			valueOf().SetFloat(scanned.Float64)
		}
	case *sql.NullBool:
		if scanned.Valid {
			valueOf().SetBool(scanned.Bool)
		}
	case *sql.NullString:
		if scanned.Valid {
			valueOf().SetString(scanned.String)
		}
	case *[]byte:
		if *scanned == nil {
			return nil
		}
		value := valueOf()
		if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			value.SetBytes(append([]byte(nil), *scanned...))
			return nil
		}
		if isNumberSlice, err := decodeNumberSlice(*scanned, value); isNumberSlice {
			return err
		}
		return colbin.Unmarshal(*scanned, value.Addr().Interface())
	}
	return nil
}

// encodeNumberSlice stores a non-empty slice of fixed-width numbers with colbin.EncodeSlice: a count
// and a column, with no header, far smaller than a colbin message for ids and decimal prices. false
// for any other slice ([]string, []int, structs), which stays a colbin message. The field may be a
// named slice or element type; it is viewed as its underlying element type, which has its layout.
func encodeNumberSlice(sliceValue reflect.Value) ([]byte, bool) {
	if sliceValue.Kind() != reflect.Slice {
		return nil, false
	}
	elements, count := sliceValue.UnsafePointer(), sliceValue.Len()
	switch sliceValue.Type().Elem().Kind() {
	case reflect.Int8:
		return colbin.EncodeSlice(unsafe.Slice((*int8)(elements), count)), true
	case reflect.Int16:
		return colbin.EncodeSlice(unsafe.Slice((*int16)(elements), count)), true
	case reflect.Int32:
		return colbin.EncodeSlice(unsafe.Slice((*int32)(elements), count)), true
	case reflect.Int64:
		return colbin.EncodeSlice(unsafe.Slice((*int64)(elements), count)), true
	case reflect.Uint16:
		return colbin.EncodeSlice(unsafe.Slice((*uint16)(elements), count)), true
	case reflect.Uint32:
		return colbin.EncodeSlice(unsafe.Slice((*uint32)(elements), count)), true
	case reflect.Float32:
		return colbin.EncodeSlice(unsafe.Slice((*float32)(elements), count)), true
	case reflect.Float64:
		return colbin.EncodeSlice(unsafe.Slice((*float64)(elements), count)), true
	}
	return nil, false
}

// decodeNumberSlice is the read side of encodeNumberSlice, for the same element kinds; false for any
// other slice.
func decodeNumberSlice(encoded []byte, sliceValue reflect.Value) (bool, error) {
	if sliceValue.Kind() != reflect.Slice {
		return false, nil
	}
	switch sliceValue.Type().Elem().Kind() {
	case reflect.Int8:
		return true, decodeNumberSliceInto[int8](encoded, sliceValue)
	case reflect.Int16:
		return true, decodeNumberSliceInto[int16](encoded, sliceValue)
	case reflect.Int32:
		return true, decodeNumberSliceInto[int32](encoded, sliceValue)
	case reflect.Int64:
		return true, decodeNumberSliceInto[int64](encoded, sliceValue)
	case reflect.Uint16:
		return true, decodeNumberSliceInto[uint16](encoded, sliceValue)
	case reflect.Uint32:
		return true, decodeNumberSliceInto[uint32](encoded, sliceValue)
	case reflect.Float32:
		return true, decodeNumberSliceInto[float32](encoded, sliceValue)
	case reflect.Float64:
		return true, decodeNumberSliceInto[float64](encoded, sliceValue)
	}
	return false, nil
}

// decodeNumberSliceInto decodes into a []T and sets it on the field: the slice header is shared
// as is, since a named slice or element type has T's layout.
func decodeNumberSliceInto[T colbin.Number](encoded []byte, sliceValue reflect.Value) error {
	decoded, err := colbin.DecodeSlice[T](encoded)
	if err != nil {
		return err
	}
	sliceValue.Set(reflect.NewAt(sliceValue.Type(), unsafe.Pointer(&decoded)).Elem())
	return nil
}
