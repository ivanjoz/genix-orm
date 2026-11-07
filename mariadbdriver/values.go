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
// NULL (an empty one is `[]`, which a Scylla list also reads back as); any other slice or complex
// value is colbin bytes, NULL when empty (a Scylla collection reads back nil then).

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
		return colbin.Unmarshal(*scanned, value.Addr().Interface())
	}
	return nil
}
