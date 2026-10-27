package dataframe

import (
	"fmt"
	"reflect"
	"unsafe"
)

// ─────────────────────────────────────────────────────────────────────────────
// Projection: a frame declared by a typed method instead of the record's columns. The
// method turns a record into the frame's lines, one struct each, whose fields carry
// their role in a tag:
//
//	type SaleSummaryFrame struct {
//	    Date       int16 `frame:"key"`  // the file
//	    ProductID  int32 `frame:"row"`  // the row inside it
//	    SubDivisor int16 `frame:"row"`
//	    AmountPEN  int32 `frame:"sum"`  // what the row adds up
//	}
//	func (e *SaleOrder) ToSaleSummaryFrames() []SaleSummaryFrame
//
// Keys, Rows and Sums follow the field order. The method must be pure: it is called on
// what the record held before a write and on what it holds after it, and the frame
// subtracts one from the other. A change of its logic needs the frame's Version bumped.
// ─────────────────────────────────────────────────────────────────────────────

// Projection is a compiled ToFrames method: the frame's columns, taken from the frame struct, and how
// to turn a record into lines. A Spec with a Projection takes its columns and its LinesOf from it.
type Projection struct {
	Keys []Column
	Rows []Column
	Sums []Column
	// fields are the frame struct's tagged fields in line order: Keys, Rows, then Sums.
	fields  []projectedField
	linesOf func(record unsafe.Pointer) ([]Line, error)
}

type projectedField struct {
	name   string
	offset uintptr
	kind   reflect.Kind
}

// ProjectionOf compiles method, a ToFrames method of the record R returning its lines as F structs.
// It panics on a frame struct it can't take, when the schema compiles.
func ProjectionOf[R any, F any](method func(*R) []F) *Projection {
	frameType := reflect.TypeFor[F]()
	projection := &Projection{}
	if frameType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("dataframe: ToFrames must return a slice of structs, not of %v", frameType))
	}
	var keyFields, rowFields, sumFields []projectedField
	for i := range frameType.NumField() {
		field := frameType.Field(i)
		role := field.Tag.Get("frame")
		if role == "" {
			continue
		}
		kind := field.Type.Kind()
		isFloat := kind == reflect.Float32 || kind == reflect.Float64
		if !isFloat && !isIntegerKind(kind) {
			panic(fmt.Sprintf("dataframe: %v field %s must be an integer or a float, not %v", frameType, field.Name, field.Type))
		}
		// The field position names the column in the frame's shape: moving a field rebuilds the frame.
		column := Column{FieldName: field.Name, ColumnID: i + 1, IsFloat: isFloat}
		projected := projectedField{name: field.Name, offset: field.Offset, kind: kind}
		switch role {
		case "key":
			projection.Keys, keyFields = append(projection.Keys, column), append(keyFields, projected)
		case "row":
			projection.Rows, rowFields = append(projection.Rows, column), append(rowFields, projected)
		case "sum":
			projection.Sums, sumFields = append(projection.Sums, column), append(sumFields, projected)
		default:
			panic(fmt.Sprintf(`dataframe: %v field %s has frame:%q: take "key", "row" or "sum"`, frameType, field.Name, role))
		}
	}
	projection.fields = append(append(keyFields, rowFields...), sumFields...)
	if len(keyFields) == 0 || len(rowFields) == 0 {
		panic(fmt.Sprintf(`dataframe: %v needs a frame:"key" and a frame:"row" field`, frameType))
	}

	projection.linesOf = func(record unsafe.Pointer) ([]Line, error) {
		frames := method((*R)(record))
		if len(frames) == 0 {
			return nil, nil
		}
		lineLength := len(projection.fields)
		// One backing array for every line of the record.
		backing := make([]int64, len(frames)*lineLength)
		lines := make([]Line, len(frames))
		for i := range frames {
			line := Line(backing[i*lineLength : (i+1)*lineLength : (i+1)*lineLength])
			framePointer := unsafe.Pointer(&frames[i])
			for v, field := range projection.fields {
				value, err := field.read(framePointer)
				if err != nil {
					return nil, fmt.Errorf("%v %w", frameType, err)
				}
				line[v] = value
			}
			lines[i] = line
		}
		return lines, nil
	}
	return projection
}

func isIntegerKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return true
	}
	return false
}

// read is the field's value in the struct at framePointer as a line keeps it (floats × FloatScale).
func (field projectedField) read(framePointer unsafe.Pointer) (int64, error) {
	pointer := unsafe.Add(framePointer, field.offset)
	switch field.kind {
	case reflect.Int:
		return int64(*(*int)(pointer)), nil
	case reflect.Int8:
		return int64(*(*int8)(pointer)), nil
	case reflect.Int16:
		return int64(*(*int16)(pointer)), nil
	case reflect.Int32:
		return int64(*(*int32)(pointer)), nil
	case reflect.Int64:
		return *(*int64)(pointer), nil
	case reflect.Uint8:
		return int64(*(*uint8)(pointer)), nil
	case reflect.Uint16:
		return int64(*(*uint16)(pointer)), nil
	case reflect.Uint32:
		return int64(*(*uint32)(pointer)), nil
	}
	floatValue := float64(*(*float32)(pointer))
	if field.kind == reflect.Float64 {
		floatValue = *(*float64)(pointer)
	}
	scaled, isKept := ScaledFloat(floatValue)
	if !isKept {
		return 0, fmt.Errorf("field %s holds %v, outside what a frame keeps (±1.1e12, 6 decimals)", field.name, floatValue)
	}
	return scaled, nil
}
