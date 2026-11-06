package fdbdriver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db"
)

// Row codec (decision D1, docs/FOUNDATIONDB_PLAN.md §6.3): a row holds every stored column of the
// record, each entry keyed by the column's 16-bit ID, so a managed column with no record field
// survives, a renamed field keeps its data as long as the column name stays, and a read decodes
// only the columns it projects.
//
//	row    [rowVersion:u64 BE] [flags:u8] [entryCount:u16 BE] { [columnID:u16 BE] [length:uvarint] [bytes] }*
//
// rowVersion is the driver's optimistic-concurrency counter (a write's commit condition compares
// it), not the ORM's managed version column. A zero value (0, "", empty slice, nil pointer) is not
// written: an absent column decodes to the zero value, as a null Scylla column does.
//
// Column bytes: integers zigzag varint, floats fixed little-endian, bool one byte, strings and
// []byte raw, integer / float / string slices count-prefixed, any other type colbin.

const (
	rowHeaderBytes = 11
	// maxRowBytes is FoundationDB's value limit. Larger rows would need chunk keys (§6.3), which this
	// driver does not write yet: the write fails instead.
	maxRowBytes = 100_000
)

// managedStamp carries the managed columns a write stamped that have no record field to hold them.
type managedStamp struct {
	created, updated int64
}

// rowVersionOf reads the optimistic-concurrency counter of a stored row.
func rowVersionOf(row []byte) uint64 {
	if len(row) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(row)
}

// encodeRow appends the row of one record.
func (table *FdbTable) encodeRow(row []byte, rowVersion uint64, recordPointer unsafe.Pointer, stamp managedStamp) ([]byte, error) {
	row = binary.BigEndian.AppendUint64(row, rowVersion)
	row = append(row, 0)
	countOffset := len(row)
	row = append(row, 0, 0)
	entryCount := uint16(0)
	var valueBytes []byte
	for index := range table.storedColumns {
		stored := &table.storedColumns[index]
		valueBytes = valueBytes[:0]
		var isPresent bool
		var err error
		if stored.column.Field == nil {
			valueBytes, isPresent = encodeManagedValue(valueBytes, stored.column.Name, stamp)
		} else {
			valueBytes, isPresent, err = encodeColumnValue(valueBytes, stored.column, stored.column.Field.Pointer(recordPointer))
			if err != nil {
				return nil, fmt.Errorf("table %q column %q: %w", table.Name, stored.column.Name, err)
			}
		}
		if !isPresent {
			continue
		}
		row = binary.BigEndian.AppendUint16(row, stored.id)
		row = binary.AppendUvarint(row, uint64(len(valueBytes)))
		row = append(row, valueBytes...)
		entryCount++
	}
	binary.BigEndian.PutUint16(row[countOffset:], entryCount)
	return row, nil
}

func encodeManagedValue(valueBytes []byte, columnName string, stamp managedStamp) ([]byte, bool) {
	switch columnName {
	case managedCreatedColumnName:
		return binary.AppendVarint(valueBytes, stamp.created), stamp.created != 0
	case managedUpdatedColumnName:
		return binary.AppendVarint(valueBytes, stamp.updated), stamp.updated != 0
	}
	return valueBytes, false
}

// columnFilter marks, by position in storedColumns, the columns a read decodes. nil decodes all.
type columnFilter []bool

// decodeRow fills a record from its row. Columns the filter leaves out keep the record's value.
func (table *FdbTable) decodeRow(row []byte, recordPointer unsafe.Pointer, filter columnFilter) error {
	if len(row) < rowHeaderBytes {
		return errors.New("row shorter than its header")
	}
	if row[8]&1 != 0 {
		return fmt.Errorf("table %q: chunked rows are not supported yet", table.Name)
	}
	entryCount := int(binary.BigEndian.Uint16(row[9:11]))
	offset := rowHeaderBytes
	// Rows are written in storedColumns order, so the next entry is usually the next column: a
	// cursor finds it without the map.
	columnCursor := 0
	for range entryCount {
		if offset+2 > len(row) {
			return errors.New("truncated row entry")
		}
		columnID := binary.BigEndian.Uint16(row[offset:])
		valueLength, lengthBytes := binary.Uvarint(row[offset+2:])
		if lengthBytes <= 0 || offset+2+lengthBytes+int(valueLength) > len(row) {
			return errors.New("truncated row entry")
		}
		valueBytes := row[offset+2+lengthBytes : offset+2+lengthBytes+int(valueLength)]
		offset += 2 + lengthBytes + int(valueLength)

		columnPosition := -1
		for scanPosition := columnCursor; scanPosition < len(table.storedColumns); scanPosition++ {
			if table.storedColumns[scanPosition].id == columnID {
				columnPosition = scanPosition
				break
			}
		}
		if columnPosition < 0 {
			knownPosition, isKnown := table.storedColumnPositionByID[columnID]
			if !isKnown {
				continue // a column the schema dropped
			}
			columnPosition = knownPosition
		}
		columnCursor = columnPosition + 1
		if filter != nil && !filter[columnPosition] {
			continue
		}
		column := table.storedColumns[columnPosition].column
		if column.Field == nil {
			continue // a managed column the record does not declare
		}
		if err := decodeColumnValue(column, column.Field.Pointer(recordPointer), valueBytes); err != nil {
			return fmt.Errorf("table %q column %q: %w", table.Name, column.Name, err)
		}
	}
	return nil
}

// encodeColumnValue appends one field's bytes. isPresent is false for a zero value, which is not
// written.
func encodeColumnValue(valueBytes []byte, column *db.ColumnInfo, fieldPointer unsafe.Pointer) ([]byte, bool, error) {
	typeID := column.Type
	if column.IsPointer {
		pointed := *(*unsafe.Pointer)(fieldPointer)
		if pointed == nil {
			return valueBytes, false, nil
		}
		fieldPointer, typeID = pointed, typeID-20
		// A non-nil pointer is a value even when it points to zero: keep it.
		encoded, _, err := encodeValueOfType(valueBytes, column, typeID, fieldPointer)
		return encoded, true, err
	}
	return encodeValueOfType(valueBytes, column, typeID, fieldPointer)
}

func encodeValueOfType(valueBytes []byte, column *db.ColumnInfo, typeID int8, fieldPointer unsafe.Pointer) ([]byte, bool, error) {
	switch typeID {
	case 1:
		text := *(*string)(fieldPointer)
		return append(valueBytes, text...), text != "", nil
	case 2:
		value := *(*int64)(fieldPointer)
		return binary.AppendVarint(valueBytes, value), value != 0, nil
	case 3:
		value := *(*int32)(fieldPointer)
		return binary.AppendVarint(valueBytes, int64(value)), value != 0, nil
	case 4:
		value := *(*int16)(fieldPointer)
		return binary.AppendVarint(valueBytes, int64(value)), value != 0, nil
	case 5:
		value := *(*int8)(fieldPointer)
		return binary.AppendVarint(valueBytes, int64(value)), value != 0, nil
	case 10:
		value := *(*int)(fieldPointer)
		return binary.AppendVarint(valueBytes, int64(value)), value != 0, nil
	case 6:
		value := *(*float32)(fieldPointer)
		return binary.LittleEndian.AppendUint32(valueBytes, math.Float32bits(value)), value != 0, nil
	case 7:
		value := *(*float64)(fieldPointer)
		return binary.LittleEndian.AppendUint64(valueBytes, math.Float64bits(value)), value != 0, nil
	case 8:
		if *(*bool)(fieldPointer) {
			return append(valueBytes, 1), true, nil
		}
		return valueBytes, false, nil
	case 11:
		values := *(*[]string)(fieldPointer)
		valueBytes = binary.AppendUvarint(valueBytes, uint64(len(values)))
		for _, text := range values {
			valueBytes = binary.AppendUvarint(valueBytes, uint64(len(text)))
			valueBytes = append(valueBytes, text...)
		}
		return valueBytes, len(values) > 0, nil
	case 12:
		return appendIntSlice(valueBytes, *(*[]int64)(fieldPointer))
	case 13:
		return appendIntSlice(valueBytes, *(*[]int32)(fieldPointer))
	case 14:
		return appendIntSlice(valueBytes, *(*[]int16)(fieldPointer))
	case 15:
		return appendIntSlice(valueBytes, *(*[]int8)(fieldPointer))
	case 16:
		values := *(*[]float32)(fieldPointer)
		valueBytes = binary.AppendUvarint(valueBytes, uint64(len(values)))
		for _, value := range values {
			valueBytes = binary.LittleEndian.AppendUint32(valueBytes, math.Float32bits(value))
		}
		return valueBytes, len(values) > 0, nil
	case 17:
		values := *(*[]float64)(fieldPointer)
		valueBytes = binary.AppendUvarint(valueBytes, uint64(len(values)))
		for _, value := range values {
			valueBytes = binary.LittleEndian.AppendUint64(valueBytes, math.Float64bits(value))
		}
		return valueBytes, len(values) > 0, nil
	}
	return encodeBlobValue(valueBytes, column, fieldPointer)
}

func appendIntSlice[T ~int64 | ~int32 | ~int16 | ~int8](valueBytes []byte, values []T) ([]byte, bool, error) {
	valueBytes = binary.AppendUvarint(valueBytes, uint64(len(values)))
	for _, value := range values {
		valueBytes = binary.AppendVarint(valueBytes, int64(value))
	}
	return valueBytes, len(values) > 0, nil
}

// encodeBlobValue covers every type with no native form: []byte raw, unsigned integers as uvarint,
// anything else colbin.
func encodeBlobValue(valueBytes []byte, column *db.ColumnInfo, fieldPointer unsafe.Pointer) ([]byte, bool, error) {
	fieldType := column.Field.Type
	if isByteSliceField(column) {
		raw := *(*[]byte)(fieldPointer)
		return append(valueBytes, raw...), len(raw) > 0, nil
	}
	if isUnsignedKind(fieldType.Kind()) {
		value := reflect.NewAt(fieldType, fieldPointer).Elem().Uint()
		return binary.AppendUvarint(valueBytes, value), value != 0, nil
	}
	fieldValue := reflect.NewAt(fieldType, fieldPointer).Elem()
	if fieldValue.IsZero() {
		return valueBytes, false, nil
	}
	encoded, err := colbin.Marshal(fieldValue.Interface())
	if err != nil {
		return nil, false, err
	}
	return append(valueBytes, encoded...), true, nil
}

// decodeColumnValue writes one entry's bytes into its field.
func decodeColumnValue(column *db.ColumnInfo, fieldPointer unsafe.Pointer, valueBytes []byte) error {
	if column.IsPointer {
		pointed := reflect.New(column.Field.Type.Elem())
		if err := decodeValueOfType(column, column.Type-20, pointed.UnsafePointer(), valueBytes, column.Field.Type.Elem()); err != nil {
			return err
		}
		*(*unsafe.Pointer)(fieldPointer) = pointed.UnsafePointer()
		return nil
	}
	return decodeValueOfType(column, column.Type, fieldPointer, valueBytes, column.Field.Type)
}

func decodeValueOfType(column *db.ColumnInfo, typeID int8, fieldPointer unsafe.Pointer, valueBytes []byte, fieldType reflect.Type) error {
	switch typeID {
	case 1:
		*(*string)(fieldPointer) = string(valueBytes)
		return nil
	case 2, 3, 4, 5, 10:
		value, readBytes := binary.Varint(valueBytes)
		if readBytes <= 0 {
			return errors.New("bad varint")
		}
		switch typeID {
		case 2:
			*(*int64)(fieldPointer) = value
		case 3:
			*(*int32)(fieldPointer) = int32(value)
		case 4:
			*(*int16)(fieldPointer) = int16(value)
		case 5:
			*(*int8)(fieldPointer) = int8(value)
		case 10:
			*(*int)(fieldPointer) = int(value)
		}
		return nil
	case 6:
		if len(valueBytes) < 4 {
			return errors.New("short float32")
		}
		*(*float32)(fieldPointer) = math.Float32frombits(binary.LittleEndian.Uint32(valueBytes))
		return nil
	case 7:
		if len(valueBytes) < 8 {
			return errors.New("short float64")
		}
		*(*float64)(fieldPointer) = math.Float64frombits(binary.LittleEndian.Uint64(valueBytes))
		return nil
	case 8:
		*(*bool)(fieldPointer) = len(valueBytes) > 0 && valueBytes[0] != 0
		return nil
	case 11:
		count, offset := binary.Uvarint(valueBytes)
		values := make([]string, 0, count)
		for range count {
			textLength, lengthBytes := binary.Uvarint(valueBytes[offset:])
			if lengthBytes <= 0 || offset+lengthBytes+int(textLength) > len(valueBytes) {
				return errors.New("truncated string list")
			}
			offset += lengthBytes
			values = append(values, string(valueBytes[offset:offset+int(textLength)]))
			offset += int(textLength)
		}
		*(*[]string)(fieldPointer) = values
		return nil
	case 12:
		return decodeIntSlice((*[]int64)(fieldPointer), valueBytes)
	case 13:
		return decodeIntSlice((*[]int32)(fieldPointer), valueBytes)
	case 14:
		return decodeIntSlice((*[]int16)(fieldPointer), valueBytes)
	case 15:
		return decodeIntSlice((*[]int8)(fieldPointer), valueBytes)
	case 16:
		count, offset := binary.Uvarint(valueBytes)
		if offset <= 0 || offset+int(count)*4 > len(valueBytes) {
			return errors.New("truncated float32 list")
		}
		values := make([]float32, count)
		for index := range values {
			values[index] = math.Float32frombits(binary.LittleEndian.Uint32(valueBytes[offset+index*4:]))
		}
		*(*[]float32)(fieldPointer) = values
		return nil
	case 17:
		count, offset := binary.Uvarint(valueBytes)
		if offset <= 0 || offset+int(count)*8 > len(valueBytes) {
			return errors.New("truncated float64 list")
		}
		values := make([]float64, count)
		for index := range values {
			values[index] = math.Float64frombits(binary.LittleEndian.Uint64(valueBytes[offset+index*8:]))
		}
		*(*[]float64)(fieldPointer) = values
		return nil
	}
	// Blob: the same three shapes encodeBlobValue writes.
	if fieldType.Kind() == reflect.Slice && fieldType.Elem().Kind() == reflect.Uint8 {
		*(*[]byte)(fieldPointer) = append([]byte(nil), valueBytes...)
		return nil
	}
	if isUnsignedKind(fieldType.Kind()) {
		value, readBytes := binary.Uvarint(valueBytes)
		if readBytes <= 0 {
			return errors.New("bad uvarint")
		}
		reflect.NewAt(fieldType, fieldPointer).Elem().SetUint(value)
		return nil
	}
	return colbin.Unmarshal(valueBytes, reflect.NewAt(fieldType, fieldPointer).Interface())
}

func decodeIntSlice[T ~int64 | ~int32 | ~int16 | ~int8](destination *[]T, valueBytes []byte) error {
	count, offset := binary.Uvarint(valueBytes)
	if offset <= 0 {
		return errors.New("bad list count")
	}
	values := make([]T, 0, count)
	for range count {
		value, readBytes := binary.Varint(valueBytes[offset:])
		if readBytes <= 0 {
			return errors.New("truncated integer list")
		}
		values = append(values, T(value))
		offset += readBytes
	}
	*destination = values
	return nil
}
