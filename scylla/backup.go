package scylla

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/ivanjoz/colbin"
)

// exportableColumns are the columns a backup can round-trip: the clustering keys and
// every plain column that lives in the record struct.
//
// Three kinds are left out. The partition key, because it holds one value for the whole
// export and the restore sets it back on every record — writing it per row pays for a
// constant. Virtual columns, because the ORM recomputes them on insert. And columns with
// no struct field (ORM-managed, write-only), because there is nowhere in T to put them.
func exportableColumns(scyllaTable *ScyllaTable) []IColInfo {
	partKey := scyllaTable.GetPartKey()
	isPartKey := func(column IColInfo) bool {
		return partKey != nil && !partKey.IsNil() && partKey.GetInfo().Idx == column.GetInfo().Idx
	}

	columns := []IColInfo{}
	taken := map[string]bool{}

	appendColumn := func(column IColInfo) {
		if column == nil || column.IsNil() || isPartKey(column) {
			return
		}
		info := column.GetInfo()
		if info.IsVirtual || info.Field == nil || taken[column.GetName()] {
			return
		}
		taken[column.GetName()] = true
		columns = append(columns, column)
	}

	for _, column := range scyllaTable.Keys {
		appendColumn(column)
	}
	for _, column := range scyllaTable.Columns {
		appendColumn(column)
	}

	return columns
}

// exportToColbin streams one partition out in batches: it rebuilds each scanned row into
// a record and hands every full batch of batchSize records to emitBatch, already encoded
// with colbin. Batching is what keeps a big table from being materialized whole — only
// one batch is ever in memory — and it is why the caller gets a callback instead of a blob.
//
// The batch is a reflect-built []recordType rather than a []T so this body compiles once instead
// of once per table; colbin encodes through reflection either way.
func exportToColbin(
	scyllaTable *ScyllaTable, recordType reflect.Type, partValue int32, batchSize int,
	emitBatch func(encoded []byte, rowsCount int32) error,
) (int32, error) {

	columns := exportableColumns(scyllaTable)
	columnNames := make([]string, len(columns))
	for index, column := range columns {
		columnNames[index] = column.GetName()
	}

	whereClause := ""
	if partKey := scyllaTable.GetPartKey(); partKey != nil && !partKey.IsNil() {
		whereClause = fmt.Sprintf(" WHERE %v = %v", partKey.GetName(), partValue)
	}

	queryStr := fmt.Sprintf("SELECT %v FROM %v%v",
		strings.Join(columnNames, ", "), scyllaTable.GetFullName(), whereClause)

	fmt.Println("Table:", scyllaTable.Name, "|", queryStr)

	iter := getScyllaConnection().Query(queryStr).Iter()
	rowData, err := iter.RowData()
	if err != nil {
		return 0, Err("Error on RowData for table", scyllaTable.Name, ":", err)
	}

	batchPointer := reflect.New(reflect.SliceOf(recordType))
	batch := batchPointer.Elem()
	batch.Set(reflect.MakeSlice(batch.Type(), 0, batchSize))
	zeroRecord := reflect.Zero(recordType)
	rowsTotal := int32(0)

	emitCurrentBatch := func() error {
		if batch.Len() == 0 {
			return nil
		}
		encoded, err := colbin.Marshal(batchPointer.Interface())
		if err != nil {
			return Err("Error al codificar el batch colbin de", scyllaTable.Name, ":", err)
		}
		if err := emitBatch(encoded, int32(batch.Len())); err != nil {
			return err
		}
		batch.SetLen(0)
		return nil
	}

	scanner := iter.Scanner()
	for scanner.Next() {
		if err := scanner.Scan(rowData.Values...); err != nil {
			return rowsTotal, Err("Error on scan for table", scyllaTable.Name, ":", err)
		}

		// Appending a zero record also clears the slot a previous batch left behind.
		batch.Set(reflect.Append(batch, zeroRecord))
		recordPointer := batch.Index(batch.Len() - 1).Addr().UnsafePointer()
		for valueIndex, column := range columns {
			value := rowData.Values[valueIndex]
			if value == nil {
				continue
			}
			column.SetValue(recordPointer, value)
		}
		rowsTotal++

		if batch.Len() >= batchSize {
			if err := emitCurrentBatch(); err != nil {
				return rowsTotal, err
			}
		}
	}

	if err := iter.Close(); err != nil {
		return rowsTotal, Err("Error al cerrar el iterador de", scyllaTable.Name, ":", err)
	}

	return rowsTotal, emitCurrentBatch()
}

// colbinToRecords decodes one exported batch back into records, with the partition key
// written on each of them — it was never encoded, so it is the one value the batch itself
// cannot supply. The records come back as a recordSlice over a reflect-built []recordType.
func colbinToRecords(scyllaTable *ScyllaTable, recordType reflect.Type, encoded []byte, partValue any) (recordSlice, error) {
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	if err := colbin.Unmarshal(encoded, recordsPointer.Interface()); err != nil {
		return recordSlice{}, Err("Error al decodificar el batch colbin de", scyllaTable.Name, ":", err)
	}

	records := makeReflectRecordSlice(recordsPointer)
	partKey := scyllaTable.GetPartKey()
	if partKey != nil && !partKey.IsNil() {
		for index := range records.len() {
			partKey.SetValue(records.at(index), partValue)
		}
	}

	return records, nil
}
