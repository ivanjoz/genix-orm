package scylla

import (
	"fmt"
	"strings"
	"unsafe"

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
func exportToColbin[T any](
	scyllaTable *ScyllaTable, partValue int32, batchSize int,
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

	batch := make([]T, 0, batchSize)
	rowsTotal := int32(0)

	emitCurrentBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		encoded, err := colbin.Marshal(&batch)
		if err != nil {
			return Err("Error al codificar el batch colbin de", scyllaTable.Name, ":", err)
		}
		if err := emitBatch(encoded, int32(len(batch))); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	scanner := iter.Scanner()
	for scanner.Next() {
		if err := scanner.Scan(rowData.Values...); err != nil {
			return rowsTotal, Err("Error on scan for table", scyllaTable.Name, ":", err)
		}

		batch = append(batch, *new(T))
		recordPointer := unsafe.Pointer(&batch[len(batch)-1])
		for valueIndex, column := range columns {
			value := rowData.Values[valueIndex]
			if value == nil {
				continue
			}
			column.SetValue(recordPointer, value)
		}
		rowsTotal++

		if len(batch) >= batchSize {
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
// cannot supply.
func colbinToRecords[T any](scyllaTable *ScyllaTable, encoded []byte, partValue any) ([]T, error) {
	records := []T{}
	if err := colbin.Unmarshal(encoded, &records); err != nil {
		return nil, Err("Error al decodificar el batch colbin de", scyllaTable.Name, ":", err)
	}

	partKey := scyllaTable.GetPartKey()
	if partKey != nil && !partKey.IsNil() {
		for index := range records {
			partKey.SetValue(unsafe.Pointer(&records[index]), partValue)
		}
	}

	return records, nil
}
