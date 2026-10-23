package scylla

import (
	"encoding/json"
	"reflect"
	"sort"

	"github.com/ivanjoz/genix-orm/db"
)

// The name-addressed data surface of db.Controller. Everything here resolves columns from strings
// and then goes through the ordinary paths the typed API uses — the same select, the same managed
// write — so a caller that only knows a table's name still gets every managed behaviour:
// autoincrement keys, created/updated, version, virtual columns, view fan-out and the text-search
// index. Like the rest of the controller, each method is a shim over a non-generic body (see
// readTableRecords in deploy.go).

// DescribeTable publishes this table's schema in serializable form.
func (e *ScyllaController[T, E]) DescribeTable() db.TableDescription {
	return describeTable(&e.Table, reflect.TypeFor[T]())
}

func describeTable(scyllaTable *ScyllaTable, recordType reflect.Type) db.TableDescription {
	keyNames := make([]string, 0, len(scyllaTable.Keys))
	for _, keyColumn := range scyllaTable.Keys {
		if keyColumn != nil && !keyColumn.IsNil() {
			keyNames = append(keyNames, keyColumn.GetName())
		}
	}

	// Query shapes are the whole point of describing a table before reading it: a predicate set
	// outside this list can only run with ALLOW FILTERING, which is a full partition scan.
	// Several indexes routinely yield the same shape (every view can serve a bare partition
	// read), so the listing is deduplicated and sorted rather than mirroring the index list.
	queryShapes := make([]string, 0, len(scyllaTable.capabilities))
	alreadyListed := map[string]bool{}
	for _, capability := range scyllaTable.capabilities {
		if alreadyListed[capability.Signature] {
			continue
		}
		alreadyListed[capability.Signature] = true
		queryShapes = append(queryShapes, capability.Signature)
	}
	sort.Strings(queryShapes)

	partitionColumn := scyllaTable.GetPartKey()
	partitionName := ""
	if partitionColumn != nil && !partitionColumn.IsNil() {
		partitionName = partitionColumn.GetName()
	}

	managedColumns := []IColInfo{
		scyllaTable.CreatedCol, scyllaTable.UpdatedCol, scyllaTable.VersionCol,
	}

	return db.TableDescription{
		Name:      scyllaTable.Name,
		Namespace: scyllaTable.Namespace,
		ID:        scyllaTable.ID,
		Partition: partitionName,
		Keys:      keyNames,
		Columns: db.DescribeColumns(scyllaTable.Columns, scyllaTable.Keys, partitionColumn,
			managedColumns, recordType),
		QueryShapes: queryShapes,
		CacheByIDs:  scyllaTable.CacheByIDs,
	}
}

// QueryRecordsJSON runs a read described only with strings and marshals the rows with the record
// struct's own json tags, which is the shape InsertRecordsJSON and UpdateRecordsJSON accept back.
func (e *ScyllaController[T, E]) QueryRecordsJSON(spec db.QuerySpec) ([]byte, int, error) {
	return queryRecordsJSON(&e.Table, reflect.TypeFor[T](), spec)
}

func queryRecordsJSON(scyllaTable *ScyllaTable, recordType reflect.Type, spec db.QuerySpec) ([]byte, int, error) {
	tableInfo := &TableInfo{Limit: spec.Limit, AllowFilter: spec.AllowFilter}
	if err := db.ApplyFilters(tableInfo, scyllaTable, spec.Filters); err != nil {
		return nil, 0, err
	}
	if len(spec.Columns) > 0 {
		projection, err := db.ColnsByName(scyllaTable, spec.Columns)
		if err != nil {
			return nil, 0, err
		}
		for _, column := range projection {
			tableInfo.ColumnsInclude = append(tableInfo.ColumnsInclude, column.GetInfo())
		}
	}
	if spec.OrderDesc {
		tableInfo.SetOrderDesc()
	}

	recordsPointer, err := readTableRecords(scyllaTable, recordType, tableInfo)
	if err != nil {
		return nil, 0, err
	}
	payload, err := json.Marshal(recordsPointer.Interface())
	if err != nil {
		return nil, 0, Err("no se pudo serializar los registros de", scyllaTable.Name, ":", err)
	}
	return payload, recordsPointer.Elem().Len(), nil
}

// InsertRecordsJSON inserts a JSON array of records.
func (e *ScyllaController[T, E]) InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error) {
	return insertRecordsJSON(&e.Table, reflect.TypeFor[T](), payload, columnsToExclude)
}

func insertRecordsJSON(scyllaTable *ScyllaTable, recordType reflect.Type, payload []byte, columnsToExclude []string) (int, error) {
	records, err := parseRecordsJSON(scyllaTable, recordType, payload)
	if err != nil {
		return 0, err
	}
	excluded, err := db.ColnsByName(scyllaTable, columnsToExclude)
	if err != nil {
		return 0, err
	}
	if err := insertTableRecords(scyllaTable, records, excluded); err != nil {
		return 0, err
	}
	return records.len(), nil
}

// UpdateRecordsJSON updates only columnsToInclude on every record of the JSON array. The list is
// required: an update with no columns would be a no-op, and defaulting it to "everything" would
// let a partial payload blank out fields the caller never read.
func (e *ScyllaController[T, E]) UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error) {
	return updateRecordsJSON(&e.Table, reflect.TypeFor[T](), payload, columnsToInclude)
}

func updateRecordsJSON(scyllaTable *ScyllaTable, recordType reflect.Type, payload []byte, columnsToInclude []string) (int, error) {
	if len(columnsToInclude) == 0 {
		return 0, Err("update de", scyllaTable.Name, "sin columnas: indique cuáles actualizar")
	}
	records, err := parseRecordsJSON(scyllaTable, recordType, payload)
	if err != nil {
		return 0, err
	}
	included, err := db.ColnsByName(scyllaTable, columnsToInclude)
	if err != nil {
		return 0, err
	}
	err = executeInsertUpdateBatchErased(erasedInsertUpdateParams{
		updateRecords:           records,
		compiledTable:           *scyllaTable,
		useIncludeUpdateColumns: true,
		columnsToUpdate:         included,
	})
	if err != nil {
		return 0, err
	}
	return records.len(), nil
}

// parseRecordsJSON decodes the payload into the table's record type. This is the HTTP payload, which
// is JSON — colbin is the storage encoding and never appears here. Going through encoding/json is
// what makes complex fields work: nested structs, slices and the blob-backed columns all decode by
// their own rules, with no per-type assignment code here.
func parseRecordsJSON(scyllaTable *ScyllaTable, recordType reflect.Type, payload []byte) (recordSlice, error) {
	recordsPointer := reflect.New(reflect.SliceOf(recordType))
	if err := json.Unmarshal(payload, recordsPointer.Interface()); err != nil {
		return recordSlice{}, Err("no se pudo interpretar los registros de", scyllaTable.Name, ":", err)
	}
	records := makeReflectRecordSlice(recordsPointer)
	if records.len() == 0 {
		return recordSlice{}, Err("no se envió ningún registro para", scyllaTable.Name)
	}
	return records, nil
}
