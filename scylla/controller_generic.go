package scylla

import (
	"encoding/json"
	"reflect"
	"sort"

	"github.com/ivanjoz/genix-orm/db"
)

// The name-addressed data surface of db.Controller. Everything here resolves columns from strings
// and then goes through the ordinary typed paths — Query, Insert, Update — so a caller that only
// knows a table's name still gets every managed behaviour: autoincrement keys, created/updated,
// updated_version, virtual columns, view fan-out and the text-search index.

// DescribeTable publishes this table's schema in serializable form.
func (e *ScyllaController[T, E]) DescribeTable() db.TableDescription {
	scyllaTable := &e.Table

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
		scyllaTable.CreatedCol, scyllaTable.UpdatedCol, scyllaTable.UpdatedVersionCol,
	}

	return db.TableDescription{
		Name:      scyllaTable.Name,
		Namespace: scyllaTable.Namespace,
		ID:        scyllaTable.ID,
		Partition: partitionName,
		Keys:      keyNames,
		Columns: db.DescribeColumns(scyllaTable.Columns, scyllaTable.Keys, partitionColumn,
			managedColumns, reflect.TypeOf(*new(T))),
		QueryShapes:        queryShapes,
		SaveUpdatedVersion: scyllaTable.SaveUpdatedVersion,
	}
}

// QueryRecordsJSON runs a read described only with strings and marshals the rows with the record
// struct's own json tags, which is the shape InsertRecordsJSON and UpdateRecordsJSON accept back.
func (e *ScyllaController[T, E]) QueryRecordsJSON(spec db.QuerySpec) ([]byte, int, error) {
	records := []T{}
	query := any(Query(&records)).(db.TableGenericQuery[E])

	if err := db.ApplyFilters(query, &e.Table, spec.Filters); err != nil {
		return nil, 0, err
	}

	if len(spec.Columns) > 0 {
		projection, err := db.ColnsByName(&e.Table, spec.Columns)
		if err != nil {
			return nil, 0, err
		}
		query.Select(projection...)
	}
	if spec.Limit > 0 {
		query.Limit(spec.Limit)
	}
	if spec.AllowFilter {
		query.AllowFilter()
	}
	if spec.OrderDesc {
		query.OrderDesc()
	}

	if err := query.Exec(); err != nil {
		return nil, 0, err
	}

	payload, err := json.Marshal(records)
	if err != nil {
		return nil, 0, Err("no se pudo serializar los registros de", e.Table.Name, ":", err)
	}
	return payload, len(records), nil
}

// InsertRecordsJSON inserts a JSON array of records.
func (e *ScyllaController[T, E]) InsertRecordsJSON(payload []byte, columnsToExclude []string) (int, error) {
	records, err := e.parseRecords(payload)
	if err != nil {
		return 0, err
	}
	excluded, err := db.ColnsByName(&e.Table, columnsToExclude)
	if err != nil {
		return 0, err
	}
	if err := Insert(&records, excluded...); err != nil {
		return 0, err
	}
	return len(records), nil
}

// UpdateRecordsJSON updates only columnsToInclude on every record of the JSON array. The list is
// required: an update with no columns would be a no-op, and defaulting it to "everything" would
// let a partial payload blank out fields the caller never read.
func (e *ScyllaController[T, E]) UpdateRecordsJSON(payload []byte, columnsToInclude []string) (int, error) {
	if len(columnsToInclude) == 0 {
		return 0, Err("update de", e.Table.Name, "sin columnas: indique cuáles actualizar")
	}
	records, err := e.parseRecords(payload)
	if err != nil {
		return 0, err
	}
	included, err := db.ColnsByName(&e.Table, columnsToInclude)
	if err != nil {
		return 0, err
	}
	if err := Update(&records, included...); err != nil {
		return 0, err
	}
	return len(records), nil
}

// parseRecords decodes the payload into the table's record type. Going through encoding/json is
// what makes complex fields work — nested structs, slices and CBOR-backed blobs all decode by
// their own rules, with no per-type assignment code here.
func (e *ScyllaController[T, E]) parseRecords(payload []byte) ([]T, error) {
	records := []T{}
	if err := json.Unmarshal(payload, &records); err != nil {
		return nil, Err("no se pudo interpretar los registros de", e.Table.Name, ":", err)
	}
	if len(records) == 0 {
		return nil, Err("no se envió ningún registro para", e.Table.Name)
	}
	return records, nil
}
