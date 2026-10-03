package scylla

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// configureTextSearchIndex validates a table's TextSearch declaration: a partition column, exactly
// one numeric key (the document ID), persisted string columns for the text and an optional int8
// status column. Schema mistakes panic when the table compiles; they are never recoverable at
// runtime.
func configureTextSearchIndex(scyllaTable *ScyllaTable, schema TableSchema) {
	textSearch := schema.TextSearch
	if textSearch == nil {
		return
	}

	fail := func(format string, args ...any) {
		panic(fmt.Sprintf(`Table "%v": TextSearch `, scyllaTable.Name) + fmt.Sprintf(format, args...))
	}

	if scyllaTable.PartKey == nil || scyllaTable.PartKey.IsNil() {
		fail("requires a partition column")
	}
	if fieldType := scyllaTable.PartKey.GetType().FieldType; fieldType != "int32" && fieldType != "int" {
		fail(`partition column "%v" must be int32. Found: %v`, scyllaTable.PartKey.GetName(), fieldType)
	}
	if len(scyllaTable.Keys) != 1 {
		fail("requires exactly one key column. Found: %v", len(scyllaTable.Keys))
	}
	if fieldType := scyllaTable.Keys[0].GetType().FieldType; !slices.Contains([]string{"int16", "int32", "int64", "int"}, fieldType) {
		fail(`key column "%v" must be numeric. Found: %v`, scyllaTable.Keys[0].GetName(), fieldType)
	}
	if len(textSearch.Columns) == 0 {
		fail("declares no Columns")
	}

	info := &textSearchIndexInfo{manual: textSearch.Manual}

	for _, declared := range textSearch.Columns {
		column := scyllaTable.ColumnsMap[declared.GetName()]
		if column == nil {
			fail(`column "%v" was not found`, declared.GetName())
		}
		if fieldType := column.GetType().FieldType; fieldType != "string" {
			fail(`column "%v" must be string. Found: %v`, column.GetName(), fieldType)
		}
		// A reindex reads the text back from the table, so it has to be stored there.
		if column.GetInfo().IsVirtual {
			fail(`column "%v" is virtual; the indexed text must be persisted`, column.GetName())
		}
		info.columns = append(info.columns, column)
	}

	if textSearch.Status != nil {
		column := scyllaTable.ColumnsMap[textSearch.Status.GetName()]
		if column == nil {
			fail(`status column "%v" was not found`, textSearch.Status.GetName())
		}
		if fieldType := column.GetType().FieldType; fieldType != "int8" {
			fail(`status column "%v" must be int8. Found: %v`, column.GetName(), fieldType)
		}
		info.statusColumn = column
	}

	scyllaTable.textSearchIndex = info
}

// textOf is the indexed text of a record: its TextSearch columns joined by a space.
func (info *textSearchIndexInfo) textOf(recordPointer unsafe.Pointer) string {
	values := make([]string, len(info.columns))
	for i, column := range info.columns {
		values[i], _ = column.GetRawValue(recordPointer).(string)
	}
	return db.JoinSearchText(values...)
}

// activeOf reports whether a record belongs in the index: its status is not 0, or the table has
// no status column.
func (info *textSearchIndexInfo) activeOf(recordPointer unsafe.Pointer) bool {
	return info.statusColumn == nil || convertToInt64(info.statusColumn.GetRawValue(recordPointer)) != 0
}

// writesAll reports whether a write that set columns wrote every column the index depends on, so
// the records in memory hold their stored text and status.
func (info *textSearchIndexInfo) writesAll(columns []IColInfo) bool {
	written := make(map[string]bool, len(columns))
	for _, column := range columns {
		if column != nil {
			written[column.GetName()] = true
		}
	}

	for _, column := range info.columns {
		if !written[column.GetName()] {
			return false
		}
	}
	return info.statusColumn == nil || written[info.statusColumn.GetName()]
}

// touches reports whether a write that set columns can change the index at all.
func (info *textSearchIndexInfo) touches(columns []IColInfo) bool {
	for _, column := range columns {
		if column == nil {
			continue
		}
		if info.statusColumn != nil && column.GetName() == info.statusColumn.GetName() {
			return true
		}
		for _, textColumn := range info.columns {
			if column.GetName() == textColumn.GetName() {
				return true
			}
		}
	}
	return false
}

// textSearchWrite is one phase of a write, as the index sees it.
type textSearchWrite struct {
	records recordSlice
	// columns are the columns the phase wrote.
	columns []IColInfo
	// skip lists the IDs whose text and status are known not to have changed (Merge holds the
	// previous record and compares).
	skip map[int64]bool
}

// textSearchPartitionChanges are the index writes of one partition.
type textSearchPartitionChanges struct {
	puts []db.TextDoc
	dels []int32
	// reread are records whose stored text or status this write did not fully set: they are read
	// back from the table, which already holds their new values.
	reread []int64
}

// syncTextSearchAfterWrite brings the index in line with what a write stored: an active record is
// put with its text, an inactive one is deleted (a delete of a record the index does not hold
// costs nothing: it travels in the same batch). Each partition gets one Apply.
func syncTextSearchAfterWrite(scyllaTable *ScyllaTable, writes ...textSearchWrite) error {
	info := scyllaTable.textSearchIndex
	if info == nil || info.manual {
		return nil
	}

	byPartition := map[int64]*textSearchPartitionChanges{}
	partitions := []int64{}

	for _, write := range writes {
		if write.records.len() == 0 || !info.touches(write.columns) {
			continue
		}
		known := info.writesAll(write.columns)

		for i := range write.records.len() {
			recordPointer := write.records.at(i)
			recordID := convertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
			if write.skip[recordID] {
				continue
			}
			if recordID < math.MinInt32 || recordID > math.MaxInt32 {
				return fmt.Errorf(`text search: table "%v": key %v does not fit int32`, scyllaTable.Name, recordID)
			}

			partValue := scyllaTable.GetPartValue(recordPointer)
			changes := byPartition[partValue]
			if changes == nil {
				changes = &textSearchPartitionChanges{}
				byPartition[partValue] = changes
				partitions = append(partitions, partValue)
			}

			switch {
			case !known:
				changes.reread = append(changes.reread, recordID)
			case info.activeOf(recordPointer):
				changes.puts = append(changes.puts, db.TextDoc{ID: int32(recordID), Text: info.textOf(recordPointer)})
			default:
				changes.dels = append(changes.dels, int32(recordID))
			}
		}
	}

	if len(partitions) == 0 {
		return nil
	}

	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return fmt.Errorf(`text search: table "%v": %w`, scyllaTable.Name, err)
	}

	start := time.Now()
	ctx := context.Background()

	for _, partValue := range partitions {
		changes := byPartition[partValue]

		if len(changes.reread) > 0 {
			found := map[int64]bool{}
			err := scanTextSearchDocs(scyllaTable, partValue, changes.reread, func(doc textSearchStoredDoc) error {
				found[doc.id] = true
				if doc.active {
					changes.puts = append(changes.puts, db.TextDoc{ID: int32(doc.id), Text: doc.text})
				} else {
					changes.dels = append(changes.dels, int32(doc.id))
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf(`text search: table "%v": reading back partition %v: %w`, scyllaTable.Name, partValue, err)
			}
			// Not there anymore: nothing to index.
			for _, recordID := range changes.reread {
				if !found[recordID] {
					changes.dels = append(changes.dels, int32(recordID))
				}
			}
		}

		partition := db.TextSearchPartition(partValue)
		if err := backend.Apply(ctx, scyllaTable.Name, partition, changes.puts, changes.dels); err != nil {
			return fmt.Errorf(`text search: table "%v" partition %v: %w`, scyllaTable.Name, partition, err)
		}
	}

	if DebugFull {
		fmt.Printf("TextSearch sync: table=%s partitions=%d elapsed=%s\n", scyllaTable.Name, len(partitions), time.Since(start))
	}
	return nil
}

// textSearchStoredDoc is a record's index state as the table stores it.
type textSearchStoredDoc struct {
	id     int64
	text   string
	active bool
}

// scanTextSearchDocs reads the key, text columns and status of a partition's records: all of
// them, or those of ids.
func scanTextSearchDocs(scyllaTable *ScyllaTable, partValue int64, ids []int64, emit func(textSearchStoredDoc) error) error {
	info := scyllaTable.textSearchIndex

	columnNames := []string{scyllaTable.Keys[0].GetName()}
	for _, column := range info.columns {
		columnNames = append(columnNames, column.GetName())
	}
	if info.statusColumn != nil {
		columnNames = append(columnNames, info.statusColumn.GetName())
	}

	queryBase := fmt.Sprintf("SELECT %v FROM %v WHERE %v = ?",
		strings.Join(columnNames, ", "), scyllaTable.GetFullName(), scyllaTable.PartKey.GetName())

	run := func(queryString string, values []any) error {
		iter := getScyllaConnection().Query(queryString, values...).Iter()
		scanner := iter.Scanner()

		var recordID, status int64
		texts := make([]string, len(info.columns))
		targets := []any{&recordID}
		for i := range texts {
			targets = append(targets, &texts[i])
		}
		if info.statusColumn != nil {
			targets = append(targets, &status)
		}

		for scanner.Next() {
			status = 1
			if err := scanner.Scan(targets...); err != nil {
				iter.Close()
				return err
			}
			doc := textSearchStoredDoc{id: recordID, text: db.JoinSearchText(texts...), active: status != 0}
			if err := emit(doc); err != nil {
				iter.Close()
				return err
			}
		}
		return iter.Close()
	}

	partitionValue := partitionQueryValue(scyllaTable, partValue)

	if ids == nil {
		return run(queryBase, []any{partitionValue})
	}

	for _, batch := range splitIDsIntoBatches(ids, queryCachedIDsMaxBatchSize) {
		values := []any{partitionValue}
		placeholders := make([]string, len(batch))
		for i, id := range batch {
			values = append(values, id)
			placeholders[i] = "?"
		}
		queryString := fmt.Sprintf("%v AND %v IN (%v)", queryBase, scyllaTable.Keys[0].GetName(), strings.Join(placeholders, ", "))
		if err := run(queryString, values); err != nil {
			return err
		}
	}
	return nil
}

// partitionQueryValue is a partition value typed as its column, for binding.
func partitionQueryValue(scyllaTable *ScyllaTable, partValue int64) any {
	if scyllaTable.PartKey.GetType().FieldType == "int32" {
		return int32(partValue)
	}
	return partValue
}

// textSearchSource streams a partition's active records, for a reindex.
func textSearchSource(scyllaTable *ScyllaTable, partValue int64) db.TextSource {
	return func(_ context.Context, yield func(db.TextDoc) error) error {
		return scanTextSearchDocs(scyllaTable, partValue, nil, func(doc textSearchStoredDoc) error {
			if !doc.active {
				return nil
			}
			if doc.id < math.MinInt32 || doc.id > math.MaxInt32 {
				return fmt.Errorf(`key %v does not fit int32`, doc.id)
			}
			return yield(db.TextDoc{ID: int32(doc.id), Text: doc.text})
		})
	}
}
