package scylla

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// textSearchWrite is one phase of a write, as the index sees it.
type textSearchWrite struct {
	records db.RecordSlice
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
	info := scyllaTable.TextSearchIndex
	if info == nil || info.Manual {
		return nil
	}

	byPartition := map[int64]*textSearchPartitionChanges{}
	partitions := []int64{}

	for _, write := range writes {
		if write.records.Len() == 0 || !info.Touches(write.columns) {
			continue
		}
		known := info.WritesAll(write.columns)

		for i := range write.records.Len() {
			recordPointer := write.records.At(i)
			recordID := db.ConvertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
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
			case info.ActiveOf(recordPointer):
				changes.puts = append(changes.puts, db.TextDoc{ID: int32(recordID), Text: info.TextOf(recordPointer)})
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
	info := scyllaTable.TextSearchIndex

	columnNames := []string{scyllaTable.Keys[0].GetName()}
	for _, column := range info.Columns {
		columnNames = append(columnNames, column.GetName())
	}
	if info.StatusColumn != nil {
		columnNames = append(columnNames, info.StatusColumn.GetName())
	}

	queryBase := fmt.Sprintf("SELECT %v FROM %v WHERE %v = ?",
		strings.Join(columnNames, ", "), scyllaTable.GetFullName(), scyllaTable.PartKey.GetName())

	run := func(queryString string, values []any) error {
		iter := getScyllaConnection().Query(queryString, values...).Iter()
		scanner := iter.Scanner()

		var recordID, status int64
		texts := make([]string, len(info.Columns))
		targets := []any{&recordID}
		for i := range texts {
			targets = append(targets, &texts[i])
		}
		if info.StatusColumn != nil {
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
