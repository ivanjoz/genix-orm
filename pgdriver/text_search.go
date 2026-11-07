package pgdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// Text search (plan §15: the existing backend interface, db.TextSearchBackend), as Scylla's driver
// keeps it (scylla/text_search_index.go, text_search_query.go): after a write commits, an active record
// is put with its text, an inactive one deleted, one Apply per partition. A phase that did not write
// every text and status column reads its records back from the table, which holds their new values.

// textSearchWrite is one phase of a write, as the index sees it.
type textSearchWrite struct {
	records db.RecordSlice
	columns []db.IColInfo
}

type textSearchPartitionChanges struct {
	puts   []db.TextDoc
	dels   []int32
	reread []int64
}

func (table *PgTable) syncTextSearchAfterWrite(writes []textSearchWrite) error {
	info := table.TextSearchIndex
	if info == nil || info.Manual {
		return nil
	}
	byPartition := map[int64]*textSearchPartitionChanges{}
	partitions := []int64{}
	for _, write := range writes {
		if write.records.Len() == 0 || !info.Touches(write.columns) {
			continue
		}
		isKnown := info.WritesAll(write.columns)
		for recordIndex := range write.records.Len() {
			recordPointer := write.records.At(recordIndex)
			recordID := db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))
			if recordID < math.MinInt32 || recordID > math.MaxInt32 {
				return fmt.Errorf(`text search: table "%v": key %v does not fit int32`, table.Name, recordID)
			}
			partition := table.GetPartValue(recordPointer)
			changes := byPartition[partition]
			if changes == nil {
				changes = &textSearchPartitionChanges{}
				byPartition[partition] = changes
				partitions = append(partitions, partition)
			}
			switch {
			case !isKnown:
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
		return fmt.Errorf(`text search: table "%v": %w`, table.Name, err)
	}
	for _, partition := range partitions {
		changes := byPartition[partition]
		if len(changes.reread) > 0 {
			found := map[int64]bool{}
			err := table.scanTextSearchDocs(partition, changes.reread, func(doc textSearchStoredDoc) error {
				found[doc.id] = true
				if doc.active {
					changes.puts = append(changes.puts, db.TextDoc{ID: int32(doc.id), Text: doc.text})
				} else {
					changes.dels = append(changes.dels, int32(doc.id))
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf(`text search: table "%v": reading back partition %v: %w`, table.Name, partition, err)
			}
			for _, recordID := range changes.reread {
				if !found[recordID] {
					changes.dels = append(changes.dels, int32(recordID))
				}
			}
		}
		if err := backend.Apply(context.Background(), table.Name, db.TextSearchPartition(partition), changes.puts, changes.dels); err != nil {
			return fmt.Errorf(`text search: table "%v" partition %v: %w`, table.Name, partition, err)
		}
	}
	return nil
}

// textSearchStoredDoc is a record's index state as the table stores it.
type textSearchStoredDoc struct {
	id     int64
	text   string
	active bool
}

// scanTextSearchDocs reads the key, text columns and status of a partition's records: all of them,
// or those of ids.
func (table *PgTable) scanTextSearchDocs(partition int64, ids []int64, emit func(textSearchStoredDoc) error) error {
	info := table.TextSearchIndex
	selectedNames := []string{table.Keys[0].GetName()}
	for _, column := range info.Columns {
		selectedNames = append(selectedNames, column.GetName())
	}
	if info.StatusColumn != nil {
		selectedNames = append(selectedNames, info.StatusColumn.GetName())
	}
	statementBase := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", quotedNames(selectedNames), table.qualifiedName(), quoteName(table.GetPartKey().GetName()))
	pool, err := sharedConnection()
	if err != nil {
		return err
	}
	run := func(statement string, args []any) error {
		rows, err := queryStatement(pool, context.Background(), statement, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var recordID, status int64
		texts := make([]sql.NullString, len(info.Columns))
		targets := []any{&recordID}
		for position := range texts {
			targets = append(targets, &texts[position])
		}
		if info.StatusColumn != nil {
			targets = append(targets, &status)
		}
		for rows.Next() {
			status = 1
			if err := rows.Scan(targets...); err != nil {
				return err
			}
			textValues := make([]string, len(texts))
			for position, text := range texts {
				textValues[position] = text.String
			}
			if err := emit(textSearchStoredDoc{id: recordID, text: db.JoinSearchText(textValues...), active: status != 0}); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	if ids == nil {
		return run(statementBase, []any{partition})
	}
	for batchStart := 0; batchStart < len(ids); batchStart += writeChunkRecords {
		batch := ids[batchStart:min(batchStart+writeChunkRecords, len(ids))]
		args := []any{partition}
		for _, id := range batch {
			args = append(args, id)
		}
		statement := fmt.Sprintf("%s AND %s IN (%s)", statementBase, quoteName(table.Keys[0].GetName()), strings.TrimSuffix(strings.Repeat("?, ", len(batch)), ", "))
		if err := run(statement, args); err != nil {
			return err
		}
	}
	return nil
}

// textSearchSource streams a partition's active records, for a reindex.
func (table *PgTable) textSearchSource(partition int64) db.TextSource {
	return func(_ context.Context, yield func(db.TextDoc) error) error {
		return table.scanTextSearchDocs(partition, nil, func(doc textSearchStoredDoc) error {
			if !doc.active {
				return nil
			}
			return yield(db.TextDoc{ID: int32(doc.id), Text: doc.text})
		})
	}
}

// searchTextIDs queries the text index of the table in one partition: the matching active records,
// best first.
func (table *PgTable) searchTextIDs(partition int32, query string, limit, offset int) ([]db.TextHit, error) {
	if table.TextSearchIndex == nil {
		return nil, fmt.Errorf(`Table "%v": SearchText requires TextSearch in GetSchema`, table.Name)
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return nil, err
	}
	return backend.Search(context.Background(), table.Name, db.TextSearchPartition(int64(partition)), query, limit, offset)
}

// appendTextSearchRecords reads the records of the hits into sink, in the order of the hits.
func (table *PgTable) appendTextSearchRecords(sink recordSink, partition int32, hits []db.TextHit) error {
	if len(hits) == 0 {
		return nil
	}
	selectedColumns := table.projectedColumns(&db.TableInfo{})
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	args := []any{partition}
	for _, hit := range hits {
		args = append(args, hit.ID)
	}
	var fetched []unsafe.Pointer
	statement := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? AND %s IN (%s)", quotedNames(selectedNames), table.qualifiedName(),
		quoteName(table.GetPartKey().GetName()), quoteName(table.Keys[0].GetName()), strings.TrimSuffix(strings.Repeat("?, ", len(hits)), ", "))
	if err := table.queryInto(statement, args, selectedColumns, sink.collectInto(&fetched)); err != nil {
		return err
	}
	rankByID := make(map[int64]int, len(hits))
	for rank, hit := range hits {
		rankByID[int64(hit.ID)] = rank
	}
	slices.SortStableFunc(fetched, func(left, right unsafe.Pointer) int {
		return rankByID[db.ConvertToInt64(table.Keys[0].GetRawValue(left))] - rankByID[db.ConvertToInt64(table.Keys[0].GetRawValue(right))]
	})
	for _, record := range fetched {
		sink.appendRecord(record)
	}
	return nil
}

// reindexTextSearch rebuilds one partition's text index from the table's active records. Writes that
// land while it runs reach the index after their commit, on top of what the rebuild read.
func (table *PgTable) reindexTextSearch(partition int32) error {
	if table.TextSearchIndex == nil {
		return nil
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return err
	}
	if err := backend.Rebuild(context.Background(), table.Name, db.TextSearchPartition(int64(partition)), table.textSearchSource(int64(partition))); err != nil {
		return fmt.Errorf(`ReindexTextSearch: table "%v" partition %v: %w`, table.Name, partition, err)
	}
	return nil
}

// listPartitions lists the partition values that have records, ascending. Text-search partitions are
// int32: a value out of range fails instead of indexing under a wrapped number.
func (table *PgTable) listPartitions() ([]int32, error) {
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	partitionName := quoteName(table.GetPartKey().GetName())
	rows, err := pool.Query(fmt.Sprintf("SELECT DISTINCT %s FROM %s ORDER BY %s", partitionName, table.qualifiedName(), partitionName))
	if err != nil {
		return nil, fmt.Errorf(`listing the partitions of "%v": %w`, table.Name, err)
	}
	defer rows.Close()
	var partitions []int32
	for rows.Next() {
		var partition int64
		if err := rows.Scan(&partition); err != nil {
			return nil, err
		}
		if partition < math.MinInt32 || partition > math.MaxInt32 {
			return nil, fmt.Errorf(`table "%v": partition %v does not fit int32`, table.Name, partition)
		}
		partitions = append(partitions, int32(partition))
	}
	return partitions, rows.Err()
}
