package scylla

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ivanjoz/genix-orm/db"

	"github.com/viant/xunsafe"
)

// resolveTextSearchTable compiles table E and asserts it declares TextSearch, so a misconfigured
// table fails with a clear error instead of querying an index that does not exist.
func resolveTextSearchTable[T TableBaseInterface[E, T], E TableSchemaInterface[E]]() (ScyllaTable, error) {
	scyllaTable := MakeScyllaTable[T, E]()
	if scyllaTable.textSearchIndex == nil {
		return scyllaTable, fmt.Errorf(`Table "%v": SearchText requires TextSearch in GetSchema`, scyllaTable.Name)
	}
	return scyllaTable, nil
}

// SearchTextIDs queries the text index of table E in one partition and returns the matching
// active records, best first, without reading ScyllaDB. limit <= 0 lets the backend apply its
// default page size.
func SearchTextIDs[T TableBaseInterface[E, T], E TableSchemaInterface[E]](partition int32, query string, limit, offset int) ([]TextHit, error) {
	scyllaTable, err := resolveTextSearchTable[T, E]()
	if err != nil {
		return nil, err
	}
	backend, err := db.GetTextSearchBackend()
	if err != nil {
		return nil, err
	}
	return backend.Search(context.Background(), scyllaTable.Name, db.TextSearchPartition(int64(partition)), query, limit, offset)
}

// SearchText runs SearchTextIDs, then reads the records from ScyllaDB into refSlice in the order
// of the hits. It returns the hits so callers keep the scores. A hit the table no longer holds is
// simply absent from refSlice.
func SearchText[T TableBaseInterface[E, T], E TableSchemaInterface[E]](refSlice *[]T, partition int32, query string, limit, offset int) ([]TextHit, error) {
	hits, err := SearchTextIDs[T, E](partition, query, limit, offset)
	if err != nil || len(hits) == 0 {
		return hits, err
	}
	scyllaTable, err := resolveTextSearchTable[T, E]()
	if err != nil {
		return hits, err
	}

	ids := make([]int64, len(hits))
	for i, hit := range hits {
		ids[i] = int64(hit.ID)
	}
	fetched := []T{}
	if err := selectRecordsByPartitionIDs(scyllaTable, partition, ids, &fetched); err != nil {
		return hits, err
	}

	// The IN-clause select returns rows in clustering-key order, not relevance order.
	rankByID := make(map[int32]int, len(hits))
	for i, hit := range hits {
		rankByID[hit.ID] = i
	}
	sort.SliceStable(fetched, func(a, b int) bool {
		return rankByID[recordKeyID(scyllaTable, &fetched[a])] < rankByID[recordKeyID(scyllaTable, &fetched[b])]
	})
	*refSlice = append(*refSlice, fetched...)
	return hits, nil
}

// recordKeyID reads a record's single primary-key value as int32, the same way the text-search
// write path reads it.
func recordKeyID[T any](scyllaTable ScyllaTable, record *T) int32 {
	return convertToInt32(scyllaTable.Keys[0].GetRawValue(xunsafe.AsPointer(record)))
}

// selectRecordsByPartitionIDs loads records by primary key within one
// partition into refSlice, reusing the batched IN-clause select that
// QueryCachedIDs uses. No cache-version filtering — every requested ID is
// fetched.
func selectRecordsByPartitionIDs[T any](scyllaTable ScyllaTable, partition int32, ids []int64, refSlice *[]T) error {
	if len(ids) == 0 {
		return nil
	}
	if len(scyllaTable.Namespace) == 0 {
		scyllaTable.Namespace = connParams.Keyspace
	}
	partitionColumn := scyllaTable.PartKey
	keyColumn := scyllaTable.Keys[0]

	columnNames := make([]string, 0, len(scyllaTable.Columns))
	for _, column := range scyllaTable.Columns {
		if column.GetInfo().IsVirtual {
			continue
		}
		columnNames = append(columnNames, column.GetName())
	}

	for _, batch := range splitIDsIntoBatches(ids, queryCachedIDsMaxBatchSize) {
		queryValues := make([]any, 0, len(batch)+1)
		placeholders := make([]string, 0, len(batch))
		queryValues = append(queryValues, partition)
		for _, id := range batch {
			queryValues = append(queryValues, id)
			placeholders = append(placeholders, "?")
		}
		queryString := fmt.Sprintf(
			"SELECT %v FROM %v.%v WHERE %v = ? AND %v IN (%v)",
			strings.Join(columnNames, ", "),
			scyllaTable.Namespace, scyllaTable.Name,
			partitionColumn.GetName(), keyColumn.GetName(),
			strings.Join(placeholders, ", "),
		)
		if err := scanSelectQueryRows(
			queryString, queryValues, buildDefaultScanColumns(columnNames),
			scyllaTable, makeRecordSink(refSlice), nil, nil, time.Now(),
		); err != nil {
			return err
		}
	}
	return nil
}
