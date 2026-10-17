package scylla

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// The by-IDs cache lets a client ask "give me these records" and get back only the ones that moved.
// The protocol lives in db/cache: every record belongs to the slot uint8(ID), each partition keeps a
// year-cycle slot file of 256 ticks in the ORM's file store, and a write raises its record's slot to
// the tick of the Updated it was stamped with. This file is scylla's side: the partition's base pk,
// the post-write raise, and the reads of the IDs whose slot moved.
//
// On the by-IDs path only, a returned record's Updated is overwritten with its slot tick as the
// client holds it, not its own write time: that is the value the client compares next time.

// configureCacheByIDs validates the table shape the by-IDs cache needs: one integer key (its value is
// the record's ID and slot) and an integer partition.
func configureCacheByIDs(scyllaTable *ScyllaTable) {
	if !scyllaTable.CacheByIDs {
		return
	}
	if len(scyllaTable.Keys) != 1 || !isIntegerFieldType(scyllaTable.Keys[0].GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": CacheByIDs requires exactly one integer key column.`, scyllaTable.Name))
	}
	if partitionColumn := scyllaTable.GetPartKey(); partitionColumn == nil || partitionColumn.IsNil() ||
		!isIntegerFieldType(partitionColumn.GetType().FieldType) {
		panic(fmt.Sprintf(`Table "%v": CacheByIDs requires an integer partition column.`, scyllaTable.Name))
	}
}

func isIntegerFieldType(fieldType string) bool {
	return fieldType == "int16" || fieldType == "int32" || fieldType == "int64"
}

// cacheBasePK names one partition of the table in the cache files: its keyspace, table ID and
// partition value, so tables and keyspaces sharing a file store never collide.
func cacheBasePK(scyllaTable ScyllaTable, partitionValue int64) string {
	return fmt.Sprintf("%s/%d/%d", resolveKeyspace(scyllaTable), scyllaTable.ID, partitionValue)
}

func resolveKeyspace(scyllaTable ScyllaTable) string {
	if scyllaTable.Namespace != "" {
		return scyllaTable.Namespace
	}
	return connParams.Keyspace
}

// raiseWrittenSlots raises the slot of every written record to the tick of its stamped Updated. It
// runs after the rows are written: a slot raised first would let a client read the old row, hold the
// new tick, and never read the record again.
func raiseWrittenSlots(records recordSliceGroup, scyllaTable ScyllaTable) error {
	if !scyllaTable.CacheByIDs || records.len() == 0 {
		return nil
	}
	if cache.FileStore() == nil {
		return fmt.Errorf(`table %q uses CacheByIDs and no file store holds its slot files: call db.SetFileStore at boot`, scyllaTable.Name)
	}
	slotWrites := make([]cache.SlotWrite, records.len())
	for i := range records.len() {
		recordPointer := records.at(i)
		slotWrites[i] = cache.SlotWrite{
			BasePK:  cacheBasePK(scyllaTable, convertToInt64(scyllaTable.GetPartKey().GetRawValue(recordPointer))),
			ID:      convertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer)),
			Updated: convertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer)),
		}
	}
	if err := cache.RaiseWrittenSlots(slotWrites); err != nil {
		return fmt.Errorf("table %q: %w", scyllaTable.Name, err)
	}
	return nil
}

/* Selecting by IDs */

const queryCachedIDsMaxBatchSize = 100

func splitIDsIntoBatches(ids []int64, batchSize int) [][]int64 {
	if len(ids) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = len(ids)
	}

	idBatches := make([][]int64, 0, (len(ids)+batchSize-1)/batchSize)
	for startIndex := 0; startIndex < len(ids); startIndex += batchSize {
		endIndex := min(startIndex+batchSize, len(ids))
		// Copy the batch to keep query input deterministic and isolated from later mutations.
		idBatches = append(idBatches, append([]int64(nil), ids[startIndex:endIndex]...))
	}
	return idBatches
}

// planCachedIDsRead checks a by-IDs request against the slot files of its partitions.
func planCachedIDsRead(scyllaTable ScyllaTable, request cache.CachedIDsRequest, callerName string) (*cache.CachedIDsPlan, error) {
	if !scyllaTable.CacheByIDs {
		return nil, fmt.Errorf(`table %q: %v requires CacheByIDs`, scyllaTable.Name, callerName)
	}
	if cache.FileStore() == nil {
		return nil, fmt.Errorf(`table %q: %v reads slot files and no file store holds them: call db.SetFileStore at boot`, scyllaTable.Name, callerName)
	}
	basePKByPartition := map[int64]string{}
	for _, cachedID := range request.IDs {
		basePKByPartition[cachedID.Partition] = cacheBasePK(scyllaTable, cachedID.Partition)
	}
	return cache.ReadCachedIDsPlan(request, basePKByPartition, db.Now())
}

// idsByPartition groups the IDs to read by partition, each list ascending so the batches are stable.
func idsByPartition(cachedIDs []cache.CachedID) map[int64][]int64 {
	grouped := map[int64][]int64{}
	for _, cachedID := range cachedIDs {
		grouped[cachedID.Partition] = append(grouped[cachedID.Partition], cachedID.ID)
	}
	for _, ids := range grouped {
		slices.Sort(ids)
	}
	return grouped
}

// forEachIDsBatch runs one "partition = ? AND key IN (...)" select per batch of IDs, capped to
// Scylla's clustering-key restriction limit. The caller decides how to scan each batch.
func forEachIDsBatch(
	scyllaTable ScyllaTable, idsOfPartition map[int64][]int64, projection string,
	runBatch func(queryString string, queryValues []any, partition int64) error,
) error {
	for partition, ids := range idsOfPartition {
		for _, idBatch := range splitIDsIntoBatches(ids, queryCachedIDsMaxBatchSize) {
			queryValues := make([]any, 0, len(idBatch)+1)
			queryValues = append(queryValues, coerceManagedIntegerValue(scyllaTable.GetPartKey(), partition))
			for _, id := range idBatch {
				queryValues = append(queryValues, coerceManagedIntegerValue(scyllaTable.Keys[0], id))
			}
			queryString := fmt.Sprintf("SELECT %v FROM %v.%v WHERE %v = ? AND %v IN (%v)",
				projection, resolveKeyspace(scyllaTable), scyllaTable.Name,
				scyllaTable.GetPartKey().GetName(), scyllaTable.Keys[0].GetName(),
				strings.TrimSuffix(strings.Repeat("?, ", len(idBatch)), ", "))
			if err := runBatch(queryString, queryValues, partition); err != nil {
				return err
			}
		}
	}
	return nil
}

// readRecordsByIDs reads whole records by ID at LocalQuorum: a read that follows a slot or log
// write must not hit a replica the row write has not reached yet.
func readRecordsByIDs[T any](scyllaTable ScyllaTable, idsOfPartition map[int64][]int64) ([]T, error) {
	columnNames := make([]string, 0, len(scyllaTable.Columns))
	for _, column := range scyllaTable.Columns {
		if !column.GetInfo().IsVirtual {
			columnNames = append(columnNames, column.GetName())
		}
	}
	scyllaTable.readConsistency = gocql.LocalQuorum
	records := []T{}
	scanColumns := buildDefaultScanColumns(columnNames)
	err := forEachIDsBatch(scyllaTable, idsOfPartition, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, _ int64) error {
			return scanSelectQueryRows(queryString, queryValues, scanColumns, scyllaTable,
				makeRecordSink(&records), nil, nil, time.Now())
		})
	return records, err
}

// QueryCachedIDs returns, per partition, the requested records whose slot moved since the tick the
// client holds, each with Updated set to the tick it now holds.
func QueryCachedIDs[T TableBaseInterface[E, T], E TableSchemaInterface[E]](request cache.CachedIDsRequest) ([]cache.CachedPartition[T], error) {
	if len(request.IDs) == 0 {
		return nil, nil
	}
	scyllaTable := MakeScyllaTable[T, E]()
	plan, err := planCachedIDsRead(scyllaTable, request, "QueryCachedIDs")
	if err != nil || len(plan.IDsToRead) == 0 {
		return nil, err
	}
	records, err := readRecordsByIDs[T](scyllaTable, idsByPartition(plan.IDsToRead))
	if err != nil {
		return nil, err
	}

	recordsByPartition := map[int64][]T{}
	recordsView := makeRecordSlice(&records)
	for i := range recordsView.len() {
		recordPointer := recordsView.at(i)
		partition := convertToInt64(scyllaTable.GetPartKey().GetRawValue(recordPointer))
		id := convertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
		scyllaTable.UpdatedCol.SetValue(recordPointer,
			coerceManagedIntegerValue(scyllaTable.UpdatedCol, plan.HeldUpdated(partition, id)))
		recordsByPartition[partition] = append(recordsByPartition[partition], records[i])
	}
	return groupCachedPartitions(recordsByPartition), nil
}

// groupCachedPartitions lists the partitions in ascending order, so the response is stable.
func groupCachedPartitions[T any](recordsByPartition map[int64][]T) []cache.CachedPartition[T] {
	partitions := make([]cache.CachedPartition[T], 0, len(recordsByPartition))
	for partition, records := range recordsByPartition {
		partitions = append(partitions, cache.CachedPartition[T]{Partition: partition, Records: records})
	}
	slices.SortFunc(partitions, func(left, right cache.CachedPartition[T]) int {
		return cmp.Compare(left.Partition, right.Partition)
	})
	return partitions
}
