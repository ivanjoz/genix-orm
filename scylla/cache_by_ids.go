package scylla

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// The by-IDs cache lets a client ask "give me these records" and get back only the ones that moved.
// The protocol lives in db/cache: every record belongs to the slot uint8(ID), and a write raises its
// record's slot to the Updated it was stamped with. Other drivers keep the slots in year-cycle files
// of the file store; scylla keeps them in its own table, in the same keyspace as the records:
//
//	cache_by_ids_slots: PRIMARY KEY ((table_id, partition_id)), slots map<tinyint, int>
//	                    one row per table partition, slot int8(uint8(ID)) → the full Updated (SUnixTime)
//
// Slots and rows then share one system and one consistency level (LocalQuorum both ways). A map cell
// is last-write-wins, so every slot write carries USING TIMESTAMP = its Updated: the cell with the
// latest stamp wins, a max() with no read and no LWT. On read the full Updated is converted to the
// current cycle's year tick (cache.YearTickOfUpdated), so the client sees what the file-store drivers
// send, yearly reset included.
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

func isIntegerFieldType(fieldType string) bool { return db.IsIntegerFieldType(fieldType) }

func resolveKeyspace(scyllaTable ScyllaTable) string {
	if scyllaTable.Namespace != "" {
		return scyllaTable.Namespace
	}
	return connParams.Keyspace
}

const cacheSlotsTableName = "cache_by_ids_slots"

// cacheSlotsCreateScript is the CREATE of a keyspace's slots table, shared by its CacheByIDs tables.
func cacheSlotsCreateScript(keyspace string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v.%v (
			table_id smallint, partition_id bigint, slots map<tinyint, int>,
			PRIMARY KEY ((table_id, partition_id))
		);`, keyspace, cacheSlotsTableName)
}

// raiseWrittenSlots raises the slot of every written record to its stamped Updated. It runs after
// the rows are written: a slot raised first would let a client read the old row, hold the new tick,
// and never read the record again.
func raiseWrittenSlots(records recordSliceGroup, scyllaTable ScyllaTable) error {
	if !scyllaTable.CacheByIDs || records.len() == 0 {
		return nil
	}
	// USING TIMESTAMP applies to a whole statement, so there is one per partition and stamp. A write
	// call stamps its records with one Updated, so that is normally one per partition.
	type partitionStamp struct {
		partition int64
		updated   int32
	}
	slotsByStamp := map[partitionStamp]map[int8]int32{}
	for i := range records.len() {
		recordPointer := records.at(i)
		stamp := partitionStamp{
			partition: db.ConvertToInt64(scyllaTable.GetPartKey().GetRawValue(recordPointer)),
			updated:   int32(db.ConvertToInt64(scyllaTable.UpdatedCol.GetRawValue(recordPointer))),
		}
		if slotsByStamp[stamp] == nil {
			slotsByStamp[stamp] = map[int8]int32{}
		}
		recordID := db.ConvertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
		slotsByStamp[stamp][int8(cache.SlotOfRecordID(recordID))] = stamp.updated
	}

	// "slots = slots + {…}" sets the listed cells without the tombstone a whole-map assignment writes.
	statement := fmt.Sprintf("UPDATE %v.%v USING TIMESTAMP ? SET slots = slots + ? WHERE table_id = ? AND partition_id = ?",
		resolveKeyspace(scyllaTable), cacheSlotsTableName)
	session := getScyllaConnection()
	batch := session.NewBatch(gocql.UnloggedBatch)
	batch.SetConsistency(gocql.LocalQuorum)
	for stamp, slots := range slotsByStamp {
		// The cell timestamp is the stamp's real time: the latest Updated wins, whatever the arrival order.
		cellTimestamp := cache.SUnixTimeToTime(int64(stamp.updated)).UnixMicro()
		batch.Query(statement, cellTimestamp, slots, scyllaTable.ID, stamp.partition)
	}
	if err := session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("table %q: raising %d by-IDs slot stamp(s): %w", scyllaTable.Name, len(slotsByStamp), err)
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

// planCachedIDsRead checks a by-IDs request against the slots of its partitions, read at LocalQuorum
// like the slot writes, each slot's Updated converted to the current cycle's year tick.
func planCachedIDsRead(scyllaTable ScyllaTable, request cache.CachedIDsRequest, callerName string) (*cache.CachedIDsPlan, error) {
	if !scyllaTable.CacheByIDs {
		return nil, fmt.Errorf(`table %q: %v requires CacheByIDs`, scyllaTable.Name, callerName)
	}
	now := db.Now()
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cache.YearCycleOf(now)))

	var partitions []int64
	for _, cachedID := range request.IDs {
		if !slices.Contains(partitions, cachedID.Partition) {
			partitions = append(partitions, cachedID.Partition)
		}
	}
	queryString := fmt.Sprintf("SELECT partition_id, slots FROM %v.%v WHERE table_id = ? AND partition_id IN ?",
		resolveKeyspace(scyllaTable), cacheSlotsTableName)
	// A partition with no row has had no write: PlanCachedIDs reads it as all slots unwritten.
	slotTicksByPartition := map[int64]*cache.SlotTicks{}
	for _, partitionBatch := range splitIDsIntoBatches(partitions, queryCachedIDsMaxBatchSize) {
		iterator := getScyllaConnection().Query(queryString, scyllaTable.ID, partitionBatch).Consistency(gocql.LocalQuorum).Iter()
		var partition int64
		var updatedBySlot map[int8]int32
		for iterator.Scan(&partition, &updatedBySlot) {
			slotTicks := &cache.SlotTicks{}
			for slot, updated := range updatedBySlot {
				slotTicks[uint8(slot)] = cache.YearTickOfUpdated(int64(updated), cycleStart)
			}
			slotTicksByPartition[partition] = slotTicks
			updatedBySlot = nil
		}
		if err := iterator.Close(); err != nil {
			return nil, fmt.Errorf("table %q: reading by-IDs slots: %w", scyllaTable.Name, err)
		}
	}
	return cache.PlanCachedIDs(request, slotTicksByPartition, now), nil
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
func readRecordsByIDs(sink recordSink, scyllaTable ScyllaTable, idsOfPartition map[int64][]int64) error {
	columnNames := make([]string, 0, len(scyllaTable.Columns))
	for _, column := range scyllaTable.Columns {
		if !column.GetInfo().IsVirtual {
			columnNames = append(columnNames, column.GetName())
		}
	}
	scyllaTable.readConsistency = gocql.LocalQuorum
	scanColumns := buildDefaultScanColumns(columnNames)
	return forEachIDsBatch(scyllaTable, idsOfPartition, strings.Join(columnNames, ", "),
		func(queryString string, queryValues []any, _ int64) error {
			return scanSelectQueryRows(queryString, queryValues, scanColumns, scyllaTable,
				sink, nil, nil, time.Now())
		})
}

// QueryCachedIDs returns, per partition, the requested records whose slot moved since the tick the
// client holds, each with Updated set to the tick it now holds.
func QueryCachedIDs[T TableBaseInterface[E, T], E TableSchemaInterface[E]](request cache.CachedIDsRequest) ([]cache.CachedPartition[T], error) {
	if len(request.IDs) == 0 {
		return nil, nil
	}
	var emptyRecords []T
	partitionsRead, err := queryCachedIDs(MakeScyllaTable[T, E](), request, makeRecordSink(&emptyRecords))
	if err != nil || partitionsRead == nil {
		return nil, err
	}
	partitions := make([]cache.CachedPartition[T], len(partitionsRead))
	for i, partitionRead := range partitionsRead {
		partitions[i].Partition = partitionRead.partition
		partitions[i].Records = make([]T, len(partitionRead.records))
		for j, record := range partitionRead.records {
			partitions[i].Records[j] = *(*T)(record)
		}
	}
	return partitions, nil
}

// cachedPartitionRead is one partition of a QueryCachedIDs answer, its records by pointer.
type cachedPartitionRead struct {
	partition int64
	records   []unsafe.Pointer
}

// queryCachedIDs is QueryCachedIDs over records by pointer, its partitions in ascending order so
// the response is stable. sink only allocates the records: they come back grouped, not appended.
func queryCachedIDs(scyllaTable ScyllaTable, request cache.CachedIDsRequest, sink recordSink) ([]cachedPartitionRead, error) {
	plan, err := planCachedIDsRead(scyllaTable, request, "QueryCachedIDs")
	if err != nil || len(plan.IDsToRead) == 0 {
		return nil, err
	}
	var records []unsafe.Pointer
	if err := readRecordsByIDs(sink.collectInto(&records), scyllaTable, idsByPartition(plan.IDsToRead)); err != nil {
		return nil, err
	}

	recordsByPartition := map[int64][]unsafe.Pointer{}
	for _, recordPointer := range records {
		partition := db.ConvertToInt64(scyllaTable.GetPartKey().GetRawValue(recordPointer))
		id := db.ConvertToInt64(scyllaTable.Keys[0].GetRawValue(recordPointer))
		scyllaTable.UpdatedCol.SetValue(recordPointer,
			coerceManagedIntegerValue(scyllaTable.UpdatedCol, plan.HeldUpdated(partition, id)))
		recordsByPartition[partition] = append(recordsByPartition[partition], recordPointer)
	}
	partitions := make([]cachedPartitionRead, 0, len(recordsByPartition))
	for partition, partitionRecords := range recordsByPartition {
		partitions = append(partitions, cachedPartitionRead{partition: partition, records: partitionRecords})
	}
	slices.SortFunc(partitions, func(left, right cachedPartitionRead) int {
		return cmp.Compare(left.partition, right.partition)
	})
	return partitions, nil
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
