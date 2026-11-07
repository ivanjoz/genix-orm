package obdriver

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// The by-IDs cache (db/cache by_ids.go) as Scylla keeps it (scylla/cache_by_ids.go): every record
// belongs to the slot uint8(ID), and `<database>._cache_by_ids_slots` holds per table partition and
// slot the full Updated of its latest write. A write raises its slots in its own transaction, so the
// slot and the row commit together; a read converts each slot's Updated to the current cycle's year
// tick, so clients see the ticks every driver sends.

// raiseWrittenSlots raises, inside the write's transaction, the slot of every record of [chunkStart,
// chunkEnd) to the Updated it was stamped with; a slot never goes back.
func (table *ObTable) raiseWrittenSlots(transaction *sql.Tx, ctx context.Context, records db.RecordSlice, chunkStart, chunkEnd int) error {
	type slotKey struct {
		partition int64
		slot      uint8
	}
	updatedBySlot := map[slotKey]int64{}
	for recordIndex := chunkStart; recordIndex < chunkEnd; recordIndex++ {
		recordPointer := records.At(recordIndex)
		key := slotKey{
			partition: db.ConvertToInt64(table.GetPartKey().GetRawValue(recordPointer)),
			slot:      cache.SlotOfRecordID(db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))),
		}
		updatedBySlot[key] = max(updatedBySlot[key], db.ConvertToInt64(table.UpdatedCol.GetRawValue(recordPointer)))
	}
	args := make([]any, 0, len(updatedBySlot)*4)
	for key, updated := range updatedBySlot {
		args = append(args, table.ID, key.partition, key.slot, updated)
	}
	statement := fmt.Sprintf("INSERT INTO %s (`table_id`, `partition_id`, `slot`, `updated`) VALUES %s ON DUPLICATE KEY UPDATE `updated` = GREATEST(`updated`, VALUES(`updated`))",
		cacheSlotsTableSpec(table.Namespace).qualifiedName(), strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?), ", len(updatedBySlot)), ", "))
	if _, err := transaction.ExecContext(ctx, statement, args...); err != nil {
		return fmt.Errorf("table %q: raising %d by-IDs slot(s): %w", table.Name, len(updatedBySlot), err)
	}
	return nil
}

// planCachedIDsRead checks a by-IDs request against the slots of its partitions.
func (table *ObTable) planCachedIDsRead(request cache.CachedIDsRequest) (*cache.CachedIDsPlan, error) {
	if !table.CacheByIDs {
		return nil, fmt.Errorf(`table %q: QueryCachedIDs requires CacheByIDs`, table.Name)
	}
	pool, err := sharedConnection()
	if err != nil {
		return nil, err
	}
	now := db.Now()
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cache.YearCycleOf(now)))
	partitions := []any{}
	for _, cachedID := range request.IDs {
		if !slices.Contains(partitions, any(cachedID.Partition)) {
			partitions = append(partitions, cachedID.Partition)
		}
	}
	// A partition with no row has had no write: PlanCachedIDs reads it as all slots unwritten.
	rows, err := pool.Query(fmt.Sprintf("SELECT `partition_id`, `slot`, `updated` FROM %s WHERE `table_id` = ? AND `partition_id` IN (%s)",
		cacheSlotsTableSpec(table.Namespace).qualifiedName(), strings.TrimSuffix(strings.Repeat("?, ", len(partitions)), ", ")),
		append([]any{table.ID}, partitions...)...)
	if err != nil {
		return nil, fmt.Errorf("obdriver: reading the by-IDs slots of %q: %w", table.Name, err)
	}
	defer rows.Close()
	slotTicksByPartition := map[int64]*cache.SlotTicks{}
	for rows.Next() {
		var partition, updated int64
		var slot uint8
		if err := rows.Scan(&partition, &slot, &updated); err != nil {
			return nil, err
		}
		if slotTicksByPartition[partition] == nil {
			slotTicksByPartition[partition] = &cache.SlotTicks{}
		}
		slotTicksByPartition[partition][slot] = cache.YearTickOfUpdated(updated, cycleStart)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cache.PlanCachedIDs(request, slotTicksByPartition, now), nil
}

// cachedPartitionRead is one partition of a QueryCachedIDs answer, its records by pointer.
type cachedPartitionRead struct {
	partition int64
	records   []unsafe.Pointer
}

// queryCachedIDs returns, per partition in ascending order, the requested records whose slot moved
// since the tick the client holds, each with Updated set to the tick it now holds.
func (table *ObTable) queryCachedIDs(request cache.CachedIDsRequest, sink recordSink) ([]cachedPartitionRead, error) {
	if len(request.IDs) == 0 {
		return nil, nil
	}
	plan, err := table.planCachedIDsRead(request)
	if err != nil || len(plan.IDsToRead) == 0 {
		return nil, err
	}
	var records []unsafe.Pointer
	keyNames := []string{table.GetPartKey().GetName(), table.Keys[0].GetName()}
	selectedColumns := table.projectedColumns(&db.TableInfo{})
	selectedNames := make([]string, len(selectedColumns))
	for position, stored := range selectedColumns {
		selectedNames[position] = stored.name
	}
	for batchStart := 0; batchStart < len(plan.IDsToRead); batchStart += writeChunkRecords {
		batch := plan.IDsToRead[batchStart:min(batchStart+writeChunkRecords, len(plan.IDsToRead))]
		args := make([]any, 0, len(batch)*2)
		for _, cachedID := range batch {
			args = append(args, cachedID.Partition, cachedID.ID)
		}
		statement := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s)", quotedNames(selectedNames), table.qualifiedName(),
			quotedNames(keyNames), strings.TrimSuffix(strings.Repeat("(?, ?), ", len(batch)), ", "))
		if err := table.queryInto(statement, args, selectedColumns, sink.collectInto(&records)); err != nil {
			return nil, err
		}
	}

	recordsByPartition := map[int64][]unsafe.Pointer{}
	for _, recordPointer := range records {
		partition := db.ConvertToInt64(table.GetPartKey().GetRawValue(recordPointer))
		id := db.ConvertToInt64(table.Keys[0].GetRawValue(recordPointer))
		table.UpdatedCol.SetValue(recordPointer, db.CoerceManagedInteger(table.UpdatedCol, plan.HeldUpdated(partition, id)))
		recordsByPartition[partition] = append(recordsByPartition[partition], recordPointer)
	}
	partitions := make([]cachedPartitionRead, 0, len(recordsByPartition))
	for partition, partitionRecords := range recordsByPartition {
		partitions = append(partitions, cachedPartitionRead{partition: partition, records: partitionRecords})
	}
	slices.SortFunc(partitions, func(left, right cachedPartitionRead) int { return cmp.Compare(left.partition, right.partition) })
	return partitions, nil
}
