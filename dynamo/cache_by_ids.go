package dynamo

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/ivanjoz/genix-orm/db/cache"
)

// By-IDs cache (Schema.CacheByIDs): the protocol, its slot files and its request live in db/cache
// (by_ids.go). This file is dynamo's side: the base pk that names a partition's slot file, the write
// hook and the consistent BatchGetItem of the IDs that moved.

type (
	CachedID               = cache.CachedID
	CachedIDsRequest       = cache.CachedIDsRequest
	CachedPartition[E any] = cache.CachedPartition[E]
)

// DecodeCachedIDsRequest decodes genix-ui's by-IDs request (the "cc" query parameter).
func DecodeCachedIDsRequest(encodedRequest string) (CachedIDsRequest, error) {
	return cache.DecodeCachedIDsRequest(encodedRequest)
}

// CurrentYearCycle is the year cycle a client must compute its ticks in.
func CurrentYearCycle() int { return cache.YearCycleOf(Now()) }

type cacheByIDsConfig struct {
	idColumn keyCol // the single integer Keys column; its value picks the slot
}

// resolveCacheByIDs validates CacheByIDs' requirements at compile time. The Updated field itself is
// validated by resolveUpdated; every Partition column is an integer already.
func resolveCacheByIDs(recordType reflect.Type, partition, keys []keyCol) *cacheByIDsConfig {
	if len(keys) != 1 || !keys[0].kind.isInteger() {
		panic(fmt.Sprintf("db: %s uses CacheByIDs and needs exactly one integer Keys column (the ID)", recordType.Name()))
	}
	if len(partition) > 1 {
		panic(fmt.Sprintf("db: %s uses CacheByIDs and allows at most one Partition column: a request names its partition by one integer", recordType.Name()))
	}
	return &cacheByIDsConfig{idColumn: keys[0]}
}

// setSlotsUpdated raises the slot of every written record to the tick of the Updated it was written
// with (Delete stamps its key record too). It must run after the records are written.
func (m *tableMeta) setSlotsUpdated(ptrs []unsafe.Pointer) error {
	if m.cacheByIDs == nil {
		return nil
	}
	if cache.FileStore() == nil {
		return fmt.Errorf("db: %s uses CacheByIDs and no file store is set (SetFileStore)", m.recordType.Name())
	}
	slotWrites := make([]cache.SlotWrite, len(ptrs))
	for i, ptr := range ptrs {
		slotWrites[i] = cache.SlotWrite{BasePK: m.pkValue(ptr), ID: m.cacheByIDs.idColumn.acc.getI64(ptr), Updated: m.updated.acc.getI64(ptr)}
	}
	if err := cache.RaiseWrittenSlots(slotWrites); err != nil {
		return fmt.Errorf("db: %s %w", m.recordType.Name(), err)
	}
	return nil
}

// QueryCachedIDs resolves records by ID for a CacheByIDs entity, skipping every ID whose held tick
// still equals its slot's. IDs that don't exist are left out, like the unchanged ones. The records
// come grouped by partition, ascending, and carry toUpdated(slot tick) in Updated, or 0 while the
// tick is younger than slotTrustTicks.
func (r *Repo[T, E]) QueryCachedIDs(request CachedIDsRequest) ([]CachedPartition[E], error) {
	m := r.meta
	if m.cacheByIDs == nil {
		return nil, fmt.Errorf("db: %s QueryCachedIDs needs CacheByIDs in its schema", m.recordType.Name())
	}
	if cache.FileStore() == nil {
		return nil, fmt.Errorf("db: %s uses CacheByIDs and no file store is set (SetFileStore)", m.recordType.Name())
	}
	if len(request.IDs) == 0 {
		return nil, nil
	}
	idColumn := m.cacheByIDs.idColumn

	// A scratch record carries each partition and ID in turn, so the keys are built by the same code
	// as on write.
	var keyRecord E
	keyPtr := unsafe.Pointer(&keyRecord)
	basePKByPartition := map[int64]string{}
	for _, cachedID := range request.IDs {
		if cachedID.ID < 0 || uint64(cachedID.ID) > maxValueForBits(idColumn.bits) {
			return nil, fmt.Errorf("db: %s ID %d is outside %s's Size(%d)", m.recordType.Name(), cachedID.ID, idColumn.fieldName, idColumn.bits)
		}
		if _, isKnown := basePKByPartition[cachedID.Partition]; isKnown {
			continue
		}
		if len(m.partition) == 0 {
			if cachedID.Partition != 0 {
				return nil, fmt.Errorf("db: %s has no Partition, got partition %d", m.recordType.Name(), cachedID.Partition)
			}
		} else {
			partitionColumn := m.partition[0]
			if cachedID.Partition < 0 || uint64(cachedID.Partition) > maxValueForBits(partitionColumn.bits) {
				return nil, fmt.Errorf("db: %s partition %d is outside %s's Size(%d)", m.recordType.Name(), cachedID.Partition, partitionColumn.fieldName, partitionColumn.bits)
			}
			partitionColumn.acc.setI64(keyPtr, cachedID.Partition)
		}
		basePKByPartition[cachedID.Partition] = m.pkValue(keyPtr)
	}

	plan, err := cache.ReadCachedIDsPlan(request, basePKByPartition, Now())
	if err != nil {
		return nil, err
	}
	keysToRead := make([]map[string]types.AttributeValue, len(plan.IDsToRead)) // BatchGetItem rejects duplicate keys: the plan has none
	for i, cachedID := range plan.IDsToRead {
		if len(m.partition) > 0 {
			m.partition[0].acc.setI64(keyPtr, cachedID.Partition)
		}
		idColumn.acc.setI64(keyPtr, cachedID.ID)
		keysToRead[i] = m.keyOnly(keyPtr)
	}
	if len(keysToRead) == 0 {
		return nil, nil
	}

	client, err := Client()
	if err != nil {
		return nil, err
	}
	// Consistent, so a record is at least as new as the slot tick stamped on it.
	items, _, err := batchGet(client, keysToRead, true)
	if err != nil {
		return nil, err
	}
	recordsByPartition := map[int64][]E{}
	for _, item := range items {
		var record E
		if err := m.unmarshalItem(item, &record); err != nil {
			return nil, err
		}
		recordPtr := unsafe.Pointer(&record)
		var partition int64
		if len(m.partition) > 0 {
			partition = m.partition[0].acc.getI64(recordPtr)
		}
		m.updated.acc.setI64(recordPtr, plan.HeldUpdated(partition, idColumn.acc.getI64(recordPtr)))
		recordsByPartition[partition] = append(recordsByPartition[partition], record)
	}
	cachedPartitions := make([]CachedPartition[E], 0, len(recordsByPartition))
	for partition, records := range recordsByPartition {
		cachedPartitions = append(cachedPartitions, CachedPartition[E]{Partition: partition, Records: records})
	}
	slices.SortFunc(cachedPartitions, func(a, b CachedPartition[E]) int { return cmp.Compare(a.Partition, b.Partition) })
	return cachedPartitions, nil
}
