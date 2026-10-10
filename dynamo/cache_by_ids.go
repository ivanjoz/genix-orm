package dynamo

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// By-IDs cache (Schema.CacheByIDs). A client asks "give me records [12, 87, 412]"
// and gets back only the ones that moved since the value it holds for each.
//
// It works on slots, not records: a record belongs to slot uint8(ID), and each
// base pk (one entity partition) has one slot item holding, per slot, the
// Updated of the last write to it:
//
//	pk = base pk ‖ 000   sk = "v"   v0..v255 = <Updated of the slot's last write, native number>
//
// 000 is the fan-out row suffix no cb id takes (they are 1..999), so the item
// sits in the entity's fan-out pk range, where no query or Scan looks.
//
// Write path: after the records are written, one UpdateItem per touched pk SETs
// every touched slot to the write's Updated. Nothing is read first. Setting it
// after the write is what keeps it race-free: a reader that sees the new value
// then reads the records consistently, so it gets the new content; a reader that
// sees the old value only makes the client refetch later. A late write can set a
// slot back to an older Updated; the client compares for equality, so any change
// still makes it refetch.
//
// Read path: one GetItem returns every slot of the partition. A requested ID
// whose client-held value still equals its slot's is not read at all; the rest
// are read with one consistent BatchGetItem, and their Updated is overwritten
// with the slot's (the value the client sends back next time): on a by-IDs read
// "upd" is the slot's last write, everywhere else the record's own.
//
// Updated is a clock, not a sequence: two processes can stamp the same
// millisecond on two records of one slot, and a late write lands after a read
// that already returned its Updated. So a slot value is trusted only once it is
// DeltaOverlap old (the same window the delta cache resends): a younger one is
// returned as 0, which forces a read on the client's next revalidation.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// slotsColumnID is appended to the base pk, where a fan-out index appends its cb id.
	slotsColumnID = "000"
	slotsSK       = "v"
)

// CachedID is one ID of a by-IDs request, with the slot Updated the client holds for it. 0 means
// "nothing held" and always forces a read.
type CachedID struct {
	ID      int64
	Updated int64
}

type cacheByIDsConfig struct {
	idColumn keyCol // the single integer Keys column; its value picks the slot
}

// resolveCacheByIDs validates CacheByIDs' requirements at compile time. The Updated field itself is
// validated by resolveUpdated.
func resolveCacheByIDs(recordType reflect.Type, keys []keyCol) *cacheByIDsConfig {
	if len(keys) != 1 || !keys[0].kind.isInteger() {
		panic(fmt.Sprintf("db: %s uses CacheByIDs and needs exactly one integer Keys column (the ID)", recordType.Name()))
	}
	return &cacheByIDsConfig{idColumn: keys[0]}
}

// slotOfRecordID buckets a record into one of 256 slots; every path must agree on it.
func slotOfRecordID(recordID int64) uint8 { return uint8(recordID) }

func slotAttr(slot uint8) string { return "v" + strconv.Itoa(int(slot)) }

func slotsKey(basePK string) map[string]types.AttributeValue {
	return itemKey(basePK+slotsColumnID, slotsSK)
}

// isSlotsPK tells the slot item apart from the fan-out rows sharing its pk range.
func isSlotsPK(pk string) bool { return strings.HasSuffix(pk, slotsColumnID) }

// slotUpdatedOf reads one slot's Updated from the slot item; 0 (never written) means "unknown".
func slotUpdatedOf(slotsItem map[string]types.AttributeValue, slot uint8) int64 {
	return numberAttrValue(slotsItem, slotAttr(slot))
}

// setSlotsUpdated SETs the slot of every written record to the Updated it was written with (Delete
// stamps its key record too), in one UpdateItem per base pk. It must run after the records are
// written.
func (m *tableMeta) setSlotsUpdated(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	if m.cacheByIDs == nil {
		return nil
	}
	slotUpdatedByPK := map[string]map[uint8]int64{}
	for _, ptr := range ptrs {
		basePK := m.pkValue(ptr)
		if slotUpdatedByPK[basePK] == nil {
			slotUpdatedByPK[basePK] = map[uint8]int64{}
		}
		slot := slotOfRecordID(m.cacheByIDs.idColumn.acc.getI64(ptr))
		slotUpdatedByPK[basePK][slot] = max(slotUpdatedByPK[basePK][slot], m.updated.acc.getI64(ptr))
	}
	for basePK, slotUpdated := range slotUpdatedByPK {
		setClauses := make([]string, 0, len(slotUpdated))
		values := map[string]types.AttributeValue{}
		for slot, updated := range slotUpdated {
			setClauses = append(setClauses, slotAttr(slot)+" = :"+slotAttr(slot))
			values[":"+slotAttr(slot)] = numberAttr(updated)
		}
		_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(tableName()),
			Key:                       slotsKey(basePK),
			UpdateExpression:          aws.String("SET " + strings.Join(setClauses, ", ")),
			ExpressionAttributeValues: values,
		})
		if err != nil {
			return fmt.Errorf("db: %s setting %d slot(s): %w", m.recordType.Name(), len(slotUpdated), err)
		}
	}
	return nil
}

// QueryCachedIDs resolves records by ID for a CacheByIDs entity, skipping every ID whose client-held
// value still equals its slot's. Pass one value per Partition column, in schema order (none without
// Partition); every ID belongs to that partition. IDs that don't exist are left out, like the
// unchanged ones. The returned records carry their slot's Updated in Updated, or 0 while it is
// younger than DeltaOverlap.
func (r *Repo[T, E]) QueryCachedIDs(cachedIDs []CachedID, partitionValues ...any) ([]E, error) {
	m := r.meta
	if m.cacheByIDs == nil {
		return nil, fmt.Errorf("db: %s QueryCachedIDs needs CacheByIDs in its schema", m.recordType.Name())
	}
	if len(partitionValues) != len(m.partition) {
		return nil, fmt.Errorf("db: %s has %d partition column(s), got %d value(s)",
			m.recordType.Name(), len(m.partition), len(partitionValues))
	}
	if len(cachedIDs) == 0 {
		return nil, nil
	}

	// A scratch record carries the partition and, one at a time, each ID, so the
	// keys are built by the same code as on write.
	var keyRecord E
	keyPtr := unsafe.Pointer(&keyRecord)
	for i, partitionColumn := range m.partition {
		partitionColumn.acc.setI64(keyPtr, valueToInt64(partitionValues[i], partitionColumn.fieldName))
	}
	basePK := m.pkValue(keyPtr)

	client, err := Client()
	if err != nil {
		return nil, err
	}
	// Eventually consistent is enough: a lagging slot is older, which only
	// makes the client ask again on its next revalidation.
	slotsOutput, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName()),
		Key:       slotsKey(basePK),
	})
	if err != nil {
		return nil, err
	}
	slots := slotsOutput.Item

	// Slot values at or above it may still be stamped again by a late or concurrent write.
	trustedBelow := UpdatedNow() - DeltaOverlap.Milliseconds()
	trustedSlotUpdated := func(slot uint8) int64 {
		if slotUpdated := slotUpdatedOf(slots, slot); slotUpdated < trustedBelow {
			return slotUpdated
		}
		return 0
	}

	idColumn := m.cacheByIDs.idColumn
	var keysToRead []map[string]types.AttributeValue
	isRequested := map[int64]bool{} // BatchGetItem rejects duplicate keys
	for _, cachedID := range cachedIDs {
		if cachedID.ID < 0 || uint64(cachedID.ID) > maxValueForBits(idColumn.bits) {
			return nil, fmt.Errorf("db: %s ID %d is outside %s's Size(%d)", m.recordType.Name(), cachedID.ID, idColumn.fieldName, idColumn.bits)
		}
		if isRequested[cachedID.ID] {
			continue
		}
		isRequested[cachedID.ID] = true
		slotUpdated := trustedSlotUpdated(slotOfRecordID(cachedID.ID))
		if slotUpdated != 0 && slotUpdated == cachedID.Updated {
			continue
		}
		idColumn.acc.setI64(keyPtr, cachedID.ID)
		keysToRead = append(keysToRead, m.keyOnly(keyPtr))
	}
	if len(keysToRead) == 0 {
		return nil, nil
	}

	// Consistent, so a record is at least as new as the slot value stamped on it.
	items, _, err := batchGet(client, keysToRead, true)
	if err != nil {
		return nil, err
	}
	records := make([]E, len(items))
	for i, item := range items {
		if err := m.unmarshalItem(item, &records[i]); err != nil {
			return nil, err
		}
		recordPtr := unsafe.Pointer(&records[i])
		m.updated.acc.setI64(recordPtr, trustedSlotUpdated(slotOfRecordID(idColumn.acc.getI64(recordPtr))))
	}
	return records, nil
}
