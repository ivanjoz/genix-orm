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
// By-IDs cache (Schema.SaveUpdatedVersion), the port of genix-orm/scylla's
// cache_updated_version.go. A client asks "give me records [12, 87, 412]" and
// gets back only the ones that moved since the version it holds for each.
//
// It works on slots, not records: a record belongs to slot uint8(ID), and each
// base pk (one entity partition) has one slot-versions item holding a counter
// per slot:
//
//	pk = base pk ‖ 000   sk = "v"   v0..v255 = <writes to that slot, native number>
//
// 000 is the fan-out row suffix no cb id takes (they are 1..999), so the item
// sits in the entity's fan-out pk range, where no query or Scan looks.
//
// Write path: after the records are written, one UpdateItem per touched pk ADDs
// 1 to every touched slot. Nothing is read first. Bumping after the write is
// what keeps it race-free: a reader that sees the new version then reads the
// records consistently, so it gets the new content; a reader that sees the old
// version only makes the client refetch later.
//
// Read path: one GetItem returns every slot version of the partition. A
// requested ID whose client-held version still equals its slot version is not
// read at all; the rest are read with one consistent BatchGetItem, and their
// UpdatedVersion is stamped with the slot version (the value the client sends
// back next time). Versions are compared truncated to uint16, so a wrap-around
// alias (1 in 65536) is accepted, as in genix.
// ─────────────────────────────────────────────────────────────────────────────

const (
	updatedVersionFieldName = "UpdatedVersion"
	// slotVersionsColumnID is appended to the base pk, where a fan-out index appends its cb id.
	slotVersionsColumnID = "000"
	slotVersionsSK       = "v"
)

// IDUpdatedVersion is one ID of a by-IDs request, with the slot version the
// client holds for it. 0 means "no version held" and always forces a read.
type IDUpdatedVersion struct {
	ID             int64
	UpdatedVersion uint16
}

type updatedVersionConfig struct {
	idColumn keyCol // the single integer Keys column; its value picks the slot
	set      func(unsafe.Pointer, int64)
}

// resolveUpdatedVersion validates SaveUpdatedVersion's requirements at compile time.
func resolveUpdatedVersion(recordType reflect.Type, accessors map[string]*colAccessor, keys []keyCol) *updatedVersionConfig {
	if len(keys) != 1 || !keys[0].kind.isInteger() {
		panic(fmt.Sprintf("db: %s uses SaveUpdatedVersion and needs exactly one integer Keys column (the ID)", recordType.Name()))
	}
	field, ok := recordType.FieldByName(updatedVersionFieldName)
	if !ok || field.Type.Kind() != reflect.Uint16 {
		panic(fmt.Sprintf("db: %s uses SaveUpdatedVersion and needs a uint16 field %q (json \"upv\")", recordType.Name(), updatedVersionFieldName))
	}
	return &updatedVersionConfig{idColumn: keys[0], set: accessors[updatedVersionFieldName].setI64}
}

// slotOfRecordID buckets a record into one of 256 slots; every path must agree on it.
func slotOfRecordID(recordID int64) uint8 { return uint8(recordID) }

func slotVersionAttr(slot uint8) string { return "v" + strconv.Itoa(int(slot)) }

func slotVersionsKey(basePK string) map[string]types.AttributeValue {
	return itemKey(basePK+slotVersionsColumnID, slotVersionsSK)
}

// isSlotVersionsPK tells the slot-versions item apart from the fan-out rows sharing its pk range.
func isSlotVersionsPK(pk string) bool { return strings.HasSuffix(pk, slotVersionsColumnID) }

// slotVersionOf reads one slot's version from the slot-versions item, truncated
// to the uint16 the client holds. A missing slot is 0, "unknown"; a stored
// counter never truncates to 0, which is reserved for that.
func slotVersionOf(slotVersionsItem map[string]types.AttributeValue, slot uint8) uint16 {
	counter, ok := slotVersionsItem[slotVersionAttr(slot)].(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	writeCount, _ := strconv.ParseInt(counter.Value, 10, 64)
	if version := uint16(writeCount); version != 0 {
		return version
	}
	return 1
}

// prepareUpdatedVersions zeroes the managed UpdatedVersion before a write: a
// stored value would be a stale slot version that a client posted back.
func (m *tableMeta) prepareUpdatedVersions(ptrs []unsafe.Pointer) {
	if m.updatedVersion == nil {
		return
	}
	for _, ptr := range ptrs {
		m.updatedVersion.set(ptr, 0)
	}
}

// bumpSlotVersions ADDs 1 to the slot of every written record, in one
// UpdateItem per base pk. It must run after the records are written.
func (m *tableMeta) bumpSlotVersions(client *dynamodb.Client, ptrs []unsafe.Pointer) error {
	if m.updatedVersion == nil {
		return nil
	}
	touchedSlotsByPK := map[string]map[uint8]bool{}
	for _, ptr := range ptrs {
		basePK := m.pkValue(ptr)
		if touchedSlotsByPK[basePK] == nil {
			touchedSlotsByPK[basePK] = map[uint8]bool{}
		}
		touchedSlotsByPK[basePK][slotOfRecordID(m.updatedVersion.idColumn.acc.getI64(ptr))] = true
	}
	for basePK, touchedSlots := range touchedSlotsByPK {
		addClauses := make([]string, 0, len(touchedSlots))
		for slot := range touchedSlots {
			addClauses = append(addClauses, slotVersionAttr(slot)+" :one")
		}
		_, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
			TableName:                 aws.String(tableName()),
			Key:                       slotVersionsKey(basePK),
			UpdateExpression:          aws.String("ADD " + strings.Join(addClauses, ", ")),
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
		})
		if err != nil {
			return fmt.Errorf("db: %s bumping %d slot version(s): %w", m.recordType.Name(), len(touchedSlots), err)
		}
	}
	return nil
}

// QueryCachedIDs resolves records by ID for a SaveUpdatedVersion entity,
// skipping every ID whose client-held version still equals its slot version.
// Pass one value per Partition column, in schema order (none without Partition);
// every ID belongs to that partition. IDs that don't exist are left out, like
// the unchanged ones. The returned records carry their slot version in
// UpdatedVersion.
func (r *Repo[T, E]) QueryCachedIDs(cachedIDs []IDUpdatedVersion, partitionValues ...any) ([]E, error) {
	m := r.meta
	if m.updatedVersion == nil {
		return nil, fmt.Errorf("db: %s QueryCachedIDs needs SaveUpdatedVersion in its schema", m.recordType.Name())
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
	// Eventually consistent is enough: a lagging version is older, which only
	// makes the client ask again on its next revalidation.
	slotVersionsOutput, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(tableName()),
		Key:       slotVersionsKey(basePK),
	})
	if err != nil {
		return nil, err
	}
	slotVersions := slotVersionsOutput.Item

	idColumn := m.updatedVersion.idColumn
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
		serverVersion := slotVersionOf(slotVersions, slotOfRecordID(cachedID.ID))
		if serverVersion != 0 && serverVersion == cachedID.UpdatedVersion {
			continue
		}
		idColumn.acc.setI64(keyPtr, cachedID.ID)
		keysToRead = append(keysToRead, m.keyOnly(keyPtr))
	}
	if len(keysToRead) == 0 {
		return nil, nil
	}

	// Consistent, so a record is at least as new as the slot version stamped on it.
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
		slotVersion := slotVersionOf(slotVersions, slotOfRecordID(idColumn.acc.getI64(recordPtr)))
		m.updatedVersion.set(recordPtr, int64(slotVersion))
	}
	return records, nil
}
