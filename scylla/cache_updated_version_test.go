package scylla

import (
	"strings"
	"testing"
)

func TestSlotVersionPartitionIDPacksLosslessly(t *testing.T) {
	// Both components must survive the round trip at their extremes, since a collision here would
	// silently merge two tenants' or two tables' cached slot versions.
	for _, testCase := range []struct {
		partitionID int32
		tableID     int16
	}{
		{partitionID: 0, tableID: 1},
		{partitionID: 1, tableID: 1},
		{partitionID: 7, tableID: 12},
		{partitionID: MaxCachePartitionID, tableID: MaxTableID},
		{partitionID: MaxCachePartitionID, tableID: 1},
		{partitionID: 1, tableID: MaxTableID},
	} {
		packed, err := makeSlotVersionPartitionID(testCase.partitionID, testCase.tableID)
		if err != nil {
			t.Fatalf("packing partition=%d table=%d failed: %v", testCase.partitionID, testCase.tableID, err)
		}
		decodedTableID := int16(uint32(packed) & uint32(MaxTableID))
		decodedPartitionID := int32(uint32(packed) >> 14)
		if decodedPartitionID != testCase.partitionID || decodedTableID != testCase.tableID {
			t.Fatalf("packed %d decoded to partition=%d table=%d, want partition=%d table=%d",
				packed, decodedPartitionID, decodedTableID, testCase.partitionID, testCase.tableID)
		}
	}
}

func TestSlotVersionPartitionIDRejectsOutOfRangeComponents(t *testing.T) {
	if _, err := makeSlotVersionPartitionID(MaxCachePartitionID+1, 1); err == nil {
		t.Fatal("expected a partition past the 18-bit range to be rejected")
	}
	if _, err := makeSlotVersionPartitionID(1, 0); err == nil {
		t.Fatal("expected table ID 0 to be rejected")
	}
	if _, err := makeSlotVersionPartitionID(-1, 1); err == nil {
		t.Fatal("expected a negative partition to be rejected")
	}
}

func TestStoredSlotVersionNeverTakesTheUnknownSentinel(t *testing.T) {
	// 0 means "the client holds no version", so a stored version must never be 0 — otherwise a
	// wrapped counter would read as a match and a stale record would never be refetched.
	if stored := toStoredSlotVersion(1 << 16); stored != 1 {
		t.Fatalf("expected a wrapped version to skip 0, got %d", stored)
	}
	if stored := toStoredSlotVersion(0); stored != 1 {
		t.Fatalf("expected version 0 to be stored as 1, got %d", stored)
	}
	if stored := toStoredSlotVersion(70_000); stored != uint16(70_000-1<<16) {
		t.Fatalf("expected plain int16 truncation, got %d", stored)
	}
}

func TestSlotOfRecordIDMatchesTheDocumentedBucketing(t *testing.T) {
	// Read and write paths must agree exactly, so this is pinned rather than left implicit.
	if slotOfRecordID(0) != 0 || slotOfRecordID(255) != 255 || slotOfRecordID(256) != 0 || slotOfRecordID(513) != 1 {
		t.Fatal("slotOfRecordID must bucket by the low byte of the record ID")
	}
}

func TestTableIDCollisionIsRejected(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected two tables sharing one TableSchema.ID to panic")
		}
		if !strings.Contains(strings.ToLower(toString(recovered)), "declared by two tables") {
			t.Fatalf("expected the panic to name the collision, got: %v", recovered)
		}
	}()

	MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	MakeScyllaTable[tableIDCollisionRecord, tableIDCollisionSchema]()
}

func toString(value any) string {
	if err, isError := value.(error); isError {
		return err.Error()
	}
	if text, isText := value.(string); isText {
		return text
	}
	return ""
}

// Declares the same ID as deltaViewSchema, which must be refused at compile time.
type tableIDCollisionRecord struct {
	TableStruct[tableIDCollisionSchema, tableIDCollisionRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
}

type tableIDCollisionSchema struct {
	TableStruct[tableIDCollisionSchema, tableIDCollisionRecord]
	CompanyID Col[*tableIDCollisionSchema, int32]
	ID        Col[*tableIDCollisionSchema, int32]
}

func (e tableIDCollisionSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10008,
		Name:      "table_id_collision_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
	}
}

func TestWriteRefusesAVersionWiderThanTheDeltaSlot(t *testing.T) {
	resetORMTableCachesForTesting()

	originalCounterFetcher := getWriteCounterValue
	defer func() { getWriteCounterValue = originalCounterFetcher }()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "test_keyspace"

	// The packer trims overruns from the right, which would bucket versions in groups of ten and
	// break the ">" watermark, so the write has to fail instead.
	getWriteCounterValue = func(string, string, int) (int64, error) {
		return scyllaTable.maxDeltaVersionValue + 1, nil
	}

	records := []deltaViewRecord{{CompanyID: 7, ID: 1}}
	_, err := fetchManagedCounterValues(recordSliceGroup{makeRecordSlice(&records)}, scyllaTable)
	if err == nil {
		t.Fatal("expected a version past the delta slot to be refused")
	}
	if !strings.Contains(err.Error(), "exhausted the delta view slot") {
		t.Fatalf("expected the error to explain the exhausted slot, got: %v", err)
	}

	// One below the limit still packs losslessly and must be accepted.
	getWriteCounterValue = func(string, string, int) (int64, error) {
		return scyllaTable.maxDeltaVersionValue, nil
	}
	if _, err := fetchManagedCounterValues(recordSliceGroup{makeRecordSlice(&records)}, scyllaTable); err != nil {
		t.Fatalf("expected the widest fitting version to be accepted, got: %v", err)
	}
}
