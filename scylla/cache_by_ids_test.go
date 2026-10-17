package scylla

import (
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

func withMemoryFileStore(t *testing.T) *utils.MemoryFileStore {
	t.Helper()
	store := utils.NewMemoryFileStore()
	cache.SetFileStore(store)
	t.Cleanup(func() { cache.SetFileStore(nil) })
	return store
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

// A written record raises its slot in its partition's file to the tick of its Updated.
func TestRaiseWrittenSlotsRaisesThePartitionFile(t *testing.T) {
	resetORMTableCachesForTesting()
	withMemoryFileStore(t)

	scyllaTable := MakeScyllaTable[genericLabelRecord, genericLabelSchema]()
	scyllaTable.Namespace = "genix_test"
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(2026))
	records := []genericLabelRecord{{CompanyID: 7, ID: 300, Updated: int32(cycleStart + 10_000)}}

	if err := raiseWrittenSlots(recordSliceGroup{makeRecordSlice(&records)}, scyllaTable); err != nil {
		t.Fatal(err)
	}
	slotFile, _, err := cache.ReadSlotFile(cache.SlotFileKey(2026, "genix_test/10000/7"))
	if err != nil || cache.SlotTickOf(slotFile, cache.SlotOfRecordID(300)) != cache.WrittenYearTick(cycleStart+10_000, cycleStart) {
		t.Fatalf("slot 44 = %d, err %v", cache.SlotTickOf(slotFile, 44), err)
	}
}

// A written record appends its version to the folder of its pinned values; one whose pinned value
// changed also appends a deleted entry to the folder it left, and a soft delete logs deleted.
func TestAppendDeltaLogsWritesEntriesAndLeftFolders(t *testing.T) {
	resetORMTableCachesForTesting()
	store := withMemoryFileStore(t)

	scyllaTable := MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()
	scyllaTable.Namespace = "genix_test"
	err := appendDeltaLogs(scyllaTable, []deltaWrittenRecord{
		{partition: 7, logID: 1, version: 3, writtenValues: map[string]int64{"warehouse_id": 42, "status": 1},
			storedValues: map[string]int64{"warehouse_id": 41, "status": 1}},
		{partition: 7, logID: 2, version: 1, isDeleted: true, writtenValues: map[string]int64{"warehouse_id": 42, "status": 0}},
	})
	if err != nil {
		t.Fatal(err)
	}

	readEntries := func(folder string) []cache.DeltaLogEntry {
		t.Helper()
		files, _ := store.ListFiles(folder)
		if len(files) != 1 {
			t.Fatalf("%s: expected one bucket file, got %v", folder, files)
		}
		content, _, _ := store.GetFile(files[0])
		entries, err := cache.DecodeDeltaLogEntries(content, false)
		if err != nil {
			t.Fatal(err)
		}
		return entries
	}
	base := "delta-log/genix_test/10028/7/"
	if entries := readEntries(base + "warehouse_id-status/42/"); len(entries) != 2 ||
		entries[0] != (cache.DeltaLogEntry{LogID: 1, Version: 3}) || !entries[1].IsDeleted {
		t.Fatalf("warehouse 42 entries = %+v", entries)
	}
	if entries := readEntries(base + "warehouse_id-status/41/"); len(entries) != 1 || !entries[0].IsDeleted {
		t.Fatalf("warehouse 41 must log the record that left it: %+v", entries)
	}
	if entries := readEntries(base + "status/"); len(entries) != 2 {
		t.Fatalf("the status index logs both records: %+v", entries)
	}
}

// The Metadata record carries −bucket as its key and the Metadata text.
func TestDeltaMetadataRecord(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	record, err := deltaMetadataRecord[deltaViewRecord](scyllaTable, cache.DeltaMetadataFile{Bucket: 5000, Text: "abc"})
	if err != nil || record.ID != -5000 || record.Metadata == nil || *record.Metadata != "abc" {
		t.Fatalf("metadata record = %+v, err %v", record, err)
	}
	// A record without the Metadata field cannot carry the cursor.
	if _, err := deltaMetadataRecord[deltaMultiShapeRecord](scyllaTable, cache.DeltaMetadataFile{Bucket: 1}); err == nil {
		t.Fatal("expected an error for a record without Metadata")
	}
}
