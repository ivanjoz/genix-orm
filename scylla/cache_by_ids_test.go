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

	MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
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

// Declares the same ID as fixedViewViewSchema, which must be refused at compile time.
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
