package dynamo

import (
	"github.com/ivanjoz/genix-orm/db/cache"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/utils"
)

// ── By-IDs test entity: partitioned by store, one integer ID ─────────────────

type cachedItem struct {
	StoreID int32  `cb:"1"`
	ID      int64  `cb:"2"`
	Name    string `cb:"3"`
	Updated int32  `cb:"4"`
	Version int16  `json:"ver,omitempty" cb:"-"`
}

type cachedItemTable struct {
	Model[cachedItemTable, cachedItem]
	StoreID Col[*cachedItemTable, int32]
	ID      Col[*cachedItemTable, int64]
	Name    Col[*cachedItemTable, string]
	Updated Col[*cachedItemTable, int32]
}

func (t cachedItemTable) GetSchema() Schema {
	return Schema{
		Entity:     "cached_item",
		TableID:    45678901,
		Partition:  Cols(t.StoreID.Size(16)),
		Keys:       Cols(t.ID.Size(48)),
		CacheByIDs: true,
	}
}

func TestSlotFileWrites(t *testing.T) {
	SetFileStore(utils.NewMemoryFileStore())
	defer SetFileStore(nil)
	items := NewRepo[cachedItemTable, cachedItem]()
	cycleStart := int32(utils.SUnixTimeOf(cache.CycleStartTime(2026)))

	written := []cachedItem{
		{StoreID: 7, ID: 300, Updated: cycleStart + 10_000}, // slot 44, tick 10002
		{StoreID: 7, ID: 556, Updated: cycleStart + 4_000},  // slot 44 too, a lower tick
		{StoreID: 7, ID: 1, Updated: cycleStart + 2_000},    // slot 1, tick 2002
		{StoreID: 8, ID: 300, Updated: cycleStart - 1},      // another partition, the previous cycle
	}
	ptrs := make([]unsafe.Pointer, len(written))
	for i := range written {
		ptrs[i] = unsafe.Pointer(&written[i])
	}
	if err := items.meta.setSlotsUpdated(ptrs); err != nil {
		t.Fatal(err)
	}
	basePKOf := func(storeID int32) string {
		return items.meta.pkValue(unsafe.Pointer(&cachedItem{StoreID: storeID}))
	}
	slotFile, _, err := cache.ReadSlotFile(cache.SlotFileKey(2026, basePKOf(7)))
	if err != nil || cache.SlotTickOf(slotFile, 44) != 10_002 || cache.SlotTickOf(slotFile, 1) != 2_002 || cache.SlotTickOf(slotFile, 2) != 0 {
		t.Fatalf("store 7: slot 44 = %d, slot 1 = %d, slot 2 = %d, err %v; want 10002, 2002, 0",
			cache.SlotTickOf(slotFile, 44), cache.SlotTickOf(slotFile, 1), cache.SlotTickOf(slotFile, 2), err)
	}
	lastCycleFile, _, _ := cache.ReadSlotFile(cache.SlotFileKey(2025, basePKOf(8)))
	if cache.SlotTickOf(lastCycleFile, 44) == 0 {
		t.Fatal("a write stamped before the cycle start must land in the previous cycle's file")
	}

	// A late write with a lower tick never lowers a slot.
	late := cachedItem{StoreID: 7, ID: 300, Updated: cycleStart}
	if err := items.meta.setSlotsUpdated([]unsafe.Pointer{unsafe.Pointer(&late)}); err != nil {
		t.Fatal(err)
	}
	if slotFile, _, _ = cache.ReadSlotFile(cache.SlotFileKey(2026, basePKOf(7))); cache.SlotTickOf(slotFile, 44) != 10_002 {
		t.Fatalf("slot 44 = %d after a late write, want it kept at 10002", cache.SlotTickOf(slotFile, 44))
	}

}

type badCachedRecord struct {
	StoreID int32
	ID      int32
	Code    string
	Updated int32
	Version int16 `json:"ver,omitempty" cb:"-"`
}

type badCachedTable struct {
	Model[badCachedTable, badCachedRecord]
	StoreID Col[*badCachedTable, int32]
	ID      Col[*badCachedTable, int32]
	Code    Col[*badCachedTable, string]
	Updated Col[*badCachedTable, int32]
}

type uncachedRecord struct {
	ID      int32
	Version int16 `json:"ver,omitempty" cb:"-"`
}

type int64UpdatedCachedRecord struct {
	ID      int32
	Updated int64
	Version int16 `json:"ver,omitempty" cb:"-"`
}

func TestCacheByIDsDeclarationRules(t *testing.T) {
	tablePtr := new(badCachedTable)
	populateColumnNames(tablePtr)
	for name, testCase := range map[string]struct {
		build     func()
		wantPanic string
	}{
		"two Keys columns": {func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Keys: Cols(tablePtr.ID.Size(32), tablePtr.Code), CacheByIDs: true},
				reflect.TypeFor[badCachedRecord]())
		}, "exactly one integer Keys column"},
		"a string key": {func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Keys: Cols(tablePtr.Code), CacheByIDs: true},
				reflect.TypeFor[badCachedRecord]())
		}, "exactly one integer Keys column"},
		"two Partition columns": {func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Partition: Cols(tablePtr.StoreID.Size(16), tablePtr.Updated.Size(32)),
				Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true}, reflect.TypeFor[badCachedRecord]())
		}, "at most one Partition column"},
		"an int64 Updated": {func() {
			buildTableMeta(Schema{Entity: "int64_updated_cached", TableID: 67890124, Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true},
				reflect.TypeFor[int64UpdatedCachedRecord]())
		}, "must be an int32"},
		"no Updated field": {func() {
			buildTableMeta(Schema{Entity: "uncached", TableID: 67890123, Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true},
				reflect.TypeFor[uncachedRecord]())
		}, "needs an int32 field"},
	} {
		expectPanic(t, name, testCase.wantPanic, testCase.build)
	}
}

// expectPanic runs build and fails unless it panics with a message holding wantPanic.
func expectPanic(t *testing.T, name, wantPanic string, build func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if recovered, _ := recover().(string); !strings.Contains(recovered, wantPanic) || recovered == "" {
			t.Fatalf("%s: panic %q, want one holding %q", name, recovered, wantPanic)
		}
	}()
	build()
}

// TestQueryCachedIDs needs a live DynamoDB whose table already exists, so it only runs when
// DYNAMO_ENDPOINT points at one. The slot files live in memory.
func TestQueryCachedIDs(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: QueryCachedIDs needs a live DynamoDB")
	}
	SetFileStore(utils.NewMemoryFileStore())
	defer SetFileStore(nil)
	items := NewRepo[cachedItemTable, cachedItem]()
	// A fresh ID per run; ID+256 shares its slot, ID+1 does not. Store 9 holds the same ID as store 7.
	firstID := time.Now().UnixMilli() % 1_000_000_000 * 1000
	written := []cachedItem{
		{StoreID: 7, ID: firstID, Name: "a"},
		{StoreID: 7, ID: firstID + 1, Name: "b"},
		{StoreID: 7, ID: firstID + 256, Name: "c"},
		{StoreID: 9, ID: firstID, Name: "other store"},
	}
	t.Cleanup(func() {
		for i := range written {
			items.Delete(&written[i])
		}
	})
	if err := items.PutMany(written); err != nil {
		t.Fatal(err)
	}
	cycleYear := CurrentYearCycle()
	request := func(cachedIDs ...CachedID) CachedIDsRequest {
		return CachedIDsRequest{CycleYear: cycleYear, IDs: cachedIDs}
	}
	countRecords := func(partitions []CachedPartition[cachedItem]) int {
		count := 0
		for _, partition := range partitions {
			count += len(partition.Records)
		}
		return count
	}
	allIDs := request(CachedID{Partition: 7, ID: firstID}, CachedID{Partition: 7, ID: firstID + 1},
		CachedID{Partition: 7, ID: firstID + 256}, CachedID{Partition: 9, ID: firstID})

	// A slot younger than cache.SlotTrustTicks is not trusted yet: the records come back with Updated 0.
	fetched, err := items.QueryCachedIDs(allIDs)
	if err != nil || countRecords(fetched) != 4 || fetched[0].Records[0].Updated != 0 {
		t.Fatalf("fresh read: got %d records, err %v; want 4 with Updated 0", countRecords(fetched), err)
	}

	// The reads below run as if the trust window had passed since every write: a tick is held as the
	// end of its 2-second window, trusted cache.SlotTrustTicks after it.
	readAfterOverlap := func(request CachedIDsRequest) ([]CachedPartition[cachedItem], error) {
		Now = func() time.Time { return time.Now().Add((cache.SlotTrustTicks + 2) * 2 * time.Second) }
		defer func() { Now = time.Now }()
		return items.QueryCachedIDs(request)
	}
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cycleYear))

	// Nothing held: every record comes back, grouped by partition, stamped with its slot's tick.
	fetched, err = readAfterOverlap(allIDs)
	if err != nil || len(fetched) != 2 || fetched[0].Partition != 7 || fetched[1].Partition != 9 || countRecords(fetched) != 4 {
		t.Fatalf("cold read: got %d partitions, %d records, err %v; want partitions 7 and 9, 4 records", len(fetched), countRecords(fetched), err)
	}
	held := request()
	for _, partition := range fetched {
		for _, record := range partition.Records {
			if record.Updated == 0 {
				t.Fatalf("record %d came back without its slot's tick", record.ID)
			}
			heldTick := uint32(int64(record.Updated)-cycleStart) + 1 // what the client computes
			held.IDs = append(held.IDs, CachedID{Partition: partition.Partition, ID: record.ID, YearTick: heldTick})
		}
	}

	// Ticks held and nothing written: nothing comes back.
	if fetched, err = readAfterOverlap(held); err != nil || countRecords(fetched) != 0 {
		t.Fatalf("warm read: got %d records, err %v; want 0", countRecords(fetched), err)
	}
	// The same ticks from a client on another cycle can't be compared: everything comes back.
	if fetched, err = readAfterOverlap(CachedIDsRequest{CycleYear: cycleYear - 1, IDs: held.IDs}); err != nil || countRecords(fetched) != 4 {
		t.Fatalf("read from another cycle: got %d records, err %v; want 4", countRecords(fetched), err)
	}

	// A write to the first record moves its slot, which it shares with firstID+256, in store 7 only.
	// It is stamped 4 seconds on, past the ticks held, as a write after them would be.
	written[0].Name = "a2"
	Now = func() time.Time { return time.Now().Add(4 * time.Second) }
	err = items.Put(&written[0])
	Now = time.Now
	if err != nil {
		t.Fatal(err)
	}
	fetched, err = readAfterOverlap(held)
	if err != nil || len(fetched) != 1 || fetched[0].Partition != 7 || len(fetched[0].Records) != 2 {
		t.Fatalf("after a write: got %d records, err %v; want the 2 of store 7's moved slot", countRecords(fetched), err)
	}
	for _, record := range fetched[0].Records {
		if record.ID == firstID && record.Name != "a2" {
			t.Fatalf("the rewritten record came back stale: Name = %q", record.Name)
		}
	}
}
