package dynamo

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// ── By-IDs test entity: partitioned by store, one integer ID ─────────────────

type cachedItem struct {
	StoreID int32  `cb:"1"`
	ID      int64  `cb:"2"`
	Name    string `cb:"3"`
	Updated int64  `cb:"4"`
}

type cachedItemTable struct {
	Model[cachedItemTable, cachedItem]
	StoreID Col[*cachedItemTable, int32]
	ID      Col[*cachedItemTable, int64]
	Name    Col[*cachedItemTable, string]
	Updated Col[*cachedItemTable, int64]
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

// yearTickVectors is testdata/year_tick_vectors.json, which genix-ui's wire test reads too.
type yearTickVectors struct {
	EpochSeconds int64 `json:"epochSeconds"`
	Cycles       []struct {
		Updated    int64  `json:"updated"`
		CycleYear  int    `json:"cycleYear"`
		CycleStart *int64 `json:"cycleStart"`
	} `json:"cycles"`
	WrittenTicks []struct {
		Updated     int64  `json:"updated"`
		Tick        uint32 `json:"tick"`
		HeldUpdated int64  `json:"heldUpdated"`
	} `json:"writtenTicks"`
	Requests []struct {
		Encoded string     `json:"encoded"`
		Decoded [][3]int64 `json:"decoded"`
	} `json:"requests"`
}

// withYearTickVectors loads the shared vectors and runs fn with their epoch, restoring the previous one.
func withYearTickVectors(t *testing.T, fn func(vectors yearTickVectors)) {
	content, err := os.ReadFile("testdata/year_tick_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors yearTickVectors
	if err := json.Unmarshal(content, &vectors); err != nil {
		t.Fatal(err)
	}
	previousEpochMillis := updatedEpochMillis
	SetUpdatedEpoch(vectors.EpochSeconds)
	defer func() { updatedEpochMillis = previousEpochMillis }()
	fn(vectors)
}

func TestYearTickVectors(t *testing.T) {
	withYearTickVectors(t, func(vectors yearTickVectors) {
		for _, cycle := range vectors.Cycles {
			if got := yearCycleOf(UpdatedToTime(cycle.Updated)); got != cycle.CycleYear {
				t.Fatalf("Updated %d: cycle %d, want %d", cycle.Updated, got, cycle.CycleYear)
			}
			if cycle.CycleStart != nil && UpdatedOfTime(cycleStartTime(cycle.CycleYear)) != *cycle.CycleStart {
				t.Fatalf("cycle %d starts at %d, want %d", cycle.CycleYear, UpdatedOfTime(cycleStartTime(cycle.CycleYear)), *cycle.CycleStart)
			}
		}
		for _, written := range vectors.WrittenTicks {
			cycleStart := UpdatedOfTime(cycleStartTime(yearCycleOf(UpdatedToTime(written.Updated))))
			tick := writtenYearTick(written.Updated, cycleStart)
			if tick != written.Tick || yearTickToUpdated(tick, cycleStart) != written.HeldUpdated {
				t.Fatalf("Updated %d: tick %d held as %d, want %d held as %d",
					written.Updated, tick, yearTickToUpdated(tick, cycleStart), written.Tick, written.HeldUpdated)
			}
			if tick >= 1<<24 {
				t.Fatalf("tick %d does not fit a uint24", tick)
			}
		}
	})
}

func TestDecodeCachedIDsRequest(t *testing.T) {
	withYearTickVectors(t, func(vectors yearTickVectors) {
		for _, vector := range vectors.Requests {
			request, err := DecodeCachedIDsRequest(vector.Encoded)
			if err != nil {
				t.Fatalf("%s: %v", vector.Encoded, err)
			}
			var decoded [][3]int64
			for _, cachedID := range request.IDs {
				decoded = append(decoded, [3]int64{cachedID.Partition, cachedID.ID, int64(cachedID.YearTick)})
			}
			if request.CycleYear != 2024 || fmt.Sprint(decoded) != fmt.Sprint(vector.Decoded) {
				t.Fatalf("%s: cycle %d, IDs %v; want 2024, %v", vector.Encoded, request.CycleYear, decoded, vector.Decoded)
			}
		}
	})

	for name, encoded := range map[string]string{
		"empty":                "",
		"not base64url":        "GA==",
		"truncated slot":       "GAEAAQxk",     // year, 1 partition, delta 0, 1 slot, then 2 of its 4 bytes
		"IDs not ascending":    "GAEAAAIHAA",   // IDs 7 then delta 0
		"count beyond payload": "GAEAAH8H",     // 127 IDs announced, 1 sent
		"trailing bytes":       "GAEAAAEHAA",   // one ID, then a stray byte
		"slot tick 0":          "GAEAAQwAAAAA", // slot 12 listed with tick 0
	} {
		if _, err := DecodeCachedIDsRequest(encoded); err == nil {
			t.Fatalf("%s: %q decoded without an error", name, encoded)
		}
	}
}

func TestSlotFileWrites(t *testing.T) {
	store := dataframe.NewMemoryStore()
	SetIDsCacheStore(store)
	defer SetIDsCacheStore(nil)
	items := NewRepo[cachedItemTable, cachedItem]()
	cycleStart := UpdatedOfTime(cycleStartTime(2026))

	written := []cachedItem{
		{StoreID: 7, ID: 300, Updated: cycleStart + 10_000}, // slot 44, tick 7
		{StoreID: 7, ID: 556, Updated: cycleStart + 4_000},  // slot 44 too, a lower tick
		{StoreID: 7, ID: 1, Updated: cycleStart + 2_000},    // slot 1, tick 3
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
	slotFile, _, err := readSlotFile(slotFileKey(2026, basePKOf(7)))
	if err != nil || slotTickOf(slotFile, 44) != 7 || slotTickOf(slotFile, 1) != 3 || slotTickOf(slotFile, 2) != 0 {
		t.Fatalf("store 7: slot 44 = %d, slot 1 = %d, slot 2 = %d, err %v; want 7, 3, 0",
			slotTickOf(slotFile, 44), slotTickOf(slotFile, 1), slotTickOf(slotFile, 2), err)
	}
	lastCycleFile, _, _ := readSlotFile(slotFileKey(2025, basePKOf(8)))
	if slotTickOf(lastCycleFile, 44) == 0 {
		t.Fatal("a write stamped before the cycle start must land in the previous cycle's file")
	}

	// A late write with a lower tick never lowers a slot.
	late := cachedItem{StoreID: 7, ID: 300, Updated: cycleStart}
	if err := items.meta.setSlotsUpdated([]unsafe.Pointer{unsafe.Pointer(&late)}); err != nil {
		t.Fatal(err)
	}
	if slotFile, _, _ = readSlotFile(slotFileKey(2026, basePKOf(7))); slotTickOf(slotFile, 44) != 7 {
		t.Fatalf("slot 44 = %d after a late write, want it kept at 7", slotTickOf(slotFile, 44))
	}

	// Concurrent writers of one file, each to its own slot: the conditional PUT keeps every one.
	var waitGroup sync.WaitGroup
	for slot := range 20 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := raiseSlotTicks("2026/concurrent", map[uint8]uint32{uint8(slot): uint32(slot) + 2}); err != nil {
				t.Error(err)
			}
		}()
	}
	waitGroup.Wait()
	slotFile, _, _ = readSlotFile("2026/concurrent")
	for slot := range 20 {
		if slotTickOf(slotFile, uint8(slot)) != uint32(slot)+2 {
			t.Fatalf("slot %d = %d after concurrent writes, want %d", slot, slotTickOf(slotFile, uint8(slot)), slot+2)
		}
	}
}

type badCachedRecord struct {
	StoreID int32
	ID      int32
	Code    string
	Updated int64
}

type badCachedTable struct {
	Model[badCachedTable, badCachedRecord]
	StoreID Col[*badCachedTable, int32]
	ID      Col[*badCachedTable, int32]
	Code    Col[*badCachedTable, string]
	Updated Col[*badCachedTable, int64]
}

type uncachedRecord struct {
	ID int32
}

type int32UpdatedCachedRecord struct {
	ID      int32
	Updated int32
}

func TestCacheByIDsDeclarationRules(t *testing.T) {
	tablePtr := new(badCachedTable)
	populateColumnNames(tablePtr)
	for name, build := range map[string]func(){
		"two Keys columns": func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Keys: Cols(tablePtr.ID.Size(32), tablePtr.Code), CacheByIDs: true},
				reflect.TypeFor[badCachedRecord]())
		},
		"a string key": func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Keys: Cols(tablePtr.Code), CacheByIDs: true},
				reflect.TypeFor[badCachedRecord]())
		},
		"two Partition columns": func() {
			buildTableMeta(Schema{Entity: "bad_cached", TableID: 56789012, Partition: Cols(tablePtr.StoreID.Size(16), tablePtr.Updated.Size(42)),
				Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true}, reflect.TypeFor[badCachedRecord]())
		},
		"an int32 Updated": func() {
			buildTableMeta(Schema{Entity: "int32_updated_cached", TableID: 67890124, Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true},
				reflect.TypeFor[int32UpdatedCachedRecord]())
		},
		"no Updated field": func() {
			buildTableMeta(Schema{Entity: "uncached", TableID: 67890123, Keys: Cols(tablePtr.ID.Size(32)), CacheByIDs: true},
				reflect.TypeFor[uncachedRecord]())
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			build()
		}()
	}
}

// TestQueryCachedIDs needs a live DynamoDB whose table already exists, so it only runs when
// DYNAMO_ENDPOINT points at one. The slot files live in memory.
func TestQueryCachedIDs(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: QueryCachedIDs needs a live DynamoDB")
	}
	SetIDsCacheStore(dataframe.NewMemoryStore())
	defer SetIDsCacheStore(nil)
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

	// A slot younger than DeltaOverlap is not trusted yet: the records come back with Updated 0.
	fetched, err := items.QueryCachedIDs(allIDs)
	if err != nil || countRecords(fetched) != 4 || fetched[0].Records[0].Updated != 0 {
		t.Fatalf("fresh read: got %d records, err %v; want 4 with Updated 0", countRecords(fetched), err)
	}

	// The reads below run as if the overlap had passed since every write.
	readAfterOverlap := func(request CachedIDsRequest) ([]CachedPartition[cachedItem], error) {
		Now = func() time.Time { return time.Now().Add(DeltaOverlap + 3*time.Second) }
		defer func() { Now = time.Now }()
		return items.QueryCachedIDs(request)
	}
	cycleStart := UpdatedOfTime(cycleStartTime(cycleYear))

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
			heldTick := uint32((record.Updated-cycleStart)/yearTickMillis) + 1 // what the client computes
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
	written[0].Name = "a2"
	if err := items.Put(&written[0]); err != nil {
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
