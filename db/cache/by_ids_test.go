package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/ivanjoz/genix-orm/utils"
)

// yearTickVectors is testdata/year_tick_vectors.json, which genix-ui's wire test reads too. Updated
// values are SUnixTime.
type yearTickVectors struct {
	Cycles []struct {
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

func readYearTickVectors(t *testing.T) yearTickVectors {
	t.Helper()
	content, err := os.ReadFile("testdata/year_tick_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors yearTickVectors
	if err := json.Unmarshal(content, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestYearTickVectors(t *testing.T) {
	vectors := readYearTickVectors(t)
	for _, cycle := range vectors.Cycles {
		if got := YearCycleOf(SUnixTimeToTime(cycle.Updated)); got != cycle.CycleYear {
			t.Fatalf("Updated %d: cycle %d, want %d", cycle.Updated, got, cycle.CycleYear)
		}
		if cycle.CycleStart != nil && utils.SUnixTimeOf(CycleStartTime(cycle.CycleYear)) != *cycle.CycleStart {
			t.Fatalf("cycle %d starts at %d, want %d", cycle.CycleYear, utils.SUnixTimeOf(CycleStartTime(cycle.CycleYear)), *cycle.CycleStart)
		}
	}
	for _, written := range vectors.WrittenTicks {
		cycleStart := utils.SUnixTimeOf(CycleStartTime(YearCycleOf(SUnixTimeToTime(written.Updated))))
		tick := WrittenYearTick(written.Updated, cycleStart)
		if tick != written.Tick || YearTickToUpdated(tick, cycleStart) != written.HeldUpdated {
			t.Fatalf("Updated %d: tick %d held as %d, want %d held as %d",
				written.Updated, tick, YearTickToUpdated(tick, cycleStart), written.Tick, written.HeldUpdated)
		}
		if tick >= 1<<24 {
			t.Fatalf("tick %d does not fit a uint24", tick)
		}
	}
}

func TestDecodeCachedIDsRequest(t *testing.T) {
	for _, vector := range readYearTickVectors(t).Requests {
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

	for name, encoded := range map[string]string{
		"empty":                "",
		"not base64":           "GA==",
		"truncated slot":       "5-3--FlZ",     // year, 1 partition, delta 0, 1 slot, then 2 of its 4 bytes
		"IDs not ascending":    "5-3---76--",   // IDs 7 then delta 0
		"count beyond payload": "5-3--6w6",     // 127 IDs announced, 1 sent
		"trailing bytes":       "5-3---36--",   // one ID, then a stray byte
		"slot tick 0":          "5-3--Fk-----", // slot 12 listed with tick 0
	} {
		if _, err := DecodeCachedIDsRequest(encoded); err == nil {
			t.Fatalf("%s: %q decoded without an error", name, encoded)
		}
	}
}

func TestRaiseWrittenSlots(t *testing.T) {
	SetFileStore(utils.NewMemoryFileStore())
	defer SetFileStore(nil)
	cycleStart := utils.SUnixTimeOf(CycleStartTime(2026))

	err := RaiseWrittenSlots([]SlotWrite{
		{BasePK: "p7", ID: 300, Updated: cycleStart + 10_000}, // slot 44, tick 10002
		{BasePK: "p7", ID: 556, Updated: cycleStart + 4_000},  // slot 44 too, a lower tick
		{BasePK: "p7", ID: 1, Updated: cycleStart + 2_000},    // slot 1, tick 2002
		{BasePK: "p8", ID: 300, Updated: cycleStart - 1},      // another partition, the previous cycle
	})
	if err != nil {
		t.Fatal(err)
	}
	slotFile, _, err := ReadSlotFile(SlotFileKey(2026, "p7"))
	if err != nil || SlotTickOf(slotFile, 44) != 10_002 || SlotTickOf(slotFile, 1) != 2_002 || SlotTickOf(slotFile, 2) != 0 {
		t.Fatalf("p7: slot 44 = %d, slot 1 = %d, slot 2 = %d, err %v; want 10002, 2002, 0",
			SlotTickOf(slotFile, 44), SlotTickOf(slotFile, 1), SlotTickOf(slotFile, 2), err)
	}
	if lastCycleFile, _, _ := ReadSlotFile(SlotFileKey(2025, "p8")); SlotTickOf(lastCycleFile, 44) == 0 {
		t.Fatal("a write stamped before the cycle start must land in the previous cycle's file")
	}

	// A late write with a lower tick never lowers a slot.
	if err := RaiseWrittenSlots([]SlotWrite{{BasePK: "p7", ID: 300, Updated: cycleStart}}); err != nil {
		t.Fatal(err)
	}
	if slotFile, _, _ = ReadSlotFile(SlotFileKey(2026, "p7")); SlotTickOf(slotFile, 44) != 10_002 {
		t.Fatalf("slot 44 = %d after a late write, want it kept at 10002", SlotTickOf(slotFile, 44))
	}

	// Concurrent writers of one file, each to its own slot: the conditional PUT keeps every one.
	var waitGroup sync.WaitGroup
	for slot := range 20 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if err := RaiseSlotTicks("ids-cache/2026/concurrent", map[uint8]uint32{uint8(slot): uint32(slot) + 2}); err != nil {
				t.Error(err)
			}
		}()
	}
	waitGroup.Wait()
	slotFile, _, _ = ReadSlotFile("ids-cache/2026/concurrent")
	for slot := range 20 {
		if SlotTickOf(slotFile, uint8(slot)) != uint32(slot)+2 {
			t.Fatalf("slot %d = %d after concurrent writes, want %d", slot, SlotTickOf(slotFile, uint8(slot)), slot+2)
		}
	}
}

func TestReadCachedIDsPlan(t *testing.T) {
	SetFileStore(utils.NewMemoryFileStore())
	defer SetFileStore(nil)
	cycleStart := utils.SUnixTimeOf(CycleStartTime(2026))
	// Written 100 ticks into the cycle: slot 44 of p7 holds tick 102.
	if err := RaiseWrittenSlots([]SlotWrite{{BasePK: "p7", ID: 300, Updated: cycleStart + 100}}); err != nil {
		t.Fatal(err)
	}
	basePKs := map[int64]string{7: "p7"}
	request := CachedIDsRequest{CycleYear: 2026, IDs: []CachedID{
		{Partition: 7, ID: 300, YearTick: 102}, // held the slot's tick: not read
		{Partition: 7, ID: 556, YearTick: 101}, // same slot, an older tick: read
		{Partition: 7, ID: 1, YearTick: 1},     // a slot with no write this cycle, held as such: not read
		{Partition: 7, ID: 2},                  // nothing held: read
		{Partition: 7, ID: 2},                  // a duplicate is read once
	}}

	// Long after the write, every tick is trusted.
	later := SUnixTimeToTime(cycleStart + 10_000)
	plan, err := ReadCachedIDsPlan(request, basePKs, later)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(plan.IDsToRead) != fmt.Sprint([]CachedID{{7, 556, 101}, {7, 2, 0}}) {
		t.Fatalf("IDs to read = %v", plan.IDsToRead)
	}
	if plan.HeldUpdated(7, 556) != cycleStart+101 {
		t.Fatalf("held Updated = %d, want the end of tick 102: %d", plan.HeldUpdated(7, 556), cycleStart+101)
	}

	// Inside the trust window the slot's tick is 0: every ID of it is read, and returns Updated 0.
	soon := SUnixTimeToTime(cycleStart + 102)
	if plan, _ = ReadCachedIDsPlan(request, basePKs, soon); len(plan.IDsToRead) != 3 || plan.HeldUpdated(7, 300) != 0 {
		t.Fatalf("young slot: IDs to read = %v, held %d", plan.IDsToRead, plan.HeldUpdated(7, 300))
	}

	// A client on another cycle can compare nothing.
	request.CycleYear = 2025
	if plan, _ = ReadCachedIDsPlan(request, basePKs, later); len(plan.IDsToRead) != 4 {
		t.Fatalf("another cycle: IDs to read = %v", plan.IDsToRead)
	}
}
