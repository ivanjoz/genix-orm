package dataframe

import (
	"context"
	"reflect"
	"testing"

	"github.com/ivanjoz/genix-orm/utils"
)

// TestVersionsAllocateRoundRobinAndOnlyRaise: new files take the slots in turn and wrap past the
// cycle; a raise never lowers a slot; a raise that changes nothing writes nothing.
func TestVersionsAllocateRoundRobinAndOnlyRaise(t *testing.T) {
	defer func() { slotCycle = slotCount }()
	slotCycle = 3
	store := utils.NewMemoryFileStore()
	ctx := context.Background()
	versionsKey := compileTestTable(t).versionsKeyOf(7)
	slots, err := allocateAndRaiseSlots(ctx, store, versionsKey, 4, nil, 100)
	if err != nil || !reflect.DeepEqual(slots, []uint8{1, 2, 3, 1}) {
		t.Fatalf("first allocation: %v, %v", slots, err)
	}
	if slots, _ = allocateAndRaiseSlots(ctx, store, versionsKey, 1, []uint8{3}, 90); !reflect.DeepEqual(slots, []uint8{2}) {
		t.Fatalf("second allocation: %v", slots)
	}
	versions, etag, _ := readVersions(store, versionsKey)
	if versions.Counter != 5 || versions.SlotTime(1) != 100 || versions.SlotTime(2) != 100 || versions.SlotTime(3) != 100 {
		t.Fatalf("a raise to 90 must not lower slots at 100: %+v", versions.SlotTimes[:3])
	}
	if _, err := allocateAndRaiseSlots(ctx, store, versionsKey, 0, []uint8{1, 2}, 100); err != nil {
		t.Fatal(err)
	}
	if _, etagAfter, _ := readVersions(store, versionsKey); etagAfter != etag {
		t.Fatal("a raise that changes nothing must not write the versions file")
	}
	if versions.SlotTime(0) <= 1<<40 {
		t.Fatal("slot 0 (no slot yet) must never read as unchanged")
	}
	if _, err := allocateAndRaiseSlots(ctx, store, versionsKey, 0, []uint8{2}, 120); err != nil {
		t.Fatal(err)
	}
	if versions, _, _ = readVersions(store, versionsKey); versions.SlotTime(2) != 120 || versions.SlotTime(1) != 100 {
		t.Fatalf("raise of slot 2: %+v", versions.SlotTimes[:3])
	}
}

// TestChangedFilesWire: the body and the held files read back what was written; every proper prefix
// of a body is refused.
func TestChangedFilesWire(t *testing.T) {
	answer := ChangedFiles{Generation: "P4F-6sV", RowColumnCount: 2, SumsCount: 1, Snapshot: 396172115, Files: []ChangedFile{
		{Keys: []int64{2601}, Snapshot: 396172200, Slot: 7, Content: EncodeFile(File{Snapshot: 396172200, Slot: 7, Rows: [][]int64{{3}, {9}}, Sums: [][]int64{{1}}})},
		{Keys: []int64{2602, 4}, Snapshot: 396172200},
	}}
	body := AppendChangedFiles(nil, answer)
	decoded, err := DecodeChangedFiles(body)
	if err != nil || !reflect.DeepEqual(decoded, answer) {
		t.Fatalf("decoded %+v, err %v", decoded, err)
	}
	for length := range len(body) {
		if _, err := DecodeChangedFiles(body[:length]); err == nil {
			t.Fatalf("a %d-byte prefix of a %d-byte body decoded", length, len(body))
		}
	}
	held := []HeldFile{{Keys: []int64{2601}, Snapshot: 396172200, Slot: 7}, {Keys: []int64{2602, 4, 1}, Snapshot: 5}}
	text := FormatHeldFiles(held)
	if text != "2601.396172200.7,2602_4_1.5.0" {
		t.Fatalf("held files text %q", text)
	}
	if parsed, err := ParseHeldFiles(text); err != nil || !reflect.DeepEqual(parsed, held) {
		t.Fatalf("parsed %+v, err %v", parsed, err)
	}
	for _, bad := range []string{"2601", "2601.x.1", "2601.5.256", "-1.5.1", "a_b.5.1"} {
		if _, err := ParseHeldFiles(bad); err == nil {
			t.Fatalf("held files %q parsed", bad)
		}
	}
}
