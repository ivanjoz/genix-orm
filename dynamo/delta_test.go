package dynamo

import (
	"strings"
	"testing"
	"time"
	"unsafe"
)

// ── Delta test entity: one partition, a keyless delta and a fan-out delta ────

type deltaMember struct {
	ID             int32   `cb:"1"`
	TeamIDs        []int16 `cb:"2"`
	Status         int8    `cb:"3"`
	Updated        int32   `cb:"4"`
	UpdatedVersion int32   `cb:"5"`
}

type deltaMemberTable struct {
	Model[deltaMemberTable, deltaMember]
	ID             Col[*deltaMemberTable, int32]
	TeamIDs        ColSlice[*deltaMemberTable, int16]
	Status         Col[*deltaMemberTable, int8]
	Updated        Col[*deltaMemberTable, int32]
	UpdatedVersion Col[*deltaMemberTable, int32]
}

const deltaMemberTableID = "78901234"

func (t deltaMemberTable) GetSchema() Schema {
	return Schema{
		Entity:  "delta_member",
		TableID: 78901234,
		Keys:    Cols(t.ID.Size(30)),
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(t.Status)},
			{Type: TypeDelta, Keys: Cols(t.TeamIDs.Size(8), t.Status)},
		},
	}
}

func deltaVersionPart(version uint64) string { return EncodeOrderedUint(version, 6) } // Size(31) = 6 digits

func TestDeltaIndexesCompileWithTheManagedVersionLast(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	keyless, byTeam := members.meta.arrayIndexes[0], members.meta.arrayIndexes[1]

	// Cols(Status): Status is the sync filter, so the row sk is only the version.
	if keyless.columnID != "005" || keyless.elementPosition != -1 || keyless.syncFilterField != "Status" ||
		len(keyless.keys) != 1 || keyless.keys[0].fieldName != "UpdatedVersion" {
		t.Fatalf("keyless delta = %+v", keyless)
	}
	// Cols(TeamIDs, Status): rows per team, named by the slice's cb id.
	if byTeam.columnID != "002" || byTeam.elementPosition != 0 || byTeam.syncFilterField != "Status" ||
		len(byTeam.keys) != 2 || byTeam.keys[1].fieldName != "UpdatedVersion" {
		t.Fatalf("team delta = %+v", byTeam)
	}
}

func TestDeltaRowsMoveOnEveryWrite(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	stored := deltaMember{ID: 9, TeamIDs: []int16{1, 2}, Status: 1, UpdatedVersion: 4}
	written := deltaMember{ID: 9, TeamIDs: []int16{2}, Status: 1, UpdatedVersion: 5}
	basePK := members.meta.pkValue(unsafe.Pointer(&written))
	baseSK := members.meta.skValue(unsafe.Pointer(&written))

	puts, deletes := members.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&written), nil)
	wantPuts := []string{
		basePK + "005 " + deltaVersionPart(5) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(2, 2) + "#" + deltaVersionPart(5) + "#" + baseSK,
	}
	wantDeletes := []string{
		basePK + "005 " + deltaVersionPart(4) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(1, 2) + "#" + deltaVersionPart(4) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(2, 2) + "#" + deltaVersionPart(4) + "#" + baseSK,
	}
	if got := writeKeys(puts); !equalStrings(got, wantPuts) {
		t.Fatalf("puts = %v\nwant %v", got, wantPuts)
	}
	if got := writeKeys(deletes); !equalStrings(got, wantDeletes) {
		t.Fatalf("deletes = %v\nwant %v", got, wantDeletes)
	}
}

func TestPlanDeltaReadsAfterTheWatermark(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()

	// A later sync: every status, versions from W+1, no filter.
	plan := onlyPlan(t, members.Query().Delta(7, 1))
	if plan.basePK != deltaMemberTableID || s(plan.values[":pk"]) != deltaMemberTableID+"005" {
		t.Fatalf("pk = %s, basePK = %s", s(plan.values[":pk"]), plan.basePK)
	}
	if plan.keyCond != "#pk = :pk AND #sk >= :sk" || s(plan.values[":sk"]) != deltaVersionPart(8) {
		t.Fatalf("keyCond = %q, :sk = %q", plan.keyCond, s(plan.values[":sk"]))
	}
	if len(plan.keyFilter) != 0 {
		t.Fatalf("a later sync must not filter the status, got %+v", plan.keyFilter)
	}

	// A first sync: the requested statuses only, filtered in memory.
	plan = onlyPlan(t, members.Query().Delta(0, 1))
	if len(plan.keyFilter) != 1 || plan.keyFilter[0].field != "Status" || plan.keyFilter[0].op != opIn {
		t.Fatalf("first sync keyFilter = %+v", plan.keyFilter)
	}
}

func TestPlanDeltaWithContainsRangesInsideTheElement(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	plan := onlyPlan(t, members.Query().Contains(members.T.TeamIDs, 3).Delta(7, 1))
	element := EncodeOrderedUint(3, 2)
	if s(plan.values[":pk"]) != deltaMemberTableID+"002" || plan.keyCond != "#pk = :pk AND #sk BETWEEN :lo AND :hi" {
		t.Fatalf("pk = %s, keyCond = %q", s(plan.values[":pk"]), plan.keyCond)
	}
	if s(plan.values[":lo"]) != element+"#"+deltaVersionPart(8) || s(plan.values[":hi"]) != element+keySeparatorSuccessor {
		t.Fatalf(":lo = %q, :hi = %q", s(plan.values[":lo"]), s(plan.values[":hi"]))
	}
}

func TestDeltaFirstSyncFilterKeepsTheRequestedValues(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	syncFilter := predicate{field: "Status", op: opIn, v1: []any{1, 2}}
	for status, wantKept := range map[int8]bool{0: false, 1: true, 2: true} {
		member := deltaMember{Status: status}
		if kept := members.meta.matchesFilter(unsafe.Pointer(&member), []predicate{syncFilter}); kept != wantKept {
			t.Fatalf("status %d: kept = %v, want %v", status, kept, wantKept)
		}
	}
}

// Without a delta index holding a sync filter, or with two that fit equally, Delta fails to plan.
func TestDeltaIndexSelectionRejections(t *testing.T) {
	products := newProducts(t)
	if _, err := products.Query().Eq(products.T.CategoryID, 7).Delta(0).plans(); err == nil || !strings.Contains(err.Error(), "no TypeDelta index") {
		t.Fatalf("a table without delta indexes: err = %v", err)
	}
	ambiguous := &tableMeta{recordType: products.meta.recordType, arrayIndexes: []arrayIndexMeta{
		{isDelta: true, keys: []keyCol{{fieldName: "UpdatedVersion"}}},
		{isDelta: true, keys: []keyCol{{fieldName: "UpdatedVersion"}}},
	}}
	if _, err := ambiguous.selectDeltaIndex(nil, false); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("two equal fits: err = %v", err)
	}
}

func TestStampManagedColumnsSetsUpdatedFromNow(t *testing.T) {
	originalNow := Now
	t.Cleanup(func() { Now = originalNow })
	Now = func() time.Time { return time.Unix(1_000_000_200, 0) }

	// Products have no Updated field and no delta index: nothing to stamp, no sequence to reserve.
	products := newProducts(t)
	if err := products.meta.stampManagedColumns([]unsafe.Pointer{unsafe.Pointer(&Product{})}); err != nil {
		t.Fatal(err)
	}
	stamped := deltaMember{}
	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	updatedOnly := &tableMeta{updated: meta.updated}
	if err := updatedOnly.stampManagedColumns([]unsafe.Pointer{unsafe.Pointer(&stamped)}); err != nil {
		t.Fatal(err)
	}
	if stamped.Updated != 100 { // (1_000_000_200 - 1e9) / 2
		t.Fatalf("Updated = %d, want 100", stamped.Updated)
	}
}

type deltaWithoutVersion struct {
	ID     int32 `cb:"1"`
	Status int8  `cb:"2"`
}

type deltaWithoutVersionTable struct {
	Model[deltaWithoutVersionTable, deltaWithoutVersion]
	ID     Col[*deltaWithoutVersionTable, int32]
	Status Col[*deltaWithoutVersionTable, int8]
}

func (t deltaWithoutVersionTable) GetSchema() Schema {
	return Schema{Entity: "delta_without_version", TableID: 78901235, Keys: Cols(t.ID.Size(30)),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}}}
}

func TestDeltaIndexNeedsTheManagedVersionField(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "int32 field \"UpdatedVersion\"") {
			t.Fatalf("expected the UpdatedVersion panic, got %v", recovered)
		}
	}()
	NewRepo[deltaWithoutVersionTable, deltaWithoutVersion]()
}
