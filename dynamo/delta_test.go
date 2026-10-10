package dynamo

import (
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ── Delta test entity: one partition, a keyless delta and a fan-out delta ────

type deltaMember struct {
	ID      int32   `cb:"1"`
	TeamIDs []int16 `cb:"2"`
	Status  int8    `cb:"3"`
	Updated int64   `cb:"4"`
}

type deltaMemberTable struct {
	Model[deltaMemberTable, deltaMember]
	ID      Col[*deltaMemberTable, int32]
	TeamIDs ColSlice[*deltaMemberTable, int16]
	Status  Col[*deltaMemberTable, int8]
	Updated Col[*deltaMemberTable, int64]
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

func deltaUpdatedPart(updated uint64) string { return EncodeOrderedUint(updated, 7) } // Size(42) = 7 digits

func TestDeltaIndexesCompileWithTheManagedUpdatedLast(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	keyless, byTeam := members.meta.arrayIndexes[0], members.meta.arrayIndexes[1]

	// Cols(Status): Status is the sync filter, so the row sk is only Updated, named by its cb id.
	if keyless.columnID != "004" || keyless.elementPosition != -1 || keyless.syncFilterField != "Status" ||
		len(keyless.keys) != 1 || keyless.keys[0].fieldName != "Updated" {
		t.Fatalf("keyless delta = %+v", keyless)
	}
	// Cols(TeamIDs, Status): rows per team, named by the slice's cb id.
	if byTeam.columnID != "002" || byTeam.elementPosition != 0 || byTeam.syncFilterField != "Status" ||
		len(byTeam.keys) != 2 || byTeam.keys[1].fieldName != "Updated" {
		t.Fatalf("team delta = %+v", byTeam)
	}
}

func TestDeltaRowsMoveOnEveryWrite(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	stored := deltaMember{ID: 9, TeamIDs: []int16{1, 2}, Status: 1, Updated: 4000}
	written := deltaMember{ID: 9, TeamIDs: []int16{2}, Status: 1, Updated: 5000}
	basePK := members.meta.pkValue(unsafe.Pointer(&written))
	baseSK := members.meta.skValue(unsafe.Pointer(&written))

	puts, deletes := members.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&written), nil)
	wantPuts := []string{
		basePK + "004 " + deltaUpdatedPart(5000) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(2, 2) + "#" + deltaUpdatedPart(5000) + "#" + baseSK,
	}
	wantDeletes := []string{
		basePK + "004 " + deltaUpdatedPart(4000) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(1, 2) + "#" + deltaUpdatedPart(4000) + "#" + baseSK,
		basePK + "002 " + EncodeOrderedUint(2, 2) + "#" + deltaUpdatedPart(4000) + "#" + baseSK,
	}
	if got := writeKeys(puts); !equalStrings(got, wantPuts) {
		t.Fatalf("puts = %v\nwant %v", got, wantPuts)
	}
	if got := writeKeys(deletes); !equalStrings(got, wantDeletes) {
		t.Fatalf("deletes = %v\nwant %v", got, wantDeletes)
	}
}

func TestPlanDeltaReadsTheWindowBelowTheWatermark(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()

	// A later sync: every status, from DeltaOverlap below the watermark, no filter.
	query := members.Query().Delta(DeltaSince{Updated: 10_000, Fingerprint: 77}, 1)
	plan := onlyPlan(t, query)
	if plan.basePK != deltaMemberTableID || s(plan.values[":pk"]) != deltaMemberTableID+"004" {
		t.Fatalf("pk = %s, basePK = %s", s(plan.values[":pk"]), plan.basePK)
	}
	if plan.keyCond != "#pk = :pk AND #sk >= :sk" || s(plan.values[":sk"]) != deltaUpdatedPart(6_000) {
		t.Fatalf("keyCond = %q, :sk = %q", plan.keyCond, s(plan.values[":sk"]))
	}
	if len(plan.keyFilter) != 0 || query.deltaSince.Fingerprint != 77 {
		t.Fatalf("a later sync must not filter the status and must keep the fingerprint: %+v %+v", plan.keyFilter, query.deltaSince)
	}

	// A watermark younger than the overlap reads from 0.
	if plan = onlyPlan(t, members.Query().Delta(DeltaSince{Updated: 1_000}, 1)); s(plan.values[":sk"]) != deltaUpdatedPart(0) {
		t.Fatalf(":sk = %q", s(plan.values[":sk"]))
	}

	// A first sync: every row, the requested statuses only, filtered in memory.
	query = members.Query().Delta(DeltaSince{}, 1)
	plan = onlyPlan(t, query)
	if len(plan.keyFilter) != 1 || plan.keyFilter[0].field != "Status" || plan.keyFilter[0].op != opIn || query.deltaSince.Updated != 0 {
		t.Fatalf("first sync keyFilter = %+v", plan.keyFilter)
	}
}

func TestPlanDeltaWithContainsRangesInsideTheElement(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	plan := onlyPlan(t, members.Query().Contains(members.T.TeamIDs, 3).Delta(DeltaSince{Updated: 10_000}, 1))
	element := EncodeOrderedUint(3, 2)
	if s(plan.values[":pk"]) != deltaMemberTableID+"002" || plan.keyCond != "#pk = :pk AND #sk BETWEEN :lo AND :hi" {
		t.Fatalf("pk = %s, keyCond = %q", s(plan.values[":pk"]), plan.keyCond)
	}
	if s(plan.values[":lo"]) != element+"#"+deltaUpdatedPart(6_000) || s(plan.values[":hi"]) != element+keySeparatorSuccessor {
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

// The values genix-ui's delta cache must reproduce (cache/delta-cache.watermark.test.ts).
func TestDeltaFingerprintSharedVector(t *testing.T) {
	for _, testCase := range []struct {
		updatedValues   []int64
		wantFingerprint uint32
	}{
		{nil, 0},
		{[]int64{0}, 0},
		{[]int64{1}, 1364076727},
		{[]int64{1_234_567_890_123}, 2401458028},
		{[]int64{4_398_046_511_103}, 3230535694}, // the largest Updated of Size(42)
		{[]int64{1, 1_234_567_890_123, 4_398_046_511_103}, 2701103153},
		{[]int64{4_398_046_511_103, 1, 1_234_567_890_123}, 2701103153}, // order-independent
	} {
		if got := DeltaFingerprint(testCase.updatedValues); got != testCase.wantFingerprint {
			t.Fatalf("DeltaFingerprint(%v) = %d, want %d", testCase.updatedValues, got, testCase.wantFingerprint)
		}
	}
}

func TestDropHeldWindowSendsTheWindowOnlyOnAMismatch(t *testing.T) {
	members := NewRepo[deltaMemberTable, deltaMember]()
	keyless, byTeam := &members.meta.arrayIndexes[0], &members.meta.arrayIndexes[1]
	row := func(deltaIndex *arrayIndexMeta, member deltaMember) map[string]types.AttributeValue {
		return map[string]types.AttributeValue{"sk": &types.AttributeValueMemberS{Value: members.meta.arrayRowSKs(deltaIndex, unsafe.Pointer(&member))[0]}}
	}
	updatedOfRows := func(deltaIndex *arrayIndexMeta, rowsByPlan [][]map[string]types.AttributeValue) []int64 {
		var updatedValues []int64
		for _, rows := range rowsByPlan {
			for _, row := range rows {
				updatedValues = append(updatedValues, deltaIndex.updatedOfDeltaRow(row["sk"].(*types.AttributeValueMemberS).Value))
			}
		}
		return updatedValues
	}
	since := DeltaSince{Updated: 9_000, Fingerprint: DeltaFingerprint([]int64{7_000, 9_000})}
	windowAndNew := func() [][]map[string]types.AttributeValue {
		return [][]map[string]types.AttributeValue{{
			row(keyless, deltaMember{ID: 1, Updated: 7_000}),
			row(keyless, deltaMember{ID: 2, Updated: 9_000}),
			row(keyless, deltaMember{ID: 3, Updated: 9_500}),
		}}
	}

	// The client holds the window: only the new row stays.
	if got := updatedOfRows(keyless, keyless.dropHeldWindow(windowAndNew(), since)); !equalInt64s(got, []int64{9_500}) {
		t.Fatalf("held window: kept %v", got)
	}
	// A write that landed late in the window (8_000) changes the fingerprint: everything is sent.
	lateWrite := windowAndNew()
	lateWrite[0] = append(lateWrite[0], row(keyless, deltaMember{ID: 4, Updated: 8_000}))
	if got := updatedOfRows(keyless, keyless.dropHeldWindow(lateWrite, since)); len(got) != 4 {
		t.Fatalf("late write: kept %v, want all 4 rows", got)
	}
	// A record the client holds at 7_000 that was rewritten at 8_500: same count, other value.
	rewritten := [][]map[string]types.AttributeValue{{
		row(keyless, deltaMember{ID: 1, Updated: 8_500}),
		row(keyless, deltaMember{ID: 2, Updated: 9_000}),
	}}
	if got := updatedOfRows(keyless, keyless.dropHeldWindow(rewritten, since)); len(got) != 2 {
		t.Fatalf("rewritten record: kept %v, want both rows", got)
	}
	// A record in two teams of a Contains counts once.
	twoTeams := deltaMember{ID: 5, TeamIDs: []int16{1, 2}, Updated: 7_000}
	teamRows := [][]map[string]types.AttributeValue{
		{{"sk": &types.AttributeValueMemberS{Value: members.meta.arrayRowSKs(byTeam, unsafe.Pointer(&twoTeams))[0]}}},
		{{"sk": &types.AttributeValueMemberS{Value: members.meta.arrayRowSKs(byTeam, unsafe.Pointer(&twoTeams))[1]}}},
	}
	if got := updatedOfRows(byTeam, byTeam.dropHeldWindow(teamRows, DeltaSince{Updated: 7_000, Fingerprint: DeltaFingerprint([]int64{7_000})})); len(got) != 0 {
		t.Fatalf("one record in two teams: kept %v, want none", got)
	}
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Without a delta index holding a sync filter, or with two that fit equally, Delta fails to plan.
func TestDeltaIndexSelectionRejections(t *testing.T) {
	products := newProducts(t)
	if _, err := products.Query().Eq(products.T.CategoryID, 7).Delta(DeltaSince{}).plans(); err == nil || !strings.Contains(err.Error(), "no TypeDelta index") {
		t.Fatalf("a table without delta indexes: err = %v", err)
	}
	ambiguous := &tableMeta{recordType: products.meta.recordType, arrayIndexes: []arrayIndexMeta{
		{isDelta: true, keys: []keyCol{{fieldName: "Updated"}}},
		{isDelta: true, keys: []keyCol{{fieldName: "Updated"}}},
	}}
	if _, err := ambiguous.selectDeltaIndex(nil, false); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("two equal fits: err = %v", err)
	}
}

func TestStampManagedColumnsCountsMillisecondsFromTheEpoch(t *testing.T) {
	originalNow, originalEpoch := Now, updatedEpochMillis
	t.Cleanup(func() { Now, updatedEpochMillis = originalNow, originalEpoch })
	SetUpdatedEpoch(1_800_000_000)
	Now = func() time.Time { return time.UnixMilli(1_800_000_000_250) }
	lastStampedUpdated.Store(0)

	// Products have no Updated field and no delta index: nothing to stamp.
	newProducts(t).meta.stampManagedColumns([]unsafe.Pointer{unsafe.Pointer(&Product{})}, nil)

	meta := NewRepo[deltaMemberTable, deltaMember]().meta
	first, second := deltaMember{}, deltaMember{}
	meta.stampManagedColumns([]unsafe.Pointer{unsafe.Pointer(&first), unsafe.Pointer(&second)}, nil)
	if first.Updated != 250 || second.Updated != 250 {
		t.Fatalf("one call stamps one Updated: %d, %d; want 250", first.Updated, second.Updated)
	}
	// A second call in the same millisecond still moves, and a stored Updated ahead of the clock is passed.
	again, aheadOfClock := deltaMember{}, deltaMember{ID: 1}
	stored := deltaMember{ID: 1, Updated: 900}
	storedByKey := map[string]unsafe.Pointer{meta.recordKey(unsafe.Pointer(&stored)): unsafe.Pointer(&stored)}
	meta.stampManagedColumns([]unsafe.Pointer{unsafe.Pointer(&again), unsafe.Pointer(&aheadOfClock)}, meta.aboveStored(storedByKey))
	if again.Updated != 251 || aheadOfClock.Updated != 901 {
		t.Fatalf("again = %d (want 251), aheadOfClock = %d (want 901)", again.Updated, aheadOfClock.Updated)
	}
	if got := UpdatedToTime(250); !got.Equal(time.UnixMilli(1_800_000_000_250)) {
		t.Fatalf("UpdatedToTime(250) = %v", got)
	}
}

type deltaWithoutUpdated struct {
	ID     int32 `cb:"1"`
	Status int8  `cb:"2"`
}

type deltaWithoutUpdatedTable struct {
	Model[deltaWithoutUpdatedTable, deltaWithoutUpdated]
	ID     Col[*deltaWithoutUpdatedTable, int32]
	Status Col[*deltaWithoutUpdatedTable, int8]
}

func (t deltaWithoutUpdatedTable) GetSchema() Schema {
	return Schema{Entity: "delta_without_updated", TableID: 78901235, Keys: Cols(t.ID.Size(30)),
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(t.Status)}}}
}

type int32Updated struct {
	ID      int32 `cb:"1"`
	Updated int32 `cb:"2"`
}

type int32UpdatedTable struct {
	Model[int32UpdatedTable, int32Updated]
	ID      Col[*int32UpdatedTable, int32]
	Updated Col[*int32UpdatedTable, int32]
}

func (t int32UpdatedTable) GetSchema() Schema {
	return Schema{Entity: "int32_updated", TableID: 78901236, Keys: Cols(t.ID.Size(30))}
}

func TestManagedUpdatedMustBeAnInt64(t *testing.T) {
	for name, build := range map[string]func(){
		"a delta index without Updated": func() { NewRepo[deltaWithoutUpdatedTable, deltaWithoutUpdated]() },
		"an int32 Updated":              func() { NewRepo[int32UpdatedTable, int32Updated]() },
	} {
		func() {
			defer func() {
				if recovered := recover(); recovered == nil || !strings.Contains(recovered.(string), "int64") {
					t.Fatalf("%s: expected the int64 panic, got %v", name, recovered)
				}
			}()
			build()
		}()
	}
}
