package dynamo

import (
	"reflect"
	"testing"
	"unsafe"
)

// ── Local index test entity: items looked up by a name hash ──────────────────

type localItem struct {
	ID       int32  `cb:"1"`
	NameHash int32  `cb:"2"`
	Name     string `cb:"3"`
}

type localItemTable struct {
	Model[localItemTable, localItem]
	ID       Col[*localItemTable, int32]
	NameHash Col[*localItemTable, int32]
	Name     Col[*localItemTable, string]
}

const localItemTableID = "89012345"

func (t localItemTable) GetSchema() Schema {
	return Schema{
		Entity:  "local_item",
		TableID: 89012345,
		Keys:    Cols(t.ID.Size(32)),
		Indexes: []Index{{Type: TypeLocal, Keys: Cols(t.NameHash.Size(32))}},
	}
}

func TestLocalIndexWritesOneRowPerRecord(t *testing.T) {
	items := NewRepo[localItemTable, localItem]()
	stored := localItem{ID: 4, NameHash: 10, Name: "apple"}
	renamed := localItem{ID: 4, NameHash: 11, Name: "pear"}
	baseSK := items.meta.skValue(unsafe.Pointer(&stored))
	rowPK := localItemTableID + "002" // NameHash's cb id names the rows

	puts, deletes := items.meta.arrayIndexWrites(nil, unsafe.Pointer(&stored), nil)
	if got, want := writeKeys(puts), []string{rowPK + " " + EncodeOrderedUint(10, 6) + "#" + baseSK}; !equalStrings(got, want) || len(deletes) != 0 {
		t.Fatalf("new record: puts %v deletes %v", got, writeKeys(deletes))
	}
	// A changed key moves the row: put the new one, delete the old one.
	puts, deletes = items.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&renamed), nil)
	if got, want := writeKeys(puts), []string{rowPK + " " + EncodeOrderedUint(11, 6) + "#" + baseSK}; !equalStrings(got, want) {
		t.Fatalf("puts = %v, want %v", got, want)
	}
	if got, want := writeKeys(deletes), []string{rowPK + " " + EncodeOrderedUint(10, 6) + "#" + baseSK}; !equalStrings(got, want) {
		t.Fatalf("deletes = %v, want %v", got, want)
	}
	// An unchanged key writes nothing.
	puts, deletes = items.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&stored), nil)
	if len(puts) != 0 || len(deletes) != 0 {
		t.Fatalf("unchanged key: puts %v deletes %v", writeKeys(puts), writeKeys(deletes))
	}
}

func TestPlanEqOnALocalIndexReadsItsRowsConsistently(t *testing.T) {
	items := NewRepo[localItemTable, localItem]()

	plan := onlyPlan(t, items.Query().Eq(items.T.NameHash, int32(10)).Consistent())
	if plan.indexName != "" || plan.arrayIndex == nil || !plan.arrayIndex.isLocal {
		t.Fatalf("plan = %+v, want the local index rows in the base table", plan)
	}
	if plan.basePK != localItemTableID || s(plan.values[":pk"]) != localItemTableID+"002" {
		t.Fatalf("pk = %s, basePK = %s", s(plan.values[":pk"]), plan.basePK)
	}
	if plan.keyCond != "#pk = :pk AND begins_with(#sk, :sk)" {
		t.Fatalf("keyCond = %q", plan.keyCond)
	}

	// A query on the base Keys alone stays on the base table.
	plan = onlyPlan(t, items.Query().Eq(items.T.ID, int32(4)))
	if plan.arrayIndex != nil {
		t.Fatalf("an ID query must read the base table, got the local index")
	}
}

func TestLocalIndexDeclarationRules(t *testing.T) {
	for name, index := range map[string]func(t badFanOutTable) Index{
		"a Slot":    func(t badFanOutTable) Index { return Index{Type: TypeLocal, Slot: G1, Keys: Cols(t.Price.Size(32))} },
		"no Keys":   func(t badFanOutTable) Index { return Index{Type: TypeLocal} },
		"a slice":   func(t badFanOutTable) Index { return Index{Type: TypeLocal, Keys: Cols(t.TagIDs.Size(32))} },
		"a GroupBy": func(t badFanOutTable) Index { return Index{Type: TypeLocal, Keys: Cols(t.Price.Size(32)), GroupBy: Cols(t.ID)} },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			tablePtr := new(badFanOutTable)
			populateColumnNames(tablePtr)
			schema := Schema{Entity: "bad_local", TableID: 89012346, Keys: Cols(tablePtr.ID.Size(32)), Indexes: []Index{index(*tablePtr)}}
			buildTableMeta(schema, reflect.TypeFor[badFanOutRecord]())
		}()
	}
}
