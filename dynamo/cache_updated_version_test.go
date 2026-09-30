package dynamo

import (
	"os"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ── By-IDs test entity: partitioned by store, one integer ID ─────────────────

type versionedItem struct {
	StoreID        int32  `cb:"1"`
	ID             int64  `cb:"2"`
	Name           string `cb:"3"`
	UpdatedVersion uint16 `cb:"4"`
}

type versionedItemTable struct {
	Model[versionedItemTable, versionedItem]
	StoreID        Col[*versionedItemTable, int32]
	ID             Col[*versionedItemTable, int64]
	Name           Col[*versionedItemTable, string]
	UpdatedVersion Col[*versionedItemTable, uint16]
}

func (t versionedItemTable) GetSchema() Schema {
	return Schema{
		Entity:             "versioned_item",
		TableID:            45678901,
		Partition:          Keys(t.StoreID.Size(16)),
		Keys:               Keys(t.ID.Size(48)),
		SaveUpdatedVersion: true,
	}
}

func TestSlotVersionsItemSitsAfterTheBasePK(t *testing.T) {
	items := NewRepo[versionedItemTable, versionedItem]()
	record := versionedItem{StoreID: 7, ID: 300}
	basePK := items.meta.pkValue(unsafe.Pointer(&record))

	key := slotVersionsKey(basePK)
	if got := key["pk"].(*types.AttributeValueMemberN).Value; got != "4567890100007000" {
		t.Fatalf("slot-versions pk = %s, want the base pk 4567890100007 followed by 000", got)
	}
	if !isSlotVersionsPK(key["pk"].(*types.AttributeValueMemberN).Value) {
		t.Fatal("isSlotVersionsPK must recognize its own pk")
	}
	if slotOfRecordID(record.ID) != 44 { // 300 = 256 + 44
		t.Fatalf("ID 300 must land in slot 44, got %d", slotOfRecordID(record.ID))
	}
}

func TestSlotVersionOfTruncatesAndReservesZero(t *testing.T) {
	slotVersions := map[string]types.AttributeValue{
		"v1": &types.AttributeValueMemberN{Value: "5"},
		"v2": &types.AttributeValueMemberN{Value: "65536"}, // truncates to 0, which means "unknown"
		"v3": &types.AttributeValueMemberN{Value: "65541"},
	}
	for slot, want := range map[uint8]uint16{0: 0, 1: 5, 2: 1, 3: 5} {
		if got := slotVersionOf(slotVersions, slot); got != want {
			t.Fatalf("slot %d: version %d, want %d", slot, got, want)
		}
	}
}

func TestPrepareUpdatedVersionsZeroesTheManagedField(t *testing.T) {
	items := NewRepo[versionedItemTable, versionedItem]()
	record := versionedItem{StoreID: 7, ID: 1, UpdatedVersion: 99}
	items.meta.prepareUpdatedVersions([]unsafe.Pointer{unsafe.Pointer(&record)})
	if record.UpdatedVersion != 0 {
		t.Fatalf("UpdatedVersion = %d after prepare, want 0", record.UpdatedVersion)
	}
}

type badVersionedRecord struct {
	ID             int32
	Code           string
	UpdatedVersion int32
}

type badVersionedTable struct {
	Model[badVersionedTable, badVersionedRecord]
	ID             Col[*badVersionedTable, int32]
	Code           Col[*badVersionedTable, string]
	UpdatedVersion Col[*badVersionedTable, int32]
}

type unversionedRecord struct {
	ID int32
}

func TestSaveUpdatedVersionDeclarationRules(t *testing.T) {
	tablePtr := new(badVersionedTable)
	populateColumnNames(tablePtr)
	for name, build := range map[string]func(){
		"two Keys columns": func() {
			buildTableMeta(Schema{Entity: "bad_versioned", TableID: 56789012, Keys: Keys(tablePtr.ID.Size(32), tablePtr.Code), SaveUpdatedVersion: true},
				reflect.TypeFor[badVersionedRecord]())
		},
		"a string key": func() {
			buildTableMeta(Schema{Entity: "bad_versioned", TableID: 56789012, Keys: Keys(tablePtr.Code), SaveUpdatedVersion: true},
				reflect.TypeFor[badVersionedRecord]())
		},
		"an int32 UpdatedVersion": func() {
			buildTableMeta(Schema{Entity: "bad_versioned", TableID: 56789012, Keys: Keys(tablePtr.ID.Size(32)), SaveUpdatedVersion: true},
				reflect.TypeFor[badVersionedRecord]())
		},
		"no UpdatedVersion field": func() {
			buildTableMeta(Schema{Entity: "unversioned", TableID: 67890123, Keys: Keys(tablePtr.ID.Size(32)), SaveUpdatedVersion: true},
				reflect.TypeFor[unversionedRecord]())
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
// DYNAMO_ENDPOINT points at one.
func TestQueryCachedIDs(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: QueryCachedIDs needs a live DynamoDB")
	}
	items := NewRepo[versionedItemTable, versionedItem]()
	// A fresh ID per run; ID+256 shares its slot, ID+1 does not.
	firstID := time.Now().UnixMilli() * 1000
	written := []versionedItem{
		{StoreID: 7, ID: firstID, Name: "a"},
		{StoreID: 7, ID: firstID + 1, Name: "b"},
		{StoreID: 7, ID: firstID + 256, Name: "c"},
	}
	t.Cleanup(func() {
		for i := range written {
			items.Delete(&written[i])
		}
	})
	if err := items.PutMany(written); err != nil {
		t.Fatal(err)
	}

	// No versions held: every record comes back, stamped with its slot version.
	fetched, err := items.QueryCachedIDs([]IDUpdatedVersion{{ID: firstID}, {ID: firstID + 1}, {ID: firstID + 256}}, int32(7))
	if err != nil || len(fetched) != 3 {
		t.Fatalf("cold read: got %d records, err %v; want 3", len(fetched), err)
	}
	heldVersions := make([]IDUpdatedVersion, len(fetched))
	for i, record := range fetched {
		if record.UpdatedVersion == 0 {
			t.Fatalf("record %d came back without a slot version", record.ID)
		}
		heldVersions[i] = IDUpdatedVersion{ID: record.ID, UpdatedVersion: record.UpdatedVersion}
	}

	// Versions held and nothing written: nothing comes back.
	if fetched, err = items.QueryCachedIDs(heldVersions, int32(7)); err != nil || len(fetched) != 0 {
		t.Fatalf("warm read: got %d records, err %v; want 0", len(fetched), err)
	}

	// A write to the first record moves its slot, which it shares with firstID+256.
	written[0].Name = "a2"
	if err := items.Put(&written[0]); err != nil {
		t.Fatal(err)
	}
	fetched, err = items.QueryCachedIDs(heldVersions, int32(7))
	if err != nil || len(fetched) != 2 {
		t.Fatalf("after a write: got %d records, err %v; want the 2 of the moved slot", len(fetched), err)
	}
	for _, record := range fetched {
		if record.ID == firstID+1 {
			t.Fatal("a record of an untouched slot came back")
		}
		if record.ID == firstID && record.Name != "a2" {
			t.Fatalf("the rewritten record came back stale: Name = %q", record.Name)
		}
	}
}
