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

func TestSlotsItemSitsAfterTheBasePK(t *testing.T) {
	items := NewRepo[cachedItemTable, cachedItem]()
	record := cachedItem{StoreID: 7, ID: 300}
	basePK := items.meta.pkValue(unsafe.Pointer(&record))

	key := slotsKey(basePK)
	if got := key["pk"].(*types.AttributeValueMemberN).Value; got != "4567890100007000" {
		t.Fatalf("slots pk = %s, want the base pk 4567890100007 followed by 000", got)
	}
	if !isSlotsPK(key["pk"].(*types.AttributeValueMemberN).Value) {
		t.Fatal("isSlotsPK must recognize its own pk")
	}
	if slotOfRecordID(record.ID) != 44 { // 300 = 256 + 44
		t.Fatalf("ID 300 must land in slot 44, got %d", slotOfRecordID(record.ID))
	}
}

// A slot holds the full Updated of its last write, with no truncation; a missing one reads as 0, "unknown".
func TestSlotUpdatedOfReadsTheFullValue(t *testing.T) {
	slotsItem := map[string]types.AttributeValue{
		"v1": &types.AttributeValueMemberN{Value: "5"},
		"v2": &types.AttributeValueMemberN{Value: "65536"},
		"v3": &types.AttributeValueMemberN{Value: "4398046511103"}, // the largest Updated of Size(42)
	}
	for slot, want := range map[uint8]int64{0: 0, 1: 5, 2: 65536, 3: 4_398_046_511_103} {
		if got := slotUpdatedOf(slotsItem, slot); got != want {
			t.Fatalf("slot %d: Updated %d, want %d", slot, got, want)
		}
	}
}

type badCachedRecord struct {
	ID      int32
	Code    string
	Updated int64
}

type badCachedTable struct {
	Model[badCachedTable, badCachedRecord]
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
// DYNAMO_ENDPOINT points at one.
func TestQueryCachedIDs(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: QueryCachedIDs needs a live DynamoDB")
	}
	items := NewRepo[cachedItemTable, cachedItem]()
	// A fresh ID per run; ID+256 shares its slot, ID+1 does not.
	firstID := time.Now().UnixMilli() * 1000
	written := []cachedItem{
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

	// A slot younger than DeltaOverlap is not trusted yet: the records come back with Updated 0.
	allIDs := []CachedID{{ID: firstID}, {ID: firstID + 1}, {ID: firstID + 256}}
	fetched, err := items.QueryCachedIDs(allIDs, int32(7))
	if err != nil || len(fetched) != 3 || fetched[0].Updated != 0 {
		t.Fatalf("fresh read: got %d records, err %v; want 3 with Updated 0", len(fetched), err)
	}

	// The reads below run as if the overlap had passed since every write.
	readAfterOverlap := func(cachedIDs []CachedID) ([]cachedItem, error) {
		Now = func() time.Time { return time.Now().Add(DeltaOverlap + time.Second) }
		defer func() { Now = time.Now }()
		return items.QueryCachedIDs(cachedIDs, int32(7))
	}

	// Nothing held: every record comes back, stamped with its slot's Updated.
	fetched, err = readAfterOverlap(allIDs)
	if err != nil || len(fetched) != 3 {
		t.Fatalf("cold read: got %d records, err %v; want 3", len(fetched), err)
	}
	heldIDs := make([]CachedID, len(fetched))
	for i, record := range fetched {
		if record.Updated == 0 {
			t.Fatalf("record %d came back without its slot's Updated", record.ID)
		}
		heldIDs[i] = CachedID{ID: record.ID, Updated: record.Updated}
	}

	// Values held and nothing written: nothing comes back.
	if fetched, err = readAfterOverlap(heldIDs); err != nil || len(fetched) != 0 {
		t.Fatalf("warm read: got %d records, err %v; want 0", len(fetched), err)
	}

	// A write to the first record moves its slot, which it shares with firstID+256.
	written[0].Name = "a2"
	if err := items.Put(&written[0]); err != nil {
		t.Fatal(err)
	}
	fetched, err = readAfterOverlap(heldIDs)
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
		if record.Updated != written[0].Updated {
			t.Fatalf("record %d carries Updated %d, want the slot's last write %d", record.ID, record.Updated, written[0].Updated)
		}
	}
}
