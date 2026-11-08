package scylla

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/ivanjoz/colbin"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// deltaHiddenRecord stores a column colbin drops (cb:"-"): a snapshot would serve it zeroed.
type deltaHiddenRecord struct {
	TableStruct[deltaHiddenSchema, deltaHiddenRecord]
	CompanyID int32   `db:"company_id"`
	ID        int32   `db:"id"`
	Status    int8    `db:"status"`
	Secret    string  `db:"secret" json:"-" cb:"-"`
	Updated   int32   `json:"upd,omitempty"`
	Version   int16   `json:"ver,omitempty"`
	Metadata  *string `json:"mdt,omitempty"`
}

type deltaHiddenSchema struct {
	TableStruct[deltaHiddenSchema, deltaHiddenRecord]
	CompanyID Col[*deltaHiddenSchema, int32]
	ID        Col[*deltaHiddenSchema, int32]
	Status    Col[*deltaHiddenSchema, int8]
	Secret    Col[*deltaHiddenSchema, string]
	Updated   Col[*deltaHiddenSchema, int32]
	Version   Col[*deltaHiddenSchema, int16]
}

func (e deltaHiddenSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10043,
		Name:        "delta_hidden_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes:     []Index{{Type: TypeDelta, Keys: Cols(e.Status)}},
	}
}

func TestDeltaSnapshotCodecRejectsColumnsColbinDrops(t *testing.T) {
	resetORMTableCachesForTesting()

	stockCodec := recordSnapshotCodecFor(MakeScyllaTable[deltaStockRecord, deltaStockSchema](), reflect.TypeFor[deltaStockRecord]())
	hiddenCodec := recordSnapshotCodecFor(MakeScyllaTable[deltaHiddenRecord, deltaHiddenSchema](), reflect.TypeFor[deltaHiddenRecord]())
	if !stockCodec.isEligible || stockCodec.fingerprint == 0 {
		t.Fatalf("every stored column of deltaStockRecord is encoded: %+v", stockCodec)
	}
	if hiddenCodec.isEligible {
		t.Fatal(`a stored cb:"-" column must disable snapshots`)
	}
	if hiddenCodec.fingerprint == stockCodec.fingerprint {
		t.Fatal("two record types must not share a schema fingerprint")
	}
}

// writeDeltaSnapshot stores the records with the highest updated as the cursor and the sync's read
// time as its V, skips an empty
// prefix, and never overwrites a file another request wrote since the read (its etag moved).
func TestWriteDeltaSnapshot(t *testing.T) {
	resetORMTableCachesForTesting()
	store := utils.NewMemoryFileStore()
	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	recordType := reflect.TypeFor[deltaStockRecord]()
	codec := recordSnapshotCodecFor(scyllaTable, recordType)

	records := []deltaStockRecord{
		{CompanyID: 7, ID: 2, Status: 1, Name: "dos", Updated: 900, Version: 3},
		{CompanyID: 7, ID: 1, Status: 1, Name: "uno", Updated: 800, Version: 1},
	}
	recordPointers := []unsafe.Pointer{unsafe.Pointer(&records[0]), unsafe.Pointer(&records[1])}
	writeDeltaSnapshot(store, "snapshot.cb", "", codec, scyllaTable, recordType, 905, recordPointers)

	content, etag, err := store.GetFile("snapshot.cb")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := cache.DecodeRecordSnapshot(content)
	if err != nil || snapshot.Cursor != 900 || snapshot.ReadTime != 905 || snapshot.SchemaFingerprint != codec.fingerprint {
		t.Fatalf("snapshot %+v, err %v", snapshot, err)
	}
	decodedRecords := []deltaStockRecord{}
	if err := colbin.Unmarshal(snapshot.Records, &decodedRecords); err != nil || len(decodedRecords) != 2 ||
		decodedRecords[0].Name != "dos" || decodedRecords[1].Version != 1 {
		t.Fatalf("decoded %+v, err %v", decodedRecords, err)
	}

	// A write that read no file ("" etag) loses to the one already there.
	writeDeltaSnapshot(store, "snapshot.cb", "", codec, scyllaTable, recordType, 905, recordPointers[1:])
	if _, etagAfter, _ := store.GetFile("snapshot.cb"); etagAfter != etag {
		t.Fatal("a stale write replaced the snapshot")
	}
	writeDeltaSnapshot(store, "empty.cb", "", codec, scyllaTable, recordType, 905, nil)
	if _, _, err := store.GetFile("empty.cb"); err == nil {
		t.Fatal("an empty prefix must not be stored")
	}
}
