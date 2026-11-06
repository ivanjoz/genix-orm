package fdbdriver

import (
	"os"
	"slices"
	"testing"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// The live tests run against a fdbgate daemon (fdb/container): GENIX_FDB_IT=1 FDBGATE_SOCKET=<path>.

type fixtureLine struct {
	ProductID int32
	Quantity  int32
}

type fixtureRecord struct {
	TableStruct[fixtureSchema, fixtureRecord]
	CompanyID   int32         `db:"company_id"`
	ID          int32         `db:"id"`
	WarehouseID int32         `db:"warehouse_id"`
	Status      int8          `db:"status"`
	Name        string        `db:"name"`
	Price       float64       `db:"price"`
	Tags        []int32       `db:"tags"`
	Labels      []string      `db:"labels"`
	Lines       []fixtureLine `db:"lines"`
	Note        *string       `db:"note"`
	Updated     int32         `json:"upd,omitempty"`
	Version     int16         `json:"ver,omitempty"`
	Metadata    *string       `json:"mdt,omitempty"`
}

type fixtureSchema struct {
	TableStruct[fixtureSchema, fixtureRecord]
	CompanyID   db.Col[*fixtureSchema, int32]
	ID          db.Col[*fixtureSchema, int32]
	WarehouseID db.Col[*fixtureSchema, int32]
	Status      db.Col[*fixtureSchema, int8]
	Name        db.Col[*fixtureSchema, string]
	Price       db.Col[*fixtureSchema, float64]
	Tags        db.Col[*fixtureSchema, []int32]
	Labels      db.Col[*fixtureSchema, []string]
	Lines       db.Col[*fixtureSchema, []fixtureLine]
	Note        db.Col[*fixtureSchema, *string]
	Updated     db.Col[*fixtureSchema, int32]
	Version     db.Col[*fixtureSchema, int16]
}

func (e fixtureSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:          10901,
		Name:        "fdb_fixture_records",
		Partition:   e.CompanyID,
		Keys:        db.Cols(e.ID.Autoincrement(0)),
		FixedValues: []db.FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []db.Index{
			{Type: db.TypeDelta, Keys: db.Cols(e.WarehouseID.Size(17), e.Status), DeltaReplaceThreshold: 3},
			{Type: db.TypeDelta, Keys: db.Cols(e.Status)},
			{Type: db.TypeLocalIndex, Keys: db.Cols(e.Name)},
		},
	}
}

func TestRowCodecRoundTrip(t *testing.T) {
	table := compiledTableOf[fixtureSchema, fixtureRecord]()
	note := ""
	original := fixtureRecord{
		CompanyID: 7, ID: -12, WarehouseID: 3, Status: 1, Name: "ñandú\x00x", Price: -2.5,
		Tags: []int32{-1, 0, 1 << 30}, Labels: []string{"a", "", "c"}, Lines: []fixtureLine{{ProductID: 9, Quantity: -4}},
		Note: &note, Updated: 1234, Version: 2,
	}
	row, err := table.encodeRow(nil, 5, unsafe.Pointer(&original), managedStamp{created: 99})
	if err != nil {
		t.Fatal(err)
	}
	decoded := fixtureRecord{}
	if err := table.decodeRow(row, unsafe.Pointer(&decoded), nil); err != nil {
		t.Fatal(err)
	}
	if rowVersionOf(row) != 5 || decoded.ID != -12 || decoded.Name != original.Name || decoded.Price != -2.5 ||
		!slices.Equal(decoded.Tags, original.Tags) || !slices.Equal(decoded.Labels, original.Labels) ||
		!slices.Equal(decoded.Lines, original.Lines) || decoded.Note == nil || *decoded.Note != "" || decoded.Version != 2 {
		t.Fatalf("round trip lost data: %+v", decoded)
	}
	if storedManagedInteger(table, row, managedCreatedColumnName) != 99 {
		t.Fatal("the managed created column without a field was not stored")
	}
	// A projection decodes only what it names.
	projected := fixtureRecord{}
	filter := table.projectionFilter(&db.TableInfo{ColumnsInclude: []db.ColumnInfo{{ColInfo: db.ColInfo{Name: "name"}}}}, nil, nil)
	if err := table.decodeRow(row, unsafe.Pointer(&projected), filter); err != nil {
		t.Fatal(err)
	}
	if projected.Name != original.Name || projected.Price != 0 || projected.Tags != nil {
		t.Fatalf("projection decoded too much: %+v", projected)
	}
}

func TestOrderedKeyComponents(t *testing.T) {
	values := []int64{-1 << 63, -5, -1, 0, 1, 5, 1<<63 - 1}
	for index := 1; index < len(values); index++ {
		if string(appendOrderedInt64(nil, values[index-1])) >= string(appendOrderedInt64(nil, values[index])) {
			t.Fatalf("%d does not sort before %d", values[index-1], values[index])
		}
	}
	texts := []string{"", "\x00", "\x00\x01", "a", "a\x00", "ab", "b"}
	for index := 1; index < len(texts); index++ {
		if string(appendOrderedString(nil, texts[index-1])) >= string(appendOrderedString(nil, texts[index])) {
			t.Fatalf("%q does not sort before %q", texts[index-1], texts[index])
		}
	}
}

func requireLiveGate(t *testing.T) {
	if os.Getenv("GENIX_FDB_IT") == "" || os.Getenv("FDBGATE_SOCKET") == "" {
		t.Skip("set GENIX_FDB_IT=1 and FDBGATE_SOCKET to run against fdbgate")
	}
	SetConnection(ConnParams{SocketPath: os.Getenv("FDBGATE_SOCKET"), Keyspace: "genix_fdbdriver_test"})
}

func TestWritesReadsAndDeltaOnFDB(t *testing.T) {
	requireLiveGate(t)
	const companyID = 777
	table := compiledTableOf[fixtureSchema, fixtureRecord]()
	if err := WipePartition("", table.ID, companyID); err != nil {
		t.Fatal(err)
	}
	baseUnix := int64(1_800_000_000)
	db.SetHistoricalUnix(baseUnix)
	defer db.SetHistoricalUnix(0)

	records := []fixtureRecord{}
	for index := range 5 {
		records = append(records, fixtureRecord{
			CompanyID: companyID, WarehouseID: 1, Status: 1, Name: "item", Price: float64(index),
			Tags: []int32{int32(index)}, Lines: []fixtureLine{{ProductID: int32(index), Quantity: 1}},
		})
	}
	if err := db.Insert(&records); err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if record.ID <= 0 || record.Version != 1 || record.Updated == 0 {
			t.Fatalf("insert did not assign the key and managed columns: %+v", record)
		}
	}

	stored := []fixtureRecord{}
	if err := db.Query(&stored).CompanyID.Equals(companyID).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(stored) != 5 || stored[0].Lines[0].Quantity != 1 {
		t.Fatalf("partition read returned %+v", stored)
	}
	byKey := []fixtureRecord{}
	if err := db.Query(&byKey).CompanyID.Equals(companyID).ID.Equals(records[2].ID).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(byKey) != 1 || byKey[0].Price != 2 {
		t.Fatalf("key read returned %+v", byKey)
	}
	filtered := []fixtureRecord{}
	if err := db.Query(&filtered).CompanyID.Equals(companyID).Price.GreaterEqual(3).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered read returned %d records", len(filtered))
	}

	firstSync := []fixtureRecord{}
	if err := db.Query(&firstSync).CompanyID.Equals(companyID).Delta(cache.DeltaWindowCursor{}, 1).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(firstSync) != 6 || firstSync[5].ID != -1 || firstSync[5].Metadata == nil {
		t.Fatalf("first sync returned %d records", len(firstSync))
	}
	cursor := cache.DeltaWindowCursor{Updated: records[0].Updated, ReadTime: records[0].Updated}

	// A later write: one record leaves status 1, one is renamed (a partial update).
	db.SetHistoricalUnix(baseUnix + 100)
	moved := []fixtureRecord{{CompanyID: companyID, ID: records[0].ID, Status: 0}}
	if err := db.Update(&moved, mustColn(t, table, "status")); err != nil {
		t.Fatal(err)
	}
	renamed := []fixtureRecord{{CompanyID: companyID, ID: records[1].ID, Name: "renamed"}}
	if err := db.Update(&renamed, mustColn(t, table, "name")); err != nil {
		t.Fatal(err)
	}
	afterRename := []fixtureRecord{}
	if err := db.Query(&afterRename).CompanyID.Equals(companyID).ID.Equals(records[1].ID).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(afterRename) != 1 || afterRename[0].Name != "renamed" || afterRename[0].Price != 1 ||
		!slices.Equal(afterRename[0].Tags, []int32{1}) || afterRename[0].Version != 2 {
		t.Fatalf("a partial update lost the columns it did not write: %+v", afterRename)
	}

	incremental := []fixtureRecord{}
	if err := db.Query(&incremental).CompanyID.Equals(companyID).Delta(cursor, 1).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(incremental) != 2 || incremental[0].ID != records[1].ID {
		t.Fatalf("incremental sync returned %+v", incremental)
	}
	statusIndex := table.deltaIndexes[1]
	gate, _ := gateClient()
	movedIDs, err := table.readDeltaMovedIDs(gate, statusIndex, companyID, []int64{1}, int64(cursor.Updated), 10)
	if err != nil || !slices.Equal(movedIDs, []int64{int64(records[0].ID)}) {
		t.Fatalf("expected the move of %d, got %v (%v)", records[0].ID, movedIDs, err)
	}
	statusZero := []fixtureRecord{}
	if err := db.Query(&statusZero).CompanyID.Equals(companyID).Delta(cursor, 0).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(statusZero) != 2 || statusZero[0].ID != records[0].ID {
		t.Fatalf("the moved record is not in its new prefix: %+v", statusZero)
	}
}

func mustColn(t *testing.T, table *FdbTable, columnName string) db.Coln {
	column, err := db.ColnByName(table, columnName)
	if err != nil {
		t.Fatal(err)
	}
	return column
}
