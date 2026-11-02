package scylla

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// A table on both cache protocols: by-IDs, and two delta indexes, one pinning the warehouse.
type cacheITRecord struct {
	TableStruct[cacheITSchema, cacheITRecord]
	CompanyID   int32   `db:"company_id"`
	ID          int32   `db:"id"`
	WarehouseID int32   `db:"warehouse_id"`
	Name        string  `db:"name"`
	Status      int8    `db:"status" json:"ss,omitempty"`
	Updated     int32   `json:"upd,omitempty"`
	Version     int16   `json:"ver,omitempty"`
	Metadata    *string `json:"mdt,omitempty"`
}

type cacheITSchema struct {
	TableStruct[cacheITSchema, cacheITRecord]
	CompanyID   Col[*cacheITSchema, int32]
	ID          Col[*cacheITSchema, int32]
	WarehouseID Col[*cacheITSchema, int32]
	Name        Col[*cacheITSchema, string]
	Status      Col[*cacheITSchema, int8]
	Updated     Col[*cacheITSchema, int32]
	Version     Col[*cacheITSchema, int16]
}

func (e cacheITSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10040,
		Name:        "cache_it_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		CacheByIDs:  true,
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(e.Status)},
			{Type: TypeDelta, Keys: Cols(e.WarehouseID, e.Status)},
		},
	}
}

// deltaSync runs one Delta() read and splits the records from the Metadata text.
func deltaSync(t *testing.T, warehouseID int32, cursor int32) (records []cacheITRecord, metadataText string) {
	t.Helper()
	response := []cacheITRecord{}
	query := Query(&response)
	query.CompanyID.Equals(7)
	if warehouseID > 0 {
		query.WarehouseID.Equals(warehouseID)
	}
	if err := query.Delta(cursor, int8(1)).Exec(); err != nil {
		t.Fatal(err)
	}
	for _, record := range response {
		if record.Metadata != nil {
			metadataText = *record.Metadata
		} else {
			records = append(records, record)
		}
	}
	if metadataText == "" {
		t.Fatalf("expected a Metadata record, got %+v", response)
	}
	return records, metadataText
}

func recordIDs(records []cacheITRecord) []int32 {
	ids := []int32{}
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

// deployCacheITTable recreates cache_it_records on the live node and puts the ORM on a clock the
// test sets: setClock(offset) stamps writes at clockStart + offset.
func deployCacheITTable(t *testing.T) (table *cacheITSchema, clockStart int32, setClock func(offset int32)) {
	t.Helper()
	scyllaForTest(t)
	resetORMTableCachesForTesting()
	withMemoryFileStore(t)

	for _, statement := range []string{
		"DROP MATERIALIZED VIEW IF EXISTS genix_orm_it.cache_it_records__dlt_status_view",
		"DROP MATERIALIZED VIEW IF EXISTS genix_orm_it.cache_it_records__dlt_warehouse_id_status_view",
		"DROP TABLE IF EXISTS genix_orm_it.cache_it_records__delta_moves",
		"DROP TABLE IF EXISTS genix_orm_it.cache_it_records",
	} {
		if err := QueryExec(statement); err != nil {
			t.Fatal(err)
		}
	}
	DeployScylla(0, &ScyllaController[cacheITRecord, cacheITSchema]{
		Table:  MakeScyllaTable[cacheITRecord, cacheITSchema](),
		Schema: MakeSchema[cacheITRecord, cacheITSchema](),
	})

	clockStart = currentManagedUnixTime() - 3600
	setClock = func(offset int32) { getManagedUnixTime = func() int32 { return clockStart + offset } }
	t.Cleanup(func() { getManagedUnixTime = currentManagedUnixTime })
	return TableOf[cacheITRecord](), clockStart, setClock
}

// TestCacheProtocolsOnScylla runs both caches against a real node, on a clock the test sets: a
// replace, a sync whose window holds a late write, a move between warehouses, and a partial update
// that leaves the pinned column out.
func TestCacheProtocolsOnScylla(t *testing.T) {
	table, clockStart, setClock := deployCacheITTable(t)

	setClock(0)
	if err := Insert(&[]cacheITRecord{
		{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
		{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
		{CompanyID: 7, ID: 3, WarehouseID: 5, Name: "tres", Status: 0},
	}); err != nil {
		t.Fatal(err)
	}

	// By-IDs: nothing held, so all come back.
	partitions, err := QueryCachedIDs[cacheITRecord, cacheITSchema](cache.CachedIDsRequest{
		CycleYear: cache.YearCycleOf(db.Now()),
		IDs:       []cache.CachedID{{Partition: 7, ID: 1}, {Partition: 7, ID: 2}},
	})
	if err != nil || len(partitions) != 1 || len(partitions[0].Records) != 2 {
		t.Fatalf("by-IDs: %+v, err %v", partitions, err)
	}

	// 10 minutes later record 1 is renamed; the first sync is a replace of the active records.
	setClock(600)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	records, metadataText := deltaSync(t, 0, 0)
	if ids := recordIDs(records); len(ids) != 2 || records[0].Version != 2 || records[1].Version != 1 {
		t.Fatalf("first sync: %+v", records)
	}
	expected := cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, NextCursor: clockStart + 600, FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("first sync: unexpected Metadata")
	}

	// A late write: record 2 lands stamped 5 minutes before the client's cursor. The sync resends only
	// what is at the cursor (record 1), and the window hash now holds record 2's new version, which
	// the client's hash (record 2 at its old updated) can't match.
	setClock(300)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 2, Name: "dos bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	cursor := clockStart + 600
	setClock(610)
	records, metadataText = deltaSync(t, 0, cursor)
	if ids := recordIDs(records); len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("window sync: %+v", records)
	}
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: cursor, WindowFrom: cursor - cache.DeltaWindowTicks, WindowTo: cursor - cache.DeltaWindowMarginTicks,
		WindowHash: cache.DeltaRecordHash(2, 2), FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("window sync: unexpected Metadata")
	}

	// Record 1 moves to warehouse 6: warehouse 5's clients get it as removed, warehouse 6's as a record.
	_, metadataText = deltaSync(t, 5, 0)
	warehouseCursor := clockStart + 600
	setClock(700)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, WarehouseID: 6, Name: "uno bis"}}, table.WarehouseID, table.Name); err != nil {
		t.Fatal(err)
	}
	setClock(710)
	records, metadataText = deltaSync(t, 5, warehouseCursor)
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: warehouseCursor, WindowFrom: warehouseCursor - cache.DeltaWindowTicks,
		WindowTo: warehouseCursor - cache.DeltaWindowMarginTicks, WindowHash: cache.DeltaRecordHash(2, 2),
		FilterField: "ss", FilterValues: []int64{1}, RemovedIDs: []int64{1},
	})
	if len(records) != 0 || metadataText != expected {
		t.Fatalf("warehouse 5 after the move: %+v", records)
	}

	// A partial update that leaves the warehouse out keeps record 1 in warehouse 6.
	setClock(800)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno ter"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	records, _ = deltaSync(t, 6, 0)
	if len(records) != 1 || records[0].ID != 1 || records[0].Name != "uno ter" || records[0].WarehouseID != 6 {
		t.Fatalf("warehouse 6: %+v", records)
	}

	// A cache 40 days old with 3 changes since gets those 3 by ID, in every status: age is no replace.
	oldCursor := clockStart - 40*24*1800
	records, metadataText = deltaSync(t, 0, oldCursor)
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: clockStart + 800, WindowFrom: oldCursor - cache.DeltaWindowTicks,
		WindowTo: oldCursor - cache.DeltaWindowMarginTicks, FilterField: "ss", FilterValues: []int64{1},
	})
	if ids := recordIDs(records); len(ids) != 3 || metadataText != expected {
		t.Fatalf("40-day-old cursor: %+v", records)
	}
}

// TestDeltaReplacesPastTheThreshold writes real records past the default threshold (100): exactly
// 100 changed or moved records still sync by ID, the 101st answers a replace. The cursors sit past
// the insert, so the inserted rows are neither changed nor in the hashed window.
func TestDeltaReplacesPastTheThreshold(t *testing.T) {
	table, clockStart, setClock := deployCacheITTable(t)
	threshold := cache.DefaultDeltaReplaceThreshold
	recordsCount := threshold + 1

	setClock(0)
	insertedRecords := make([]cacheITRecord, recordsCount)
	for i := range insertedRecords {
		insertedRecords[i] = cacheITRecord{CompanyID: 7, ID: int32(i + 1), WarehouseID: 5, Name: "nuevo", Status: 1}
	}
	if err := Insert(&insertedRecords); err != nil {
		t.Fatal(err)
	}
	// recordsFrom builds the records firstID..lastID as an update writes them.
	recordsFrom := func(firstID, lastID int, warehouseID int32, name string) *[]cacheITRecord {
		updatedRecords := []cacheITRecord{}
		for id := firstID; id <= lastID; id++ {
			updatedRecords = append(updatedRecords, cacheITRecord{CompanyID: 7, ID: int32(id), WarehouseID: warehouseID, Name: name})
		}
		return &updatedRecords
	}
	windowMetadata := func(cursor int32, nextCursor int32, removedIDs []int64) string {
		return cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
			NextCursor: nextCursor, WindowFrom: cursor - cache.DeltaWindowTicks, WindowTo: cursor - cache.DeltaWindowMarginTicks,
			FilterField: "ss", FilterValues: []int64{1}, RemovedIDs: removedIDs,
		})
	}

	// Changed records, on the partition-wide index: 100 come by ID, the 101st turns it into a replace.
	changesCursor := clockStart + 1000
	setClock(1100)
	if err := Update(recordsFrom(1, threshold, 5, "cambio"), table.Name); err != nil {
		t.Fatal(err)
	}
	records, metadataText := deltaSync(t, 0, changesCursor)
	if len(records) != threshold || metadataText != windowMetadata(changesCursor, clockStart+1100, nil) {
		t.Fatalf("%d changed records: got %d records, Metadata %q", threshold, len(records), metadataText)
	}
	setClock(1200)
	if err := Update(recordsFrom(recordsCount, recordsCount, 5, "cambio"), table.Name); err != nil {
		t.Fatal(err)
	}
	records, metadataText = deltaSync(t, 0, changesCursor)
	expectedReplace := cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, NextCursor: clockStart + 1200, FilterField: "ss", FilterValues: []int64{1},
	})
	if len(records) != recordsCount || metadataText != expectedReplace {
		t.Fatalf("%d changed records: got %d records, expected a replace", recordsCount, len(records))
	}

	// Moves out of warehouse 5: 100 come as removed IDs, the 101st turns it into a replace (of a
	// now empty prefix). Warehouse 5's own rows are older than the window, so its hash is 0.
	movesCursor := clockStart + 2000
	setClock(2100)
	if err := Update(recordsFrom(1, threshold, 6, "cambio"), table.WarehouseID); err != nil {
		t.Fatal(err)
	}
	movedIDs := make([]int64, threshold)
	for i := range movedIDs {
		movedIDs[i] = int64(i + 1)
	}
	records, metadataText = deltaSync(t, 5, movesCursor)
	if len(records) != 0 || metadataText != windowMetadata(movesCursor, movesCursor, movedIDs) {
		t.Fatalf("%d moves: got %d records, Metadata %q", threshold, len(records), metadataText)
	}
	setClock(2200)
	if err := Update(recordsFrom(recordsCount, recordsCount, 6, "cambio"), table.WarehouseID); err != nil {
		t.Fatal(err)
	}
	records, metadataText = deltaSync(t, 5, movesCursor)
	expectedReplace = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, FilterField: "ss", FilterValues: []int64{1},
	})
	if len(records) != 0 || metadataText != expectedReplace {
		t.Fatalf("%d moves: got %d records, expected a replace", recordsCount, len(records))
	}
}
