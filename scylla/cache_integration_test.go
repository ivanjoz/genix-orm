package scylla

import (
	"slices"
	"testing"

	"github.com/ivanjoz/colbin"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// A slot write arriving after a newer one never lowers the slot: USING TIMESTAMP turns the
// last-write-wins map cell into a max() of the stamps.
func TestCacheByIDsSlotKeepsTheLatestStamp(t *testing.T) {
	deployCacheITTable(t)
	scyllaTable := MakeScyllaTable[cacheITRecord, cacheITSchema]()
	now := utils.SUnixTimeOf(db.Now())
	for _, updated := range []int64{now - 1000, now - 2000} { // the newer stamp lands first
		records := []cacheITRecord{{CompanyID: 99, ID: 300, Updated: int32(updated)}}
		if err := raiseWrittenSlots(recordSliceGroup{makeRecordSlice(&records)}, scyllaTable); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := planCachedIDsRead(scyllaTable, cache.CachedIDsRequest{
		CycleYear: cache.YearCycleOf(db.Now()), IDs: []cache.CachedID{{Partition: 99, ID: 300}},
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cache.YearCycleOf(db.Now())))
	expected := cache.YearTickToUpdated(cache.YearTickOfUpdated(now-1000, cycleStart), cycleStart)
	if plan.HeldUpdated(99, 300) != expected {
		t.Fatalf("slot 44 held as %d, want the newer stamp's %d", plan.HeldUpdated(99, 300), expected)
	}
}

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
func deltaSync(t *testing.T, warehouseID int32, cursor cache.DeltaWindowCursor) (records []cacheITRecord, metadataText string) {
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
		"DROP TABLE IF EXISTS genix_orm_it.cache_by_ids_slots",
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
// replace, a sync whose unsettled slice holds a late write and the resend it triggers, a move
// between warehouses, and a partial update that leaves the pinned column out.
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
	// Holding the ticks just returned (the write is an hour old, so trusted), nothing comes back.
	cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cache.YearCycleOf(db.Now())))
	var heldIDs []cache.CachedID
	for _, record := range partitions[0].Records {
		heldIDs = append(heldIDs, cache.CachedID{Partition: 7, ID: int64(record.ID),
			YearTick: uint32(int64(record.Updated) - cycleStart + 1)})
	}
	partitions, err = QueryCachedIDs[cacheITRecord, cacheITSchema](cache.CachedIDsRequest{
		CycleYear: cache.YearCycleOf(db.Now()), IDs: heldIDs,
	})
	if err != nil || len(partitions) != 0 {
		t.Fatalf("by-IDs holding the current ticks: %+v, err %v", partitions, err)
	}

	// 10 minutes later record 1 is renamed; the first sync is a replace of the active records.
	setClock(600)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	setClock(601)
	records, metadataText := deltaSync(t, 0, cache.DeltaWindowCursor{})
	if ids := recordIDs(records); len(ids) != 2 || records[0].Version != 2 || records[1].Version != 1 {
		t.Fatalf("first sync: %+v", records)
	}
	expected := cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, NextCursor: clockStart + 600, ReadTime: clockStart + 601, FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("first sync: unexpected Metadata")
	}

	// A late write: record 2 lands stamped 2 ticks before the cursor, after the sync read (V = 601).
	// Record 1, at the cursor, is not resent: the unsettled slice [V − lag, C] is hashed instead, and
	// the hash now holds record 2's new version, which the client's (record 2 v1) can't match.
	setClock(598)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 2, Name: "dos bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	cursor := cache.DeltaWindowCursor{Updated: clockStart + 600, ReadTime: clockStart + 601}
	setClock(603)
	records, metadataText = deltaSync(t, 0, cursor)
	if len(records) != 0 {
		t.Fatalf("slice sync: %+v", records)
	}
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: cursor.Updated, ReadTime: clockStart + 603,
		WindowFrom: cursor.ReadTime - cache.DeltaWriteLagTicks, WindowTo: cursor.Updated + 1,
		WindowHash: cache.DeltaRecordHash(1, 2) + cache.DeltaRecordHash(2, 2), FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("slice sync: unexpected Metadata")
	}

	// The client's hash mismatched: it asks again with "C.V.r", and the slice comes as records.
	cursor.ResendUnsettled = true
	records, metadataText = deltaSync(t, 0, cursor)
	if ids := recordIDs(records); !slices.Equal(ids, []int32{1, 2}) {
		t.Fatalf("resend: %+v", records)
	}
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: cursor.Updated, ReadTime: clockStart + 603, FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("resend: unexpected Metadata")
	}

	// Record 1 moves to warehouse 6: warehouse 5's clients get it as removed, warehouse 6's as a record.
	warehouseCursor := cache.DeltaWindowCursor{Updated: clockStart + 600, ReadTime: clockStart + 603}
	setClock(700)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, WarehouseID: 6, Name: "uno bis"}}, table.WarehouseID, table.Name); err != nil {
		t.Fatal(err)
	}
	setClock(710)
	records, metadataText = deltaSync(t, 5, warehouseCursor)
	// Warehouse 5's slice [595, 600] holds record 2 alone now.
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: warehouseCursor.Updated, ReadTime: clockStart + 710,
		WindowFrom: warehouseCursor.ReadTime - cache.DeltaWriteLagTicks, WindowTo: warehouseCursor.Updated + 1,
		WindowHash: cache.DeltaRecordHash(2, 2), FilterField: "ss", FilterValues: []int64{1}, RemovedIDs: []int64{1},
	})
	if len(records) != 0 || metadataText != expected {
		t.Fatalf("warehouse 5 after the move: %+v", records)
	}

	// A partial update that leaves the warehouse out keeps record 1 in warehouse 6.
	setClock(800)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno ter"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	records, _ = deltaSync(t, 6, cache.DeltaWindowCursor{})
	if len(records) != 1 || records[0].ID != 1 || records[0].Name != "uno ter" || records[0].WarehouseID != 6 {
		t.Fatalf("warehouse 6: %+v", records)
	}

	// A plain query on warehouse + Status is served by the delta view's [warehouse_id, status] prefix.
	// Record 2 is annulled first so the status-1 prefix has something to leave out.
	setClock(900)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 2, Status: 0}}, table.Status); err != nil {
		t.Fatal(err)
	}
	for _, warehouseCase := range []struct {
		warehouseID int32
		expectedIDs []int32
	}{{5, []int32{}}, {6, []int32{1}}} {
		plainRecords := []cacheITRecord{}
		query := Query(&plainRecords)
		query.CompanyID.Equals(7).WarehouseID.Equals(warehouseCase.warehouseID).Status.Equals(1)
		if err := query.Exec(); err != nil {
			t.Fatal(err)
		}
		if ids := recordIDs(plainRecords); !slices.Equal(ids, warehouseCase.expectedIDs) {
			t.Fatalf("plain query on warehouse %d: got %v", warehouseCase.warehouseID, ids)
		}
	}

	// A cache 40 days old syncs by ID, since age is no replace: status is a pinned key, so the [1]
	// prefix returns record 1, record 2 (annulled) comes back as a move, and record 3 (status 0) is
	// outside the prefix. The cursor is settled: nothing is hashed.
	oldUpdated := clockStart - 40*24*1800
	oldCursor := cache.DeltaWindowCursor{Updated: oldUpdated, ReadTime: oldUpdated + 1000}
	records, metadataText = deltaSync(t, 0, oldCursor)
	expected = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		NextCursor: clockStart + 800, ReadTime: clockStart + 900, FilterField: "ss", FilterValues: []int64{1},
		RemovedIDs: []int64{2},
	})
	if ids := recordIDs(records); !slices.Equal(ids, []int32{1}) || metadataText != expected {
		t.Fatalf("40-day-old cursor: %+v", records)
	}
}

// TestDeltaSnapshotServesTheNextReplace: a first sync stores its prefix's snapshot; the next first
// sync is served from the file plus the changes since it, and writes nothing; a late write inside
// the snapshot's unsettled slice fails its hash, and the view answers and rewrites the file.
func TestDeltaSnapshotServesTheNextReplace(t *testing.T) {
	table, clockStart, setClock := deployCacheITTable(t)
	store := cache.FileStore()
	snapshotKey := cache.DeltaSnapshotKey("cache_it_records", 7, "status", []int64{1}, 0)
	readSnapshot := func() (cache.RecordSnapshot, []cacheITRecord, string) {
		t.Helper()
		content, etag, err := store.GetFile(snapshotKey)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := cache.DecodeRecordSnapshot(content)
		snapshotRecords := []cacheITRecord{}
		if err == nil {
			err = colbin.Unmarshal(snapshot.Records, &snapshotRecords)
		}
		if err != nil {
			t.Fatal(err)
		}
		return snapshot, snapshotRecords, etag
	}
	namesByID := func(records []cacheITRecord) map[int32]string {
		names := map[int32]string{}
		for _, record := range records {
			names[record.ID] = record.Name
		}
		return names
	}

	setClock(0)
	if err := Insert(&[]cacheITRecord{
		{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
		{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
		{CompanyID: 7, ID: 3, WarehouseID: 5, Name: "tres", Status: 1},
	}); err != nil {
		t.Fatal(err)
	}

	// The first sync reads the view and stores the prefix, its cursor the newest updated and its read
	// time the clock: 1 tick after the insert, so the insert stays in the snapshot's unsettled slice.
	setClock(1)
	deltaSync(t, 0, cache.DeltaWindowCursor{})
	snapshot, snapshotRecords, _ := readSnapshot()
	if snapshot.Cursor != clockStart || snapshot.ReadTime != clockStart+1 || len(snapshotRecords) != 3 {
		t.Fatalf("stored snapshot: cursor %d read time %d, %+v", snapshot.Cursor, snapshot.ReadTime, snapshotRecords)
	}

	// Mark record 2 in the file only, so a response that carries the mark came from the file.
	for index := range snapshotRecords {
		if snapshotRecords[index].ID == 2 {
			snapshotRecords[index].Name = "from the snapshot"
		}
	}
	markedRecords, err := colbin.Marshal(&snapshotRecords)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Records = markedRecords
	_, _, etag := readSnapshot()
	if _, err := store.PutFileIfMatch(t.Context(), snapshotKey, cache.EncodeRecordSnapshot(snapshot), etag); err != nil {
		t.Fatal(err)
	}
	_, _, markedETag := readSnapshot()

	// Record 1 changes; the next first sync merges it over the file, and stores nothing. Record 1
	// left the slice, so the slice's hash covers records 2 and 3, as the file holds them.
	setClock(200)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	setClock(210)
	records, metadataText := deltaSync(t, 0, cache.DeltaWindowCursor{})
	names := namesByID(records)
	if len(records) != 3 || names[1] != "uno bis" || names[2] != "from the snapshot" || names[3] != "tres" {
		t.Fatalf("served from the snapshot: %+v", records)
	}
	expected := cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, NextCursor: clockStart + 200, ReadTime: clockStart + 210, FilterField: "ss", FilterValues: []int64{1},
	})
	if metadataText != expected {
		t.Fatal("served from the snapshot: unexpected Metadata")
	}
	if _, _, etagAfter := readSnapshot(); etagAfter != markedETag {
		t.Fatal("a merged snapshot must never be stored")
	}

	// Record 2 lands stamped 2 ticks before the snapshot's cursor, after its read: inside the
	// snapshot's unsettled slice, so the hash fails, the view answers (the mark is gone) and the full
	// read rewrites the file.
	setClock(-2)
	if err := Update(&[]cacheITRecord{{CompanyID: 7, ID: 2, Name: "dos bis"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	setClock(220)
	records, _ = deltaSync(t, 0, cache.DeltaWindowCursor{})
	if names = namesByID(records); len(records) != 3 || names[2] != "dos bis" {
		t.Fatalf("after the late write: %+v", records)
	}
	snapshot, snapshotRecords, _ = readSnapshot()
	if snapshot.Cursor != clockStart+200 || snapshot.ReadTime != clockStart+220 || namesByID(snapshotRecords)[2] != "dos bis" {
		t.Fatalf("rewritten snapshot: cursor %d read time %d, %+v", snapshot.Cursor, snapshot.ReadTime, snapshotRecords)
	}
}

// TestDeltaReplacesPastTheThreshold writes real records past the default threshold (100): exactly
// 100 changed or moved records still sync by ID, the 101st answers a replace. The cursors sit past
// the insert and are settled, so the inserted rows are neither changed nor hashed.
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
	settledMetadata := func(nextCursor, readTime int32, removedIDs []int64) string {
		return cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
			NextCursor: nextCursor, ReadTime: readTime, FilterField: "ss", FilterValues: []int64{1}, RemovedIDs: removedIDs,
		})
	}

	// Changed records, on the partition-wide index: 100 come by ID, the 101st turns it into a replace.
	changesCursor := cache.DeltaWindowCursor{Updated: clockStart + 1000, ReadTime: clockStart + 1050}
	setClock(1100)
	if err := Update(recordsFrom(1, threshold, 5, "cambio"), table.Name); err != nil {
		t.Fatal(err)
	}
	records, metadataText := deltaSync(t, 0, changesCursor)
	if len(records) != threshold || metadataText != settledMetadata(clockStart+1100, clockStart+1100, nil) {
		t.Fatalf("%d changed records: got %d records, Metadata %q", threshold, len(records), metadataText)
	}
	setClock(1200)
	if err := Update(recordsFrom(recordsCount, recordsCount, 5, "cambio"), table.Name); err != nil {
		t.Fatal(err)
	}
	records, metadataText = deltaSync(t, 0, changesCursor)
	expectedReplace := cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, NextCursor: clockStart + 1200, ReadTime: clockStart + 1200, FilterField: "ss", FilterValues: []int64{1},
	})
	if len(records) != recordsCount || metadataText != expectedReplace {
		t.Fatalf("%d changed records: got %d records, expected a replace", recordsCount, len(records))
	}

	// Moves out of warehouse 5: 100 come as removed IDs, the 101st turns it into a replace (of a
	// now empty prefix).
	movesCursor := cache.DeltaWindowCursor{Updated: clockStart + 2000, ReadTime: clockStart + 2050}
	setClock(2100)
	if err := Update(recordsFrom(1, threshold, 6, "cambio"), table.WarehouseID); err != nil {
		t.Fatal(err)
	}
	movedIDs := make([]int64, threshold)
	for i := range movedIDs {
		movedIDs[i] = int64(i + 1)
	}
	records, metadataText = deltaSync(t, 5, movesCursor)
	if len(records) != 0 || metadataText != settledMetadata(movesCursor.Updated, clockStart+2100, movedIDs) {
		t.Fatalf("%d moves: got %d records, Metadata %q", threshold, len(records), metadataText)
	}
	setClock(2200)
	if err := Update(recordsFrom(recordsCount, recordsCount, 6, "cambio"), table.WarehouseID); err != nil {
		t.Fatal(err)
	}
	records, metadataText = deltaSync(t, 5, movesCursor)
	expectedReplace = cache.EncodeDeltaWindowMetadata(cache.DeltaWindowMetadata{
		IsReplace: true, ReadTime: clockStart + 2200, FilterField: "ss", FilterValues: []int64{1},
	})
	if len(records) != 0 || metadataText != expectedReplace {
		t.Fatalf("%d moves: got %d records, expected a replace", recordsCount, len(records))
	}
}
