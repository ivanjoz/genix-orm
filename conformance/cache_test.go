package conformance

import (
	"cmp"
	"slices"
	"testing"

	"github.com/ivanjoz/colbin"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// Seeded from scylla/cache_integration_test.go: both cache protocols, on the managed clock the test
// sets. Cache responses are observed as the client gets them: the records, and the Metadata text.

// A table on both cache protocols: by-IDs, and two delta indexes, one pinning the warehouse.
type cacheRecord struct {
	TableStruct[cacheSchema, cacheRecord]
	CompanyID   int32   `db:"company_id"`
	ID          int32   `db:"id"`
	WarehouseID int32   `db:"warehouse_id"`
	Name        string  `db:"name"`
	Status      int8    `db:"status" json:"ss,omitempty"`
	Updated     int32   `json:"upd,omitempty"`
	Version     int16   `json:"ver,omitempty"`
	Metadata    *string `json:"mdt,omitempty"`
}

type cacheSchema struct {
	TableStruct[cacheSchema, cacheRecord]
	CompanyID   db.Col[*cacheSchema, int32]
	ID          db.Col[*cacheSchema, int32]
	WarehouseID db.Col[*cacheSchema, int32]
	Name        db.Col[*cacheSchema, string]
	Status      db.Col[*cacheSchema, int8]
	Updated     db.Col[*cacheSchema, int32]
	Version     db.Col[*cacheSchema, int16]
}

func (e cacheSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:          12004,
		Name:        "conformance_cache_records",
		Partition:   e.CompanyID,
		Keys:        db.Cols(e.ID),
		CacheByIDs:  true,
		FixedValues: []db.FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []db.Index{
			{Type: db.TypeDelta, Keys: db.Cols(e.Status)},
			{Type: db.TypeDelta, Keys: db.Cols(e.WarehouseID, e.Status)},
		},
	}
}

// deltaResponse is one Delta() read as the client gets it.
type deltaResponse struct {
	Records  []cacheRecord
	Metadata string
}

// deltaSync runs one Delta() read of company 7, optionally on one warehouse, projected or not.
func deltaSync(warehouseID int32, cursor cache.DeltaWindowCursor, excludeName bool) func() (any, error) {
	return func() (any, error) {
		response := []cacheRecord{}
		query := db.Query(&response)
		if excludeName {
			query.Exclude(query.Name)
		}
		query.CompanyID.Equals(7)
		if warehouseID > 0 {
			query.WarehouseID.Equals(warehouseID)
		}
		err := query.Delta(cursor, int8(1)).Exec()
		split := deltaResponse{Records: []cacheRecord{}}
		for _, record := range response {
			if record.Metadata != nil {
				split.Metadata = *record.Metadata
			} else {
				split.Records = append(split.Records, record)
			}
		}
		sortedByID(split.Records, cacheRecordID)
		return split, err
	}
}

// sortedByID orders records by ID: no read promises an order within a partition, and two drivers
// need not agree on one.
func sortedByID[RecordT any](records []RecordT, idOf func(record RecordT) int64) []RecordT {
	slices.SortFunc(records, func(left, right RecordT) int { return cmp.Compare(idOf(left), idOf(right)) })
	return records
}

func cacheRecordID(record cacheRecord) int64 { return int64(record.ID) }

// withMemoryFileStore puts the cache files (slot files, delta snapshots) in memory for one test.
func withMemoryFileStore(t *testing.T) *utils.MemoryFileStore {
	store := utils.NewMemoryFileStore()
	cache.SetFileStore(store)
	t.Cleanup(func() { cache.SetFileStore(nil) })
	return store
}

func cacheControllers() []db.Controller {
	return []db.Controller{makeController[cacheRecord, cacheSchema]()}
}

// TestCacheProtocols: by-IDs with nothing held and with the current ticks held; a first delta sync
// (a replace); a sync whose unsettled slice holds a late write, and the resend it triggers; a move
// between warehouses; a partial update that leaves the pinned column out; a plain query served by
// a delta index prefix; and a 40-day-old cursor.
func TestCacheProtocols(t *testing.T) {
	withMemoryFileStore(t)
	runConformance(t, cacheControllers(), func(run *conformanceRun) {
		table := db.TableOf[cacheRecord]()
		run.must("Insert", db.Insert(&[]cacheRecord{
			{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
			{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
			{CompanyID: 7, ID: 3, WarehouseID: 5, Name: "tres", Status: 0},
		}))

		// By-IDs, an hour after the write so its slot is trusted: nothing held, then the ticks just
		// returned held.
		run.setClock(1800)
		cycleYear := cache.YearCycleOf(db.Now())
		readByIDs := func(ids []cache.CachedID) func() (any, error) {
			return func() (any, error) {
				partitions, err := db.QueryCachedIDs[cacheRecord](cache.CachedIDsRequest{CycleYear: cycleYear, IDs: ids})
				for _, partition := range partitions {
					sortedByID(partition.Records, cacheRecordID)
				}
				return partitions, err
			}
		}
		// A by-IDs record's Updated is its slot tick: the client holds exactly that.
		cycleStart := utils.SUnixTimeOf(cache.CycleStartTime(cycleYear))
		heldIDs := []cache.CachedID{}
		readNothingHeld := readByIDs([]cache.CachedID{{Partition: 7, ID: 1}, {Partition: 7, ID: 2}})
		run.read("by-IDs, nothing held", func() (any, error) {
			partitions, err := readNothingHeld()
			heldIDs = heldIDs[:0]
			for _, partition := range partitions.([]cache.CachedPartition[cacheRecord]) {
				for _, record := range partition.Records {
					heldIDs = append(heldIDs, cache.CachedID{Partition: 7, ID: int64(record.ID),
						YearTick: uint32(int64(record.Updated) - cycleStart + 1)})
				}
			}
			return partitions, err
		})
		run.read("by-IDs, holding the current ticks", readByIDs(heldIDs))

		// 10 minutes in, record 1 is renamed; the first sync is a replace of the active records.
		run.setClock(600)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name))
		run.setClock(601)
		run.read("first sync", deltaSync(0, cache.DeltaWindowCursor{}, false))

		// A late write: record 2 lands stamped 2 ticks before the cursor, after the sync read. The
		// unsettled slice is hashed instead of resent, and the hash no longer matches the client's.
		run.setClock(598)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 2, Name: "dos bis"}}, table.Name))
		cursor := cache.DeltaWindowCursor{Updated: clockStart + 600, ReadTime: clockStart + 601}
		run.setClock(603)
		run.read("slice sync", deltaSync(0, cursor, false))
		cursor.ResendUnsettled = true
		run.read("resend of the slice", deltaSync(0, cursor, false))

		// Record 1 moves to warehouse 6: warehouse 5's clients get it as removed.
		warehouseCursor := cache.DeltaWindowCursor{Updated: clockStart + 600, ReadTime: clockStart + 603}
		run.setClock(700)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 1, WarehouseID: 6, Name: "uno bis"}}, table.WarehouseID, table.Name))
		run.setClock(710)
		run.read("warehouse 5 after the move", deltaSync(5, warehouseCursor, false))

		// A partial update that leaves the warehouse out keeps record 1 in warehouse 6.
		run.setClock(800)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 1, Name: "uno ter"}}, table.Name))
		run.read("warehouse 6 after a partial update", deltaSync(6, cache.DeltaWindowCursor{}, false))

		// A plain query on warehouse + status, served by the [warehouse_id, status] prefix. Record 2 is
		// annulled first so the status-1 prefix has something to leave out.
		run.setClock(900)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 2, Status: 0}}, table.Status))
		for _, warehouseID := range []int32{5, 6} {
			run.read("plain query on warehouse and status", func() (any, error) {
				records := []cacheRecord{}
				err := db.Query(&records).CompanyID.Equals(7).WarehouseID.Equals(warehouseID).Status.Equals(1).Exec()
				return sortedByID(records, cacheRecordID), err
			})
		}

		// A 40-day-old cursor syncs by ID: record 2 (annulled) comes back as a move, record 3 (status 0)
		// is outside the prefix.
		oldUpdated := clockStart - 40*24*1800
		run.read("40-day-old cursor", deltaSync(0, cache.DeltaWindowCursor{Updated: oldUpdated, ReadTime: oldUpdated + 1000}, false))
	})
}

// readDeltaSnapshot decodes one stored delta snapshot.
func readDeltaSnapshot(run *conformanceRun, snapshotKey string) (cache.RecordSnapshot, []cacheRecord, string) {
	run.t.Helper()
	content, etag, err := cache.FileStore().GetFile(snapshotKey)
	run.must("read the snapshot", err)
	snapshot, err := cache.DecodeRecordSnapshot(content)
	run.must("decode the snapshot", err)
	snapshotRecords := []cacheRecord{}
	run.must("decode the snapshot records", colbin.Unmarshal(snapshot.Records, &snapshotRecords))
	return snapshot, snapshotRecords, etag
}

// rewriteDeltaSnapshot stores a snapshot with its records changed by mark, so a response carrying
// the mark came from the file.
func rewriteDeltaSnapshot(run *conformanceRun, snapshotKey string, mark func(record *cacheRecord)) string {
	run.t.Helper()
	snapshot, snapshotRecords, etag := readDeltaSnapshot(run, snapshotKey)
	for index := range snapshotRecords {
		mark(&snapshotRecords[index])
	}
	markedRecords, err := colbin.Marshal(&snapshotRecords)
	run.must("encode the snapshot records", err)
	snapshot.Records = markedRecords
	_, err = cache.FileStore().PutFileIfMatch(run.t.Context(), snapshotKey, cache.EncodeRecordSnapshot(snapshot), etag)
	run.must("store the snapshot", err)
	_, _, markedETag := readDeltaSnapshot(run, snapshotKey)
	return markedETag
}

// TestDeltaSnapshotServesTheNextReplace: a first sync stores its prefix's snapshot; the next first
// sync is served from the file plus the changes since it and stores nothing; a late write inside the
// snapshot's unsettled slice fails its hash, and the index answers and rewrites the file.
func TestDeltaSnapshotServesTheNextReplace(t *testing.T) {
	withMemoryFileStore(t)
	runConformance(t, cacheControllers(), func(run *conformanceRun) {
		table := db.TableOf[cacheRecord]()
		snapshotKey := cache.DeltaSnapshotKey("conformance_cache_records", 7, "status", []int64{1}, 0, nil)
		observeSnapshot := func(step string) {
			snapshot, snapshotRecords, _ := readDeltaSnapshot(run, snapshotKey)
			run.observe(step, map[string]any{"cursor": snapshot.Cursor, "readTime": snapshot.ReadTime, "records": sortedByID(snapshotRecords, cacheRecordID)})
		}

		run.must("Insert", db.Insert(&[]cacheRecord{
			{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
			{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
			{CompanyID: 7, ID: 3, WarehouseID: 5, Name: "tres", Status: 1},
		}))

		// The first sync stores the prefix, 1 tick after the insert: the insert stays in the
		// snapshot's unsettled slice.
		run.setClock(1)
		run.read("first sync", deltaSync(0, cache.DeltaWindowCursor{}, false))
		observeSnapshot("stored snapshot")
		markedETag := rewriteDeltaSnapshot(run, snapshotKey, func(record *cacheRecord) {
			if record.ID == 2 {
				record.Name = "from the snapshot"
			}
		})

		// Record 1 changes; the next first sync merges it over the file, and stores nothing.
		run.setClock(200)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name))
		run.setClock(210)
		run.read("served from the snapshot", deltaSync(0, cache.DeltaWindowCursor{}, false))
		_, _, etagAfter := readDeltaSnapshot(run, snapshotKey)
		run.observe("the merged snapshot was not stored", etagAfter == markedETag)

		// Record 2 lands stamped inside the snapshot's unsettled slice: the hash fails, the index
		// answers (the mark is gone) and the full read rewrites the file.
		run.setClock(-2)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 2, Name: "dos bis"}}, table.Name))
		run.setClock(220)
		run.read("after the late write", deltaSync(0, cache.DeltaWindowCursor{}, false))
		observeSnapshot("rewritten snapshot")
	})
}

// TestDeltaSnapshotOfAProjectedRead: a first sync that excludes a column stores its own snapshot,
// apart from the unprojected one, and the next one is served from it, the changed record merged in
// read with the same projection.
func TestDeltaSnapshotOfAProjectedRead(t *testing.T) {
	store := withMemoryFileStore(t)
	runConformance(t, cacheControllers(), func(run *conformanceRun) {
		table := db.TableOf[cacheRecord]()
		run.must("Insert", db.Insert(&[]cacheRecord{
			{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
			{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
		}))
		run.setClock(1)
		run.read("first projected sync", deltaSync(0, cache.DeltaWindowCursor{}, true))
		_, _, err := store.GetFile(cache.DeltaSnapshotKey("conformance_cache_records", 7, "status", []int64{1}, 0, nil))
		run.observe("the unprojected snapshot was not written", err != nil)

		snapshotKeys, err := store.ListFiles("delta-snapshot/conformance_cache_records/7/status/1.l0.p")
		run.must("list the projected snapshots", err)
		run.observe("projected snapshot files", len(snapshotKeys))
		if len(snapshotKeys) != 1 {
			return
		}
		rewriteDeltaSnapshot(run, snapshotKeys[0], func(record *cacheRecord) {
			if record.ID == 2 {
				record.WarehouseID = 99
			}
		})

		run.setClock(200)
		run.must("Update", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 1, Name: "uno bis"}}, table.Name))
		run.setClock(210)
		run.read("served from the projected snapshot", deltaSync(0, cache.DeltaWindowCursor{}, true))
	})
}

// TestDeltaReplacesPastTheThreshold: exactly DefaultDeltaReplaceThreshold changed or moved records
// still sync by ID, one more answers a replace. The cursors sit past the insert and are settled.
func TestDeltaReplacesPastTheThreshold(t *testing.T) {
	withMemoryFileStore(t)
	runConformance(t, cacheControllers(), func(run *conformanceRun) {
		table := db.TableOf[cacheRecord]()
		threshold := cache.DefaultDeltaReplaceThreshold
		recordsCount := threshold + 1

		insertedRecords := make([]cacheRecord, recordsCount)
		for index := range insertedRecords {
			insertedRecords[index] = cacheRecord{CompanyID: 7, ID: int32(index + 1), WarehouseID: 5, Name: "nuevo", Status: 1}
		}
		run.must("Insert", db.Insert(&insertedRecords))
		recordsFrom := func(firstID, lastID int, warehouseID int32) *[]cacheRecord {
			updatedRecords := []cacheRecord{}
			for id := firstID; id <= lastID; id++ {
				updatedRecords = append(updatedRecords, cacheRecord{CompanyID: 7, ID: int32(id), WarehouseID: warehouseID, Name: "cambio"})
			}
			return &updatedRecords
		}

		// Changed records, on the partition-wide index.
		changesCursor := cache.DeltaWindowCursor{Updated: clockStart + 1000, ReadTime: clockStart + 1050}
		run.setClock(1100)
		run.must("Update", db.Update(recordsFrom(1, threshold, 5), table.Name))
		run.read("threshold changed records", deltaSync(0, changesCursor, false))
		run.setClock(1200)
		run.must("Update", db.Update(recordsFrom(recordsCount, recordsCount, 5), table.Name))
		run.read("threshold + 1 changed records", deltaSync(0, changesCursor, false))

		// Moves out of warehouse 5.
		movesCursor := cache.DeltaWindowCursor{Updated: clockStart + 2000, ReadTime: clockStart + 2050}
		run.setClock(2100)
		run.must("Update", db.Update(recordsFrom(1, threshold, 6), table.WarehouseID))
		run.read("threshold moves", deltaSync(5, movesCursor, false))
		run.setClock(2200)
		run.must("Update", db.Update(recordsFrom(recordsCount, recordsCount, 6), table.WarehouseID))
		run.read("threshold + 1 moves", deltaSync(5, movesCursor, false))
	})
}
