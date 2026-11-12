package conformance

import (
	"os"
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// changedSinceIDs runs one ChangedSince() read of partition 7, optionally pinning the second delta
// index's first key (an equality that makes the read pick that index), and answers the records,
// ordered by ID.
func changedSinceIDs(t *testing.T, from int32, pinnedKeyValue int32, projectName bool) ([]int32, []cacheRecord) {
	t.Helper()
	records := []cacheRecord{}
	query := db.Query(&records)
	if projectName {
		query.Select(query.ID, query.Name)
	}
	query.CompanyID.Equals(7)
	if pinnedKeyValue > 0 {
		query.WarehouseID.Equals(pinnedKeyValue)
	}
	if err := query.ChangedSince(from).Exec(); err != nil {
		t.Fatalf("ChangedSince(%v): %v", from, err)
	}
	sortedByID(records, cacheRecordID)
	ids := []int32{}
	for _, record := range records {
		if record.Metadata != nil {
			t.Fatalf("ChangedSince() answered a Metadata record: %v", *record.Metadata)
		}
		ids = append(ids, record.ID)
	}
	return ids, records
}

// TestChangedSince: the records written at or after a stamp, on every pinned prefix (a record that
// moved to Status 0 included), on the index an equality picks, projected, and with nothing to read.
// SQL drivers only; asserted inline because the golden files are recorded on Scylla.
func TestChangedSince(t *testing.T) {
	if os.Getenv("GENIX_CONFORMANCE") != "1" {
		t.Skip("set GENIX_CONFORMANCE=1 to run the conformance suite")
	}
	if driverName != "postgres" && driverName != "mariadb" && driverName != "oceanbase" {
		t.Skip("ChangedSince() is implemented by the SQL drivers")
	}
	withMemoryFileStore(t)
	run := &conformanceRun{t: t}
	run.setClock(0)
	t.Cleanup(func() { db.SetHistoricalUnix(0) })
	resetAndDeploy(t, cacheControllers()...)
	table := db.TableOf[cacheRecord]()

	run.must("Insert", db.Upsert(&[]cacheRecord{
		{CompanyID: 7, ID: 1, WarehouseID: 5, Name: "uno", Status: 1},
		{CompanyID: 7, ID: 2, WarehouseID: 5, Name: "dos", Status: 1},
		{CompanyID: 7, ID: 3, WarehouseID: 6, Name: "tres", Status: 1},
		{CompanyID: 7, ID: 4, WarehouseID: 5, Name: "cuatro", Status: 0},
	}))
	// A write to another partition in the same window must never be read.
	run.must("Insert other partition", db.Upsert(&[]cacheRecord{{CompanyID: 8, ID: 1, WarehouseID: 5, Name: "otra", Status: 1}}))

	// At +100: record 1 renamed, record 2 soft-deleted (moves to the Status 0 prefix), record 3 renamed.
	run.setClock(100)
	run.must("Update names", db.Update(&[]cacheRecord{
		{CompanyID: 7, ID: 1, Name: "uno bis"}, {CompanyID: 7, ID: 3, Name: "tres bis"},
	}, table.Name))
	run.must("Soft delete", db.Update(&[]cacheRecord{{CompanyID: 7, ID: 2, Status: 0}}, table.Status))
	run.setClock(110)

	if ids, _ := changedSinceIDs(t, clockStart, 0, false); !slices.Equal(ids, []int32{1, 2, 3, 4}) {
		t.Fatalf("since the insert: got %v, want every record of partition 7 [1 2 3 4]", ids)
	}
	ids, records := changedSinceIDs(t, clockStart+100, 0, false)
	if !slices.Equal(ids, []int32{1, 2, 3}) {
		t.Fatalf("since +100: got %v, want [1 2 3] (2 read from the Status 0 prefix)", ids)
	}
	if records[1].Status != 0 || records[0].Name != "uno bis" {
		t.Fatalf("since +100: stale values %+v", records)
	}
	if ids, _ := changedSinceIDs(t, clockStart+100, 5, false); !slices.Equal(ids, []int32{1, 2}) {
		t.Fatalf("[warehouse_id 5, status] prefixes since +100: got %v, want [1 2]", ids)
	}
	_, projected := changedSinceIDs(t, clockStart+100, 0, true)
	if projected[0].Name != "uno bis" || projected[0].Updated == 0 {
		t.Fatalf("projected read lost the name or the stamp: %+v", projected[0])
	}
	if ids, _ := changedSinceIDs(t, clockStart+101, 0, false); len(ids) != 0 {
		t.Fatalf("since +101: got %v, want nothing", ids)
	}
}
