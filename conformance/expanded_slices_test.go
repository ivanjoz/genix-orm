//go:build mariadb && !oceanbase && !postgres

package conformance

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// Expanded slices are MariaDB-only, so these tests assert directly instead of against Scylla's golden
// files. clientWeekRecord is the case they exist for: one row per client and week, its sales and
// payments summed per weekday.

type clientWeekRecord struct {
	TableStruct[clientWeekSchema, clientWeekRecord]
	CompanyID int32
	ID        int32
	Week      int16
	Purchases []int32
	Payments  []int64
	Updated   int32   `json:"upd,omitempty"`
	Version   int16   `json:"ver,omitempty"`
	Metadata  *string `json:"mdt,omitempty"`
}

type clientWeekSchema struct {
	TableStruct[clientWeekSchema, clientWeekRecord]
	CompanyID db.Col[*clientWeekSchema, int32]
	ID        db.Col[*clientWeekSchema, int32]
	Week      db.Col[*clientWeekSchema, int16]
	Purchases db.ColSlice[*clientWeekSchema, int32]
	Payments  db.ColSlice[*clientWeekSchema, int64]
	Updated   db.Col[*clientWeekSchema, int32]
	Version   db.Col[*clientWeekSchema, int16]
}

// The keyless TypeDelta exercises the in-SQL path's version and packed delta column.
func (e clientWeekSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID: 12101, Name: "conformance_client_weeks", Partition: e.CompanyID, Keys: db.Cols(e.ID),
		ExpandedSlices: db.Cols(e.Purchases.Expand(7), e.Payments.Expand(7)),
		Indexes:        []db.Index{{Type: db.TypeDelta}},
	}
}

// clientWeekGroupRecord is the same table with a cache group on the week: an increment goes through
// the stored record, so the group's mark moves.
type clientWeekGroupRecord struct {
	TableStruct[clientWeekGroupSchema, clientWeekGroupRecord]
	CompanyID int32
	ID        int32
	Week      int16
	Purchases []int32
	Updated   int32 `json:"upd,omitempty"`
}

type clientWeekGroupSchema struct {
	TableStruct[clientWeekGroupSchema, clientWeekGroupRecord]
	CompanyID db.Col[*clientWeekGroupSchema, int32]
	ID        db.Col[*clientWeekGroupSchema, int32]
	Week      db.Col[*clientWeekGroupSchema, int16]
	Purchases db.ColSlice[*clientWeekGroupSchema, int32]
	Updated   db.Col[*clientWeekGroupSchema, int32]
}

func (e clientWeekGroupSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID: 12102, Name: "conformance_client_week_groups", Partition: e.CompanyID, Keys: db.Cols(e.ID),
		ExpandedSlices: db.Cols(e.Purchases.Expand(7)),
		Indexes:        []db.Index{{Type: db.TypeCacheGroup, Keys: db.Cols(e.Week)}},
	}
}

func startExpandedSlicesTest(t *testing.T, controllers ...db.Controller) {
	if os.Getenv("GENIX_CONFORMANCE") != "1" {
		t.Skip("set GENIX_CONFORMANCE=1 to run the conformance suite")
	}
	run := &conformanceRun{t: t}
	run.setClock(0)
	t.Cleanup(func() { db.SetHistoricalUnix(0) })
	resetAndDeploy(t, controllers...)
}

func readClientWeek(t *testing.T, id int32) clientWeekRecord {
	t.Helper()
	records := []clientWeekRecord{}
	if err := db.Query(&records).CompanyID.Equals(1).ID.Equals(id).Exec(); err != nil || len(records) != 1 {
		t.Fatalf("reading client week %d: %v (%d records)", id, err, len(records))
	}
	return records[0]
}

func requireError(t *testing.T, operation string, err error, wantText string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), wantText) {
		t.Fatalf("%s: got error %v, want one containing %q", operation, err, wantText)
	}
}

// TestExpandedSlicesInSQL: a record reads back every element; Increment adds in SQL, refuses a
// missing record and an element out of range; IncrementIn rolls back with its transaction; the
// delta cache sees an increment as a write.
func TestExpandedSlicesInSQL(t *testing.T) {
	startExpandedSlicesTest(t, makeController[clientWeekRecord, clientWeekSchema]())
	run := &conformanceRun{t: t}
	table := db.TableOf[clientWeekRecord]()

	if err := db.Upsert(&[]clientWeekRecord{{CompanyID: 1, ID: 10, Week: 2900, Purchases: []int32{1, 2}}}); err != nil {
		t.Fatal(err)
	}
	stored := readClientWeek(t, 10)
	if !slices.Equal(stored.Purchases, []int32{1, 2, 0, 0, 0, 0, 0}) || !slices.Equal(stored.Payments, make([]int64, 7)) {
		t.Fatalf("read back %v / %v, want every element", stored.Purchases, stored.Payments)
	}

	run.setClock(100)
	key := clientWeekRecord{CompanyID: 1, ID: 10}
	if err := db.Increment(key, table.Purchases.At(3).Add(5), table.Payments.At(6).Add(100), table.Purchases.At(3).Add(1)); err != nil {
		t.Fatal(err)
	}
	stored = readClientWeek(t, 10)
	if !slices.Equal(stored.Purchases, []int32{1, 2, 0, 6, 0, 0, 0}) || stored.Payments[6] != 100 || stored.Week != 2900 {
		t.Fatalf("after Increment: %+v", stored)
	}
	if stored.Version != 2 || stored.Updated != clockStart+100 {
		t.Fatalf("after Increment: version %d updated %d, want 2 and %d", stored.Version, stored.Updated, clockStart+100)
	}

	if err := db.Increment(clientWeekRecord{CompanyID: 1, ID: 99}, table.Purchases.At(0).Add(1)); !errors.Is(err, db.ErrIncrementNotStored) {
		t.Fatalf("Increment of a missing record: %v, want ErrIncrementNotStored", err)
	}
	requireError(t, "Increment out of range", db.Increment(key, table.Purchases.At(7).Add(1)), "out of its size 7")
	requireError(t, "Increment of a plain column", db.Increment(key, db.ColumnIncrement{ColumnName: "week", Delta: 1}), "not declared in ExpandedSlices")
	requireError(t, "Upsert of a slice longer than its size", db.Upsert(&[]clientWeekRecord{{CompanyID: 1, ID: 11, Purchases: make([]int32, 8)}}), "its size is 7")

	bodyFailure := errors.New("body failed")
	err := db.RunTransaction(func(transaction *db.Transaction) error {
		if err := db.IncrementIn(transaction, key, table.Purchases.At(0).Add(1000)); err != nil {
			return err
		}
		return bodyFailure
	})
	if !errors.Is(err, bodyFailure) || readClientWeek(t, 10).Purchases[0] != 1 {
		t.Fatalf("a failed transaction kept its increment: %v", err)
	}
	if err := db.RunTransaction(func(transaction *db.Transaction) error {
		return db.IncrementIn(transaction, key, table.Purchases.At(0).Add(-1))
	}); err != nil || readClientWeek(t, 10).Purchases[0] != 0 {
		t.Fatalf("IncrementIn: %v", err)
	}

	// A sync from before the increment returns the record: the packed delta column took its updated.
	response := []clientWeekRecord{}
	if err := db.Query(&response).CompanyID.Equals(1).Delta(cache.DeltaWindowCursor{Updated: clockStart + 50, ReadTime: clockStart + 60}).Exec(); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(response, func(record clientWeekRecord) bool { return record.ID == 10 && record.Purchases[3] == 6 }) {
		t.Fatalf("the delta sync after the increment misses it: %+v", response)
	}
}

// TestExpandedSlicesThroughStoredRecord: on a table with a cache group the increment reads the
// stored record and writes it as an Update, so the group read afterwards holds the new values.
func TestExpandedSlicesThroughStoredRecord(t *testing.T) {
	startExpandedSlicesTest(t, makeController[clientWeekGroupRecord, clientWeekGroupSchema]())
	run := &conformanceRun{t: t}
	table := db.TableOf[clientWeekGroupRecord]()
	if err := db.Upsert(&[]clientWeekGroupRecord{{CompanyID: 1, ID: 10, Week: 2900, Purchases: []int32{1}}}); err != nil {
		t.Fatal(err)
	}
	run.setClock(10)
	if err := db.Increment(clientWeekGroupRecord{CompanyID: 1, ID: 10}, table.Purchases.At(2).Add(4)); err != nil {
		t.Fatal(err)
	}
	requireError(t, "Increment of a missing record", db.Increment(clientWeekGroupRecord{CompanyID: 1, ID: 99}, table.Purchases.At(0).Add(1)), "no record is stored")

	run.setClock(groupSettleTicks() + 20)
	groups := []db.RecordGroup[clientWeekGroupRecord]{}
	query := db.QueryIndexGroup(&groups)
	query.CompanyID.Equals(1)
	if err := query.Week.Between(2900, 2900).Exec(); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || len(groups[0].Records) != 1 || !slices.Equal(groups[0].Records[0].Purchases, []int32{1, 0, 4, 0, 0, 0, 0}) {
		t.Fatalf("the group after the increment: %+v", groups)
	}
	if groups[0].LastWrite != clockStart+10 {
		t.Fatalf("the group's last write is %d, want the increment's %d", groups[0].LastWrite, clockStart+10)
	}
}
