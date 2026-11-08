package conformance

import (
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// A TypeUniqueIndex is a native unique index, so it runs on the SQL drivers only and checks its
// outcomes directly instead of against a golden file recorded on Scylla.

type uniqueLotRecord struct {
	TableStruct[uniqueLotSchema, uniqueLotRecord]
	CompanyID  int32
	ID         int32
	Date       int16
	SupplierID int32
	Name       string
	Note       string
	Updated    int32 `json:"upd,omitempty"`
}

type uniqueLotSchema struct {
	TableStruct[uniqueLotSchema, uniqueLotRecord]
	CompanyID  db.Col[*uniqueLotSchema, int32]
	ID         db.Col[*uniqueLotSchema, int32]
	Date       db.Col[*uniqueLotSchema, int16]
	SupplierID db.Col[*uniqueLotSchema, int32]
	Name       db.Col[*uniqueLotSchema, string]
	Note       db.Col[*uniqueLotSchema, string]
	Updated    db.Col[*uniqueLotSchema, int32]
}

func (e uniqueLotSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12011,
		Name:      "conformance_unique_lots",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		Indexes: []db.Index{
			{Type: db.TypeUniqueIndex, Keys: db.Cols(e.Date, e.SupplierID, e.Name)},
		},
	}
}

func readUniqueLots(companyID int32, date int16, supplierIDs []int32, names []string) ([]uniqueLotRecord, error) {
	lots := []uniqueLotRecord{}
	query := db.Query(&lots)
	query.CompanyID.Equals(companyID).
		Date.Equals(date).
		SupplierID.In(supplierIDs...).
		Name.In(names...)
	err := query.Exec()
	slices.SortFunc(lots, func(left, right uniqueLotRecord) int { return int(left.ID - right.ID) })
	return lots, err
}

func runUniqueIndexConformance(t *testing.T) {
	t.Helper()
	if os.Getenv("GENIX_CONFORMANCE") != "1" {
		t.Skip("set GENIX_CONFORMANCE=1 to run the conformance suite")
	}
	if driverName != "postgres" && driverName != "mariadb" {
		t.Skip("TypeUniqueIndex is a SQL-driver index")
	}
	resetAndDeploy(t, makeController[uniqueLotRecord, uniqueLotSchema]())
}

// TestUniqueIndexWrites: a duplicate Insert fails, a read with an equality or IN on every key routes
// through the index, an Update that leaves the keys out goes through, and a write that may overwrite
// a stored row may not write the keys.
func TestUniqueIndexWrites(t *testing.T) {
	runUniqueIndexConformance(t)
	if err := db.Insert(&[]uniqueLotRecord{
		{CompanyID: 5, ID: 1, Date: 100, SupplierID: 7, Name: "L-1"},
		{CompanyID: 5, ID: 2, Date: 100, SupplierID: 8, Name: "L-1"},
		// Another partition may repeat the keys.
		{CompanyID: 6, ID: 1, Date: 100, SupplierID: 7, Name: "L-1"},
	}); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := db.Insert(&[]uniqueLotRecord{{CompanyID: 5, ID: 3, Date: 100, SupplierID: 7, Name: "L-1"}}); err == nil {
		t.Fatal("an Insert repeating a unique key must fail")
	}

	lots, err := readUniqueLots(5, 100, []int32{7, 8}, []string{"L-1", "L-2"})
	if err != nil {
		t.Fatalf("read through the unique index: %v", err)
	}
	if len(lots) != 2 || lots[0].ID != 1 || lots[1].ID != 2 {
		t.Fatalf("read through the unique index returned %+v, want lots 1 and 2", lots)
	}

	table := db.TableOf[uniqueLotRecord]()
	if err := db.Update(&[]uniqueLotRecord{{CompanyID: 5, ID: 1, Note: "checked"}}, table.Note); err != nil {
		t.Fatalf("an Update leaving the unique keys out: %v", err)
	}
	if err := db.Update(&[]uniqueLotRecord{{CompanyID: 5, ID: 1, Name: "L-2"}}, table.Name); err == nil {
		t.Fatal("an Update writing a unique key must be refused")
	}
	// On MariaDB this upsert would otherwise update lot 2 (the clash on the unique index) instead of lot 1.
	if err := db.Upsert(&[]uniqueLotRecord{{CompanyID: 5, ID: 1, Date: 100, SupplierID: 8, Name: "L-1"}}); err == nil {
		t.Fatal("an Upsert of given keys writing a unique key must be refused")
	}

	lots, err = readUniqueLots(5, 100, []int32{7, 8}, []string{"L-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lots) != 2 || lots[0].Note != "checked" || lots[1].SupplierID != 8 || lots[1].Note != "" {
		t.Fatalf("the refused writes changed the lots: %+v", lots)
	}
}

// TestUniqueIndexTransactionRace: two transactions both find a lot missing and both insert it under
// different IDs. The loser's commit hits the unique index, RunTransaction reruns its body, and the
// rerun finds the winner's lot: one lot, and both bodies resolve its ID.
func TestUniqueIndexTransactionRace(t *testing.T) {
	runUniqueIndexConformance(t)

	// Both first attempts read before either commits.
	var bothRead sync.WaitGroup
	bothRead.Add(2)
	var attempts atomic.Int32
	resolvedIDs := make([]int32, 2)
	resolveLot := func(bodyIndex int, newLotID int32) error {
		isFirstAttempt := true
		return db.RunTransaction(func(transaction *db.Transaction) error {
			attempts.Add(1)
			lots, err := readUniqueLots(9, 200, []int32{3}, []string{"race"})
			if err != nil {
				return err
			}
			if isFirstAttempt {
				isFirstAttempt = false
				bothRead.Done()
				bothRead.Wait()
			}
			if len(lots) > 0 {
				resolvedIDs[bodyIndex] = lots[0].ID
				return nil
			}
			resolvedIDs[bodyIndex] = newLotID
			return db.InsertIn(transaction, &[]uniqueLotRecord{{CompanyID: 9, ID: newLotID, Date: 200, SupplierID: 3, Name: "race"}})
		})
	}

	bodyErrors := make([]error, 2)
	var bodies sync.WaitGroup
	for bodyIndex := range 2 {
		bodies.Add(1)
		go func() {
			defer bodies.Done()
			bodyErrors[bodyIndex] = resolveLot(bodyIndex, int32(bodyIndex+1))
		}()
	}
	bodies.Wait()
	for bodyIndex, err := range bodyErrors {
		if err != nil {
			t.Fatalf("body %d: %v", bodyIndex, err)
		}
	}

	lots, err := readUniqueLots(9, 200, []int32{3}, []string{"race"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lots) != 1 {
		t.Fatalf("the race left %d lots, want 1: %+v", len(lots), lots)
	}
	if resolvedIDs[0] != lots[0].ID || resolvedIDs[1] != lots[0].ID {
		t.Fatalf("the bodies resolved lots %v, want both %d", resolvedIDs, lots[0].ID)
	}
	if attempts.Load() != 3 {
		t.Fatalf("%d body attempts, want 3 (the loser reruns once)", attempts.Load())
	}
}
