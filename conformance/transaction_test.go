package conformance

import (
	"errors"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// TestRunTransaction: LockByKeys reads the stored records by key (dropping a key with nothing stored),
// and InsertIn and UpdateIn write inside the body. On a SQL driver a failing body rolls back every
// write it queued; elsewhere each call is its own write, so that check runs on SQL drivers only.
func TestRunTransaction(t *testing.T) {
	runConformance(t, []db.Controller{makeController[controllerRecord, controllerSchema]()}, func(run *conformanceRun) {
		run.must("Upsert", db.Upsert(&[]controllerRecord{
			{CompanyID: 41, ID: 1, Name: "a", Tags: []string{"x"}},
			{CompanyID: 41, ID: 2, Name: "b", Tags: []string{"y"}},
		}))
		run.setClock(10)

		lockedRecords := []controllerRecord{}
		run.must("RunTransaction", db.RunTransaction(func(transaction *db.Transaction) error {
			lockedRecords = []controllerRecord{{CompanyID: 41, ID: 1}, {CompanyID: 41, ID: 3}}
			if err := db.LockByKeys(transaction, &lockedRecords); err != nil {
				return err
			}
			if err := db.InsertIn(transaction, &[]controllerRecord{{CompanyID: 41, ID: 3, Name: "c"}}); err != nil {
				return err
			}
			updated := []controllerRecord{}
			for _, record := range lockedRecords {
				record.Name = record.Name + "2"
				updated = append(updated, record)
			}
			return db.UpdateIn(transaction, &updated, db.TableOf[controllerRecord]().Name)
		}))
		run.observe("locked records", lockedRecords)
		run.read("after the transaction", func() (any, error) { return readControllerRecords(41) })

		if driverName != "postgres" && driverName != "mariadb" {
			return
		}
		before, err := readControllerRecords(41)
		run.must("read before the rollback", err)
		bodyFailure := errors.New("body failed")
		err = db.RunTransaction(func(transaction *db.Transaction) error {
			if err := db.InsertIn(transaction, &[]controllerRecord{{CompanyID: 41, ID: 4, Name: "d"}}); err != nil {
				return err
			}
			if err := db.UpdateIn(transaction, &[]controllerRecord{{CompanyID: 41, ID: 2, Name: "lost"}}, db.TableOf[controllerRecord]().Name); err != nil {
				return err
			}
			return bodyFailure
		})
		if !errors.Is(err, bodyFailure) {
			t.Fatalf("RunTransaction returned %v, want the body's error", err)
		}
		after, err := readControllerRecords(41)
		run.must("read after the rollback", err)
		if string(run.encode("before", before)) != string(run.encode("after", after)) {
			t.Fatalf("a failed transaction left writes behind:\nbefore %+v\nafter  %+v", before, after)
		}
	})
}

// TestRunTransactionLockedWrites: on a table with cache groups a write needs the stored rows. A write
// of rows LockByKeys locked reuses them; a second write of the same row in the transaction must see
// the first one. UpsertIn and InsertUpdateIncludeIn write inside the body. The groups read after it
// hold every record where its last write left it.
func TestRunTransactionLockedWrites(t *testing.T) {
	runConformance(t, []db.Controller{makeController[scalarGroupRecord, scalarGroupSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]scalarGroupRecord{
			{CompanyID: 12, ID: 1, Date: 100, Status: 1, ProductID: 7, Type: 1, Quantity: 3},
			{CompanyID: 12, ID: 2, Date: 100, Status: 1, ProductID: 7, Type: 1, Quantity: 4},
		}))
		run.setClock(10)
		table := db.TableOf[scalarGroupRecord]()
		run.must("RunTransaction", db.RunTransaction(func(transaction *db.Transaction) error {
			lockedRecords := []scalarGroupRecord{{CompanyID: 12, ID: 1}, {CompanyID: 12, ID: 2}}
			if err := db.LockByKeys(transaction, &lockedRecords); err != nil {
				return err
			}
			// Record 1 moves to product 9, then on to product 8: the second write reads the first.
			lockedRecords[0].ProductID = 9
			if err := db.UpdateIn(transaction, &[]scalarGroupRecord{lockedRecords[0]}, table.Date, table.ProductID, table.Type); err != nil {
				return err
			}
			lockedRecords[0].ProductID = 8
			if err := db.UpdateIn(transaction, &[]scalarGroupRecord{lockedRecords[0]}, table.Date, table.ProductID, table.Type); err != nil {
				return err
			}
			// Record 2 is updated through the split write, beside a new record 3.
			lockedRecords[1].Status = 2
			split := []scalarGroupRecord{lockedRecords[1], {CompanyID: 12, ID: 3, Date: 100, Status: 1, ProductID: 9, Type: 2, Quantity: 5, Updated: 1}}
			if err := db.InsertUpdateIncludeIn(transaction, &split, func(record *scalarGroupRecord) bool { return record.ID == 3 },
				db.Cols(table.Date, table.Status, table.ProductID, table.Type)); err != nil {
				return err
			}
			return db.UpsertIn(transaction, &[]scalarGroupRecord{{CompanyID: 12, ID: 4, Date: 101, Status: 1, ProductID: 9, Type: 1, Quantity: 6}})
		}))

		readGroups := func(pinGroup func(query *scalarGroupSchema)) func() (any, error) {
			return func() (any, error) {
				groups := []db.RecordGroup[scalarGroupRecord]{}
				query := db.QueryIndexGroup(&groups)
				query.CompanyID.Equals(12)
				query.Date.Between(100, 101)
				pinGroup(query)
				err := query.Exec()
				for _, group := range groups {
					sortedByID(group.Records, func(record scalarGroupRecord) int64 { return int64(record.ID) })
				}
				return groups, err
			}
		}
		run.setClock(groupSettleTicks() + 20)
		run.read("by date and product 7", readGroups(func(query *scalarGroupSchema) { query.ProductID.Equals(7) }))
		run.read("by date and product 8", readGroups(func(query *scalarGroupSchema) { query.ProductID.Equals(8) }))
		run.read("by date and product 9", readGroups(func(query *scalarGroupSchema) { query.ProductID.Equals(9) }))
		run.read("by date and status 2", readGroups(func(query *scalarGroupSchema) { query.Status.Equals(2) }))
	})
}
