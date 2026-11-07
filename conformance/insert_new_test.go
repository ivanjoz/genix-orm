package conformance

import (
	"sync"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// TestInsertNewKeys: db.Insert writes records the caller promises are new. The SQL drivers skip the
// read before writing, so the frame must still count them once; a second Insert of a stored key fails
// there instead of overwriting it (Scylla writes it as an upsert, unchecked).
func TestInsertNewKeys(t *testing.T) {
	// Scylla keeps the frame as files under a lock: in memory, one process.
	withMemoryFileStore(t)
	var lockMutex sync.Mutex
	setFrameLock(func(lockName string, wait, lease time.Duration) (func(), bool, error) {
		lockMutex.Lock()
		return lockMutex.Unlock, true, nil
	})
	t.Cleanup(func() { setFrameLock(nil) })

	runConformance(t, []db.Controller{makeController[frameRecord, frameSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Insert(&[]frameRecord{
			{CompanyID: 2, ID: 1, Year: 2026, ProductID: 7, Quantity: 3, Status: 1},
			{CompanyID: 2, ID: 2, Year: 2026, ProductID: 7, Quantity: 4, Status: 1},
		}))
		readFrame := func() (any, error) {
			table := db.TableOf[frameRecord]()
			rows, err := db.QueryFrame[frameRecord](2, "quantity-by-year").Between(table.Year, 2026, 2026).Fresh().Exec()
			sums := [][]int64{}
			for _, row := range rows {
				sums = append(sums, append([]int64{int64(row.Key.ProductID)}, row.Sums...))
			}
			return sums, err
		}
		run.read("after the insert", readFrame)

		run.setClock(10)
		overwrite := []frameRecord{{CompanyID: 2, ID: 1, Year: 2026, ProductID: 7, Quantity: 50, Status: 1}}
		// Only a SQL driver checks the promise; elsewhere an Insert over a stored key breaks the frame.
		if driverName == "postgres" || driverName == "mariadb" {
			if err := db.Insert(&overwrite); err == nil {
				t.Fatal("an Insert of a stored key must fail on a SQL driver")
			}
		}
		// The failed Insert changed nothing: the Upsert overwrites record 1 (3 → 50).
		run.must("Upsert", db.Upsert(&overwrite))
		run.read("after the overwrite", readFrame)
	})
}
