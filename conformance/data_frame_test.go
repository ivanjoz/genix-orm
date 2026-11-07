package conformance

import (
	"cmp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/db"
)

// Seeded from scylla/data_frame_test.go: a frame declared by columns, summed per (year, product).

type frameRecord struct {
	TableStruct[frameSchema, frameRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Year      int16 `db:"year"`
	ProductID int32 `db:"product_id"`
	Quantity  int32 `db:"quantity"`
	Status    int8  `db:"status"`
	Updated   int32 `json:"upd,omitempty"`
}

type frameSchema struct {
	TableStruct[frameSchema, frameRecord]
	CompanyID db.Col[*frameSchema, int32]
	ID        db.Col[*frameSchema, int32]
	Year      db.Col[*frameSchema, int16]
	ProductID db.Col[*frameSchema, int32]
	Quantity  db.Col[*frameSchema, int32]
	Status    db.Col[*frameSchema, int8]
	Updated   db.Col[*frameSchema, int32]
}

func (e frameSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12008,
		Name:      "conformance_frame_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		DataFrames: []db.DataFrame{{
			Name: "quantity-by-year", Keys: db.Cols(e.Year), Rows: db.Cols(e.ProductID), Sums: db.Cols(e.Quantity), Count: true,
		}},
	}
}

// TestDataFrames: a fresh read sums the records, a partial update moves its quantity, a record with
// status 0 leaves the frame, and a rebuild from the records gives the same sums.
func TestDataFrames(t *testing.T) {
	withMemoryFileStore(t)
	// One process: a mutex is the frame lock.
	var lockMutex sync.Mutex
	setFrameLock(func(lockName string, wait, lease time.Duration) (func(), bool, error) {
		lockMutex.Lock()
		return lockMutex.Unlock, true, nil
	})
	t.Cleanup(func() { setFrameLock(nil) })

	controller := makeController[frameRecord, frameSchema]()
	runConformance(t, []db.Controller{controller}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]frameRecord{
			{CompanyID: 2, ID: 1, Year: 2026, ProductID: 7, Quantity: 3, Status: 1},
			{CompanyID: 2, ID: 2, Year: 2026, ProductID: 7, Quantity: 4, Status: 1},
			{CompanyID: 2, ID: 3, Year: 2026, ProductID: 8, Quantity: 5, Status: 1},
			{CompanyID: 2, ID: 4, Year: 2025, ProductID: 7, Quantity: 6, Status: 1},
		}))
		readFrame := func() (any, error) {
			table := db.TableOf[frameRecord]()
			rows, err := db.QueryFrame[frameRecord](2, "quantity-by-year").Between(table.Year, 2025, 2026).Fresh().Exec()
			type frameSums struct {
				Year      int16
				ProductID int32
				Sums      []int64
			}
			sums := []frameSums{}
			for _, row := range rows {
				sums = append(sums, frameSums{Year: row.Key.Year, ProductID: row.Key.ProductID, Sums: row.Sums})
			}
			slices.SortFunc(sums, func(left, right frameSums) int {
				return cmp.Or(cmp.Compare(left.Year, right.Year), cmp.Compare(left.ProductID, right.ProductID))
			})
			return sums, err
		}
		run.read("after the insert", readFrame)

		run.setClock(10)
		table := db.TableOf[frameRecord]()
		run.must("Update quantity", db.Update(&[]frameRecord{{CompanyID: 2, ID: 2, Quantity: 10}}, table.Quantity))
		run.must("Update status", db.Update(&[]frameRecord{{CompanyID: 2, ID: 3, Status: 0}}, table.Status))
		run.read("after the updates", readFrame)

		run.setClock(20)
		run.must("RebuildDataFramesPartition", controller.RebuildDataFramesPartition(2))
		run.read("after a rebuild", readFrame)
	})
}
