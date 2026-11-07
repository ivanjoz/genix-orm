package conformance

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Cache groups over several scalar keys, beside a packed view on the same columns. Scylla serves each
// group through a hash column; MariaDB through a composite index on the real columns.
type scalarGroupRecord struct {
	TableStruct[scalarGroupSchema, scalarGroupRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Date      int16 `db:"date"`
	Status    int8  `db:"status"`
	ProductID int32 `db:"product_id"`
	Type      int8  `db:"type"`
	Quantity  int32 `db:"quantity"`
	Updated   int32 `json:"upd,omitempty"`
}

type scalarGroupSchema struct {
	TableStruct[scalarGroupSchema, scalarGroupRecord]
	CompanyID db.Col[*scalarGroupSchema, int32]
	ID        db.Col[*scalarGroupSchema, int32]
	Date      db.Col[*scalarGroupSchema, int16]
	Status    db.Col[*scalarGroupSchema, int8]
	ProductID db.Col[*scalarGroupSchema, int32]
	Type      db.Col[*scalarGroupSchema, int8]
	Quantity  db.Col[*scalarGroupSchema, int32]
	Updated   db.Col[*scalarGroupSchema, int32]
}

func (e scalarGroupSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12010,
		Name:      "conformance_scalar_group_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		Indexes: []db.Index{
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date)},
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.Status)},
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.ProductID)},
			{Type: db.TypeView, Keys: db.Cols(e.Date, e.ProductID.Size(20), e.Type.Size(4)), Cols: db.Cols(e.Quantity)},
		},
	}
}

// TestQueryIndexGroupScalarKeys: each kind of scalar group returns exactly its records, before and
// after a record moves between groups.
func TestQueryIndexGroupScalarKeys(t *testing.T) {
	runConformance(t, []db.Controller{makeController[scalarGroupRecord, scalarGroupSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]scalarGroupRecord{
			{CompanyID: 8, ID: 1, Date: 100, Status: 1, ProductID: 7, Type: 1, Quantity: 3},
			{CompanyID: 8, ID: 2, Date: 100, Status: 2, ProductID: 7, Type: 2, Quantity: 4},
			{CompanyID: 8, ID: 3, Date: 100, Status: 1, ProductID: 9, Type: 1, Quantity: 5},
			{CompanyID: 8, ID: 4, Date: 101, Status: 1, ProductID: 7, Type: 1, Quantity: 6},
			{CompanyID: 9, ID: 5, Date: 100, Status: 1, ProductID: 7, Type: 1, Quantity: 7},
		}))
		readGroups := func(pinGroup func(query *scalarGroupSchema)) func() (any, error) {
			return func() (any, error) {
				groups := []db.RecordGroup[scalarGroupRecord]{}
				query := db.QueryIndexGroup(&groups)
				query.CompanyID.Equals(8)
				query.Date.Between(100, 101)
				pinGroup(query)
				err := query.Exec()
				for _, group := range groups {
					sortedByID(group.Records, func(record scalarGroupRecord) int64 { return int64(record.ID) })
				}
				return groups, err
			}
		}
		byDate := readGroups(func(query *scalarGroupSchema) {})
		byStatus := readGroups(func(query *scalarGroupSchema) { query.Status.Equals(1) })
		byProduct := readGroups(func(query *scalarGroupSchema) { query.ProductID.Equals(7) })

		run.setClock(db.IndexGroupSettleTicks)
		run.read("by date", byDate)
		run.read("by date and status", byStatus)
		run.read("by date and product", byProduct)

		// Record 1 moves to product 9 and status 2: it leaves both groups it was in.
		run.setClock(db.IndexGroupSettleTicks + 10)
		table := db.TableOf[scalarGroupRecord]()
		run.must("Update", db.Update(&[]scalarGroupRecord{{CompanyID: 8, ID: 1, Date: 100, Status: 2, ProductID: 9, Type: 1}}, table.Date, table.Status, table.ProductID, table.Type))

		run.setClock(2*db.IndexGroupSettleTicks + 10)
		run.read("after the update: by date and status", byStatus)
		run.read("after the update: by date and product", byProduct)
	})
}
