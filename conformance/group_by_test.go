package conformance

import (
	"cmp"
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Shaped like the sales summary table (sale_order_v2_detail): a packed view keyed by date, product
// and status, read with GroupBy over a date range and summed per key.

type groupByRecord struct {
	TableStruct[groupBySchema, groupByRecord]
	CompanyID int32 `db:"company_id"`
	SaleID    int64 `db:"sale_id"`
	Line      int8  `db:"line"`
	Date      int16 `db:"date"`
	ProductID int32 `db:"product_id"`
	Status    int8  `db:"status"`
	Units     int32 `db:"units"`
	Amount    int32 `db:"amount"`
	Updated   int32 `json:"upd,omitempty"`
}

type groupBySchema struct {
	TableStruct[groupBySchema, groupByRecord]
	CompanyID db.Col[*groupBySchema, int32]
	SaleID    db.Col[*groupBySchema, int64]
	Line      db.Col[*groupBySchema, int8]
	Date      db.Col[*groupBySchema, int16]
	ProductID db.Col[*groupBySchema, int32]
	Status    db.Col[*groupBySchema, int8]
	Units     db.Col[*groupBySchema, int32]
	Amount    db.Col[*groupBySchema, int32]
	Updated   db.Col[*groupBySchema, int32]
}

func (e groupBySchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12007,
		Name:      "conformance_group_by_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.SaleID, e.Line),
		Indexes: []db.Index{{
			Type: db.TypeView,
			Keys: db.Cols(e.Date, e.ProductID.Size(27), e.Status.Size(3)),
			Cols: db.Cols(e.Units, e.Amount),
		}},
	}
}

// TestGroupBy: lines sum per (date, product, status) inside the date range; a line outside it, or
// of another company, does not count; a status change moves the line's sums to its new status.
func TestGroupBy(t *testing.T) {
	runConformance(t, []db.Controller{makeController[groupByRecord, groupBySchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]groupByRecord{
			{CompanyID: 3, SaleID: 1, Line: 1, Date: 100, ProductID: 7, Status: 1, Units: 2, Amount: 20},
			{CompanyID: 3, SaleID: 1, Line: 2, Date: 100, ProductID: 8, Status: 1, Units: 1, Amount: 15},
			{CompanyID: 3, SaleID: 2, Line: 1, Date: 100, ProductID: 7, Status: 1, Units: 3, Amount: 30},
			{CompanyID: 3, SaleID: 3, Line: 1, Date: 101, ProductID: 7, Status: 2, Units: 5, Amount: 50},
			{CompanyID: 3, SaleID: 4, Line: 1, Date: 105, ProductID: 7, Status: 1, Units: 9, Amount: 90},
			{CompanyID: 4, SaleID: 5, Line: 1, Date: 100, ProductID: 7, Status: 1, Units: 4, Amount: 40},
		}))
		readSums := func() (any, error) {
			sums := []groupByRecord{}
			query := db.Query(&sums)
			query.CompanyID.Equals(3).Date.Between(100, 102)
			err := query.GroupBy(query.Date, query.ProductID, query.Status, query.Units.Sum(), query.Amount.Sum()).Exec()
			slices.SortFunc(sums, func(left, right groupByRecord) int {
				return cmp.Or(cmp.Compare(left.Date, right.Date), cmp.Compare(left.ProductID, right.ProductID), cmp.Compare(left.Status, right.Status))
			})
			return sums, err
		}
		run.read("sums per date, product and status", readSums)

		run.setClock(10)
		// The view's key columns are written together.
		table := db.TableOf[groupByRecord]()
		run.must("Update status", db.Update(&[]groupByRecord{{CompanyID: 3, SaleID: 2, Line: 1, Date: 100, ProductID: 7, Status: 2}},
			table.Date, table.ProductID, table.Status))
		run.read("after a status change", readSums)
	})
}
