package conformance

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Cache groups over a slice, shaped as SaleOrder's product groups: a computed slice (the product of
// every line) grouped with the date, and with the date and the client.

type sliceGroupRecord struct {
	TableStruct[sliceGroupSchema, sliceGroupRecord]
	CompanyID int32   `db:"company_id"`
	ID        int32   `db:"id"`
	Date      int16   `db:"date"`
	ClientID  int32   `db:"client_id"`
	Lines     []int32 `db:"lines"`
	Updated   int32   `json:"upd,omitempty"`
}

// LineProductIDs is the product of every line, in line order: the computed slice the groups key on.
func (record *sliceGroupRecord) LineProductIDs() []int32 { return record.Lines }

type sliceGroupSchema struct {
	TableStruct[sliceGroupSchema, sliceGroupRecord]
	CompanyID      db.Col[*sliceGroupSchema, int32]
	ID             db.Col[*sliceGroupSchema, int32]
	Date           db.Col[*sliceGroupSchema, int16]
	ClientID       db.Col[*sliceGroupSchema, int32]
	Lines          db.Col[*sliceGroupSchema, []int32]
	LineProductIDs db.Col[*sliceGroupSchema, []int32]
	Updated        db.Col[*sliceGroupSchema, int32]
}

func (e sliceGroupSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12009,
		Name:      "conformance_slice_group_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		Computed: []db.ComputedCol{
			{Col: e.LineProductIDs, From: db.Cols(e.Lines), Value: db.Compute((*sliceGroupRecord).LineProductIDs)},
		},
		Indexes: []db.Index{
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.ClientID.Size(26), e.LineProductIDs.Size(23))},
			{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date, e.LineProductIDs)},
		},
	}
}

// TestQueryIndexGroupOverSlice: a group of (date, product) holds every record with a line of the
// product that day, one of (date, client, product) only the client's; a record leaves the groups of
// the client and the products it no longer has.
func TestQueryIndexGroupOverSlice(t *testing.T) {
	runConformance(t, []db.Controller{makeController[sliceGroupRecord, sliceGroupSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]sliceGroupRecord{
			{CompanyID: 8, ID: 1, Date: 100, ClientID: 5, Lines: []int32{7, 8}},
			{CompanyID: 8, ID: 2, Date: 100, ClientID: 6, Lines: []int32{7}},
			{CompanyID: 8, ID: 3, Date: 101, ClientID: 5, Lines: []int32{8, 9}},
			{CompanyID: 9, ID: 4, Date: 100, ClientID: 5, Lines: []int32{7}},
		}))
		readGroups := func(clientID int32, productID int32) func() (any, error) {
			return func() (any, error) {
				groups := []db.RecordGroup[sliceGroupRecord]{}
				query := db.QueryIndexGroup(&groups)
				query.CompanyID.Equals(8)
				query.Date.Between(100, 102)
				if clientID > 0 {
					query.ClientID.Equals(clientID)
				}
				query.LineProductIDs.Contains(productID)
				err := query.Exec()
				for _, group := range groups {
					sortedByID(group.Records, func(record sliceGroupRecord) int64 { return int64(record.ID) })
				}
				return groups, err
			}
		}

		run.setClock(db.IndexGroupSettleTicks)
		run.read("product 7", readGroups(0, 7))
		run.read("product 8", readGroups(0, 8))
		run.read("client 5, product 8", readGroups(5, 8))

		// Record 1 moves to client 6 and drops product 8; record 3 moves to date 102. A partial update
		// carries every source of the groups it touches.
		run.setClock(db.IndexGroupSettleTicks + 10)
		table := db.TableOf[sliceGroupRecord]()
		run.must("Update client and lines", db.Update(&[]sliceGroupRecord{{CompanyID: 8, ID: 1, Date: 100, ClientID: 6, Lines: []int32{7}}}, table.ClientID, table.Lines))
		run.must("Update date", db.Update(&[]sliceGroupRecord{{CompanyID: 8, ID: 3, Date: 102, ClientID: 5, Lines: []int32{8, 9}}}, table.Date))

		run.setClock(2*db.IndexGroupSettleTicks + 10)
		run.read("after the updates: product 8", readGroups(0, 8))
		run.read("after the updates: client 6, product 7", readGroups(6, 7))
		run.read("after the updates: client 5, product 8", readGroups(5, 8))
	})
}
