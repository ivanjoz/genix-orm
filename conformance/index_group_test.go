package conformance

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Seeded from TestQueryIndexGroupOnScylla.

type indexGroupRecord struct {
	TableStruct[indexGroupSchema, indexGroupRecord]
	CompanyID int32  `db:"company_id"`
	ID        int32  `db:"id"`
	Date      int16  `db:"date"`
	Name      string `db:"name"`
	Updated   int32  `json:"upd,omitempty"`
}

type indexGroupSchema struct {
	TableStruct[indexGroupSchema, indexGroupRecord]
	CompanyID db.Col[*indexGroupSchema, int32]
	ID        db.Col[*indexGroupSchema, int32]
	Date      db.Col[*indexGroupSchema, int16]
	Name      db.Col[*indexGroupSchema, string]
	Updated   db.Col[*indexGroupSchema, int32]
}

func (e indexGroupSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12002,
		Name:      "conformance_index_group_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		Indexes:   []db.Index{{Type: db.TypeCacheGroup, Keys: db.Cols(e.Date)}},
	}
}

// TestQueryIndexGroup: while the groups settle they come back labelled 0; once settled they come in
// hash order with their records, a date with no records is absent, and a group the client holds at
// its current label comes back empty.
func TestQueryIndexGroup(t *testing.T) {
	runConformance(t, []db.Controller{makeController[indexGroupRecord, indexGroupSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]indexGroupRecord{
			{CompanyID: 8, ID: 1, Date: 100, Name: "a"},
			{CompanyID: 8, ID: 2, Date: 100, Name: "b"},
			{CompanyID: 8, ID: 3, Date: 101, Name: "c"},
		}))
		readGroups := func(heldGroups ...db.RecordGroup[indexGroupRecord]) func() (any, error) {
			return func() (any, error) {
				groups := []db.RecordGroup[indexGroupRecord]{}
				query := db.QueryIndexGroup(&groups)
				query.CompanyID.Equals(8)
				query.Date.Between(100, 102)
				for _, heldGroup := range heldGroups {
					query.IncludeCachedGroup(heldGroup.GroupHash, heldGroup.LastWrite)
				}
				err := query.Exec()
				// Groups come in hash order (the contract); the records inside one do not.
				for _, group := range groups {
					sortedByID(group.Records, func(record indexGroupRecord) int64 { return int64(record.ID) })
				}
				return groups, err
			}
		}

		run.setClock(groupSettleTicks() - 1)
		run.read("still settling", readGroups())

		run.setClock(groupSettleTicks())
		settledGroups := []db.RecordGroup[indexGroupRecord]{}
		run.read("settled", func() (any, error) {
			groups, err := readGroups()()
			settledGroups = groups.([]db.RecordGroup[indexGroupRecord])
			return groups, err
		})

		// The client holds date 100 at its current label.
		heldGroups := []db.RecordGroup[indexGroupRecord]{}
		for _, group := range settledGroups {
			if group.IndexGroupValues[0] == 100 {
				heldGroups = append(heldGroups, group)
			}
		}
		run.read("holding date 100", readGroups(heldGroups...))
	})
}
