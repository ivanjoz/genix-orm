package conformance

import (
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// The docs/YDB_PLAN.md §15 rows the seeded tests do not reach: autoincrement keys (sequences and a
// packed key), and reads through each scalar and slice index kind.

// A key packing (date, warehouse_id, autoincrement), its counter per date, as the stock ledger.
type packedKeyRecord struct {
	TableStruct[packedKeySchema, packedKeyRecord]
	CompanyID   int32  `db:"company_id"`
	ID          int64  `db:"id"`
	Date        int16  `db:"date"`
	WarehouseID int32  `db:"warehouse_id"`
	Kind        int16  `db:"kind"`
	Name        string `db:"name"`
}

type packedKeySchema struct {
	TableStruct[packedKeySchema, packedKeyRecord]
	CompanyID   db.Col[*packedKeySchema, int32]
	ID          db.Col[*packedKeySchema, int64]
	Date        db.Col[*packedKeySchema, int16]
	WarehouseID db.Col[*packedKeySchema, int32]
	Kind        db.Col[*packedKeySchema, int16]
	Name        db.Col[*packedKeySchema, string]
}

func (e packedKeySchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12005,
		Name:      "conformance_packed_key_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
		// No random suffix bits: the golden files hold the generated keys.
		KeyIntPacking:     db.Cols(e.Date.Size(16), e.WarehouseID.Size(16), e.Autoincrement(0)),
		AutoincrementPart: e.Date,
		Indexes:           []db.Index{{Type: db.TypeLocalIndex, Keys: db.Cols(e.Kind)}},
	}
}

// One table per scalar and slice index kind, its key from the partition's sequence.
type indexedRecord struct {
	TableStruct[indexedSchema, indexedRecord]
	CompanyID int32   `db:"company_id"`
	ID        int32   `db:"id"`
	Email     string  `db:"email"`
	ClientID  int32   `db:"client_id"`
	Tags      []int32 `db:"tags,list"` // an index needs an unfrozen collection on Scylla
	Status    int8    `db:"status"`
	Date      int16   `db:"date"`
	Name      string  `db:"name"`
	Updated   int32   `json:"upd,omitempty"`
}

type indexedSchema struct {
	TableStruct[indexedSchema, indexedRecord]
	CompanyID db.Col[*indexedSchema, int32]
	ID        db.Col[*indexedSchema, int32]
	Email     db.Col[*indexedSchema, string]
	ClientID  db.Col[*indexedSchema, int32]
	Tags      db.Col[*indexedSchema, []int32]
	Status    db.Col[*indexedSchema, int8]
	Date      db.Col[*indexedSchema, int16]
	Name      db.Col[*indexedSchema, string]
	Updated   db.Col[*indexedSchema, int32]
}

func (e indexedSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:           12006,
		Name:         "conformance_indexed_records",
		Partition:    e.CompanyID,
		Keys:         db.Cols(e.ID.Autoincrement(0)),
		UseSequences: true,
		Indexes: []db.Index{
			{Type: db.TypeGlobalIndex, Keys: db.Cols(e.Email)},
			{Type: db.TypeLocalIndex, Keys: db.Cols(e.ClientID)},
			{Type: db.TypeGlobalIndex, Keys: db.Cols(e.Tags)},
			{Type: db.TypeView, Keys: db.Cols(e.Status, e.Date)},
		},
	}
}

func packedKeyRecordID(record packedKeyRecord) int64 { return record.ID }

// indexedRecordID orders by (company, ID): a global index read spans companies, and IDs repeat.
func indexedRecordID(record indexedRecord) int64 {
	return int64(record.CompanyID)<<32 | int64(record.ID)
}

// TestAutoincrementKeys: a packed key takes its counter per AutoincrementPart value, a sequence key
// one per partition; both start from an empty keyspace, so the assigned keys are stable.
func TestAutoincrementKeys(t *testing.T) {
	controllers := []db.Controller{makeController[packedKeyRecord, packedKeySchema](), makeController[indexedRecord, indexedSchema]()}
	runConformance(t, controllers, func(run *conformanceRun) {
		packedRecords := []packedKeyRecord{
			{CompanyID: 3, Date: 20000, WarehouseID: 1, Kind: 1, Name: "a"},
			{CompanyID: 3, Date: 20000, WarehouseID: 2, Kind: 2, Name: "b"},
			{CompanyID: 3, Date: 20001, WarehouseID: 1, Kind: 1, Name: "c"},
		}
		run.must("Insert packed", db.Insert(&packedRecords))
		run.observe("packed keys assigned", packedRecords)

		sequenceRecords := []indexedRecord{{CompanyID: 3, Name: "uno"}, {CompanyID: 3, Name: "dos"}, {CompanyID: 4, Name: "otro"}}
		run.must("Insert sequenced", db.Insert(&sequenceRecords))
		run.must("Insert sequenced", db.Insert(&[]indexedRecord{{CompanyID: 3, Name: "tres"}}))
		run.read("sequence keys of company 3", func() (any, error) {
			records := []indexedRecord{}
			err := db.Query(&records).CompanyID.Equals(3).Exec()
			return sortedByID(records, indexedRecordID), err
		})

		run.read("packed key by ID", func() (any, error) {
			records := []packedKeyRecord{}
			return records, db.Query(&records).CompanyID.Equals(3).ID.Equals(packedRecords[1].ID).Exec()
		})
		run.read("packed key by local index on kind", func() (any, error) {
			records := []packedKeyRecord{}
			err := db.Query(&records).CompanyID.Equals(3).Kind.Equals(1).Exec()
			return sortedByID(records, packedKeyRecordID), err
		})
	})
}

// TestIndexReads: one read through each index kind, plus a read in descending key order with a
// limit.
func TestIndexReads(t *testing.T) {
	runConformance(t, []db.Controller{makeController[indexedRecord, indexedSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Insert(&[]indexedRecord{
			{CompanyID: 2, ID: 1, Email: "a@x.pe", ClientID: 10, Tags: []int32{1, 2}, Status: 1, Date: 100, Name: "uno"},
			{CompanyID: 2, ID: 2, Email: "b@x.pe", ClientID: 10, Tags: []int32{2}, Status: 1, Date: 101, Name: "dos"},
			{CompanyID: 2, ID: 3, Email: "c@x.pe", ClientID: 11, Tags: []int32{3}, Status: 0, Date: 101, Name: "tres"},
			{CompanyID: 2, ID: 4, Email: "d@x.pe", ClientID: 11, Tags: nil, Status: 1, Date: 103, Name: "cuatro"},
			{CompanyID: 5, ID: 1, Email: "a@x.pe", ClientID: 10, Tags: []int32{2}, Status: 1, Date: 100, Name: "otra empresa"},
		}))
		readIndexed := func(build func(query *indexedSchema)) func() (any, error) {
			return func() (any, error) {
				records := []indexedRecord{}
				query := db.Query(&records)
				build(query)
				err := query.Exec()
				return sortedByID(records, indexedRecordID), err
			}
		}

		run.read("global index on email", readIndexed(func(query *indexedSchema) { query.Email.Equals("a@x.pe") }))
		run.read("local index on client_id", readIndexed(func(query *indexedSchema) { query.CompanyID.Equals(2).ClientID.Equals(10) }))
		run.read("global index on the tags slice", readIndexed(func(query *indexedSchema) { query.Tags.Contains(2) }))
		// The hash view is read by equality only: a range on one of its columns is planned onto it but
		// read as a single hash (a Scylla bug, not yet fixed), which no driver should reproduce.
		run.read("view on status and date", readIndexed(func(query *indexedSchema) {
			query.CompanyID.Equals(2).Status.Equals(1).Date.Equals(101)
		}))
		run.read("descending key order with a limit", func() (any, error) {
			records := []indexedRecord{}
			return records, db.Query(&records).CompanyID.Equals(2).OrderDesc().Limit(2).Exec()
		})

		// An update moves a record between index entries: the old ones must stop answering. The
		// view's columns are written too: a composite view's columns are updated together.
		run.setClock(10)
		table := db.TableOf[indexedRecord]()
		run.must("Update", db.Update(&[]indexedRecord{{CompanyID: 2, ID: 1, Email: "z@x.pe", Tags: []int32{3}, Status: 1, Date: 102}},
			table.Email, table.Tags, table.Status, table.Date))
		run.read("global index on email after the update", readIndexed(func(query *indexedSchema) { query.Email.Equals("a@x.pe") }))
		run.read("global index on the tags slice after the update", readIndexed(func(query *indexedSchema) { query.Tags.Contains(3) }))
		run.read("view on status and date after the update", readIndexed(func(query *indexedSchema) {
			query.CompanyID.Equals(2).Status.Equals(1).Date.Equals(102)
		}))
		run.read("view on the old status and date after the update", readIndexed(func(query *indexedSchema) {
			query.CompanyID.Equals(2).Status.Equals(1).Date.Equals(100)
		}))
	})
}
