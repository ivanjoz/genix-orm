package conformance

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Seeded from scylla/erased_paths_integration_test.go: the controller's runtime-typed paths, Merge
// and InsertUpdateInclude.

// A slice column, so colbin and JSON have a complex field to round-trip, and SelfParse, so the
// reflect-built records prove they still reach the record's own methods.
type controllerRecord struct {
	TableStruct[controllerSchema, controllerRecord]
	CompanyID int32    `db:"company_id"`
	ID        int32    `db:"id"`
	Name      string   `db:"name" json:"name"`
	NameUpper string   `db:"name_upper" json:"nameUpper"`
	Tags      []string `db:"tags" json:"tags"`
	Updated   int32    `json:"upd,omitempty"`
	Version   int16    `json:"ver,omitempty"`
}

func (e *controllerRecord) SelfParse() { e.NameUpper = strings.ToUpper(e.Name) }

type controllerSchema struct {
	TableStruct[controllerSchema, controllerRecord]
	CompanyID db.Col[*controllerSchema, int32]
	ID        db.Col[*controllerSchema, int32]
	Name      db.Col[*controllerSchema, string]
	NameUpper db.Col[*controllerSchema, string]
	Tags      db.Col[*controllerSchema, []string]
	Updated   db.Col[*controllerSchema, int32]
	Version   db.Col[*controllerSchema, int16]
}

func (e controllerSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:        12001,
		Name:      "conformance_controller_records",
		Partition: e.CompanyID,
		Keys:      db.Cols(e.ID),
	}
}

func readControllerRecords(companyID int32) (any, error) {
	records := []controllerRecord{}
	err := db.Query(&records).CompanyID.Equals(companyID).Exec()
	slices.SortFunc(records, func(left, right controllerRecord) int { return int(left.ID - right.ID) })
	return records, err
}

// TestControllerPaths: JSON insert, update and query, GetRecords, the colbin export and its restore,
// ReloadRecords and DescribeTable.
func TestControllerPaths(t *testing.T) {
	controller := makeController[controllerRecord, controllerSchema]()
	runConformance(t, []db.Controller{controller}, func(run *conformanceRun) {
		readPartition5 := func() (any, error) { return readControllerRecords(5) }

		// JSON insert runs SelfParse and stamps the managed columns.
		inserted, err := controller.InsertRecordsJSON([]byte(`[
			{"CompanyID":5,"ID":1,"name":"uno","tags":["a","b"]},
			{"CompanyID":5,"ID":2,"name":"dos"},
			{"CompanyID":5,"ID":3,"name":"tres","tags":["c"]}]`), nil)
		run.must("InsertRecordsJSON", err)
		run.observe("InsertRecordsJSON count", inserted)
		run.read("after the JSON insert", readPartition5)

		// JSON update writes only the included column; an update naming no column is refused.
		updated, err := controller.UpdateRecordsJSON([]byte(`[{"CompanyID":5,"ID":2,"name":"ignored","tags":["z"]}]`), []string{"tags"})
		run.must("UpdateRecordsJSON", err)
		run.observe("UpdateRecordsJSON count", updated)
		run.read("after the JSON update", readPartition5)
		_, err = controller.UpdateRecordsJSON([]byte(`[{"CompanyID":5,"ID":2}]`), nil)
		run.observe("an update with no columns is refused", err != nil)

		// JSON query: filters, projection, order and limit, rendered with the record's json tags.
		for _, spec := range []db.QuerySpec{
			{
				Filters: []db.FilterSpec{{Column: "company_id", Operator: "=", Value: float64(5)}, {Column: "id", Operator: ">=", Value: float64(2)}},
				Columns: []string{"id", "name"}, OrderDesc: true, Limit: 1,
			},
			{Filters: []db.FilterSpec{{Column: "company_id", Operator: "=", Value: float64(99)}}},
		} {
			payload, count, err := controller.QueryRecordsJSON(spec)
			run.must("QueryRecordsJSON", err)
			run.observe("QueryRecordsJSON", map[string]any{"count": count, "payload": json.RawMessage(payload)})
		}

		// GetRecords hands back record values, GetRecordsGob their encoding.
		records := controller.GetRecords(5, 0, nil)
		elementTypes := []string{}
		for _, record := range records {
			_, isRecord := record.(controllerRecord)
			elementTypes = append(elementTypes, map[bool]string{true: "controllerRecord", false: "other"}[isRecord])
		}
		run.observe("GetRecords element types", elementTypes)
		encoded, err := controller.GetRecordsGob(5, 0, nil)
		run.must("GetRecordsGob", err)
		run.observe("GetRecordsGob is not empty", len(encoded) > 0)

		// Colbin export in batches of 2, then a restore that wipes the partition with the first batch:
		// the stray record written in between is gone.
		batches := [][]byte{}
		exported, err := controller.ExportRecordsColbin(5, 2, func(encoded []byte, rowsCount int32) error {
			batches = append(batches, encoded)
			return nil
		})
		run.must("ExportRecordsColbin", err)
		run.observe("ExportRecordsColbin rows and batches", []int{int(exported), len(batches)})
		run.must("insert the stray record", db.Upsert(&[]controllerRecord{{CompanyID: 5, ID: 9, Name: "stray"}}))
		restored := 0
		for batchIndex, encoded := range batches {
			count, err := controller.RestoreRecordsColbin(5, encoded, batchIndex == 0)
			run.must("RestoreRecordsColbin", err)
			restored += count
		}
		run.observe("RestoreRecordsColbin count", restored)
		run.read("after the restore", readPartition5)

		// ReloadRecords writes the partition back through the managed insert.
		run.setClock(10)
		run.must("ReloadRecords", controller.ReloadRecords(5))
		run.read("after the reload", readPartition5)

		description := controller.DescribeTable()
		run.observe("DescribeTable", map[string]any{
			"name": description.Name, "id": description.ID, "partition": description.Partition,
			"keys": description.Keys, "columns": description.Columns, "cacheByIDs": description.CacheByIDs,
		})
	})
}

// TestMerge: a stored record the handler accepts is updated, one it rejects is left alone, a new one
// goes through onInsert, and what the insert stamped reaches the caller's slice.
func TestMerge(t *testing.T) {
	runConformance(t, []db.Controller{makeController[controllerRecord, controllerSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]controllerRecord{
			{CompanyID: 6, ID: 1, Name: "uno"},
			{CompanyID: 6, ID: 2, Name: "dos"},
		}))

		run.setClock(10)
		merged := []controllerRecord{
			{CompanyID: 6, ID: 1, Name: "uno bis"},
			{CompanyID: 6, ID: 2, Name: "dos bis"},
			{CompanyID: 6, ID: 3, Name: "tres"},
		}
		previousNames := []string{}
		run.must("Merge", db.Merge(&merged, nil,
			func(previous, current *controllerRecord) bool {
				previousNames = append(previousNames, previous.Name)
				return current.ID == 1
			},
			func(record *controllerRecord) { record.Tags = []string{"new"} }))
		slices.Sort(previousNames)
		run.observe("onUpdate saw the previous names", previousNames)
		run.observe("the caller's slice after Merge", merged)
		run.read("stored after Merge", func() (any, error) { return readControllerRecords(6) })
	})
}

// TestInsertUpdateInclude: the classifier splits one payload into an insert and an update that
// writes only the included column.
func TestInsertUpdateInclude(t *testing.T) {
	runConformance(t, []db.Controller{makeController[controllerRecord, controllerSchema]()}, func(run *conformanceRun) {
		run.must("Insert", db.Upsert(&[]controllerRecord{{CompanyID: 4, ID: 1, Name: "uno", Tags: []string{"a"}}}))

		run.setClock(10)
		table := db.TableOf[controllerRecord]()
		run.must("InsertUpdateInclude", db.InsertUpdateInclude(&[]controllerRecord{
			{CompanyID: 4, ID: 1, Name: "uno bis", Tags: []string{"ignored"}},
			{CompanyID: 4, ID: 2, Name: "dos", Tags: []string{"b"}},
		}, func(record *controllerRecord) bool { return record.ID == 2 }, []db.Coln{table.Name}))
		run.read("stored after InsertUpdateInclude", func() (any, error) { return readControllerRecords(4) })
	})
}
