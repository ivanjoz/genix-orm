package scylla

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// End-to-end checks of the paths that run on records by pointer or by reflect.Type instead of a
// type parameter (see RATIONALE.md, "Type-erased read, merge and controller paths"): the controller,
// Merge, InsertUpdateInclude and QueryIndexGroup. Each passed unchanged against the typed code
// before the erasure, so they pin the behaviour it must keep.

// A table for the controller's runtime-typed paths: a slice column, so colbin and JSON have a
// complex field to round-trip, and SelfParse, so the reflect-built records prove they still reach
// the record's own methods.
type controllerITRecord struct {
	TableStruct[controllerITSchema, controllerITRecord]
	CompanyID int32    `db:"company_id"`
	ID        int32    `db:"id"`
	Name      string   `db:"name" json:"name"`
	NameUpper string   `db:"name_upper" json:"nameUpper"`
	Tags      []string `db:"tags" json:"tags"`
	Updated   int32    `json:"upd,omitempty"`
	Version   int16    `json:"ver,omitempty"`
}

func (e *controllerITRecord) SelfParse() { e.NameUpper = strings.ToUpper(e.Name) }

type controllerITSchema struct {
	TableStruct[controllerITSchema, controllerITRecord]
	CompanyID Col[*controllerITSchema, int32]
	ID        Col[*controllerITSchema, int32]
	Name      Col[*controllerITSchema, string]
	NameUpper Col[*controllerITSchema, string]
	Tags      Col[*controllerITSchema, []string]
	Updated   Col[*controllerITSchema, int32]
	Version   Col[*controllerITSchema, int16]
}

func (e controllerITSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10041,
		Name:      "controller_it_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
	}
}

func readControllerIT(t *testing.T, companyID int32) []controllerITRecord {
	t.Helper()
	records := []controllerITRecord{}
	query := Query(&records)
	query.CompanyID.Equals(companyID)
	if err := query.Exec(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(records, func(left, right controllerITRecord) int { return int(left.ID - right.ID) })
	return records
}

type indexGroupITRecord struct {
	TableStruct[indexGroupITSchema, indexGroupITRecord]
	CompanyID int32  `db:"company_id"`
	ID        int32  `db:"id"`
	Date      int16  `db:"date"`
	Name      string `db:"name"`
	Updated   int32  `json:"upd,omitempty"`
}

type indexGroupITSchema struct {
	TableStruct[indexGroupITSchema, indexGroupITRecord]
	CompanyID Col[*indexGroupITSchema, int32]
	ID        Col[*indexGroupITSchema, int32]
	Date      Col[*indexGroupITSchema, int16]
	Name      Col[*indexGroupITSchema, string]
	Updated   Col[*indexGroupITSchema, int32]
}

func (e indexGroupITSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10042,
		Name:      "index_group_it_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Keys: Cols(e.Date), UseIndexGroup: true}},
	}
}

// TestQueryIndexGroupOnScylla: groups come back in hash order with their records, a date with no
// records is absent, and a group the client holds at the current counter comes back empty.
func TestQueryIndexGroupOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()

	for _, statement := range []string{
		"DROP TABLE IF EXISTS genix_orm_it.index_group_it_records",
		"DROP TABLE IF EXISTS genix_orm_it.index_group_it_records__index_updated",
	} {
		if err := QueryExec(statement); err != nil {
			t.Fatal(err)
		}
	}
	DeployScylla(0, &ScyllaController[indexGroupITRecord, indexGroupITSchema]{
		Table:  MakeScyllaTable[indexGroupITRecord, indexGroupITSchema](),
		Schema: MakeSchema[indexGroupITRecord, indexGroupITSchema](),
	})
	if err := Insert(&[]indexGroupITRecord{
		{CompanyID: 8, ID: 1, Date: 100, Name: "a"},
		{CompanyID: 8, ID: 2, Date: 100, Name: "b"},
		{CompanyID: 8, ID: 3, Date: 101, Name: "c"},
	}); err != nil {
		t.Fatal(err)
	}

	readGroups := func(cachedGroups []RecordGroup[indexGroupITRecord]) []RecordGroup[indexGroupITRecord] {
		t.Helper()
		groups := []RecordGroup[indexGroupITRecord]{}
		query := QueryIndexGroup(&groups)
		query.CompanyID.Equals(8)
		query.Date.Between(100, 102)
		for _, cachedGroup := range cachedGroups {
			query.IncludeCachedGroup(cachedGroup.GroupHash, cachedGroup.UpdateCounter)
		}
		if err := query.Exec(); err != nil {
			t.Fatal(err)
		}
		return groups
	}
	recordCountByDate := func(groups []RecordGroup[indexGroupITRecord]) map[int64]int {
		counts := map[int64]int{}
		for _, group := range groups {
			counts[group.IndexGroupValues[0]] = len(group.Records)
		}
		return counts
	}

	groups := readGroups(nil)
	if len(groups) != 2 || groups[0].GroupHash > groups[1].GroupHash {
		t.Fatalf("groups = %+v", groups)
	}
	if counts := recordCountByDate(groups); counts[100] != 2 || counts[101] != 1 {
		t.Fatalf("records by date = %v", counts)
	}

	// The client holds date 100 at its current counter: it comes back without records.
	var heldGroup RecordGroup[indexGroupITRecord]
	for _, group := range groups {
		if group.IndexGroupValues[0] == 100 {
			heldGroup = group
		}
	}
	groups = readGroups([]RecordGroup[indexGroupITRecord]{heldGroup})
	if counts := recordCountByDate(groups); len(groups) != 2 || counts[100] != 0 || counts[101] != 1 {
		t.Fatalf("with a held group, records by date = %v", counts)
	}
	for _, group := range groups {
		if group.Records == nil {
			t.Fatalf("a group's Records must be [] rather than null: %+v", group)
		}
	}
}

// TestMergeOnScylla: a stored record the handler accepts is updated, one it rejects is left alone, a
// new one goes through onInsert, and what the insert stamped reaches the caller's slice.
func TestMergeOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()

	if err := QueryExec("DROP TABLE IF EXISTS genix_orm_it.controller_it_records"); err != nil {
		t.Fatal(err)
	}
	DeployScylla(0, &ScyllaController[controllerITRecord, controllerITSchema]{
		Table:  MakeScyllaTable[controllerITRecord, controllerITSchema](),
		Schema: MakeSchema[controllerITRecord, controllerITSchema](),
	})
	if err := Insert(&[]controllerITRecord{
		{CompanyID: 6, ID: 1, Name: "uno"},
		{CompanyID: 6, ID: 2, Name: "dos"},
	}); err != nil {
		t.Fatal(err)
	}

	merged := []controllerITRecord{
		{CompanyID: 6, ID: 1, Name: "uno bis"},
		{CompanyID: 6, ID: 2, Name: "dos bis"},
		{CompanyID: 6, ID: 3, Name: "tres"},
	}
	previousNames := []string{}
	err := Merge(&merged, nil,
		func(previous, current *controllerITRecord) bool {
			previousNames = append(previousNames, previous.Name)
			return current.ID == 1
		},
		func(record *controllerITRecord) { record.Tags = []string{"new"} })
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(previousNames)
	if !slices.Equal(previousNames, []string{"dos", "uno"}) {
		t.Fatalf("onUpdate saw previous names %v", previousNames)
	}
	if merged[2].Updated == 0 || merged[2].NameUpper != "TRES" || !slices.Equal(merged[2].Tags, []string{"new"}) {
		t.Fatalf("the insert did not propagate back: %+v", merged[2])
	}

	stored := readControllerIT(t, 6)
	if len(stored) != 3 || stored[0].Name != "uno bis" || stored[1].Name != "dos" || stored[2].NameUpper != "TRES" {
		t.Fatalf("after Merge: %+v", stored)
	}
}

// TestInsertUpdateIncludeOnScylla: the classifier splits one payload into an insert and an update
// that writes only the included column.
func TestInsertUpdateIncludeOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()

	if err := QueryExec("DROP TABLE IF EXISTS genix_orm_it.controller_it_records"); err != nil {
		t.Fatal(err)
	}
	DeployScylla(0, &ScyllaController[controllerITRecord, controllerITSchema]{
		Table:  MakeScyllaTable[controllerITRecord, controllerITSchema](),
		Schema: MakeSchema[controllerITRecord, controllerITSchema](),
	})
	if err := Insert(&[]controllerITRecord{{CompanyID: 4, ID: 1, Name: "uno", Tags: []string{"a"}}}); err != nil {
		t.Fatal(err)
	}

	table := TableOf[controllerITRecord]()
	err := InsertUpdateInclude(&[]controllerITRecord{
		{CompanyID: 4, ID: 1, Name: "uno bis", Tags: []string{"ignored"}},
		{CompanyID: 4, ID: 2, Name: "dos", Tags: []string{"b"}},
	}, func(record *controllerITRecord) bool { return record.ID == 2 }, []Coln{table.Name})
	if err != nil {
		t.Fatal(err)
	}
	stored := readControllerIT(t, 4)
	if len(stored) != 2 || stored[0].Name != "uno bis" || !slices.Equal(stored[0].Tags, []string{"a"}) ||
		stored[1].NameUpper != "DOS" || !slices.Equal(stored[1].Tags, []string{"b"}) {
		t.Fatalf("after InsertUpdateInclude: %+v", stored)
	}
}

// TestControllerPathsOnScylla runs every runtime-typed controller body against a real node: JSON
// insert, update and query, GetRecords, the colbin export and its restore, and DescribeTable.
func TestControllerPathsOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()

	if err := QueryExec("DROP TABLE IF EXISTS genix_orm_it.controller_it_records"); err != nil {
		t.Fatal(err)
	}
	controller := &ScyllaController[controllerITRecord, controllerITSchema]{
		Table:  MakeScyllaTable[controllerITRecord, controllerITSchema](),
		Schema: MakeSchema[controllerITRecord, controllerITSchema](),
	}
	DeployScylla(0, controller)

	// JSON insert runs SelfParse and stamps the managed columns.
	inserted, err := controller.InsertRecordsJSON([]byte(`[
		{"CompanyID":5,"ID":1,"name":"uno","tags":["a","b"]},
		{"CompanyID":5,"ID":2,"name":"dos"},
		{"CompanyID":5,"ID":3,"name":"tres","tags":["c"]}]`), nil)
	if err != nil || inserted != 3 {
		t.Fatalf("InsertRecordsJSON = %v, %v", inserted, err)
	}
	stored := readControllerIT(t, 5)
	if len(stored) != 3 || stored[0].NameUpper != "UNO" || !slices.Equal(stored[0].Tags, []string{"a", "b"}) ||
		stored[0].Updated == 0 {
		t.Fatalf("after JSON insert: %+v", stored)
	}

	// JSON update writes only the included column.
	updated, err := controller.UpdateRecordsJSON([]byte(`[{"CompanyID":5,"ID":2,"name":"ignored","tags":["z"]}]`), []string{"tags"})
	if err != nil || updated != 1 {
		t.Fatalf("UpdateRecordsJSON = %v, %v", updated, err)
	}
	stored = readControllerIT(t, 5)
	if stored[1].Name != "dos" || !slices.Equal(stored[1].Tags, []string{"z"}) {
		t.Fatalf("after JSON update: %+v", stored[1])
	}
	if _, err := controller.UpdateRecordsJSON([]byte(`[{"CompanyID":5,"ID":2}]`), nil); err == nil {
		t.Fatal("an update with no columns must be refused")
	}

	// JSON query: filters, projection, order and limit, rendered with the record's json tags.
	payload, count, err := controller.QueryRecordsJSON(db.QuerySpec{
		Filters:   []db.FilterSpec{{Column: "company_id", Operator: "=", Value: float64(5)}, {Column: "id", Operator: ">=", Value: float64(2)}},
		Columns:   []string{"id", "name"},
		OrderDesc: true,
		Limit:     1,
	})
	if err != nil || count != 1 {
		t.Fatalf("QueryRecordsJSON = %s, %v, %v", payload, count, err)
	}
	queried := []controllerITRecord{}
	if err := json.Unmarshal(payload, &queried); err != nil || queried[0].ID != 3 || queried[0].Name != "tres" || queried[0].Tags != nil {
		t.Fatalf("QueryRecordsJSON payload = %s, err %v", payload, err)
	}
	if payload, count, err := controller.QueryRecordsJSON(db.QuerySpec{
		Filters: []db.FilterSpec{{Column: "company_id", Operator: "=", Value: float64(99)}},
	}); err != nil || count != 0 || string(payload) != "[]" {
		t.Fatalf("an empty read must render [], got %s, %v, %v", payload, count, err)
	}

	// GetRecords hands back record values.
	if records := controller.GetRecords(5, 0, nil); len(records) != 3 {
		t.Fatalf("GetRecords = %+v", records)
	} else if _, isRecord := records[0].(controllerITRecord); !isRecord {
		t.Fatalf("GetRecords element is %T", records[0])
	}
	if encoded, err := controller.GetRecordsGob(5, 0, nil); err != nil || len(encoded) == 0 {
		t.Fatalf("GetRecordsGob = %v bytes, %v", len(encoded), err)
	}

	// Colbin export in batches of 2, then a restore that wipes the partition with the first batch.
	batches := [][]byte{}
	exported, err := controller.ExportRecordsColbin(5, 2, func(encoded []byte, rowsCount int32) error {
		batches = append(batches, encoded)
		return nil
	})
	if err != nil || exported != 3 || len(batches) != 2 {
		t.Fatalf("ExportRecordsColbin = %v rows in %v batches, %v", exported, len(batches), err)
	}
	if err := QueryExec("INSERT INTO genix_orm_it.controller_it_records (company_id, id, name) VALUES (5, 9, 'stray')"); err != nil {
		t.Fatal(err)
	}
	restored := 0
	for batchIndex, encoded := range batches {
		count, err := controller.RestoreRecordsColbin(5, encoded, batchIndex == 0)
		if err != nil {
			t.Fatal(err)
		}
		restored += count
	}
	stored = readControllerIT(t, 5)
	if restored != 3 || len(stored) != 3 || stored[0].CompanyID != 5 || !slices.Equal(stored[0].Tags, []string{"a", "b"}) ||
		stored[2].NameUpper != "TRES" {
		t.Fatalf("after restore (%v): %+v", restored, stored)
	}

	// ReloadRecords writes the partition back through the managed insert.
	if err := controller.ReloadRecords(5); err != nil {
		t.Fatal(err)
	}
	if reloaded := readControllerIT(t, 5); len(reloaded) != 3 || reloaded[2].NameUpper != "TRES" {
		t.Fatalf("after reload: %+v", reloaded)
	}

	description := controller.DescribeTable()
	if description.Name != "controller_it_records" || description.Partition != "company_id" ||
		!slices.Equal(description.Keys, []string{"id"}) || len(description.Columns) == 0 {
		t.Fatalf("DescribeTable = %+v", description)
	}
}
