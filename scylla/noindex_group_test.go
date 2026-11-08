package scylla

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/ivanjoz/genix-orm/db"
)

// ledgerRecord is shaped like a stock ledger: an ID packing (date, warehouse_id, autoincrement), a
// FromKey group on date and a no-index group on (date, kind) that reads through it.
type ledgerRecord struct {
	TableStruct[ledgerSchema, ledgerRecord]
	CompanyID   int32 `db:"empresa_id"`
	ID          int64 `db:"id"`
	Date        int16 `db:"date"`
	WarehouseID int32 `db:"warehouse_id"`
	Kind        int16 `db:"kind"`
}

type ledgerSchema struct {
	TableStruct[ledgerSchema, ledgerRecord]
	CompanyID   Col[*ledgerSchema, int32]
	ID          Col[*ledgerSchema, int64]
	Date        Col[*ledgerSchema, int16]
	WarehouseID Col[*ledgerSchema, int32]
	Kind        Col[*ledgerSchema, int16]
}

func (e ledgerSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:                10060,
		Namespace:         "test_keyspace",
		Name:              "ledger_records",
		Partition:         e.CompanyID,
		Keys:              Cols(e.ID),
		KeyIntPacking:     Cols(e.Date.Size(16), e.WarehouseID.Size(16), e.Autoincrement(8)),
		AutoincrementPart: e.Date,
		Indexes: []Index{
			{Type: TypeCacheGroupFromKey, Keys: Cols(e.Date)},
			{Type: TypeCacheGroupNoIndex, Keys: Cols(e.Date, e.Kind)},
		},
	}
}

func ledgerGroupNamed(t *testing.T, scyllaTable ScyllaTable, name string) db.IndexGroupInfo {
	for _, indexGroup := range scyllaTable.IndexGroups {
		if indexGroup.Name == name {
			return indexGroup
		}
	}
	t.Fatalf("no group %q in %+v", name, scyllaTable.IndexGroups)
	return db.IndexGroupInfo{}
}

func TestNoIndexGroupIsPlannedAndReadsItsBaseKeyRange(t *testing.T) {
	scyllaTable := MakeScyllaTable[ledgerRecord, ledgerSchema]()
	tableInfo := &TableInfo{Statements: []ColumnStatement{
		{Col: "empresa_id", Operator: "=", Value: int32(7)},
		{Col: "date", Operator: "BETWEEN", From: []ColumnStatement{{Col: "date", Value: int16(20000)}}, To: []ColumnStatement{{Col: "date", Value: int16(20001)}}},
		{Col: "kind", Operator: "=", Value: int16(8)},
	}}

	queryPlan, err := scyllaTable.PlanIndexGroupRead(tableInfo)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if queryPlan.IndexGroup.Name != "date_kind" || queryPlan.IndexGroup.BaseGroup == nil || queryPlan.IndexGroup.BaseGroup.Name != "date" {
		t.Fatalf("planned %q with base %+v", queryPlan.IndexGroup.Name, queryPlan.IndexGroup.BaseGroup)
	}

	// The fetch is the base group's primary-key range: no secondary index, no hash column.
	fetchState := db.IndexGroupFetchState{HashValue: db.HashInt64(20000, 8), IndexGroupValues: []int64{20000, 8}}
	queryStr, queryValues := buildIndexGroupFetchQuery(scyllaTable, queryPlan, fetchState, []string{"*"})
	if !strings.HasSuffix(queryStr, "WHERE empresa_id = ? AND id >= ? AND id < ?") || len(queryValues) != 3 {
		t.Fatalf("query = %s %v", queryStr, queryValues)
	}
}

func TestNoIndexGroupCarriesItsBaseGroupLastWrite(t *testing.T) {
	scyllaTable := MakeScyllaTable[ledgerRecord, ledgerSchema]()
	queryPlan := &db.IndexGroupReadPlan{
		PartitionValue: 7,
		IndexGroup:     ledgerGroupNamed(t, scyllaTable, "date_kind"),
		HashGroups: []db.IndexGroupHash{
			{HashValue: db.HashInt64(20000, 8), IndexGroupValues: []int64{20000, 8}},
			{HashValue: db.HashInt64(20000, 3), IndexGroupValues: []int64{20000, 3}},
			{HashValue: db.HashInt64(20001, 8), IndexGroupValues: []int64{20001, 8}},
		},
	}

	originalLoader := loadIndexGroupFreshnessRows
	defer func() { loadIndexGroupFreshnessRows = originalLoader }()
	var probedGroup string
	loadIndexGroupFreshnessRows = func(_ ScyllaTable, indexGroup db.IndexGroupInfo, _ int32, hashValues []int32) (map[int32]int32, error) {
		probedGroup = indexGroup.Name
		// Only day 20000 has records on the server.
		return map[int32]int32{db.HashInt64(20000): 555}, nil
	}

	freshness, err := loadPlannedGroupFreshness(scyllaTable, queryPlan, queryPlan.HashValues())
	if err != nil {
		t.Fatal(err)
	}
	if probedGroup != "date" {
		t.Fatalf("probed the marks of %q, want the base group", probedGroup)
	}
	if freshness[db.HashInt64(20000, 8)] != 555 || freshness[db.HashInt64(20000, 3)] != 555 {
		t.Fatalf("both kinds of day 20000 carry its last_write: %v", freshness)
	}
	if _, isOnServer := freshness[db.HashInt64(20001, 8)]; isOnServer {
		t.Fatalf("a day absent on the server must leave its groups absent: %v", freshness)
	}
}

// With kind IN (3, 8) each (date, kind) group reads the same day range: each must keep only its kind.
func TestNoIndexGroupPostFilterPinsTheGroupsOwnValues(t *testing.T) {
	scyllaTable := MakeScyllaTable[ledgerRecord, ledgerSchema]()
	queryStatements := []ColumnStatement{{Col: "kind", Operator: "IN", Values: []any{int16(3), int16(8)}}}
	groupStatements := db.GroupPostFilterStatements(ledgerGroupNamed(t, scyllaTable, "date_kind"),
		db.IndexGroupFetchState{IndexGroupValues: []int64{20000, 8}}, queryStatements)

	kindThree := ledgerRecord{Date: 20000, Kind: 3}
	kindEight := ledgerRecord{Date: 20000, Kind: 8}
	if recordMatchesPostFilter(unsafe.Pointer(&kindThree), groupStatements, scyllaTable) {
		t.Fatal("the kind-8 group kept a kind-3 record")
	}
	if !recordMatchesPostFilter(unsafe.Pointer(&kindEight), groupStatements, scyllaTable) {
		t.Fatal("the kind-8 group dropped its own record")
	}
	if len(queryStatements) != 1 {
		t.Fatal("the query's statements must not be modified")
	}
}

func TestNoIndexGroupWritesNoMarks(t *testing.T) {
	scyllaTable := MakeScyllaTable[ledgerRecord, ledgerSchema]()
	record := ledgerRecord{CompanyID: 7, ID: 1, Date: 20000, WarehouseID: 2, Kind: 8}
	var marks []indexUpdatedRow
	appendIndexUpdatedRowsForRecord(unsafe.Pointer(&record),&scyllaTable, 7, 0, map[string]indexUpdatedRow{}, &marks)

	baseGroup := ledgerGroupNamed(t, scyllaTable, "date")
	if len(marks) != 1 || marks[0].indexID != baseGroup.IndexID || marks[0].indexHash != db.HashInt64(20000) {
		t.Fatalf("marks = %+v, want only the base group's", marks)
	}
	for _, column := range indexGroupStoredColumns(&scyllaTable) {
		if column.GetName() == "kind" {
			t.Fatal("a write must not pre-read a column only a no-index group names")
		}
	}
}
