package scylla

import (
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db/cache"
)

// The delta fixture mirrors warehouse_product_stock: one index pinning the warehouse, one over the
// whole partition.
type deltaStockRecord struct {
	TableStruct[deltaStockSchema, deltaStockRecord]
	CompanyID   int32   `db:"company_id"`
	ID          int32   `db:"id"`
	WarehouseID int32   `db:"warehouse_id"`
	Status      int8    `db:"status"`
	Name        string  `db:"name"`
	Updated     int32   `json:"upd,omitempty"`
	Version     int16   `json:"ver,omitempty"`
	Metadata    *string `json:"mdt,omitempty"`
}

type deltaStockSchema struct {
	TableStruct[deltaStockSchema, deltaStockRecord]
	CompanyID   Col[*deltaStockSchema, int32]
	ID          Col[*deltaStockSchema, int32]
	WarehouseID Col[*deltaStockSchema, int32]
	Status      Col[*deltaStockSchema, int8]
	Name        Col[*deltaStockSchema, string]
	Updated     Col[*deltaStockSchema, int32]
	Version     Col[*deltaStockSchema, int16]
}

func (e deltaStockSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10028,
		Name:        "delta_stock_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(e.WarehouseID.Size(17), e.Status), DeltaReplaceThreshold: 3},
			{Type: TypeDelta, Keys: Cols(e.Status)},
		},
	}
}

func TestDeltaIndexCompilesASlimViewOutsideThePlanner(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	scyllaTable.Namespace = "genix_test"
	if len(scyllaTable.views) != 0 {
		t.Fatalf("expected the delta views out of the planner, got %d views", len(scyllaTable.views))
	}

	warehouseIndex, statusIndex := scyllaTable.deltaIndexes[0], scyllaTable.deltaIndexes[1]
	if warehouseIndex.tag != "warehouse_id_status" || warehouseIndex.pinned[0].GetName() != "warehouse_id" ||
		warehouseIndex.syncFilter.GetName() != "status" || !slices.Equal(warehouseIndex.pinnedSlotBits, []int64{17}) ||
		warehouseIndex.packedColumn.GetName() != "zz_dlt_warehouse_id" || warehouseIndex.replaceThreshold != 3 {
		t.Fatalf("unexpected warehouse index %+v", warehouseIndex)
	}
	if statusIndex.packedColumn != nil || statusIndex.replaceThreshold != cache.DefaultDeltaReplaceThreshold {
		t.Fatalf("expected the status index to key on updated itself, got %+v", statusIndex)
	}

	warehouseScript := deltaViewCreateScript(scyllaTable, warehouseIndex)
	for _, expected := range []string{
		"genix_test.delta_stock_records__dlt_warehouse_id_status_view",
		"SELECT company_id, id, updated, version, zz_dlt_warehouse_id, status FROM",
		"PRIMARY KEY ((company_id), zz_dlt_warehouse_id, id)",
		"CLUSTERING ORDER BY (zz_dlt_warehouse_id DESC, id ASC)",
	} {
		if !strings.Contains(warehouseScript, expected) {
			t.Fatalf("expected %q in:\n%s", expected, warehouseScript)
		}
	}
	if statusScript := deltaViewCreateScript(scyllaTable, statusIndex); !strings.Contains(statusScript, "PRIMARY KEY ((company_id), updated, id)") {
		t.Fatalf("expected the status view keyed on updated:\n%s", statusScript)
	}
	if !hasDeltaMoves(scyllaTable) || !strings.Contains(deltaMovesCreateScript(scyllaTable), "genix_test.delta_stock_records__delta_moves") {
		t.Fatal("expected a moves table for a table pinning a column")
	}
}

// The packed key keeps one prefix contiguous and ordered by updated, so a sync is one range.
func TestDeltaPackedKeyOrdersAPrefixByUpdated(t *testing.T) {
	resetORMTableCachesForTesting()

	warehouseIndex := MakeScyllaTable[deltaStockRecord, deltaStockSchema]().deltaIndexes[0]
	packed := func(warehouseID, updated int64) int64 {
		return warehouseIndex.packedKeyValue("delta_stock_records", []int64{warehouseID}, updated)
	}
	if !(packed(42, 0) < packed(42, 100) && packed(42, 100) < packed(42, 1<<31-1) && packed(42, 1<<31-1) < packed(43, 0)) {
		t.Fatal("expected packed keys ordered by warehouse, then updated")
	}
}

// An update that leaves the pinned column out keeps its stored value in the packed key and moves
// nothing; one that writes a new value moves the record out of its old prefix.
func TestStampDeltaWritesKeepsStoredPinnedValuesAndRecordsMoves(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	warehouseIndex := scyllaTable.deltaIndexes[0]
	stored := storedRows{
		{7, 1}: {"version": int16(3), "warehouse_id": int32(41)},
		{7, 2}: {"version": int16(9), "warehouse_id": int32(41)},
	}
	records := []deltaStockRecord{
		{CompanyID: 7, ID: 1, Name: "renamed", Updated: 500}, // WarehouseID not written: zero on the struct
		{CompanyID: 7, ID: 2, WarehouseID: 42, Updated: 500},
	}
	nameOnly := []IColInfo{scyllaTable.ColumnsMap["name"], scyllaTable.UpdatedCol}
	nameAndWarehouse := []IColInfo{scyllaTable.ColumnsMap["name"], scyllaTable.ColumnsMap["warehouse_id"], scyllaTable.UpdatedCol}

	packedByPhase, moves := stampDeltaWrites(scyllaTable, []writePhase{
		{records: makeRecordSlice(&[]deltaStockRecord{}), writtenColumns: nil},
		{records: makeRecordSlice(&records), writtenColumns: nameOnly},
	}, stored)
	packedValues := packedByPhase[1]["zz_dlt_warehouse_id"]
	if packedValues[0] != warehouseIndex.packedKeyValue(scyllaTable.Name, []int64{41}, 500) {
		t.Fatal("expected the unwritten pinned column to keep its stored warehouse 41")
	}
	if records[0].Version != 4 || records[1].Version != 10 || len(moves) != 0 {
		t.Fatalf("expected versions 4 and 10 and no move, got %d %d %+v", records[0].Version, records[1].Version, moves)
	}

	_, moves = stampDeltaWrites(scyllaTable, []writePhase{
		{records: makeRecordSlice(&[]deltaStockRecord{}), writtenColumns: nil},
		{records: makeRecordSlice(&records), writtenColumns: nameAndWarehouse},
	}, stored)
	// Record 1 writes warehouse 0 and record 2 warehouse 42: both leave 41.
	if len(moves) != 2 || moves[1].recordID != 2 || moves[1].indexTag != "warehouse_id_status" ||
		moves[1].pinnedPrefix != warehouseIndex.packedKeyValue(scyllaTable.Name, []int64{41}, 0) || moves[1].updated != 500 {
		t.Fatalf("expected both records to leave warehouse 41, got %+v", moves)
	}
}

// Every update rewrites the packed column, even one that names neither the pinned column nor updated.
func TestUpdateAlwaysRewritesTheDeltaPackedColumn(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	records := []deltaStockRecord{{CompanyID: 7, ID: 1, Name: "renamed"}}
	_, columnsToUpdate := resolveUpdateColumnsForWrite(scyllaTable, 1, makeRecordSlice(&records).at,
		[]Coln{TableOf[deltaStockRecord, deltaStockSchema]().Name}, nil, false)
	if !slices.ContainsFunc(columnsToUpdate, func(column IColInfo) bool { return column.GetName() == "zz_dlt_warehouse_id" }) {
		t.Fatalf("expected the packed column in the SET, got %v", columnsToUpdate)
	}
}

// One pinned column without a declared range takes every bit updated leaves of the bigint.
type deltaElasticPinRecord struct {
	TableStruct[deltaElasticPinSchema, deltaElasticPinRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	ListID    int32 `db:"list_id"`
	Status    int8  `db:"status"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaElasticPinSchema struct {
	TableStruct[deltaElasticPinSchema, deltaElasticPinRecord]
	CompanyID Col[*deltaElasticPinSchema, int32]
	ID        Col[*deltaElasticPinSchema, int32]
	ListID    Col[*deltaElasticPinSchema, int32]
	Status    Col[*deltaElasticPinSchema, int8]
	Version   Col[*deltaElasticPinSchema, int16]
}

func (e deltaElasticPinSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10013,
		Name:      "delta_elastic_pin_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta, Keys: Cols(e.ListID, e.Status)}},
	}
}

func TestDeltaElasticPinnedKeyTakesTheBitsUpdatedLeaves(t *testing.T) {
	resetORMTableCachesForTesting()

	deltaIndex := MakeScyllaTable[deltaElasticPinRecord, deltaElasticPinSchema]().deltaIndexes[0]
	if !slices.Equal(deltaIndex.pinnedSlotBits, []int64{64 - deltaUpdatedSlotBits}) {
		t.Fatalf("expected %d bits for list_id, got %v", 64-deltaUpdatedSlotBits, deltaIndex.pinnedSlotBits)
	}
}

func TestSelectDeltaIndexPicksTheMostSpecificFit(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	deltaIndex, err := selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7), "warehouse_id": int32(42)}, true)
	if err != nil || deltaIndex.tag != "warehouse_id_status" {
		t.Fatalf("expected the warehouse index, got %+v err %v", deltaIndex, err)
	}
	deltaIndex, err = selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7)}, true)
	if err != nil || deltaIndex.tag != "status" {
		t.Fatalf("expected the status index, got %+v err %v", deltaIndex, err)
	}
}

// Delta() only records the request: the driver picks the index and reads the view at Exec.
func TestDeltaRecordsTheCursorOnTheQuery(t *testing.T) {
	resetORMTableCachesForTesting()

	records := []deltaStockRecord{}
	query := Query[deltaStockRecord, deltaStockSchema](&records)
	query.CompanyID.Equals(7)
	query.Delta(400000000, int8(1))

	tableInfo := query.GetTableInfo()
	if tableInfo.Delta == nil || tableInfo.Delta.CursorUpdated != 400000000 || len(tableInfo.Delta.SyncFilterValues) != 1 {
		t.Fatalf("expected the cursor and the filter values on the query, got %+v", tableInfo.Delta)
	}
	if len(tableInfo.Statements) != 1 {
		t.Fatalf("expected Delta() to add no predicate, got %+v", tableInfo.Statements)
	}
}

// The Metadata record carries −1 as its key and the Metadata text; the sync filter is named by its JSON.
func TestDeltaMetadataRecord(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	sink := makeRecordSink(&[]deltaStockRecord{})
	recordPointer, err := deltaMetadataRecord(sink, scyllaTable, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if record := (*deltaStockRecord)(recordPointer); record.ID != -1 || record.Metadata == nil || *record.Metadata != "abc" {
		t.Fatalf("metadata record = %+v", record)
	}
	if fieldName := deltaFieldJSONName(sink, scyllaTable.ColumnsMap["status"]); fieldName != "Status" {
		t.Fatalf("expected the untagged Status field by its Go name, got %q", fieldName)
	}
	if _, err := deltaMetadataRecord(makeRecordSink(&[]deltaElasticPinRecord{}), scyllaTable, "abc"); err == nil {
		t.Fatal("expected an error for a record without Metadata")
	}
}
