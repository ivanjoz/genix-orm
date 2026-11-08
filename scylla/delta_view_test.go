package scylla

import (
	"github.com/ivanjoz/genix-orm/db"
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

func TestDeltaIndexCompilesAFullViewInThePlanner(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	scyllaTable.Namespace = "genix_test"
	warehouseIndex, statusIndex := scyllaTable.DeltaIndexes[0], scyllaTable.DeltaIndexes[1]
	// Every Key is a pinned slot: Status (FixedValues 0..1) takes 1 bit after the warehouse's 17.
	if warehouseIndex.Tag != "warehouse_id_status" ||
		!slices.EqualFunc(warehouseIndex.Pinned, []string{"warehouse_id", "status"}, func(column IColInfo, name string) bool { return column.GetName() == name }) ||
		!slices.Equal(warehouseIndex.PinnedSlotBits, []int64{17, 1}) ||
		warehouseIndex.PackedColumn.GetName() != "zz_dlt_warehouse_id_status" || warehouseIndex.ReplaceThreshold != 3 {
		t.Fatalf("unexpected warehouse index %+v", warehouseIndex)
	}
	if statusIndex.PackedColumn.GetName() != "zz_dlt_status" || !slices.Equal(statusIndex.PinnedSlotBits, []int64{1}) ||
		statusIndex.ReplaceThreshold != cache.DefaultDeltaReplaceThreshold {
		t.Fatalf("unexpected status index %+v", statusIndex)
	}

	// Both views are planner range views on [partition, pinned…, updated] holding every stored column.
	warehouseView := scyllaTable.views[warehouseIndex.ViewName]
	if warehouseView == nil || warehouseView.Type != 8 || !warehouseView.IsDeltaView ||
		!slices.Equal(warehouseView.Columns, []string{"company_id", "warehouse_id", "status", "updated"}) ||
		!slices.Contains(warehouseView.AvailableColumns, "name") {
		t.Fatalf("unexpected warehouse view %+v", warehouseView)
	}
	statusView := scyllaTable.views[statusIndex.ViewName]
	if statusView == nil || !slices.Equal(statusView.Columns, []string{"company_id", "status", "updated"}) {
		t.Fatalf("unexpected status view %+v", statusView)
	}
	// A plain read on Status is one span of the status view; one on the warehouse, a prefix of its view.
	for _, served := range []struct {
		view      *viewInfo
		signature string
	}{{statusView, "company_id|=|status|="}, {warehouseView, "company_id|=|warehouse_id|="}, {warehouseView, "company_id|=|warehouse_id|=|status|="}} {
		if !slices.ContainsFunc(scyllaTable.capabilities, func(capability db.QueryCapability) bool {
			return capability.Source == served.view.IndexSource && capability.Signature == served.signature
		}) {
			t.Fatalf("expected %s served by %s", served.signature, served.view.Name)
		}
	}

	warehouseScript := warehouseView.getCreateScript()
	for _, expected := range []string{
		"delta_stock_records__dlt_warehouse_id_status_view AS",
		"SELECT zz_dlt_warehouse_id_status, company_id, id, warehouse_id, status, name, updated, version, created FROM",
		"PRIMARY KEY ((company_id), zz_dlt_warehouse_id_status, id)",
		"CLUSTERING ORDER BY (zz_dlt_warehouse_id_status DESC, id ASC)",
	} {
		if !strings.Contains(warehouseScript, expected) {
			t.Fatalf("expected %q in:\n%s", expected, warehouseScript)
		}
	}
	if statusScript := statusView.getCreateScript(); !strings.Contains(statusScript, "PRIMARY KEY ((company_id), zz_dlt_status, id)") {
		t.Fatalf("expected the status view keyed on the packed status:\n%s", statusScript)
	}
	if !hasDeltaMoves(scyllaTable) || !strings.Contains(deltaMovesCreateScript(scyllaTable), "genix_test.delta_stock_records__delta_moves") {
		t.Fatal("expected a moves table for a table pinning a column")
	}
}

// The packed key keeps one prefix contiguous and ordered by updated, so a sync is one range.
func TestDeltaPackedKeyOrdersAPrefixByUpdated(t *testing.T) {
	resetORMTableCachesForTesting()

	warehouseIndex := MakeScyllaTable[deltaStockRecord, deltaStockSchema]().DeltaIndexes[0]
	packed := func(warehouseID, status, updated int64) int64 {
		return deltaPackedStoredValue(warehouseIndex, "delta_stock_records", []int64{warehouseID, status}, updated)
	}
	if !(packed(42, 0, 0) < packed(42, 0, 100) && packed(42, 0, 1<<31-1) < packed(42, 1, 0) &&
		packed(42, 1, 1<<31-1) < packed(43, 0, 0)) {
		t.Fatal("expected packed keys ordered by warehouse, then status, then updated")
	}
}

// An update that leaves the pinned column out keeps its stored value in the packed key and moves
// nothing; one that writes a new value moves the record out of its old prefix.
func TestStampDeltaWritesKeepsStoredPinnedValuesAndRecordsMoves(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	warehouseIndex := scyllaTable.DeltaIndexes[0]
	stored := storedRows{
		{7, 1}: {"version": int16(3), "warehouse_id": int32(41), "status": int8(1)},
		{7, 2}: {"version": int16(9), "warehouse_id": int32(41), "status": int8(1)},
	}
	records := []deltaStockRecord{
		{CompanyID: 7, ID: 1, Name: "renamed", Updated: 500}, // WarehouseID not written: zero on the struct
		{CompanyID: 7, ID: 2, WarehouseID: 42, Updated: 500},
	}
	nameOnly := []IColInfo{scyllaTable.ColumnsMap["name"], scyllaTable.UpdatedCol}
	nameAndWarehouse := []IColInfo{scyllaTable.ColumnsMap["name"], scyllaTable.ColumnsMap["warehouse_id"], scyllaTable.UpdatedCol}

	packedByPhase, moves := stampDeltaWrites(scyllaTable, []writePhase{
		{records: db.MakeRecordSlice(&[]deltaStockRecord{}), writtenColumns: nil},
		{records: db.MakeRecordSlice(&records), writtenColumns: nameOnly},
	}, stored)
	packedValues := packedByPhase[1]["zz_dlt_warehouse_id_status"]
	if packedValues[0] != deltaPackedStoredValue(warehouseIndex, scyllaTable.Name, []int64{41, 1}, 500) {
		t.Fatal("expected the unwritten pinned columns to keep their stored warehouse 41 and status 1")
	}
	if records[0].Version != 4 || records[1].Version != 10 || len(moves) != 0 {
		t.Fatalf("expected versions 4 and 10 and no move, got %d %d %+v", records[0].Version, records[1].Version, moves)
	}

	_, moves = stampDeltaWrites(scyllaTable, []writePhase{
		{records: db.MakeRecordSlice(&[]deltaStockRecord{}), writtenColumns: nil},
		{records: db.MakeRecordSlice(&records), writtenColumns: nameAndWarehouse},
	}, stored)
	// Record 1 writes warehouse 0 and record 2 warehouse 42: both leave [41, 1]. Status is not written,
	// so the status index moves nothing.
	if len(moves) != 2 || moves[1].recordID != 2 || moves[1].indexTag != "warehouse_id_status" ||
		moves[1].pinnedPrefix != deltaPackedStoredValue(warehouseIndex, scyllaTable.Name, []int64{41, 1}, 0) || moves[1].updated != 500 {
		t.Fatalf("expected both records to leave warehouse 41, got %+v", moves)
	}

	// A status change is a move like any other pinned change: record 1 leaves the [1] prefix.
	records = []deltaStockRecord{{CompanyID: 7, ID: 1, Status: 0, Updated: 600}}
	_, moves = stampDeltaWrites(scyllaTable, []writePhase{
		{records: db.MakeRecordSlice(&[]deltaStockRecord{}), writtenColumns: nil},
		{records: db.MakeRecordSlice(&records), writtenColumns: []IColInfo{scyllaTable.ColumnsMap["status"], scyllaTable.UpdatedCol}},
	}, stored)
	statusIndex := scyllaTable.DeltaIndexes[1]
	if len(moves) != 2 || moves[1].indexTag != "status" || moves[1].pinnedPrefix != deltaPackedStoredValue(statusIndex, scyllaTable.Name, []int64{1}, 0) {
		t.Fatalf("expected the soft delete to leave [41, 1] and [1], got %+v", moves)
	}
}

// Every update rewrites the packed column, even one that names neither the pinned column nor updated.
func TestUpdateAlwaysRewritesTheDeltaPackedColumn(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	records := []deltaStockRecord{{CompanyID: 7, ID: 1, Name: "renamed"}}
	columnsToUpdate := scyllaTable.ResolveUpdateColumns(db.MakeRecordSlice(&records),
		[]Coln{TableOf[deltaStockRecord, deltaStockSchema]().Name}, nil)
	if !slices.ContainsFunc(columnsToUpdate, func(column IColInfo) bool { return column.GetName() == "zz_dlt_warehouse_id_status" }) {
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
	Updated   int32 `json:"upd,omitempty"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaElasticPinSchema struct {
	TableStruct[deltaElasticPinSchema, deltaElasticPinRecord]
	CompanyID Col[*deltaElasticPinSchema, int32]
	ID        Col[*deltaElasticPinSchema, int32]
	ListID    Col[*deltaElasticPinSchema, int32]
	Status    Col[*deltaElasticPinSchema, int8]
	Updated   Col[*deltaElasticPinSchema, int32]
	Version   Col[*deltaElasticPinSchema, int16]
}

func (e deltaElasticPinSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10013,
		Name:        "delta_elastic_pin_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes:     []Index{{Type: TypeDelta, Keys: Cols(e.ListID, e.Status)}},
	}
}

func TestDeltaElasticPinnedKeyTakesTheBitsUpdatedLeaves(t *testing.T) {
	resetORMTableCachesForTesting()

	deltaIndex := MakeScyllaTable[deltaElasticPinRecord, deltaElasticPinSchema]().DeltaIndexes[0]
	if expectedListBits := int64(64 - db.DeltaUpdatedSlotBits - 1); !slices.Equal(deltaIndex.PinnedSlotBits, []int64{expectedListBits, 1}) {
		t.Fatalf("expected %d bits for list_id and 1 for status, got %v", expectedListBits, deltaIndex.PinnedSlotBits)
	}
}

func TestSelectDeltaIndexPicksTheMostSpecificFit(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaStockRecord, deltaStockSchema]()
	for _, selection := range []struct {
		equalities       map[string]any
		deltaValues      []int64
		expectedTag      string
		expectedPrefixes [][]int64
	}{
		// The warehouse equality fixes more of the warehouse index; the Delta() values name its Status.
		{map[string]any{"company_id": int32(7), "warehouse_id": int32(42)}, []int64{1}, "warehouse_id_status", [][]int64{{42, 1}}},
		// No warehouse: the warehouse index cannot resolve (no FixedValues), the status index can.
		{map[string]any{"company_id": int32(7)}, []int64{0, 1}, "status", [][]int64{{0}, {1}}},
		// No Delta() values: Status fans out over its FixedValues.
		{map[string]any{"company_id": int32(7)}, nil, "status", [][]int64{{0}, {1}}},
	} {
		tableInfo := &TableInfo{Delta: &db.DeltaRead{}}
		for column, value := range selection.equalities {
			tableInfo.SetWhere(column, "=", value)
		}
		for _, deltaValue := range selection.deltaValues {
			tableInfo.Delta.LastKeyValues = append(tableInfo.Delta.LastKeyValues, deltaValue)
		}
		deltaPlan, err := scyllaTable.PlanDeltaRead(tableInfo)
		if err != nil || deltaPlan.Index.Tag != selection.expectedTag ||
			!slices.EqualFunc(deltaPlan.Prefixes, selection.expectedPrefixes, slices.Equal[[]int64]) {
			t.Fatalf("%v %v: expected %s %v, got %+v err %v", selection.equalities, selection.deltaValues,
				selection.expectedTag, selection.expectedPrefixes, deltaPlan, err)
		}
	}
}

// Delta() only records the request: the driver picks the index and reads the view at Exec.
func TestDeltaRecordsTheCursorOnTheQuery(t *testing.T) {
	resetORMTableCachesForTesting()

	records := []deltaStockRecord{}
	query := Query[deltaStockRecord, deltaStockSchema](&records)
	query.CompanyID.Equals(7)
	cursor := cache.DeltaWindowCursor{Updated: 400000000, ReadTime: 400000005}
	query.Delta(cursor, int8(1))

	tableInfo := query.GetTableInfo()
	if tableInfo.Delta == nil || tableInfo.Delta.Cursor != cursor || len(tableInfo.Delta.LastKeyValues) != 1 {
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
