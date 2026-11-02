package scylla

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	"github.com/ivanjoz/genix-orm/db/cache"
)

// The delta fixture mirrors client_provider: a tenant partition, a two-value Status that leads the
// packed key, and a narrow Type. RegistryNumber carries a local index so the selectivity tiering in
// ComputeCapabilities can be exercised.
type deltaViewRecord struct {
	TableStruct[deltaViewSchema, deltaViewRecord]
	CompanyID      int32   `db:"company_id"`
	ID             int32   `db:"id"`
	Status         int8    `db:"status"`
	Type           int8    `db:"type"`
	RegistryNumber string  `db:"registry_number"`
	Version        int16   `json:"ver,omitempty"`
	Metadata       *string `json:"mdt,omitempty"`
}

type deltaViewSchema struct {
	TableStruct[deltaViewSchema, deltaViewRecord]
	CompanyID      Col[*deltaViewSchema, int32]
	ID             Col[*deltaViewSchema, int32]
	Status         Col[*deltaViewSchema, int8]
	Type           Col[*deltaViewSchema, int8]
	RegistryNumber Col[*deltaViewSchema, string]
	Version        Col[*deltaViewSchema, int16]
}

func (e deltaViewSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10008,
		Name:      "delta_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Status, Values: []int64{0, 1}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		Indexes: []Index{
			{Type: TypeLocalIndex, Keys: Cols(e.RegistryNumber)},
			{Type: TypeDelta, Keys: Cols(e.Status, e.Type)},
		},
	}
}

// deltaPackedView finds the compiled delta listing view and asserts the layout every other test
// builds on.
func deltaPackedView(t *testing.T, scyllaTable ScyllaTable) *viewInfo {
	t.Helper()
	for _, view := range scyllaTable.views {
		if view.Type == 8 {
			return view
		}
	}
	t.Fatal("expected a packed range view compiled from the TypeDelta declaration")
	return nil
}

func TestDeltaViewSizesSlotsFromFixedValues(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	view := deltaPackedView(t, scyllaTable)

	// Status {0,1} needs one bit and Type 1..2 two: 3 of an int's 32 bits.
	expectedSlots := []int64{1, 2}
	if !slices.Equal(view.packedSlotBitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotBitsPerColumn)
	}
	if view.column.GetType().DBType != "int" {
		t.Fatalf("expected an int packed column, got %v", view.column.GetType().DBType)
	}
	if !strings.HasSuffix(view.name, "pk_status_type_rng_view") {
		t.Fatalf("expected the view keyed on the declared keys, got %v", view.name)
	}

	// The log: Status is pinned, Type is the sync filter, the int32 ID the log ID.
	deltaIndex := scyllaTable.deltaIndexes[0]
	if deltaIndex.tag != "status-type" || len(deltaIndex.pinned) != 1 || deltaIndex.pinned[0].GetName() != "status" ||
		deltaIndex.syncFilter.GetName() != "type" || deltaIndex.is64BitScale {
		t.Fatalf("unexpected delta index %+v", deltaIndex)
	}
}

// Keys whose slots sum past the int's 32 bits spill to a bigint.
type deltaWideLeadRecord struct {
	TableStruct[deltaWideLeadSchema, deltaWideLeadRecord]
	CompanyID int32 `db:"company_id"`
	ID        int64 `db:"id"`
	Code      int64 `db:"code"`
	Status    int8  `db:"status"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaWideLeadSchema struct {
	TableStruct[deltaWideLeadSchema, deltaWideLeadRecord]
	CompanyID Col[*deltaWideLeadSchema, int32]
	ID        Col[*deltaWideLeadSchema, int64]
	Code      Col[*deltaWideLeadSchema, int64]
	Status    Col[*deltaWideLeadSchema, int8]
	Version   Col[*deltaWideLeadSchema, int16]
}

func (e deltaWideLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10009,
		Name:      "delta_wide_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Code, Min: 1, Max: 1 << 40},
			{Col: e.Status, Values: []int64{0, 1}},
		},
		// Code takes 41 bits: with Status's one, past the int's 32.
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Code, e.Status)}},
	}
}

func TestDeltaViewFallsBackToBigint(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaWideLeadRecord, deltaWideLeadSchema]()
	view := deltaPackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column when the keys overflow an int, got %v", view.column.GetType().DBType)
	}
	if expectedSlots := []int64{41, 1}; !slices.Equal(view.packedSlotBitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotBitsPerColumn)
	}
	// An int64 key logs at the 64-bit scale.
	if !scyllaTable.deltaIndexes[0].is64BitScale {
		t.Fatal("expected an int64 log ID to use the 64-bit scale")
	}
}

// A single declared key lists through a plain view on it: nothing to pack.
type deltaSingleKeyRecord struct {
	TableStruct[deltaSingleKeySchema, deltaSingleKeyRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Type      int8  `db:"type"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaSingleKeySchema struct {
	TableStruct[deltaSingleKeySchema, deltaSingleKeyRecord]
	CompanyID Col[*deltaSingleKeySchema, int32]
	ID        Col[*deltaSingleKeySchema, int32]
	Type      Col[*deltaSingleKeySchema, int8]
	Version   Col[*deltaSingleKeySchema, int16]
}

func (e deltaSingleKeySchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10010,
		Name:        "delta_single_key_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Type, Min: 1, Max: 2}},
		Indexes:     []Index{{Type: TypeDelta, Keys: Cols(e.Type)}},
	}
}

func TestDeltaWithOneKeyListsThroughASimpleView(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaSingleKeyRecord, deltaSingleKeySchema]()
	var listingView *viewInfo
	for _, view := range scyllaTable.views {
		if view.Type == 8 {
			t.Fatalf("expected no packed view for a single key, got %v", view.name)
		}
		if slices.Equal(view.columnsNoPart, []string{"type"}) {
			listingView = view
		}
	}
	if listingView == nil {
		t.Fatal("expected a view keyed on type")
	}
	if deltaIndex := scyllaTable.deltaIndexes[0]; len(deltaIndex.pinned) != 0 || deltaIndex.syncFilter.GetName() != "type" {
		t.Fatalf("expected type as the sync filter and nothing pinned, got %+v", deltaIndex)
	}
}

// A key with no FixedValues and no Size absorbs the bit remainder instead of being rejected: the
// layout goes bigint.
type deltaElasticRecord struct {
	TableStruct[deltaElasticSchema, deltaElasticRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Status    int8  `db:"status"`
	ListID    int32 `db:"list_id"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaElasticSchema struct {
	TableStruct[deltaElasticSchema, deltaElasticRecord]
	CompanyID Col[*deltaElasticSchema, int32]
	ID        Col[*deltaElasticSchema, int32]
	Status    Col[*deltaElasticSchema, int8]
	ListID    Col[*deltaElasticSchema, int32]
	Version   Col[*deltaElasticSchema, int16]
}

func (e deltaElasticSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10011,
		Name:        "delta_elastic_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		// ListID has no declared ceiling, so it takes every bit the others leave over.
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.ListID, e.Status)}},
	}
}

func TestDeltaViewLetsAnUndeclaredKeyAbsorbTheBitRemainder(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaElasticRecord, deltaElasticSchema]()
	view := deltaPackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column for an elastic layout, got %v", view.column.GetType().DBType)
	}
	// 1 bit for the declared Status and the remaining 63 of the 64-bit budget for ListID.
	if expectedSlots := []int64{63, 1}; !slices.Equal(view.packedSlotBitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotBitsPerColumn)
	}
}

// No declared keys at all: a log of the whole partition, listed by a partition read.
type deltaKeylessRecord struct {
	TableStruct[deltaKeylessSchema, deltaKeylessRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaKeylessSchema struct {
	TableStruct[deltaKeylessSchema, deltaKeylessRecord]
	CompanyID Col[*deltaKeylessSchema, int32]
	ID        Col[*deltaKeylessSchema, int32]
	Version   Col[*deltaKeylessSchema, int16]
}

func (e deltaKeylessSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10027,
		Name:      "delta_keyless_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta}},
	}
}

func TestDeltaWithNoKeysHasNoView(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaKeylessRecord, deltaKeylessSchema]()
	if len(scyllaTable.views) != 0 {
		t.Fatalf("expected no view for a keyless delta index, got %d", len(scyllaTable.views))
	}
	if deltaIndex := scyllaTable.deltaIndexes[0]; deltaIndex.tag != "all" || deltaIndex.syncFilter != nil {
		t.Fatalf("expected the keyless log tagged \"all\" with no sync filter, got %+v", deltaIndex)
	}
	// Filter values need a sync filter column, which a keyless index has not.
	if _, err := selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7)}, true); err == nil {
		t.Fatal("expected a keyless index to refuse filter values")
	}
}

// Two undeclared keys leave no unambiguous way to split the remainder.
type deltaTwoElasticRecord struct {
	TableStruct[deltaTwoElasticSchema, deltaTwoElasticRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	ListID    int32 `db:"list_id"`
	GroupID   int32 `db:"group_id"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaTwoElasticSchema struct {
	TableStruct[deltaTwoElasticSchema, deltaTwoElasticRecord]
	CompanyID Col[*deltaTwoElasticSchema, int32]
	ID        Col[*deltaTwoElasticSchema, int32]
	ListID    Col[*deltaTwoElasticSchema, int32]
	GroupID   Col[*deltaTwoElasticSchema, int32]
	Version   Col[*deltaTwoElasticSchema, int16]
}

func (e deltaTwoElasticSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10025,
		Name:      "delta_two_elastic_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta, Keys: Cols(e.ListID, e.GroupID)}},
	}
}

func TestDeltaViewRejectsTwoKeysWithoutDeclaredRange(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected a panic naming both keys that have no declared range")
		}
		if !strings.Contains(fmt.Sprint(recovered), `"group_id"`) {
			t.Fatalf("expected the panic to name the second elastic column, got: %v", recovered)
		}
	}()

	MakeScyllaTable[deltaTwoElasticRecord, deltaTwoElasticSchema]()
}

// A forced .Int32() on a budget that cannot fit must fail rather than silently overflow.
type deltaForcedInt32Record struct {
	TableStruct[deltaForcedInt32Schema, deltaForcedInt32Record]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Channel   int32 `db:"channel"`
	Type      int8  `db:"type"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaForcedInt32Schema struct {
	TableStruct[deltaForcedInt32Schema, deltaForcedInt32Record]
	CompanyID Col[*deltaForcedInt32Schema, int32]
	ID        Col[*deltaForcedInt32Schema, int32]
	Channel   Col[*deltaForcedInt32Schema, int32]
	Type      Col[*deltaForcedInt32Schema, int8]
	Version   Col[*deltaForcedInt32Schema, int16]
}

func (e deltaForcedInt32Schema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10012,
		Name:      "delta_forced_int32_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Channel, Min: 1, Max: 1 << 30},
			{Col: e.Type, Min: 1, Max: 2},
		},
		// Channel takes 31 bits and Type 2: past the int's 32.
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Channel.Int32(), e.Type)}},
	}
}

func TestDeltaViewRejectsForcedInt32ThatDoesNotFit(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected a panic when .Int32() cannot hold the declared ranges")
		}
		if !strings.Contains(fmt.Sprint(recovered), "cannot pack into an int32") {
			t.Fatalf("expected the panic to report the int32 limit, got: %v", recovered)
		}
	}()

	MakeScyllaTable[deltaForcedInt32Record, deltaForcedInt32Schema]()
}

// ─── Delta() reads ─────────────────────────────────────────────────────────────

// packDeltaValue packs a full key the way the view stores it, sign bit flipped.
func packDeltaValue(t *testing.T, view *viewInfo, componentValues ...int64) int64 {
	t.Helper()
	return convertToInt64(storeVirtualPacked(db.PackSlotValues(componentValues, view.packedSlotBitsPerColumn), view.packedIsInt32))
}

// packDeltaUpperBound is the stored inclusive upper bound of the block a prefix pins.
func packDeltaUpperBound(t *testing.T, view *viewInfo, prefixValues ...int64) int64 {
	t.Helper()
	return convertToInt64(storeVirtualPacked(db.PackSlotPrefixBound(prefixValues, view.packedSlotBitsPerColumn, true), view.packedIsInt32))
}

// Delta() only records the request: the driver reads the log and picks the index at Exec.
func TestDeltaRecordsTheCursorOnTheQuery(t *testing.T) {
	resetORMTableCachesForTesting()

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.Delta(cache.DeltaCursor{5000: 40}, int8(1))

	tableInfo := query.GetTableInfo()
	if tableInfo.Delta == nil || tableInfo.Delta.Cursor[5000] != 40 || len(tableInfo.Delta.SyncFilterValues) != 1 {
		t.Fatalf("expected the cursor and the filter values on the query, got %+v", tableInfo.Delta)
	}
	if len(tableInfo.Statements) != 2 {
		t.Fatalf("expected Delta() to add no predicate, got %+v", tableInfo.Statements)
	}
}

// The first sync lists the view with the pinned Keys and the sync filter: every key is pinned, so the
// packed listing view serves it with one exact IN over the packed values.
func TestDeltaListingViewServesTheFirstSync(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	statements := []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "status", Operator: "=", Value: int8(1)},
		{Col: "type", Operator: "IN", Values: []any{int8(1), int8(2)}},
	}
	whereStatements := view.getStatementPrepared(statements...)
	if len(whereStatements) != 1 || !strings.Contains(whereStatements[0].Clause, "IN (?, ?)") {
		t.Fatalf("expected one IN clause, got %+v", whereStatements)
	}
	assertClauseValues(t, whereStatements[0], []any{packDeltaValue(t, view, 1, 1), packDeltaValue(t, view, 1, 2)})
}

func TestSelectDeltaIndexNeedsEveryPinnedKey(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	// Status is pinned in the only index, so a read without it has no log to follow.
	if _, err := selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7)}, false); err == nil {
		t.Fatal("expected no index to fit a read that does not pin status")
	}
	if _, err := selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7), "status": int8(1)}, true); err != nil {
		t.Fatalf("expected the [status, type] index to fit, got %v", err)
	}
}

// Two delta indexes for two read shapes, as warehouse_product_stock declares them.
type deltaMultiShapeRecord struct {
	TableStruct[deltaMultiShapeSchema, deltaMultiShapeRecord]
	CompanyID   int32 `db:"company_id"`
	ID          int32 `db:"id"`
	WarehouseID int32 `db:"warehouse_id"`
	Status      int8  `db:"status"`
	Version     int16 `json:"ver,omitempty"`
}

type deltaMultiShapeSchema struct {
	TableStruct[deltaMultiShapeSchema, deltaMultiShapeRecord]
	CompanyID   Col[*deltaMultiShapeSchema, int32]
	ID          Col[*deltaMultiShapeSchema, int32]
	WarehouseID Col[*deltaMultiShapeSchema, int32]
	Status      Col[*deltaMultiShapeSchema, int8]
	Version     Col[*deltaMultiShapeSchema, int16]
}

func (e deltaMultiShapeSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10028,
		Name:        "delta_multi_shape_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(e.WarehouseID.Size(17), e.Status)},
			{Type: TypeDelta, Keys: Cols(e.Status)},
		},
	}
}

func TestSelectDeltaIndexPicksTheMostSpecificFit(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()
	// warehouse_id pinned → the [warehouse_id, status] index.
	deltaIndex, err := selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7), "warehouse_id": int32(42)}, true)
	if err != nil || deltaIndex.tag != "warehouse_id-status" {
		t.Fatalf("expected the warehouse index, got %+v err %v", deltaIndex, err)
	}
	// warehouse_id left open → only the [status] index fits.
	deltaIndex, err = selectDeltaIndex(scyllaTable, map[string]any{"company_id": int32(7)}, true)
	if err != nil || deltaIndex.tag != "status" {
		t.Fatalf("expected the status index, got %+v err %v", deltaIndex, err)
	}
}

// Each pinned value has its own log folder, under the partition's base pk and the index's tag.
func TestDeltaLogFolderNamesThePartitionAndPinnedValues(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()
	scyllaTable.Namespace = "genix_test"
	folder := deltaLogFolder(scyllaTable, 7, scyllaTable.deltaIndexes[0], []int64{42})
	if folder != "delta-log/genix_test/10028/7/warehouse_id-status/42/" {
		t.Fatalf("unexpected folder %q", folder)
	}
}

// ─── Prefix routing ────────────────────────────────────────────────────────────

func assertClauseValues(t *testing.T, clause boundWhereClause, expectedValues []any) {
	t.Helper()
	// The partition equality is prepended to every clause, so compare only the packed bounds.
	packedValues := clause.Values
	if len(packedValues) > len(expectedValues) {
		packedValues = packedValues[len(packedValues)-len(expectedValues):]
	}
	if fmt.Sprint(packedValues) != fmt.Sprint(expectedValues) {
		t.Fatalf("expected packed bounds %v, got %v (clause %q)", expectedValues, packedValues, clause.Clause)
	}
}

func TestPackedViewServesLeadingKeyAlone(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if compiledStatement.sourceView == nil || compiledStatement.sourceView.name != view.name {
		t.Fatalf("expected the packed view to serve a leading-key query, got %+v", compiledStatement.sourceView)
	}

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one prefix range, got %d", len(whereStatements))
	}
	// Status = 1 spans every Type slot value: the inclusive block [1_0, 1_3].
	assertClauseValues(t, whereStatements[0], []any{
		packDeltaValue(t, view, 1, 0),
		packDeltaUpperBound(t, view, 1),
	})
}

func TestPackedViewUpperBoundIsTheLayoutMaximum(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	// A range on the leading key spans the whole packed width. Its inclusive upper bound is every
	// slot at its max, bound as an int32 so it fits the int packed column.
	query.Status.GreaterEqual(int8(1))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one range clause, got %d", len(whereStatements))
	}
	upperBound, isInt32 := whereStatements[0].Values[len(whereStatements[0].Values)-1].(int32)
	if !isInt32 || int64(upperBound) != packDeltaUpperBound(t, view) {
		t.Fatalf("expected the int32 layout maximum as the upper bound, got %v", whereStatements[0].Values)
	}
}

func TestPackedViewServesTwoColumnPrefix(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.Type.Equals(int8(2))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one clause, got %d", len(whereStatements))
	}
	// Every key pinned: one exact packed value.
	assertClauseValues(t, whereStatements[0], []any{packDeltaValue(t, view, 1, 2)})
}

func TestPackedViewFansOutPrefixInValues(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.In(int8(0), int8(1))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 2 {
		t.Fatalf("expected one prefix range per IN value, got %d", len(whereStatements))
	}
}

func TestPackedViewKeepsBetweenOnTrailingKey(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.Type.Between(int8(1), int8(2))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected a single BETWEEN clause, got %d", len(whereStatements))
	}
	assertClauseValues(t, whereStatements[0], []any{
		packDeltaValue(t, view, 1, 1),
		packDeltaValue(t, view, 1, 2),
	})
}

func TestPackedViewPrefixRanksBelowLocalIndex(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"

	prioritiesBySignature := map[string]int{}
	for _, capability := range scyllaTable.capabilities {
		if existing, seen := prioritiesBySignature[capability.Signature]; !seen || capability.Priority > existing {
			prioritiesBySignature[capability.Signature] = capability.Priority
		}
	}

	partitionOnly := prioritiesBySignature["company_id|="]
	localIndex := prioritiesBySignature["company_id|=|registry_number|="]
	shortPrefix := prioritiesBySignature["company_id|=|status|="]
	fullKey := prioritiesBySignature["company_id|=|status|=|type|="]

	for signature, priority := range map[string]int{
		"company_id|=": partitionOnly, "company_id|=|registry_number|=": localIndex,
		"company_id|=|status|=": shortPrefix, "company_id|=|status|=|type|=": fullKey,
	} {
		if priority == 0 {
			t.Fatalf("expected a capability for %q", signature)
		}
	}

	// A narrow index must win over a broad prefix scan, or its predicate becomes an unservable
	// leftover filter on the view. The whole key pinned is an exact read and wins over both.
	if !(partitionOnly < shortPrefix && shortPrefix < localIndex && localIndex < fullKey) {
		t.Fatalf("unexpected priority ordering: partition=%d shortPrefix=%d localIndex=%d fullKey=%d",
			partitionOnly, shortPrefix, localIndex, fullKey)
	}
}

func TestNarrowLocalIndexBeatsPackedViewPrefix(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.RegistryNumber.Equals("20512345678")

	bestCapability := MatchQueryCapability(collectSelectStatements(query.GetTableInfo()), scyllaTable.capabilities)
	if bestCapability == nil || bestCapability.Source == nil {
		t.Fatalf("expected a capability match, got %+v", bestCapability)
	}
	if !strings.Contains(bestCapability.Signature, "registry_number") {
		t.Fatalf("expected the local index to win over the packed prefix, got %q", bestCapability.Signature)
	}
}

func TestPackedViewRefusesFilterBehindUnconstrainedKey(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	// Type is behind an unconstrained Status, so this cannot be one packed range and must not be
	// answered by silently dropping the Type filter.
	statements := []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "type", Operator: "=", Value: int8(2)},
	}
	if whereStatements := view.getStatementPrepared(statements...); whereStatements != nil {
		t.Fatalf("expected the view to refuse a gapped predicate set, got %+v", whereStatements)
	}
}

// ─── FixedValues fan-out ───────────────────────────────────────────────────────

// A query that pins Type but leaves the leading Status open cannot match the packed view's key
// prefix on its own. Status declares only two values, so enumerating it is logically identical to
// leaving it open and turns the gap into two contiguous key ranges — the alternative being a base
// table read Scylla rejects without ALLOW FILTERING.
func TestFixedValueFanoutFillsAGapInThePackedKeyPrefix(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Type.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if compiledStatement.sourceView == nil {
		t.Fatal("expected the packed delta view to serve the query, got a base-table read")
	}
	if len(compiledStatement.fixedValueFanoutStatements) != 1 ||
		compiledStatement.fixedValueFanoutStatements[0].Col != "status" {
		t.Fatalf("expected one synthesized status predicate, got %+v", compiledStatement.fixedValueFanoutStatements)
	}
	// The values must carry the column's own width: an int64 0 would bind wrong and hash wrong.
	if _, isInt8 := compiledStatement.fixedValueFanoutStatements[0].Values[0].(int8); !isInt8 {
		t.Fatalf("expected int8 fan-out values for an int8 column, got %T",
			compiledStatement.fixedValueFanoutStatements[0].Values[0])
	}

	boundPlan, err := compiledStatement.Compute(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected bind error: %v", err)
	}
	// Status s with type 1 is the single key s_1: one exact IN over both.
	view := deltaPackedView(t, scyllaTable)
	expectedValues := []any{packDeltaValue(t, view, 0, 1), packDeltaValue(t, view, 1, 1)}
	if len(boundPlan.Statements) != 1 {
		t.Fatalf("expected one IN query, got %d", len(boundPlan.Statements))
	}
	assertClauseValues(t, boundWhereClause{Values: boundPlan.Statements[0].QueryValues}, expectedValues)
}

// A plan the query already reaches on its own must not be traded for a fanned-out one.
func TestFixedValueFanoutNeverDisplacesTheKeyRoute(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.ID.Equals(int32(42))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if len(compiledStatement.fixedValueFanoutStatements) > 0 {
		t.Fatalf("expected no fan-out over a primary key read, got %+v", compiledStatement.fixedValueFanoutStatements)
	}
	if compiledStatement.sourceView != nil {
		t.Fatalf("expected the base-key route, got view %v", compiledStatement.sourceView.name)
	}
}

// Values listed one by one are three values, not the ten their span suggests.
type deltaSparseValuesRecord struct {
	TableStruct[deltaSparseValuesSchema, deltaSparseValuesRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Status    int8  `db:"status"`
	Type      int8  `db:"type"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaSparseValuesSchema struct {
	TableStruct[deltaSparseValuesSchema, deltaSparseValuesRecord]
	CompanyID Col[*deltaSparseValuesSchema, int32]
	ID        Col[*deltaSparseValuesSchema, int32]
	Status    Col[*deltaSparseValuesSchema, int8]
	Type      Col[*deltaSparseValuesSchema, int8]
	Version   Col[*deltaSparseValuesSchema, int16]
}

func (e deltaSparseValuesSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10029,
		Name:      "delta_sparse_values_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Status, Values: []int64{0, 5, 9}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Status, e.Type)}},
	}
}

func TestFixedValueFanoutUsesTheDeclaredListNotItsSpan(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaSparseValuesRecord, deltaSparseValuesSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaSparseValuesRecord{}
	query := Query[deltaSparseValuesRecord, deltaSparseValuesSchema](&records)
	query.CompanyID.Equals(7)
	query.Type.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	boundPlan, err := compiledStatement.Compute(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected bind error: %v", err)
	}
	// Three declared values, not the ten spanned by 0..9.
	if len(boundPlan.Statements) != 1 || len(boundPlan.Statements[0].QueryValues) != 4 {
		t.Fatalf("expected one IN over 3 fanned-out values, got %+v", boundPlan.Statements)
	}
	// A status reaching 9 needs 4 bits, and Type 2.
	view := deltaPackedView(t, scyllaTable)
	if !slices.Equal(view.packedSlotBitsPerColumn, []int64{4, 2}) {
		t.Fatalf("expected slot widths [4 2], got %v", view.packedSlotBitsPerColumn)
	}
	expectedValues := []any{}
	for _, statusValue := range []int64{0, 5, 9} {
		expectedValues = append(expectedValues, packDeltaValue(t, view, statusValue, 1))
	}
	assertClauseValues(t, boundWhereClause{Values: boundPlan.Statements[0].QueryValues}, expectedValues)
}

// A column too wide to enumerate keeps its gap: fanning 501 values out would cost far more than the
// scan it replaces.
type deltaUnenumerableLeadRecord struct {
	TableStruct[deltaUnenumerableLeadSchema, deltaUnenumerableLeadRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Kind      int16 `db:"kind"`
	Status    int8  `db:"status"`
	Version   int16 `json:"ver,omitempty"`
}

type deltaUnenumerableLeadSchema struct {
	TableStruct[deltaUnenumerableLeadSchema, deltaUnenumerableLeadRecord]
	CompanyID Col[*deltaUnenumerableLeadSchema, int32]
	ID        Col[*deltaUnenumerableLeadSchema, int32]
	Kind      Col[*deltaUnenumerableLeadSchema, int16]
	Status    Col[*deltaUnenumerableLeadSchema, int8]
	Version   Col[*deltaUnenumerableLeadSchema, int16]
}

func (e deltaUnenumerableLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10030,
		Name:      "delta_unenumerable_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Kind, Min: 0, Max: 500},
			{Col: e.Status, Values: []int64{0, 1}},
		},
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Kind, e.Status)}},
	}
}

func TestFixedValueFanoutSkipsAColumnTooWideToEnumerate(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaUnenumerableLeadRecord, deltaUnenumerableLeadSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaUnenumerableLeadRecord{}
	query := Query[deltaUnenumerableLeadRecord, deltaUnenumerableLeadSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if len(compiledStatement.fixedValueFanoutStatements) > 0 {
		t.Fatalf("expected no fan-out over a 501-value column, got %+v",
			compiledStatement.fixedValueFanoutStatements)
	}
	if compiledStatement.sourceView != nil {
		t.Fatalf("expected the base-table route, got view %v", compiledStatement.sourceView.name)
	}
}

// A shorter key prefix that already binds every predicate is one query over the same rows the
// fanned-out longer prefix would return in several. Capability priority ranks the longer prefix
// higher, so coverage of the caller's own predicates has to be what decides it.
func TestFixedValueFanoutSkipsAPrefixThatAlreadyCoversTheQuery(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if len(compiledStatement.fixedValueFanoutStatements) > 0 {
		t.Fatalf("expected no fan-out when [status] already binds every predicate, got %+v",
			compiledStatement.fixedValueFanoutStatements)
	}

	boundPlan, err := compiledStatement.Compute(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected bind error: %v", err)
	}
	if len(boundPlan.Statements) != 1 {
		t.Fatalf("expected a single query over the [status] prefix, got %d", len(boundPlan.Statements))
	}
}
