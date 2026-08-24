package scylla

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// The delta fixture mirrors client_provider: a tenant partition, a two-value Status that leads the
// packed key, and a narrow Type. RegistryNumber carries a local index so the selectivity tiering in
// ComputeCapabilities can be exercised.
type deltaViewRecord struct {
	TableStruct[deltaViewSchema, deltaViewRecord]
	CompanyID      int32  `db:"company_id"`
	ID             int32  `db:"id"`
	Status         int8   `db:"status"`
	Type           int8   `db:"type"`
	RegistryNumber string `db:"registry_number"`
	UpdatedVersion int32  `json:"upv,omitempty"`
}

type deltaViewSchema struct {
	TableStruct[deltaViewSchema, deltaViewRecord]
	CompanyID      Col[*deltaViewSchema, int32]
	ID             Col[*deltaViewSchema, int32]
	Status         Col[*deltaViewSchema, int8]
	Type           Col[*deltaViewSchema, int8]
	RegistryNumber Col[*deltaViewSchema, string]
	UpdatedVersion Col[*deltaViewSchema, int32]
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

// deltaPackedView finds the compiled delta view and asserts the layout every other test builds on.
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

	// Status {0,1} and Type 1..2 each need one digit, so the implicit version slot keeps 8 and the
	// packed maximum (1_2_99999999) still fits an int.
	expectedSlots := []int64{1, 1, 8}
	if !slices.Equal(view.packedSlotDigitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotDigitsPerColumn)
	}
	if view.column.GetType().DBType != "int" {
		t.Fatalf("expected an int packed column, got %v", view.column.GetType().DBType)
	}
	if !strings.Contains(view.name, "status_type_updated_version") {
		t.Fatalf("expected the implicit updated_version key in the view name, got %v", view.name)
	}

	sourceNames := []string{}
	for _, sourceColumn := range view.packedSourceColumns {
		sourceNames = append(sourceNames, sourceColumn.GetName())
	}
	if !slices.Equal(sourceNames, []string{"status", "type", "updated_version"}) {
		t.Fatalf("expected updated_version appended to the declared keys, got %v", sourceNames)
	}
}

// A wide leading slot overflows int32 even though the digit count is unchanged, because only the
// most significant slot's magnitude decides the fit.
type deltaWideLeadRecord struct {
	TableStruct[deltaWideLeadSchema, deltaWideLeadRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Status         int8  `db:"status"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaWideLeadSchema struct {
	TableStruct[deltaWideLeadSchema, deltaWideLeadRecord]
	CompanyID      Col[*deltaWideLeadSchema, int32]
	ID             Col[*deltaWideLeadSchema, int32]
	Status         Col[*deltaWideLeadSchema, int8]
	Type           Col[*deltaWideLeadSchema, int8]
	UpdatedVersion Col[*deltaWideLeadSchema, int32]
}

func (e deltaWideLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10009,
		Name:      "delta_wide_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Status, Values: []int64{0, 1}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		// Type leads, so the maximum packed value is 2_1_99999999 — past the int32 ceiling.
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Type, e.Status)}},
	}
}

func TestDeltaViewFallsBackToBigintAndWidensUpdated(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaWideLeadRecord, deltaWideLeadSchema]()
	view := deltaPackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column when the leading slot overflows, got %v", view.column.GetType().DBType)
	}
	// The extra digits are already paid for, so they are spent on sequence headroom.
	expectedSlots := []int64{1, 1, 10}
	if !slices.Equal(view.packedSlotDigitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotDigitsPerColumn)
	}
}

// A single declared key reproduces the layout the equivalent hand-decorated TypeView produced.
type deltaSingleKeyRecord struct {
	TableStruct[deltaSingleKeySchema, deltaSingleKeyRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaSingleKeySchema struct {
	TableStruct[deltaSingleKeySchema, deltaSingleKeyRecord]
	CompanyID      Col[*deltaSingleKeySchema, int32]
	ID             Col[*deltaSingleKeySchema, int32]
	Type           Col[*deltaSingleKeySchema, int8]
	UpdatedVersion Col[*deltaSingleKeySchema, int32]
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

func TestDeltaViewMatchesEquivalentDecoratedTypeView(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaSingleKeyRecord, deltaSingleKeySchema]()
	view := deltaPackedView(t, scyllaTable)

	// A single declared key plus the 8-digit version slot resolves to the same [1, 8] layout the
	// hand-decorated TypeView produced.
	expectedSlots := []int64{1, 8}
	if !slices.Equal(view.packedSlotDigitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotDigitsPerColumn)
	}
	if view.column.GetType().DBType != "int" {
		t.Fatalf("expected an int packed column, got %v", view.column.GetType().DBType)
	}
	if !strings.HasSuffix(view.name, "pk_type_updated_version_rng_view") {
		t.Fatalf("expected the same view name the decorated TypeView produced, got %v", view.name)
	}
}

// A key with no FixedValues and no DecimalSize absorbs the digit remainder instead of being
// rejected: the layout goes bigint and the version slot pins to deltaVersionDigitsElastic.
type deltaElasticRecord struct {
	TableStruct[deltaElasticSchema, deltaElasticRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Status         int8  `db:"status"`
	ListID         int32 `db:"list_id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaElasticSchema struct {
	TableStruct[deltaElasticSchema, deltaElasticRecord]
	CompanyID      Col[*deltaElasticSchema, int32]
	ID             Col[*deltaElasticSchema, int32]
	Status         Col[*deltaElasticSchema, int8]
	ListID         Col[*deltaElasticSchema, int32]
	UpdatedVersion Col[*deltaElasticSchema, int32]
}

func (e deltaElasticSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10011,
		Name:        "delta_elastic_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		// ListID has no declared ceiling, so it takes every digit the others leave over.
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Status, e.ListID)}},
	}
}

func TestDeltaViewLetsAnUndeclaredKeyAbsorbTheDigitRemainder(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaElasticRecord, deltaElasticSchema]()
	view := deltaPackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column for an elastic layout, got %v", view.column.GetType().DBType)
	}
	// 1 digit for the declared Status, 9 for the version, and the remaining 8 of the 18-digit
	// int64 budget for ListID.
	expectedSlots := []int64{1, 8, 9}
	if !slices.Equal(view.packedSlotDigitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotDigitsPerColumn)
	}
	if scyllaTable.maxDeltaVersionValue != 999_999_999 {
		t.Fatalf("expected the 9-digit version ceiling, got %v", scyllaTable.maxDeltaVersionValue)
	}
}

// A single elastic key as Keys[0]: the whole remainder is its own, and the watermark is the only
// thing a Delta() read can constrain.
type deltaElasticLeadRecord struct {
	TableStruct[deltaElasticLeadSchema, deltaElasticLeadRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	ListID         int32 `db:"list_id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaElasticLeadSchema struct {
	TableStruct[deltaElasticLeadSchema, deltaElasticLeadRecord]
	CompanyID      Col[*deltaElasticLeadSchema, int32]
	ID             Col[*deltaElasticLeadSchema, int32]
	ListID         Col[*deltaElasticLeadSchema, int32]
	UpdatedVersion Col[*deltaElasticLeadSchema, int32]
}

func (e deltaElasticLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10026,
		Name:      "delta_elastic_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta, Keys: Cols(e.ListID)}},
	}
}

func TestDeltaViewSizesALoneElasticKey(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaElasticLeadRecord, deltaElasticLeadSchema]()
	view := deltaPackedView(t, scyllaTable)

	// Nothing else claims a slot, so ListID takes 9 of the 18 digits and the version takes 9.
	expectedSlots := []int64{9, 9}
	if !slices.Equal(view.packedSlotDigitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotDigitsPerColumn)
	}
}

// No declared keys at all: a sync that filters nothing but its watermark.
type deltaWatermarkOnlyRecord struct {
	TableStruct[deltaWatermarkOnlySchema, deltaWatermarkOnlyRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaWatermarkOnlySchema struct {
	TableStruct[deltaWatermarkOnlySchema, deltaWatermarkOnlyRecord]
	CompanyID      Col[*deltaWatermarkOnlySchema, int32]
	ID             Col[*deltaWatermarkOnlySchema, int32]
	UpdatedVersion Col[*deltaWatermarkOnlySchema, int32]
}

func (e deltaWatermarkOnlySchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10027,
		Name:      "delta_watermark_only_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta}},
	}
}

func TestDeltaViewWithNoKeysIsAPlainUpdatedVersionView(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaWatermarkOnlyRecord, deltaWatermarkOnlySchema]()

	// Nothing to pack, so this must be a plain view keyed on updated_version — not a Type 8 packed
	// range view.
	var watermarkView *viewInfo
	for _, view := range scyllaTable.views {
		if slices.Equal(view.columnsNoPart, []string{"updated_version"}) {
			watermarkView = view
		}
		if view.Type == 8 {
			t.Fatalf("expected no packed range view, got %v", view.name)
		}
	}
	if watermarkView == nil {
		t.Fatal("expected a view keyed on updated_version")
	}
	// No digit slot means no trimming, so writes keep the column's full range.
	if scyllaTable.maxDeltaVersionValue != 0 {
		t.Fatalf("expected no version ceiling for an unpacked delta view, got %v", scyllaTable.maxDeltaVersionValue)
	}
}

func TestDeltaWithFilterValuesRejectsAKeylessDeltaIndex(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected Delta() to panic when given filter values with no key to filter on")
		}
		if !strings.Contains(fmt.Sprint(recovered), "unpinned") {
			t.Fatalf("expected the panic to explain the missing key, got: %v", recovered)
		}
	}()

	records := []deltaWatermarkOnlyRecord{}
	Query[deltaWatermarkOnlyRecord, deltaWatermarkOnlySchema](&records).Delta(100, 1)
}

// Two undeclared keys leave no unambiguous way to split the remainder.
type deltaTwoElasticRecord struct {
	TableStruct[deltaTwoElasticSchema, deltaTwoElasticRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	ListID         int32 `db:"list_id"`
	GroupID        int32 `db:"group_id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaTwoElasticSchema struct {
	TableStruct[deltaTwoElasticSchema, deltaTwoElasticRecord]
	CompanyID      Col[*deltaTwoElasticSchema, int32]
	ID             Col[*deltaTwoElasticSchema, int32]
	ListID         Col[*deltaTwoElasticSchema, int32]
	GroupID        Col[*deltaTwoElasticSchema, int32]
	UpdatedVersion Col[*deltaTwoElasticSchema, int32]
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
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Channel        int32 `db:"channel"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaForcedInt32Schema struct {
	TableStruct[deltaForcedInt32Schema, deltaForcedInt32Record]
	CompanyID      Col[*deltaForcedInt32Schema, int32]
	ID             Col[*deltaForcedInt32Schema, int32]
	Channel        Col[*deltaForcedInt32Schema, int32]
	Type           Col[*deltaForcedInt32Schema, int8]
	UpdatedVersion Col[*deltaForcedInt32Schema, int32]
}

func (e deltaForcedInt32Schema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10012,
		Name:      "delta_forced_int32_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Channel, Min: 1, Max: 5000},
			{Col: e.Type, Min: 1, Max: 2},
		},
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
		if !strings.Contains(fmt.Sprint(recovered), "2147483647") {
			t.Fatalf("expected the panic to report the int32 limit, got: %v", recovered)
		}
	}()

	MakeScyllaTable[deltaForcedInt32Record, deltaForcedInt32Schema]()
}

// ─── Delta() query shape ───────────────────────────────────────────────────────

func packDeltaValue(t *testing.T, view *viewInfo, componentValues ...int64) int64 {
	t.Helper()
	return computePackedInt64ValueNonNegative(componentValues, view.packedSlotDigitsPerColumn)
}

func TestDeltaFirstSyncPinsFilterColumn(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Type.Equals(int8(2))
	query.Delta(0, 1)

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected a single range clause for a first sync, got %d", len(whereStatements))
	}
	// Status pinned to 1 and Type to 2 leaves the whole version slot open. The lower bound is
	// version 1 rather than 0 because Delta() is exclusive, and no record ever carries version 0.
	expectedValues := []any{
		packDeltaValue(t, view, 1, 2, 1),
		packDeltaValue(t, view, 1, 2, 0) + 100_000_000,
	}
	assertClauseValues(t, whereStatements[0], expectedValues)
}

func TestDeltaSyncFansOutOverEveryDeclaredValue(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Type.Equals(int8(2))
	query.Delta(390_698_501, 1)

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	// Both declared Status values are scanned so rows flipped to 0 still reach the client.
	if len(whereStatements) != 2 {
		t.Fatalf("expected one range clause per declared status, got %d", len(whereStatements))
	}
	// The watermark is trimmed to the 8-digit slot, which floors it onto its 20-second bucket.
	trimmedWatermark := trimRightToDigitsNonNegative(390_698_501, 8)
	for statusIndex, statusValue := range []int64{0, 1} {
		expectedValues := []any{
			packDeltaValue(t, view, statusValue, 2, trimmedWatermark),
			packDeltaValue(t, view, statusValue, 2, 0) + 100_000_000,
		}
		assertClauseValues(t, whereStatements[statusIndex], expectedValues)
	}
}

// Delta() reads the predicates already on the query to choose its index, so calling it before the
// key columns are pinned cannot resolve — and must say so rather than route to the wrong view.
func TestDeltaRejectsBeingCalledBeforeTheKeyColumnsArePinned(t *testing.T) {
	resetORMTableCachesForTesting()

	MakeScyllaTable[deltaViewRecord, deltaViewSchema]()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected Delta() to panic when no index leaves exactly one key unpinned")
		}
		if !strings.Contains(fmt.Sprint(recovered), "unpinned") {
			t.Fatalf("expected the panic to explain the unpinned keys, got: %v", recovered)
		}
	}()

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	// "type" is still open, so both keys of the only delta index are unpinned.
	query.Delta(0, 1)
}

// The sync-filter column is the key the query left unpinned, never looked up by name.
type deltaInferredRecord struct {
	TableStruct[deltaInferredSchema, deltaInferredRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Channel        int8  `db:"channel"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaInferredSchema struct {
	TableStruct[deltaInferredSchema, deltaInferredRecord]
	CompanyID      Col[*deltaInferredSchema, int32]
	ID             Col[*deltaInferredSchema, int32]
	Channel        Col[*deltaInferredSchema, int8]
	Type           Col[*deltaInferredSchema, int8]
	UpdatedVersion Col[*deltaInferredSchema, int32]
}

func (e deltaInferredSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10013,
		Name:      "delta_inferred_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Channel, Values: []int64{0, 1}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		Indexes: []Index{{Type: TypeDelta, Keys: Cols(e.Channel, e.Type)}},
	}
}

func TestDeltaInfersFilterColumnFromTheUnpinnedKey(t *testing.T) {
	resetORMTableCachesForTesting()

	MakeScyllaTable[deltaInferredRecord, deltaInferredSchema]()

	records := []deltaInferredRecord{}
	query := Query[deltaInferredRecord, deltaInferredSchema](&records)
	query.CompanyID.Equals(7)
	// Pinning "type" leaves "channel" as the only open key, so that is what Delta() filters.
	query.Type.Equals(int8(2))
	query.Delta(0, 1)

	filteredColumns := []string{}
	for _, statement := range query.GetTableInfo().Statements {
		filteredColumns = append(filteredColumns, statement.Col)
	}
	if !slices.Contains(filteredColumns, "channel") {
		t.Fatalf("expected Delta() to filter the leading key \"channel\", got %v", filteredColumns)
	}
	if slices.Contains(filteredColumns, "status") {
		t.Fatalf("expected no name-based status lookup, got %v", filteredColumns)
	}
}

// Two delta indexes for two read shapes, as warehouse_product_stock declares them.
type deltaMultiShapeRecord struct {
	TableStruct[deltaMultiShapeSchema, deltaMultiShapeRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	WarehouseID    int32 `db:"warehouse_id"`
	Status         int8  `db:"status"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaMultiShapeSchema struct {
	TableStruct[deltaMultiShapeSchema, deltaMultiShapeRecord]
	CompanyID      Col[*deltaMultiShapeSchema, int32]
	ID             Col[*deltaMultiShapeSchema, int32]
	WarehouseID    Col[*deltaMultiShapeSchema, int32]
	Status         Col[*deltaMultiShapeSchema, int8]
	UpdatedVersion Col[*deltaMultiShapeSchema, int32]
}

func (e deltaMultiShapeSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10028,
		Name:        "delta_multi_shape_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		Indexes: []Index{
			{Type: TypeDelta, Keys: Cols(e.WarehouseID.DecimalSize(5), e.Status)},
			{Type: TypeDelta, Keys: Cols(e.Status)},
		},
	}
}

func deltaFilteredColumns(query *deltaMultiShapeSchema) []string {
	filteredColumns := []string{}
	for _, statement := range query.GetTableInfo().Statements {
		filteredColumns = append(filteredColumns, statement.Col)
	}
	return filteredColumns
}

func TestDeltaPicksTheWiderIndexWhenTheQueryPinsItsKey(t *testing.T) {
	resetORMTableCachesForTesting()

	MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()

	records := []deltaMultiShapeRecord{}
	query := Query[deltaMultiShapeRecord, deltaMultiShapeSchema](&records)
	query.CompanyID.Equals(7)
	// warehouse_id pinned → the [warehouse_id, status] index fits, leaving status as the filter.
	query.WarehouseID.Equals(int32(42))
	query.Delta(0, 1)

	filteredColumns := deltaFilteredColumns(query)
	for _, expectedColumn := range []string{"warehouse_id", "status", "updated_version"} {
		if !slices.Contains(filteredColumns, expectedColumn) {
			t.Fatalf("expected %q to be constrained, got %v", expectedColumn, filteredColumns)
		}
	}
}

func TestDeltaFallsBackToTheNarrowIndexWhenItsKeyIsOpen(t *testing.T) {
	resetORMTableCachesForTesting()

	MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()

	records := []deltaMultiShapeRecord{}
	query := Query[deltaMultiShapeRecord, deltaMultiShapeSchema](&records)
	query.CompanyID.Equals(7)
	// warehouse_id left open → only the [status] index fits, and nothing constrains warehouse_id.
	query.Delta(0, 1)

	filteredColumns := deltaFilteredColumns(query)
	if slices.Contains(filteredColumns, "warehouse_id") {
		t.Fatalf("expected no warehouse_id predicate, got %v", filteredColumns)
	}
	if !slices.Contains(filteredColumns, "status") {
		t.Fatalf("expected status to be the sync filter, got %v", filteredColumns)
	}
}

// A delta index is an abstraction over a packed TypeView, so a hand-written query that pins the key
// and ranges on updated_version must reach it without going through Delta(). That is what lets a
// handler scan a specific status bucket — for an eviction list, say — instead of the fan-out.
func TestDeltaViewServesAPlainQueryWithoutDelta(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaMultiShapeRecord, deltaMultiShapeSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []deltaMultiShapeRecord{}
	query := Query[deltaMultiShapeRecord, deltaMultiShapeSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(0))
	query.UpdatedVersion.GreaterEqual(int32(100))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if compiledStatement.sourceView == nil {
		t.Fatal("expected a delta view to serve the plain query, got a base-table read")
	}
	if !strings.Contains(compiledStatement.sourceView.name, "status_updated_version") {
		t.Fatalf("expected the [status, updated_version] delta view, got %v", compiledStatement.sourceView.name)
	}
}

// A table with no delta index at all.
type deltaMissingIndexRecord struct {
	TableStruct[deltaMissingIndexSchema, deltaMissingIndexRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaMissingIndexSchema struct {
	TableStruct[deltaMissingIndexSchema, deltaMissingIndexRecord]
	CompanyID      Col[*deltaMissingIndexSchema, int32]
	ID             Col[*deltaMissingIndexSchema, int32]
	UpdatedVersion Col[*deltaMissingIndexSchema, int32]
}

func (e deltaMissingIndexSchema) GetSchema() TableSchema {
	return TableSchema{Name: "delta_missing_index_records", Partition: e.CompanyID, Keys: Cols(e.ID)}
}

func TestDeltaRequiresADeltaIndex(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected Delta() to panic without a TypeDelta index")
		}
		if !strings.Contains(fmt.Sprint(recovered), "TypeDelta") {
			t.Fatalf("expected the panic to name the missing declaration, got: %v", recovered)
		}
	}()

	records := []deltaMissingIndexRecord{}
	Query[deltaMissingIndexRecord, deltaMissingIndexSchema](&records).Delta(0, 1)
}

func TestDeltaRequiresFixedValuesForItsFilterColumn(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected Delta() to panic without FixedValues on its filter column")
		}
		if !strings.Contains(fmt.Sprint(recovered), "FixedValues") {
			t.Fatalf("expected the panic to name FixedValues, got: %v", recovered)
		}
	}()

	records := []deltaElasticLeadRecord{}
	// A delta sync must enumerate the filter column, which needs a declared range. An elastic
	// leading key compiles fine, but only Delta() with no filter values can use it.
	Query[deltaElasticLeadRecord, deltaElasticLeadSchema](&records).Delta(100, 1)
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
	// Status = 1 spans every Type and every version. The block ends at 2_0_00000000, but no row can
	// pack past the declared maximum, so the bound is capped there — which also keeps it inside the
	// int packed column.
	assertClauseValues(t, whereStatements[0], []any{
		packDeltaValue(t, view, 1, 0, 0),
		packDeltaValue(t, view, 1, 2, 99_999_999) + 1,
	})
}

func TestPackedViewCapsUpperBoundAtTheColumnCeiling(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	// A range on the leading key spans the whole packed width, whose natural exclusive bound is
	// 10^10 — past what an int column holds.
	query.Status.GreaterEqual(int8(1))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one range clause, got %d", len(whereStatements))
	}
	upperBound := whereStatements[0].Values[len(whereStatements[0].Values)-1].(int64)
	if upperBound > math.MaxInt32 {
		t.Fatalf("upper bound %d does not fit the int packed column", upperBound)
	}
	if upperBound != packDeltaValue(t, view, 1, 2, 99_999_999)+1 {
		t.Fatalf("expected the bound capped at the declared maximum, got %d", upperBound)
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
		t.Fatalf("expected one prefix range, got %d", len(whereStatements))
	}
	assertClauseValues(t, whereStatements[0], []any{
		packDeltaValue(t, view, 1, 2, 0),
		packDeltaValue(t, view, 1, 3, 0),
	})
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
	query.Type.Equals(int8(2))
	query.UpdatedVersion.Between(int32(10_000_000), int32(20_000_000))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected a single BETWEEN clause, got %d", len(whereStatements))
	}
	assertClauseValues(t, whereStatements[0], []any{
		packDeltaValue(t, view, 1, 2, 10_000_000),
		packDeltaValue(t, view, 1, 2, 20_000_000) + 1,
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
	longPrefix := prioritiesBySignature["company_id|=|status|=|type|="]
	fullKey := prioritiesBySignature["company_id|=|status|=|type|=|updated_version|~"]

	for signature, priority := range map[string]int{
		"company_id|=": partitionOnly, "company_id|=|registry_number|=": localIndex,
		"company_id|=|status|=": shortPrefix, "company_id|=|status|=|type|=": longPrefix,
		"company_id|=|status|=|type|=|updated_version|~": fullKey,
	} {
		if priority == 0 {
			t.Fatalf("expected a capability for %q", signature)
		}
	}

	// A narrow index must win over a broad prefix scan, or its predicate becomes an unservable
	// leftover filter on the view.
	if !(partitionOnly < shortPrefix && shortPrefix < longPrefix && longPrefix < localIndex && localIndex < fullKey) {
		t.Fatalf("unexpected priority ordering: partition=%d shortPrefix=%d longPrefix=%d localIndex=%d fullKey=%d",
			partitionOnly, shortPrefix, longPrefix, localIndex, fullKey)
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

// boundPackedRanges extracts the packed [from, to) bounds of every bound statement, dropping the
// partition value each clause is prefixed with.
func boundPackedRanges(t *testing.T, boundPlan *BoundSelectPlan) [][2]int64 {
	t.Helper()
	ranges := make([][2]int64, 0, len(boundPlan.Statements))
	for _, boundStatement := range boundPlan.Statements {
		if len(boundStatement.QueryValues) < 2 {
			t.Fatalf("expected packed bounds in %q, got %v", boundStatement.QueryStr, boundStatement.QueryValues)
		}
		packedValues := boundStatement.QueryValues[len(boundStatement.QueryValues)-2:]
		ranges = append(ranges, [2]int64{convertToInt64(packedValues[0]), convertToInt64(packedValues[1])})
	}
	slices.SortFunc(ranges, func(left, right [2]int64) int { return int(left[0] - right[0]) })
	return ranges
}

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
	// Slots are [1,1,8], so status s with type 1 spans [s*10^9 + 10^8, s*10^9 + 2*10^8).
	expectedRanges := [][2]int64{{100_000_000, 200_000_000}, {1_100_000_000, 1_200_000_000}}
	if !slices.Equal(boundPackedRanges(t, boundPlan), expectedRanges) {
		t.Fatalf("expected packed ranges %v, got %v", expectedRanges, boundPackedRanges(t, boundPlan))
	}
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
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Status         int8  `db:"status"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaSparseValuesSchema struct {
	TableStruct[deltaSparseValuesSchema, deltaSparseValuesRecord]
	CompanyID      Col[*deltaSparseValuesSchema, int32]
	ID             Col[*deltaSparseValuesSchema, int32]
	Status         Col[*deltaSparseValuesSchema, int8]
	Type           Col[*deltaSparseValuesSchema, int8]
	UpdatedVersion Col[*deltaSparseValuesSchema, int32]
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
	if len(boundPlan.Statements) != 3 {
		t.Fatalf("expected 3 fanned-out queries, got %d", len(boundPlan.Statements))
	}
	// A status reaching 9 pushes the layout to a bigint, so the version slot widens to 10 digits.
	expectedRanges := [][2]int64{{10_000_000_000, 20_000_000_000}, {510_000_000_000, 520_000_000_000},
		{910_000_000_000, 920_000_000_000}}
	if !slices.Equal(boundPackedRanges(t, boundPlan), expectedRanges) {
		t.Fatalf("expected packed ranges %v, got %v", expectedRanges, boundPackedRanges(t, boundPlan))
	}
}

// A column too wide to enumerate keeps its gap: fanning 501 values out would cost far more than the
// scan it replaces.
type deltaUnenumerableLeadRecord struct {
	TableStruct[deltaUnenumerableLeadSchema, deltaUnenumerableLeadRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Kind           int16 `db:"kind"`
	Status         int8  `db:"status"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaUnenumerableLeadSchema struct {
	TableStruct[deltaUnenumerableLeadSchema, deltaUnenumerableLeadRecord]
	CompanyID      Col[*deltaUnenumerableLeadSchema, int32]
	ID             Col[*deltaUnenumerableLeadSchema, int32]
	Kind           Col[*deltaUnenumerableLeadSchema, int16]
	Status         Col[*deltaUnenumerableLeadSchema, int8]
	UpdatedVersion Col[*deltaUnenumerableLeadSchema, int32]
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
