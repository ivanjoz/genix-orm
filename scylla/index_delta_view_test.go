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
	CompanyID      Col[deltaViewSchema, int32]
	ID             Col[deltaViewSchema, int32]
	Status         Col[deltaViewSchema, int8]
	Type           Col[deltaViewSchema, int8]
	RegistryNumber Col[deltaViewSchema, string]
	UpdatedVersion Col[deltaViewSchema, int32]
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
	CompanyID      Col[deltaWideLeadSchema, int32]
	ID             Col[deltaWideLeadSchema, int32]
	Status         Col[deltaWideLeadSchema, int8]
	Type           Col[deltaWideLeadSchema, int8]
	UpdatedVersion Col[deltaWideLeadSchema, int32]
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
	CompanyID      Col[deltaSingleKeySchema, int32]
	ID             Col[deltaSingleKeySchema, int32]
	Type           Col[deltaSingleKeySchema, int8]
	UpdatedVersion Col[deltaSingleKeySchema, int32]
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

// Missing FixedValues leaves the digit slot unsizeable.
type deltaUnsizedRecord struct {
	TableStruct[deltaUnsizedSchema, deltaUnsizedRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	Type           int8  `db:"type"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaUnsizedSchema struct {
	TableStruct[deltaUnsizedSchema, deltaUnsizedRecord]
	CompanyID      Col[deltaUnsizedSchema, int32]
	ID             Col[deltaUnsizedSchema, int32]
	Type           Col[deltaUnsizedSchema, int8]
	UpdatedVersion Col[deltaUnsizedSchema, int32]
}

func (e deltaUnsizedSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10011,
		Name:      "delta_unsized_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeDelta, Keys: Cols(e.Type)}},
	}
}

func TestDeltaViewRejectsKeyWithoutDeclaredRange(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected a panic naming the key that has no declared range")
		}
		if !strings.Contains(fmt.Sprint(recovered), `"type"`) {
			t.Fatalf("expected the panic to name the column, got: %v", recovered)
		}
	}()

	MakeScyllaTable[deltaUnsizedRecord, deltaUnsizedSchema]()
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
	CompanyID      Col[deltaForcedInt32Schema, int32]
	ID             Col[deltaForcedInt32Schema, int32]
	Channel        Col[deltaForcedInt32Schema, int32]
	Type           Col[deltaForcedInt32Schema, int8]
	UpdatedVersion Col[deltaForcedInt32Schema, int32]
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

func TestDeltaComposesWhenFollowedByAnotherPredicate(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[deltaViewRecord, deltaViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := deltaPackedView(t, scyllaTable)

	records := []deltaViewRecord{}
	query := Query[deltaViewRecord, deltaViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Delta(0, 1)
	// Delta() is no longer required to be the last predicate in the chain.
	query.Type.Equals(int8(2))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected a single range clause, got %d", len(whereStatements))
	}
	if !strings.Contains(whereStatements[0].Clause, ">=") {
		t.Fatalf("expected a range clause regardless of predicate order, got %q", whereStatements[0].Clause)
	}
}

// The sync-filter column is inferred from Keys[0], never looked up by name.
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
	CompanyID      Col[deltaInferredSchema, int32]
	ID             Col[deltaInferredSchema, int32]
	Channel        Col[deltaInferredSchema, int8]
	Type           Col[deltaInferredSchema, int8]
	UpdatedVersion Col[deltaInferredSchema, int32]
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

func TestDeltaInfersFilterColumnFromLeadingKey(t *testing.T) {
	resetORMTableCachesForTesting()

	MakeScyllaTable[deltaInferredRecord, deltaInferredSchema]()

	records := []deltaInferredRecord{}
	query := Query[deltaInferredRecord, deltaInferredSchema](&records)
	query.CompanyID.Equals(7)
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

// A table with no delta index at all.
type deltaMissingIndexRecord struct {
	TableStruct[deltaMissingIndexSchema, deltaMissingIndexRecord]
	CompanyID      int32 `db:"company_id"`
	ID             int32 `db:"id"`
	UpdatedVersion int32 `json:"upv,omitempty"`
}

type deltaMissingIndexSchema struct {
	TableStruct[deltaMissingIndexSchema, deltaMissingIndexRecord]
	CompanyID      Col[deltaMissingIndexSchema, int32]
	ID             Col[deltaMissingIndexSchema, int32]
	UpdatedVersion Col[deltaMissingIndexSchema, int32]
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

	records := []deltaUnsizedRecord{}
	// A delta sync must enumerate the filter column, which needs a declared range.
	Query[deltaUnsizedRecord, deltaUnsizedSchema](&records).Delta(100, 1)
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
