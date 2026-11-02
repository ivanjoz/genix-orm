package scylla

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// The fixture mirrors client_provider: a tenant partition, a two-value Status that leads the
// packed key, and a narrow Type. RegistryNumber carries a local index so the selectivity tiering in
// ComputeCapabilities can be exercised.
type fixedViewViewRecord struct {
	TableStruct[fixedViewViewSchema, fixedViewViewRecord]
	CompanyID      int32   `db:"company_id"`
	ID             int32   `db:"id"`
	Status         int8    `db:"status"`
	Type           int8    `db:"type"`
	RegistryNumber string  `db:"registry_number"`
	Version        int16   `json:"ver,omitempty"`
	Metadata       *string `json:"mdt,omitempty"`
}

type fixedViewViewSchema struct {
	TableStruct[fixedViewViewSchema, fixedViewViewRecord]
	CompanyID      Col[*fixedViewViewSchema, int32]
	ID             Col[*fixedViewViewSchema, int32]
	Status         Col[*fixedViewViewSchema, int8]
	Type           Col[*fixedViewViewSchema, int8]
	RegistryNumber Col[*fixedViewViewSchema, string]
	Version        Col[*fixedViewViewSchema, int16]
}

func (e fixedViewViewSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10008,
		Name:      "fixed_view_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Status, Values: []int64{0, 1}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		Indexes: []Index{
			{Type: TypeLocalIndex, Keys: Cols(e.RegistryNumber)},
			{Type: TypeView, Keys: Cols(e.Status, e.Type)},
		},
	}
}

// fixedValuePackedView finds the compiled FixedValues packed view and asserts the layout every other test
// builds on.
func fixedValuePackedView(t *testing.T, scyllaTable ScyllaTable) *viewInfo {
	t.Helper()
	for _, view := range scyllaTable.views {
		if view.Type == 8 {
			return view
		}
	}
	t.Fatal("expected a packed range view compiled from the FixedValues-sized TypeView")
	return nil
}

func TestFixedValueViewSizesSlotsFromFixedValues(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	view := fixedValuePackedView(t, scyllaTable)

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
}

// Keys whose slots sum past the int's 32 bits spill to a bigint.
type fixedViewWideLeadRecord struct {
	TableStruct[fixedViewWideLeadSchema, fixedViewWideLeadRecord]
	CompanyID int32 `db:"company_id"`
	ID        int64 `db:"id"`
	Code      int64 `db:"code"`
	Status    int8  `db:"status"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewWideLeadSchema struct {
	TableStruct[fixedViewWideLeadSchema, fixedViewWideLeadRecord]
	CompanyID Col[*fixedViewWideLeadSchema, int32]
	ID        Col[*fixedViewWideLeadSchema, int64]
	Code      Col[*fixedViewWideLeadSchema, int64]
	Status    Col[*fixedViewWideLeadSchema, int8]
	Version   Col[*fixedViewWideLeadSchema, int16]
}

func (e fixedViewWideLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10009,
		Name:      "fixed_view_wide_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Code, Min: 1, Max: 1 << 40},
			{Col: e.Status, Values: []int64{0, 1}},
		},
		// Code takes 41 bits: with Status's one, past the int's 32.
		Indexes: []Index{{Type: TypeView, Keys: Cols(e.Code, e.Status)}},
	}
}

func TestFixedValueViewFallsBackToBigint(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewWideLeadRecord, fixedViewWideLeadSchema]()
	view := fixedValuePackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column when the keys overflow an int, got %v", view.column.GetType().DBType)
	}
	if expectedSlots := []int64{41, 1}; !slices.Equal(view.packedSlotBitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotBitsPerColumn)
	}
}

// A key with no FixedValues and no Size absorbs the bit remainder instead of being rejected: the
// layout goes bigint.
type fixedViewElasticRecord struct {
	TableStruct[fixedViewElasticSchema, fixedViewElasticRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Status    int8  `db:"status"`
	ListID    int32 `db:"list_id"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewElasticSchema struct {
	TableStruct[fixedViewElasticSchema, fixedViewElasticRecord]
	CompanyID Col[*fixedViewElasticSchema, int32]
	ID        Col[*fixedViewElasticSchema, int32]
	Status    Col[*fixedViewElasticSchema, int8]
	ListID    Col[*fixedViewElasticSchema, int32]
	Version   Col[*fixedViewElasticSchema, int16]
}

func (e fixedViewElasticSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:          10011,
		Name:        "fixed_view_elastic_records",
		Partition:   e.CompanyID,
		Keys:        Cols(e.ID),
		FixedValues: []FixedValues{{Col: e.Status, Values: []int64{0, 1}}},
		// ListID has no declared ceiling, so it takes every bit the others leave over.
		Indexes: []Index{{Type: TypeView, Keys: Cols(e.ListID, e.Status)}},
	}
}

func TestFixedValueViewLetsAnUndeclaredKeyAbsorbTheBitRemainder(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewElasticRecord, fixedViewElasticSchema]()
	view := fixedValuePackedView(t, scyllaTable)

	if view.column.GetType().DBType != "bigint" {
		t.Fatalf("expected a bigint packed column for an elastic layout, got %v", view.column.GetType().DBType)
	}
	// 1 bit for the declared Status and the remaining 63 of the 64-bit budget for ListID.
	if expectedSlots := []int64{63, 1}; !slices.Equal(view.packedSlotBitsPerColumn, expectedSlots) {
		t.Fatalf("expected slot widths %v, got %v", expectedSlots, view.packedSlotBitsPerColumn)
	}
}

// Two undeclared keys leave no unambiguous way to split the remainder: the view stays a hash view.
type fixedViewTwoElasticRecord struct {
	TableStruct[fixedViewTwoElasticSchema, fixedViewTwoElasticRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	ListID    int32 `db:"list_id"`
	GroupID   int32 `db:"group_id"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewTwoElasticSchema struct {
	TableStruct[fixedViewTwoElasticSchema, fixedViewTwoElasticRecord]
	CompanyID Col[*fixedViewTwoElasticSchema, int32]
	ID        Col[*fixedViewTwoElasticSchema, int32]
	ListID    Col[*fixedViewTwoElasticSchema, int32]
	GroupID   Col[*fixedViewTwoElasticSchema, int32]
	Version   Col[*fixedViewTwoElasticSchema, int16]
}

func (e fixedViewTwoElasticSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10025,
		Name:      "fixed_view_two_elastic_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		Indexes:   []Index{{Type: TypeView, Keys: Cols(e.ListID, e.GroupID)}},
	}
}

func TestFixedValueViewWithTwoUndeclaredKeysStaysAHashView(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewTwoElasticRecord, fixedViewTwoElasticSchema]()
	for _, view := range scyllaTable.views {
		if view.Type != 7 {
			t.Fatalf("expected a hash view, got type %d (%v)", view.Type, view.name)
		}
	}
}

// A forced .Int32() on a budget that cannot fit must fail rather than silently overflow.
type fixedViewForcedInt32Record struct {
	TableStruct[fixedViewForcedInt32Schema, fixedViewForcedInt32Record]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Channel   int32 `db:"channel"`
	Type      int8  `db:"type"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewForcedInt32Schema struct {
	TableStruct[fixedViewForcedInt32Schema, fixedViewForcedInt32Record]
	CompanyID Col[*fixedViewForcedInt32Schema, int32]
	ID        Col[*fixedViewForcedInt32Schema, int32]
	Channel   Col[*fixedViewForcedInt32Schema, int32]
	Type      Col[*fixedViewForcedInt32Schema, int8]
	Version   Col[*fixedViewForcedInt32Schema, int16]
}

func (e fixedViewForcedInt32Schema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10012,
		Name:      "fixed_view_forced_int32_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Channel, Min: 1, Max: 1 << 30},
			{Col: e.Type, Min: 1, Max: 2},
		},
		// Channel takes 31 bits and Type 2: past the int's 32.
		Indexes: []Index{{Type: TypeView, Keys: Cols(e.Channel.Int32(), e.Type)}},
	}
}

func TestFixedValueViewRejectsForcedInt32ThatDoesNotFit(t *testing.T) {
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

	MakeScyllaTable[fixedViewForcedInt32Record, fixedViewForcedInt32Schema]()
}

// ─── Reads ─────────────────────────────────────────────────────────────

// packFixedValueViewValue packs a full key the way the view stores it, sign bit flipped.
func packFixedValueViewValue(t *testing.T, view *viewInfo, componentValues ...int64) int64 {
	t.Helper()
	return convertToInt64(storeVirtualPacked(db.PackSlotValues(componentValues, view.packedSlotBitsPerColumn), view.packedIsInt32))
}

// packFixedValueViewUpperBound is the stored inclusive upper bound of the block a prefix pins.
func packFixedValueViewUpperBound(t *testing.T, view *viewInfo, prefixValues ...int64) int64 {
	t.Helper()
	return convertToInt64(storeVirtualPacked(db.PackSlotPrefixBound(prefixValues, view.packedSlotBitsPerColumn, true), view.packedIsInt32))
}

// Every key pinned, one by IN: the packed view serves it with one exact IN over the packed values.
func TestFixedValueViewServesAFullKeyIn(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	statements := []ColumnStatement{
		{Col: "company_id", Operator: "=", Value: int32(7)},
		{Col: "status", Operator: "=", Value: int8(1)},
		{Col: "type", Operator: "IN", Values: []any{int8(1), int8(2)}},
	}
	whereStatements := view.getStatementPrepared(statements...)
	if len(whereStatements) != 1 || !strings.Contains(whereStatements[0].Clause, "IN (?, ?)") {
		t.Fatalf("expected one IN clause, got %+v", whereStatements)
	}
	assertClauseValues(t, whereStatements[0], []any{packFixedValueViewValue(t, view, 1, 1), packFixedValueViewValue(t, view, 1, 2)})
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

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
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
		packFixedValueViewValue(t, view, 1, 0),
		packFixedValueViewUpperBound(t, view, 1),
	})
}

func TestPackedViewUpperBoundIsTheLayoutMaximum(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
	query.CompanyID.Equals(7)
	// A range on the leading key spans the whole packed width. Its inclusive upper bound is every
	// slot at its max, bound as an int32 so it fits the int packed column.
	query.Status.GreaterEqual(int8(1))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one range clause, got %d", len(whereStatements))
	}
	upperBound, isInt32 := whereStatements[0].Values[len(whereStatements[0].Values)-1].(int32)
	if !isInt32 || int64(upperBound) != packFixedValueViewUpperBound(t, view) {
		t.Fatalf("expected the int32 layout maximum as the upper bound, got %v", whereStatements[0].Values)
	}
}

func TestPackedViewServesTwoColumnPrefix(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.Type.Equals(int8(2))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected one clause, got %d", len(whereStatements))
	}
	// Every key pinned: one exact packed value.
	assertClauseValues(t, whereStatements[0], []any{packFixedValueViewValue(t, view, 1, 2)})
}

func TestPackedViewFansOutPrefixInValues(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.In(int8(0), int8(1))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 2 {
		t.Fatalf("expected one prefix range per IN value, got %d", len(whereStatements))
	}
}

func TestPackedViewKeepsBetweenOnTrailingKey(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Status.Equals(int8(1))
	query.Type.Between(int8(1), int8(2))

	whereStatements := view.getStatementPrepared(collectSelectStatements(query.GetTableInfo())...)
	if len(whereStatements) != 1 {
		t.Fatalf("expected a single BETWEEN clause, got %d", len(whereStatements))
	}
	assertClauseValues(t, whereStatements[0], []any{
		packFixedValueViewValue(t, view, 1, 1),
		packFixedValueViewValue(t, view, 1, 2),
	})
}

func TestPackedViewPrefixRanksBelowLocalIndex(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
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

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
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

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"
	view := fixedValuePackedView(t, scyllaTable)

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

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
	query.CompanyID.Equals(7)
	query.Type.Equals(int8(1))

	compiledStatement, err := tryGetOrCompileSelectStatement(query.GetTableInfo(), scyllaTable)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if compiledStatement.sourceView == nil {
		t.Fatal("expected the packed view to serve the query, got a base-table read")
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
	view := fixedValuePackedView(t, scyllaTable)
	expectedValues := []any{packFixedValueViewValue(t, view, 0, 1), packFixedValueViewValue(t, view, 1, 1)}
	if len(boundPlan.Statements) != 1 {
		t.Fatalf("expected one IN query, got %d", len(boundPlan.Statements))
	}
	assertClauseValues(t, boundWhereClause{Values: boundPlan.Statements[0].QueryValues}, expectedValues)
}

// A plan the query already reaches on its own must not be traded for a fanned-out one.
func TestFixedValueFanoutNeverDisplacesTheKeyRoute(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
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
type fixedViewSparseValuesRecord struct {
	TableStruct[fixedViewSparseValuesSchema, fixedViewSparseValuesRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Status    int8  `db:"status"`
	Type      int8  `db:"type"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewSparseValuesSchema struct {
	TableStruct[fixedViewSparseValuesSchema, fixedViewSparseValuesRecord]
	CompanyID Col[*fixedViewSparseValuesSchema, int32]
	ID        Col[*fixedViewSparseValuesSchema, int32]
	Status    Col[*fixedViewSparseValuesSchema, int8]
	Type      Col[*fixedViewSparseValuesSchema, int8]
	Version   Col[*fixedViewSparseValuesSchema, int16]
}

func (e fixedViewSparseValuesSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10029,
		Name:      "fixed_view_sparse_values_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Status, Values: []int64{0, 5, 9}},
			{Col: e.Type, Min: 1, Max: 2},
		},
		Indexes: []Index{{Type: TypeView, Keys: Cols(e.Status, e.Type)}},
	}
}

func TestFixedValueFanoutUsesTheDeclaredListNotItsSpan(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewSparseValuesRecord, fixedViewSparseValuesSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewSparseValuesRecord{}
	query := Query[fixedViewSparseValuesRecord, fixedViewSparseValuesSchema](&records)
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
	view := fixedValuePackedView(t, scyllaTable)
	if !slices.Equal(view.packedSlotBitsPerColumn, []int64{4, 2}) {
		t.Fatalf("expected slot widths [4 2], got %v", view.packedSlotBitsPerColumn)
	}
	expectedValues := []any{}
	for _, statusValue := range []int64{0, 5, 9} {
		expectedValues = append(expectedValues, packFixedValueViewValue(t, view, statusValue, 1))
	}
	assertClauseValues(t, boundWhereClause{Values: boundPlan.Statements[0].QueryValues}, expectedValues)
}

// A column too wide to enumerate keeps its gap: fanning 501 values out would cost far more than the
// scan it replaces.
type fixedViewUnenumerableLeadRecord struct {
	TableStruct[fixedViewUnenumerableLeadSchema, fixedViewUnenumerableLeadRecord]
	CompanyID int32 `db:"company_id"`
	ID        int32 `db:"id"`
	Kind      int16 `db:"kind"`
	Status    int8  `db:"status"`
	Version   int16 `json:"ver,omitempty"`
}

type fixedViewUnenumerableLeadSchema struct {
	TableStruct[fixedViewUnenumerableLeadSchema, fixedViewUnenumerableLeadRecord]
	CompanyID Col[*fixedViewUnenumerableLeadSchema, int32]
	ID        Col[*fixedViewUnenumerableLeadSchema, int32]
	Kind      Col[*fixedViewUnenumerableLeadSchema, int16]
	Status    Col[*fixedViewUnenumerableLeadSchema, int8]
	Version   Col[*fixedViewUnenumerableLeadSchema, int16]
}

func (e fixedViewUnenumerableLeadSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:        10030,
		Name:      "fixed_view_unenumerable_lead_records",
		Partition: e.CompanyID,
		Keys:      Cols(e.ID),
		FixedValues: []FixedValues{
			{Col: e.Kind, Min: 0, Max: 500},
			{Col: e.Status, Values: []int64{0, 1}},
		},
		Indexes: []Index{{Type: TypeView, Keys: Cols(e.Kind, e.Status)}},
	}
}

func TestFixedValueFanoutSkipsAColumnTooWideToEnumerate(t *testing.T) {
	resetORMTableCachesForTesting()

	scyllaTable := MakeScyllaTable[fixedViewUnenumerableLeadRecord, fixedViewUnenumerableLeadSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewUnenumerableLeadRecord{}
	query := Query[fixedViewUnenumerableLeadRecord, fixedViewUnenumerableLeadSchema](&records)
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

	scyllaTable := MakeScyllaTable[fixedViewViewRecord, fixedViewViewSchema]()
	scyllaTable.Namespace = "genix_test"

	records := []fixedViewViewRecord{}
	query := Query[fixedViewViewRecord, fixedViewViewSchema](&records)
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
