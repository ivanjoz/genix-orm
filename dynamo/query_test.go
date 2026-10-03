package dynamo

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ── Example entity used across the query/marshal tests ───────────────────────

type Product struct {
	ID         string   `cb:"1"`
	CategoryID int32    `cb:"2"`
	Brand      string   `cb:"3"`
	Price      int64    `cb:"4"`
	Stock      int32    `cb:"5"`
	Created    int64    `cb:"6"`
	Name       string   `cb:"7"`
	TagIDs     []int32  `cb:"8"`
	Labels     []string `cb:"9"`
}

type ProductTable struct {
	Model[ProductTable, Product]
	ID         Col[*ProductTable, string]
	CategoryID Col[*ProductTable, int32]
	Brand      Col[*ProductTable, string]
	Price      Col[*ProductTable, int64]
	Stock      Col[*ProductTable, int32]
	Created    Col[*ProductTable, int64]
	Name       Col[*ProductTable, string]
	TagIDs     ColSlice[*ProductTable, int32]
	Labels     ColSlice[*ProductTable, string]
}

// productTableID pins the TableID so the expected keys below are literal.
const productTableID = "12345678"

func (t ProductTable) GetSchema() Schema {
	return Schema{
		Entity:    "prod",
		TableID:   12345678,
		Partition: Cols(t.CategoryID.Size(16)),
		Keys:      Cols(t.Created.Size(48), t.ID),
		Indexes: []Index{
			{Slot: G1, Keys: Cols(t.Price.Size(40))},                                      // under the CategoryID, sorted by Price
			{Slot: G2, Keys: Cols(t.Brand)},                                               // under the CategoryID, sorted by Brand
			{Slot: G3, Partition: Cols(t.Stock.Size(32)), Keys: Cols(t.Created.Size(48))}, // its own partition
			{Keys: Cols(t.TagIDs.Size(32))},                                               // fan-out, keys-only
			{Keys: Cols(t.Labels), FullCopy: true},                                        // fan-out, FullCopy
		},
	}
}

func newProducts(t *testing.T) *Repo[ProductTable, Product] {
	t.Helper()
	return NewRepo[ProductTable, Product]()
}

// onlyPlan plans a query that must resolve to a single plan (no multi-value Contains).
func onlyPlan[E any](t *testing.T, q *QueryBuilder[E]) *queryPlan {
	t.Helper()
	plans, err := q.plans()
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	return plans[0]
}

func TestColumnNamesPopulated(t *testing.T) {
	r := newProducts(t)
	if got := r.T.CategoryID.col().fieldName; got != "CategoryID" {
		t.Fatalf("expected CategoryID, got %q", got)
	}
	if got := r.T.Price.Size(48).col().bits; got != 48 {
		t.Fatalf("expected size 48, got %d", got)
	}
}

func s(av types.AttributeValue) string {
	if m, ok := av.(*types.AttributeValueMemberS); ok {
		return m.Value
	}
	if m, ok := av.(*types.AttributeValueMemberN); ok {
		return m.Value
	}
	return ""
}

func TestMarshalItemDerivesKeys(t *testing.T) {
	r := newProducts(t)
	p := Product{ID: "sku1", CategoryID: 7, Brand: "acme", Price: 1299, Created: 1700000000, TagIDs: []int32{3}}
	item, err := r.meta.marshalItem(unsafe.Pointer(&p), &p)
	if err != nil {
		t.Fatal(err)
	}
	// pk = TableID ‖ CategoryID padded to Size(16)'s 5 decimal digits, as a number.
	if _, isNumber := item["pk"].(*types.AttributeValueMemberN); !isNumber || s(item["pk"]) != productTableID+"00007" {
		t.Fatalf("pk = %#v", item["pk"])
	}
	// sk = <created base64 width 8>#sku1
	wantSK := EncodeOrderedUint(1700000000, 8) + "#sku1"
	if got := s(item["sk"]); got != wantSK {
		t.Fatalf("sk = %q want %q", got, wantSK)
	}
	// A GSI hash defaults to the entity's partition; its range is its Keys, then the base Keys left out.
	if _, isNumber := item["h1"].(*types.AttributeValueMemberN); !isNumber || s(item["h1"]) != productTableID+"00007" {
		t.Fatalf("h1 = %#v", item["h1"])
	}
	if got, want := s(item["r1"]), EncodeOrderedUint(1299, 7)+"#"+wantSK; got != want {
		t.Fatalf("r1 = %q want %q", got, want)
	}
	if got, want := s(item["r2"]), "acme#"+wantSK; got != want {
		t.Fatalf("r2 = %q want %q", got, want)
	}
	// G3 declares its own partition (Stock, Size(32) = 10 digits), and its Keys already hold Created.
	if got := s(item["h3"]); got != productTableID+"0000000000" {
		t.Fatalf("h3 = %q", got)
	}
	if got := s(item["r3"]); got != wantSK {
		t.Fatalf("r3 = %q want %q", got, wantSK)
	}
	// The whole record lives in the binary column "d"; nothing else leaks.
	blob, ok := item["d"].(*types.AttributeValueMemberB)
	if !ok || len(blob.Value) == 0 {
		t.Fatalf("expected non-empty binary column d, got %T", item["d"])
	}
	allowed := map[string]bool{"pk": true, "sk": true, "h1": true, "r1": true, "h2": true, "r2": true, "h3": true, "r3": true, "d": true}
	for k := range item {
		if !allowed[k] {
			t.Fatalf("unexpected attribute %q in item (only keys/index/d allowed)", k)
		}
	}
	// Round-trip the blob back into a record.
	var back Product
	if err := r.meta.unmarshalItem(item, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, p) {
		t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", back, p)
	}
}

func TestPlanBaseTableBetween(t *testing.T) {
	r := newProducts(t)
	q := r.Query().Eq(r.T.CategoryID, int32(7)).Between(r.T.Created, int64(1700000000), int64(1800000000))
	plan := onlyPlan(t, q)
	if plan.indexName != "" {
		t.Fatalf("expected base table, got index %q", plan.indexName)
	}
	want := "#pk = :pk AND #sk BETWEEN :lo AND :hi"
	if plan.keyCond != want {
		t.Fatalf("keyCond = %q want %q", plan.keyCond, want)
	}
	if s(plan.values[":pk"]) != productTableID+"00007" {
		t.Fatalf("pk value = %q", s(plan.values[":pk"]))
	}
	if s(plan.values[":lo"]) != EncodeOrderedUint(1700000000, 8) {
		t.Fatalf("lo value = %q", s(plan.values[":lo"]))
	}
}

// TestPlanRangeStaysInsideEqualityPrefix: Eq(Created) + Gt(ID) must not run past
// the Created prefix into later Created values, and the strict > starts after "b".
func TestPlanRangeStaysInsideEqualityPrefix(t *testing.T) {
	r := newProducts(t)
	plan := onlyPlan(t, r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.Created, int64(5)).Gt(r.T.ID, "b"))
	createdPrefix := EncodeOrderedUint(5, 8)
	if plan.keyCond != "#pk = :pk AND #sk BETWEEN :lo AND :hi" {
		t.Fatalf("keyCond = %q", plan.keyCond)
	}
	if s(plan.values[":lo"]) != createdPrefix+"#b$" || s(plan.values[":hi"]) != createdPrefix+"$" {
		t.Fatalf("range = [%q, %q]", s(plan.values[":lo"]), s(plan.values[":hi"]))
	}
	if len(plan.postFilter) != 0 {
		t.Fatalf("a strict > needs no post-filter, got %+v", plan.postFilter)
	}
}

// TestPlanRangeOnANonLastSortColumn: sk = Created ‖ ID, so the row with Created = 5
// is enc(5)#<ID>, which sorts after enc(5). Every bound must still include or
// exclude it as asked.
func TestPlanRangeOnANonLastSortColumn(t *testing.T) {
	r := newProducts(t)
	created5 := EncodeOrderedUint(5, 8)
	for _, testCase := range []struct {
		query        *QueryBuilder[Product]
		wantKeyCond  string
		wantBoundary map[string]string
	}{
		{r.Query().Lte(r.T.Created, int64(5)), "#sk < :sk", map[string]string{":sk": created5 + "$"}},
		{r.Query().Lt(r.T.Created, int64(5)), "#sk < :sk", map[string]string{":sk": created5}},
		{r.Query().Gt(r.T.Created, int64(5)), "#sk >= :sk", map[string]string{":sk": created5 + "$"}},
		{r.Query().Gte(r.T.Created, int64(5)), "#sk >= :sk", map[string]string{":sk": created5}},
		{r.Query().Between(r.T.Created, int64(1), int64(5)), "#sk BETWEEN :lo AND :hi",
			map[string]string{":lo": EncodeOrderedUint(1, 8), ":hi": created5 + "$"}},
	} {
		plan := onlyPlan(t, testCase.query.Eq(r.T.CategoryID, int32(7)))
		if plan.keyCond != "#pk = :pk AND "+testCase.wantKeyCond {
			t.Fatalf("keyCond = %q, want %q", plan.keyCond, testCase.wantKeyCond)
		}
		for name, want := range testCase.wantBoundary {
			if got := s(plan.values[name]); got != want {
				t.Fatalf("%s: %s = %q, want %q", testCase.wantKeyCond, name, got, want)
			}
		}
	}
}

// TestPlanGSIRangesOnItsKeys: a GSI's Keys are its sort key, so they take an Eq
// prefix and a range like the base Keys, under the GSI's own hash.
func TestPlanGSIRangesOnItsKeys(t *testing.T) {
	r := newProducts(t)
	price1299 := EncodeOrderedUint(1299, 7)
	for _, testCase := range []struct {
		query       *QueryBuilder[Product]
		wantIndex   string
		wantKeyCond string
		wantValues  map[string]string
	}{
		{r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.Price, int64(1299)), "gsi-1",
			"#pk = :pk AND begins_with(#sk, :sk)", map[string]string{":pk": productTableID + "00007", ":sk": price1299 + "#"}},
		{r.Query().Eq(r.T.CategoryID, int32(7)).Between(r.T.Price, int64(1000), int64(1299)), "gsi-1",
			"#pk = :pk AND #sk BETWEEN :lo AND :hi", map[string]string{":lo": EncodeOrderedUint(1000, 7), ":hi": price1299 + "$"}},
		{r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.Price, int64(1299)).Gte(r.T.Created, int64(5)), "gsi-1",
			"#pk = :pk AND #sk BETWEEN :lo AND :hi", map[string]string{":lo": price1299 + "#" + EncodeOrderedUint(5, 8)}},
		{r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.Brand, "acme"), "gsi-2",
			"#pk = :pk AND begins_with(#sk, :sk)", map[string]string{":sk": "acme#"}},
		{r.Query().Eq(r.T.Stock, int32(4)).Gt(r.T.Created, int64(5)), "gsi-3",
			"#pk = :pk AND #sk >= :sk", map[string]string{":pk": productTableID + "0000000004", ":sk": EncodeOrderedUint(5, 8) + "$"}},
	} {
		plan := onlyPlan(t, testCase.query)
		if plan.indexName != testCase.wantIndex || plan.keyCond != testCase.wantKeyCond {
			t.Fatalf("plan = %q %q, want %q %q", plan.indexName, plan.keyCond, testCase.wantIndex, testCase.wantKeyCond)
		}
		slot := Slots[testCase.wantIndex[len("gsi-")]-'1']
		if plan.names["#pk"] != slot.hashAttr || plan.names["#sk"] != slot.rangeAttr {
			t.Fatalf("%s names = %v", testCase.wantIndex, plan.names)
		}
		for name, want := range testCase.wantValues {
			if got := s(plan.values[name]); got != want {
				t.Fatalf("%s: %s = %q, want %q", testCase.wantKeyCond, name, got, want)
			}
		}
	}
}

// TestPlanPicksThePathServingMostPredicates: the base table serves CategoryID +
// Created, gsi-1 only CategoryID, so the base table wins; with Price it is gsi-1.
func TestPlanPicksThePathServingMostPredicates(t *testing.T) {
	r := newProducts(t)
	if plan := onlyPlan(t, r.Query().Eq(r.T.CategoryID, int32(7)).Gt(r.T.Created, int64(5))); plan.indexName != "" {
		t.Fatalf("expected the base table, got %q", plan.indexName)
	}
	if plan := onlyPlan(t, r.Query().Eq(r.T.CategoryID, int32(7))); plan.indexName != "" {
		t.Fatalf("a tie goes to the base table, got %q", plan.indexName)
	}
}

func TestQueryScanFiltersANonKeyFieldInMemory(t *testing.T) {
	r := newProducts(t)
	// Stock is a non-key field (it lives inside "d"), so it becomes a post-filter.
	plan := onlyPlan(t, r.QueryScan().Eq(r.T.CategoryID, int32(7)).Gte(r.T.Stock, int32(5)))
	if len(plan.postFilter) != 1 || plan.postFilter[0].field != "Stock" {
		t.Fatalf("expected a post-filter on Stock, got %+v", plan.postFilter)
	}
}

// ── An entity without Partition: its whole-entity pk must not shadow the GSIs ──

type account struct {
	ID       int32  `cb:"1"`
	Username string `cb:"2"`
}

type accountTable struct {
	Model[accountTable, account]
	ID       Col[*accountTable, int32]
	Username Col[*accountTable, string]
}

func (t accountTable) GetSchema() Schema {
	return Schema{Entity: "account", Keys: Cols(t.ID.Size(32)), Indexes: []Index{{Slot: G1, Keys: Cols(t.Username)}}}
}

func TestNoPartitionEntityUsesItsGSI(t *testing.T) {
	accounts := NewRepo[accountTable, account]()
	plan := onlyPlan(t, accounts.Query().Eq(accounts.T.Username, "ana"))
	if plan.indexName != "gsi-1" {
		t.Fatalf("expected gsi-1, got %q (the whole-entity pk shadowed the GSI)", plan.indexName)
	}
	if plan := onlyPlan(t, accounts.Query()); plan.indexName != "" {
		t.Fatalf("without predicates the whole entity is the base pk, got %q", plan.indexName)
	}
}

func TestStrictQueryRejectsWhatNoKeyServes(t *testing.T) {
	r := newProducts(t)
	for name, query := range map[string]*QueryBuilder[Product]{
		"a non-key field":             r.Query().Eq(r.T.CategoryID, int32(7)).Gte(r.T.Stock, int32(5)),
		"a range on a GSI hash":       r.Query().Gt(r.T.Stock, int32(4)).Eq(r.T.Created, int64(5)),
		"a GSI without its partition": r.Query().Eq(r.T.Price, int64(1299)),
		"a Keys column after a gap":   r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.ID, "sku1"),
		"a filter beside a Contains":  r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.TagIDs, 3).Eq(r.T.Name, "x"),
		"no index at all (QueryScan)": r.QueryScan().Gte(r.T.Stock, int32(5)),
	} {
		if _, err := query.plans(); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
	// The key filter of a strict < under an equality prefix is the ORM's own, not a user filter.
	onlyPlan(t, r.Query().Eq(r.T.CategoryID, int32(7)).Eq(r.T.Created, int64(5)).Lt(r.T.ID, "b"))
}

func TestPostFilterEval(t *testing.T) {
	r := newProducts(t)
	p := Product{CategoryID: 7, Stock: 10, Brand: "acme"}
	ptr := unsafe.Pointer(&p)
	pass := []predicate{{field: "Stock", op: opGte, v1: int32(5)}}
	fail := []predicate{{field: "Stock", op: opGt, v1: int32(50)}}
	strOK := []predicate{{field: "Brand", op: opBeginsWith, v1: "ac"}}
	if !r.meta.matchesFilter(ptr, pass) {
		t.Fatal("Stock>=5 should pass")
	}
	if r.meta.matchesFilter(ptr, fail) {
		t.Fatal("Stock>50 should fail")
	}
	if !r.meta.matchesFilter(ptr, strOK) {
		t.Fatal("Brand begins_with ac should pass")
	}
}

// ── Accessor width coverage ──────────────────────────────────────────────────

type widths struct {
	PK  int32
	A8  int8
	A16 int16
	A32 int32
	A64 int64
	U32 uint32
}

type widthsTable struct {
	Model[widthsTable, widths]
	PK  Col[*widthsTable, int32]
	A8  Col[*widthsTable, int8]
	A16 Col[*widthsTable, int16]
	A32 Col[*widthsTable, int32]
	A64 Col[*widthsTable, int64]
	U32 Col[*widthsTable, uint32]
}

func (t widthsTable) GetSchema() Schema {
	return Schema{
		Entity:    "w",
		Partition: Cols(t.PK.Size(8)),
		Keys:      Cols(t.A8.Size(8), t.A16.Size(16), t.A32.Size(32), t.A64.Size(64), t.U32.Size(32)),
	}
}

// TestAccessorWidths verifies the precompiled xunsafe readers pick the correct
// byte width per integer type (a wrong-width read would corrupt the value) and
// that an unsigned value above int32 range is read as unsigned, not sign-flipped.
func TestAccessorWidths(t *testing.T) {
	r := NewRepo[widthsTable, widths]()
	w := widths{PK: 1, A8: 5, A16: 300, A32: 70000, A64: 1 << 40, U32: 4_000_000_000}
	item, err := r.meta.marshalItem(unsafe.Pointer(&w), &w)
	if err != nil {
		t.Fatal(err)
	}
	want := EncodeOrderedUint(5, 2) + "#" + EncodeOrderedUint(300, 3) + "#" +
		EncodeOrderedUint(70000, 6) + "#" + EncodeOrderedUint(1<<40, 11) + "#" +
		EncodeOrderedUint(4_000_000_000, 6)
	if got := s(item["sk"]); got != want {
		t.Fatalf("sk = %q\nwant %q", got, want)
	}
}

func TestPlanNoPartitionErrors(t *testing.T) {
	r := newProducts(t)
	// Only a sort predicate, no partition equality → cannot query.
	_, err := r.Query().Between(r.T.Created, int64(1), int64(2)).plans()
	if err == nil {
		t.Fatal("expected error for missing partition")
	}
}

// ── QueryRecords: strict, type-erased dynamic query ──────────────────────────
//
// These exercise only the validation the method does before it would touch
// DynamoDB (coercion → plan → post-filter rejection), so they need no client.
// Every case here must return an error, which QueryRecords does before Exec.

func TestQueryRecordsRejectsMissingPartition(t *testing.T) {
	r := newProducts(t)
	// A range on the sort key with no partition equality: no usable index.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Created", Op: ">", Value: 100},
	}, 0, false)
	if err == nil {
		t.Fatal("expected error for missing partition/index")
	}
}

func TestQueryRecordsRejectsRangeOnHash(t *testing.T) {
	r := newProducts(t)
	// Price is a numeric GSI (a hash); a range on it isn't served by any index.
	// With a valid base partition it would fall to the post-filter, which
	// QueryRecords refuses.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "CategoryID", Op: "=", Value: 7},
		{Field: "Price", Op: ">", Value: 1000},
	}, 0, false)
	if err == nil {
		t.Fatal("expected error for a range on a hash/GSI column")
	}
}

func TestQueryRecordsRejectsNonIndexedField(t *testing.T) {
	r := newProducts(t)
	// Name lives inside "d" and is not part of any key; not queryable.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "CategoryID", Op: "=", Value: 7},
		{Field: "Name", Op: "=", Value: "beans"},
	}, 0, false)
	if err == nil {
		t.Fatal("expected error for a filter on a non-indexed field")
	}
}

func TestQueryRecordsRejectsUnknownField(t *testing.T) {
	r := newProducts(t)
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Nope", Op: "=", Value: "x"},
	}, 0, false)
	if err == nil {
		t.Fatal("expected error for an unknown field")
	}
}

func TestQueryRecordsRejectsBadCoercion(t *testing.T) {
	r := newProducts(t)
	// Price is int64; a non-numeric string can't be coerced.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Price", Op: "=", Value: "not-a-number"},
	}, 0, false)
	if err == nil {
		t.Fatal("expected error coercing a non-numeric value to an integer column")
	}
}
