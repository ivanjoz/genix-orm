package dynamo

import (
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ── Example entity used across the query/marshal tests ───────────────────────

type Product struct {
	ID       string
	Category string
	Brand    string
	Price    int64
	Stock    int32
	Created  int64
	Name     string
}

type ProductTable struct {
	Model[ProductTable, Product]
	ID       Col[ProductTable, string]
	Category Col[ProductTable, string]
	Brand    Col[ProductTable, string]
	Price    Col[ProductTable, int64]
	Stock    Col[ProductTable, int32]
	Created  Col[ProductTable, int64]
	Name     Col[ProductTable, string]
}

func (t ProductTable) GetSchema() Schema {
	return Schema{
		Entity:    "prod",
		Partition: Keys(t.Category),
		Sort:      Keys(t.Created.Base(8), t.ID),
		Indexes: []Index{
			{Slot: N1, Keys: Keys(t.Price)}, // numeric GSI
			{Slot: S1, Keys: Keys(t.Brand)}, // string GSI
		},
	}
}

func newProducts(t *testing.T) *Repo[ProductTable, Product] {
	t.Helper()
	return NewRepo[ProductTable, Product]()
}

func TestColumnNamesPopulated(t *testing.T) {
	r := newProducts(t)
	if got := r.T.Category.col().fieldName; got != "Category" {
		t.Fatalf("expected Category, got %q", got)
	}
	if got := r.T.Price.Base(8).col().base; got != 8 {
		t.Fatalf("expected base 8, got %d", got)
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
	p := Product{ID: "sku1", Category: "coffee", Brand: "acme", Price: 1299, Created: 1700000000}
	item, err := r.meta.marshalItem(unsafe.Pointer(&p), &p)
	if err != nil {
		t.Fatal(err)
	}
	if got := s(item["pk"]); got != "prod#coffee" {
		t.Fatalf("pk = %q", got)
	}
	// sk = <created base64 width 8>#sku1
	wantSK := EncodeOrderedUint(1700000000, 8) + "#sku1"
	if got := s(item["sk"]); got != wantSK {
		t.Fatalf("sk = %q want %q", got, wantSK)
	}
	if got := s(item["n1"]); got != "1299" {
		t.Fatalf("n1 = %q", got)
	}
	if got := s(item["s1"]); got != "prod#acme" {
		t.Fatalf("s1 = %q", got)
	}
	// The whole record lives in the binary column "d"; nothing else leaks.
	blob, ok := item["d"].(*types.AttributeValueMemberB)
	if !ok || len(blob.Value) == 0 {
		t.Fatalf("expected non-empty binary column d, got %T", item["d"])
	}
	allowed := map[string]bool{"pk": true, "sk": true, "n1": true, "s1": true, "d": true}
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
	if back != p {
		t.Fatalf("round trip mismatch:\n got  %+v\n want %+v", back, p)
	}
}

func TestPlanBaseTableBetween(t *testing.T) {
	r := newProducts(t)
	q := r.Query().Eq(r.T.Category, "coffee").Between(r.T.Created, int64(1700000000), int64(1800000000))
	plan, err := q.plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.indexName != "" {
		t.Fatalf("expected base table, got index %q", plan.indexName)
	}
	want := "#pk = :pk AND #sk BETWEEN :lo AND :hi"
	if plan.keyCond != want {
		t.Fatalf("keyCond = %q want %q", plan.keyCond, want)
	}
	if s(plan.values[":pk"]) != "prod#coffee" {
		t.Fatalf("pk value = %q", s(plan.values[":pk"]))
	}
	if s(plan.values[":lo"]) != EncodeOrderedUint(1700000000, 8) {
		t.Fatalf("lo value = %q", s(plan.values[":lo"]))
	}
}

func TestPlanNumericGSI(t *testing.T) {
	r := newProducts(t)
	q := r.Query().Eq(r.T.Price, int64(1299))
	plan, err := q.plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.indexName != "gsi-n1" {
		t.Fatalf("expected gsi-n1, got %q", plan.indexName)
	}
	if s(plan.values[":pk"]) != "1299" {
		t.Fatalf("pk value = %q", s(plan.values[":pk"]))
	}
}

func TestPlanStringGSI(t *testing.T) {
	r := newProducts(t)
	q := r.Query().Eq(r.T.Brand, "acme")
	plan, err := q.plan()
	if err != nil {
		t.Fatal(err)
	}
	if plan.indexName != "gsi-s1" {
		t.Fatalf("expected gsi-s1, got %q", plan.indexName)
	}
	if s(plan.values[":pk"]) != "prod#acme" {
		t.Fatalf("pk value = %q", s(plan.values[":pk"]))
	}
}

func TestPlanPostFilter(t *testing.T) {
	r := newProducts(t)
	// Stock is a non-key field (it lives inside "d"), so it becomes a post-filter.
	q := r.Query().Eq(r.T.Category, "coffee").Gte(r.T.Stock, int32(5))
	plan, err := q.plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.postFilter) != 1 || plan.postFilter[0].field != "Stock" {
		t.Fatalf("expected a post-filter on Stock, got %+v", plan.postFilter)
	}
}

func TestPostFilterEval(t *testing.T) {
	r := newProducts(t)
	p := Product{Category: "coffee", Stock: 10, Brand: "acme"}
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
	PK  string
	A8  int8
	A16 int16
	A32 int32
	A64 int64
	U32 uint32
}

type widthsTable struct {
	Model[widthsTable, widths]
	PK  Col[widthsTable, string]
	A8  Col[widthsTable, int8]
	A16 Col[widthsTable, int16]
	A32 Col[widthsTable, int32]
	A64 Col[widthsTable, int64]
	U32 Col[widthsTable, uint32]
}

func (t widthsTable) GetSchema() Schema {
	return Schema{
		Entity:    "w",
		Partition: Keys(t.PK),
		Sort:      Keys(t.A8.Base(2), t.A16.Base(3), t.A32.Base(6), t.A64.Base(11), t.U32.Base(6)),
	}
}

// TestAccessorWidths verifies the precompiled xunsafe readers pick the correct
// byte width per integer type (a wrong-width read would corrupt the value) and
// that an unsigned value above int32 range is read as unsigned, not sign-flipped.
func TestAccessorWidths(t *testing.T) {
	r := NewRepo[widthsTable, widths]()
	w := widths{PK: "p", A8: 5, A16: 300, A32: 70000, A64: 1 << 40, U32: 4_000_000_000}
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
	_, err := r.Query().Between(r.T.Created, int64(1), int64(2)).plan()
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
	}, 0)
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
		{Field: "Category", Op: "=", Value: "coffee"},
		{Field: "Price", Op: ">", Value: 1000},
	}, 0)
	if err == nil {
		t.Fatal("expected error for a range on a hash/GSI column")
	}
}

func TestQueryRecordsRejectsNonIndexedField(t *testing.T) {
	r := newProducts(t)
	// Name lives inside "d" and is not part of any key; not queryable.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Category", Op: "=", Value: "coffee"},
		{Field: "Name", Op: "=", Value: "beans"},
	}, 0)
	if err == nil {
		t.Fatal("expected error for a filter on a non-indexed field")
	}
}

func TestQueryRecordsRejectsUnknownField(t *testing.T) {
	r := newProducts(t)
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Nope", Op: "=", Value: "x"},
	}, 0)
	if err == nil {
		t.Fatal("expected error for an unknown field")
	}
}

func TestQueryRecordsRejectsBadCoercion(t *testing.T) {
	r := newProducts(t)
	// Price is int64; a non-numeric string can't be coerced.
	_, err := r.QueryRecords([]QueryPredicate{
		{Field: "Price", Op: "=", Value: "not-a-number"},
	}, 0)
	if err == nil {
		t.Fatal("expected error coercing a non-numeric value to an integer column")
	}
}
