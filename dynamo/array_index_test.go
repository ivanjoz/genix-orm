package dynamo

import (
	"os"
	"reflect"
	"testing"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Product (query_test.go) declares TagIDs (cb 8, keys-only) and Labels (cb 9,
// FullCopy) as array indexes under the pinned TableID 12345678.

func productBaseKeys(r *Repo[ProductTable, Product], p *Product) (string, string) {
	return r.meta.pkValue(unsafe.Pointer(p)), r.meta.skValue(unsafe.Pointer(p))
}

func tagRowSK(tagID uint64, baseSK string) string {
	return EncodeOrderedUint(tagID, 6) + "#" + baseSK // Size(32) = 6 Base64 digits
}

func writeKeys(writes []types.WriteRequest) []string {
	var keys []string
	for _, write := range writes {
		key := map[string]types.AttributeValue(nil)
		if write.PutRequest != nil {
			key = write.PutRequest.Item
		} else {
			key = write.DeleteRequest.Key
		}
		keys = append(keys, s(key["pk"])+" "+s(key["sk"]))
	}
	return keys
}

func TestArrayIndexWritesDiffStoredAgainstWritten(t *testing.T) {
	r := newProducts(t)
	stored := Product{ID: "sku1", CategoryID: 7, Created: 5, TagIDs: []int32{1, 2, 2}, Labels: []string{"a"}}
	written := Product{ID: "sku1", CategoryID: 7, Created: 5, TagIDs: []int32{2, 3}, Labels: []string{"a", "b"}}
	basePK, baseSK := productBaseKeys(r, &written)

	puts, deletes := r.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&written), []byte("blob"))

	// Keys-only TagIDs: only the new element 3 is put. FullCopy Labels: every
	// element is put again, since the blob they copy changed.
	wantPuts := []string{
		basePK + "008 " + tagRowSK(3, baseSK),
		basePK + "009 a#" + baseSK,
		basePK + "009 b#" + baseSK,
	}
	if got := writeKeys(puts); !equalStrings(got, wantPuts) {
		t.Fatalf("puts = %v\nwant %v", got, wantPuts)
	}
	if _, hasBlob := puts[0].PutRequest.Item[dataColumn]; hasBlob {
		t.Fatal("a keys-only row must not carry the blob")
	}
	if blob, _ := puts[1].PutRequest.Item[dataColumn].(*types.AttributeValueMemberB); blob == nil || string(blob.Value) != "blob" {
		t.Fatal("a FullCopy row must carry the blob")
	}
	// The duplicate stored element 2 is still written, so only 1 goes.
	if got, want := writeKeys(deletes), []string{basePK + "008 " + tagRowSK(1, baseSK)}; !equalStrings(got, want) {
		t.Fatalf("deletes = %v\nwant %v", got, want)
	}
}

func TestArrayIndexWritesNewAndDeletedRecord(t *testing.T) {
	r := newProducts(t)
	record := Product{ID: "sku1", CategoryID: 7, Created: 5, TagIDs: []int32{4}}
	basePK, baseSK := productBaseKeys(r, &record)
	wantRows := []string{basePK + "008 " + tagRowSK(4, baseSK)}

	puts, deletes := r.meta.arrayIndexWrites(nil, unsafe.Pointer(&record), nil)
	if got := writeKeys(puts); !equalStrings(got, wantRows) || len(deletes) != 0 {
		t.Fatalf("new record: puts %v deletes %v", got, writeKeys(deletes))
	}
	puts, deletes = r.meta.arrayIndexWrites(unsafe.Pointer(&record), nil, nil)
	if got := writeKeys(deletes); !equalStrings(got, wantRows) || len(puts) != 0 {
		t.Fatalf("deleted record: puts %v deletes %v", writeKeys(puts), got)
	}
}

func TestArrayRowSKRoundTripsTheBaseSK(t *testing.T) {
	baseSK := EncodeOrderedUint(5, 8) + "#sku1"
	if got := baseSKOfArrayRow(tagRowSK(3, baseSK), 1); got != baseSK {
		t.Fatalf("baseSKOfArrayRow = %q, want %q", got, baseSK)
	}
	twoPartRowSK := EncodeOrderedUint(3, 6) + "#" + EncodeOrderedUint(100, 6) + "#" + baseSK
	if got := baseSKOfArrayRow(twoPartRowSK, 2); got != baseSK {
		t.Fatalf("baseSKOfArrayRow with 2 index keys = %q, want %q", got, baseSK)
	}
}

func TestWritesArrayRowIsTheReadSideCheck(t *testing.T) {
	r := newProducts(t)
	arrayIndex := &r.meta.arrayIndexes[0]
	record := Product{ID: "sku1", CategoryID: 7, Created: 5, TagIDs: []int32{2, 9}}
	_, baseSK := productBaseKeys(r, &record)
	if !r.meta.writesArrayRow(arrayIndex, unsafe.Pointer(&record), tagRowSK(9, baseSK)) {
		t.Fatal("record holds 9")
	}
	if r.meta.writesArrayRow(arrayIndex, unsafe.Pointer(&record), tagRowSK(3, baseSK)) {
		t.Fatal("record does not hold 3")
	}
}

func TestPlanContainsOnePlanPerValue(t *testing.T) {
	r := newProducts(t)
	plans, err := r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.TagIDs, 3, 4).plans()
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 {
		t.Fatalf("expected one plan per value, got %d", len(plans))
	}
	plan := plans[1]
	if plan.keyCond != "#pk = :pk AND begins_with(#sk, :sk)" {
		t.Fatalf("keyCond = %q", plan.keyCond)
	}
	if s(plan.values[":pk"]) != productTableID+"00007008" || plan.basePK != productTableID+"00007" {
		t.Fatalf("pk = %q, basePK = %q", s(plan.values[":pk"]), plan.basePK)
	}
	if s(plan.values[":sk"]) != EncodeOrderedUint(4, 6)+"#" {
		t.Fatalf("sk prefix = %q", s(plan.values[":sk"]))
	}
	if len(plan.postFilter) != 0 {
		t.Fatalf("Contains must not become a post-filter: %+v", plan.postFilter)
	}
}

func TestPlanContainsExtendsWithTheBaseSortRange(t *testing.T) {
	r := newProducts(t)
	plan := onlyPlan(t, r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.TagIDs, 3).Gte(r.T.Created, int64(100)))
	elementPrefix := EncodeOrderedUint(3, 6)
	if plan.keyCond != "#pk = :pk AND #sk BETWEEN :lo AND :hi" {
		t.Fatalf("keyCond = %q", plan.keyCond)
	}
	if s(plan.values[":lo"]) != elementPrefix+"#"+EncodeOrderedUint(100, 8) || s(plan.values[":hi"]) != elementPrefix+"$" {
		t.Fatalf("range = [%q, %q]", s(plan.values[":lo"]), s(plan.values[":hi"]))
	}
}

func TestPlanContainsRejections(t *testing.T) {
	r := newProducts(t)
	// Contains on a non-slice column no longer compiles: it takes a ColSlice.
	if _, err := r.Query().Contains(r.T.TagIDs, 3).plans(); err == nil {
		t.Fatal("Contains without the partition equality must fail")
	}
}

func TestHashTableIDHasEightDigits(t *testing.T) {
	for _, entity := range []string{"", "user", "profile", "cron_execution"} {
		tableID := HashTableID(entity)
		if tableID < 10_000_000 || tableID > 99_999_999 {
			t.Fatalf("HashTableID(%q) = %d, not 8 digits", entity, tableID)
		}
		if HashTableID(entity) != tableID {
			t.Fatalf("HashTableID(%q) is not deterministic", entity)
		}
	}
}

func TestPartitionRangeCoversBaseAndArrayRows(t *testing.T) {
	r := newProducts(t)
	lo, hi := r.meta.partitionRange(0)
	if lo != productTableID+"00000" || hi != productTableID+"99999" {
		t.Fatalf("base range = [%s, %s]", lo, hi)
	}
	lo, hi = r.meta.partitionRange(arrayIndexColumnIDDigits)
	if lo != productTableID+"00000000" || hi != productTableID+"99999999" {
		t.Fatalf("array range = [%s, %s]", lo, hi)
	}
}

// TestArrayIndexSyncLive drives the whole write/read cycle against a live
// DynamoDB whose table already exists (DYNAMO_ENDPOINT + DYNAMO_TABLE, as for
// TestPutIfAbsent).
func TestArrayIndexSyncLive(t *testing.T) {
	if os.Getenv("DYNAMO_ENDPOINT") == "" {
		t.Skip("DYNAMO_ENDPOINT not set: the array index sync needs a live DynamoDB")
	}
	r := newProducts(t)
	if _, err := r.DeleteRecordsAll(); err != nil {
		t.Fatal(err)
	}
	containsIDs := func(q *QueryBuilder[Product]) []string {
		t.Helper()
		var found []Product
		if err := q.Exec(&found); err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, p := range found {
			ids = append(ids, p.ID)
		}
		return ids
	}
	tagQuery := func(tagIDs ...any) []string {
		return containsIDs(r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.TagIDs, tagIDs...))
	}

	first := Product{ID: "a", CategoryID: 7, Created: 1, TagIDs: []int32{1, 2}, Labels: []string{"red"}}
	second := Product{ID: "b", CategoryID: 7, Created: 2, TagIDs: []int32{2}}
	if err := r.PutMany([]Product{first, second}); err != nil {
		t.Fatal(err)
	}
	if got := tagQuery(2); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("Contains(2) = %v", got)
	}

	first.TagIDs, first.Name = []int32{2, 3}, "renamed"
	if err := r.Put(&first); err != nil {
		t.Fatal(err)
	}
	if got := tagQuery(1); len(got) != 0 {
		t.Fatalf("Contains(1) after removing 1 = %v", got)
	}
	// "a" holds both 2 and 3 but comes back once.
	if got := tagQuery(3, 2); !equalStrings(got, []string{"a", "b"}) {
		t.Fatalf("Contains(3, 2) = %v", got)
	}
	var fromCopy []Product
	if err := r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.Labels, "red").Exec(&fromCopy); err != nil {
		t.Fatal(err)
	}
	if len(fromCopy) != 1 || fromCopy[0].Name != "renamed" {
		t.Fatalf("FullCopy row was not rewritten: %+v", fromCopy)
	}
	if got := containsIDs(r.Query().Eq(r.T.CategoryID, int32(7)).Contains(r.T.TagIDs, 2).Gt(r.T.Created, int64(1))); !equalStrings(got, []string{"b"}) {
		t.Fatalf("Contains(2) + Created > 1 = %v", got)
	}
	if scanned, err := r.Scan(0); err != nil || len(scanned) != 2 {
		t.Fatalf("Scan = %d records, %v", len(scanned), err)
	}

	if err := r.Delete(&Product{ID: "a", CategoryID: 7, Created: 1}); err != nil {
		t.Fatal(err)
	}
	if got := tagQuery(2, 3); !equalStrings(got, []string{"b"}) {
		t.Fatalf("Contains(2, 3) after deleting a = %v", got)
	}
	// Only b's base row and its one array row may be left.
	if deleted, err := r.DeleteRecordsAll(); err != nil || deleted != 2 {
		t.Fatalf("DeleteRecordsAll = %d, %v (want 2: no stale array rows)", deleted, err)
	}
}

// ── Composite fan-out indexes: scalar Keys before and after the slice ─────────

type fanOutOrder struct {
	StoreID    int32    `cb:"1"`
	ID         int32    `cb:"2"`
	Updated    int32    `cb:"3"`
	Channel    string   `cb:"4"`
	ProductIDs []int32  `cb:"5"`
	Tags       []string `cb:"6"`
}

type fanOutOrderTable struct {
	Model[fanOutOrderTable, fanOutOrder]
	StoreID    Col[*fanOutOrderTable, int32]
	ID         Col[*fanOutOrderTable, int32]
	Updated    Col[*fanOutOrderTable, int32]
	Channel    Col[*fanOutOrderTable, string]
	ProductIDs ColSlice[*fanOutOrderTable, int32]
	Tags       ColSlice[*fanOutOrderTable, string]
}

func (t fanOutOrderTable) GetSchema() Schema {
	return Schema{
		Entity:    "fan_out_order",
		TableID:   23456789,
		Partition: Cols(t.StoreID.Size(16)),
		Keys:      Cols(t.ID.Size(32)),
		Indexes: []Index{
			{Keys: Cols(t.ProductIDs.Size(32), t.Updated.Size(32))}, // slice first, range on Updated
			{Keys: Cols(t.Channel, t.Tags)},                         // slice after a scalar
		},
	}
}

func fanOutOrderRowSK(productID, updated, id uint64) string {
	return EncodeOrderedUint(productID, 6) + "#" + EncodeOrderedUint(updated, 6) + "#" + EncodeOrderedUint(id, 6)
}

func TestCompositeFanOutRowsCarryTheScalarKeys(t *testing.T) {
	orders := NewRepo[fanOutOrderTable, fanOutOrder]()
	record := fanOutOrder{StoreID: 7, ID: 1, Updated: 100, Channel: "web", ProductIDs: []int32{5, 5, 8}, Tags: []string{"gift"}}
	basePK := orders.meta.pkValue(unsafe.Pointer(&record))

	puts, deletes := orders.meta.arrayIndexWrites(nil, unsafe.Pointer(&record), nil)
	wantPuts := []string{
		basePK + "005 " + fanOutOrderRowSK(5, 100, 1),
		basePK + "005 " + fanOutOrderRowSK(8, 100, 1),
		basePK + "006 web#gift#" + EncodeOrderedUint(1, 6),
	}
	if got := writeKeys(puts); !equalStrings(got, wantPuts) || len(deletes) != 0 {
		t.Fatalf("puts = %v\nwant %v (deletes %v)", got, wantPuts, writeKeys(deletes))
	}
}

// A changed scalar index column moves every element row: the old ones go, the new ones come.
func TestCompositeFanOutRewritesRowsWhenAScalarKeyChanges(t *testing.T) {
	orders := NewRepo[fanOutOrderTable, fanOutOrder]()
	stored := fanOutOrder{StoreID: 7, ID: 1, Updated: 100, Channel: "web", ProductIDs: []int32{5}}
	written := stored
	written.Updated = 200
	basePK := orders.meta.pkValue(unsafe.Pointer(&written))

	puts, deletes := orders.meta.arrayIndexWrites(unsafe.Pointer(&stored), unsafe.Pointer(&written), nil)
	if got, want := writeKeys(puts), []string{basePK + "005 " + fanOutOrderRowSK(5, 200, 1)}; !equalStrings(got, want) {
		t.Fatalf("puts = %v, want %v", got, want)
	}
	if got, want := writeKeys(deletes), []string{basePK + "005 " + fanOutOrderRowSK(5, 100, 1)}; !equalStrings(got, want) {
		t.Fatalf("deletes = %v, want %v", got, want)
	}
	// The row still holding Updated = 100 is stale for the record as it is now.
	if orders.meta.writesArrayRow(&orders.meta.arrayIndexes[0], unsafe.Pointer(&written), fanOutOrderRowSK(5, 100, 1)) {
		t.Fatal("a row with an outdated scalar index column must not count as the record's")
	}
}

func TestPlanCompositeFanOutRangesOnTheScalarAfterTheSlice(t *testing.T) {
	orders := NewRepo[fanOutOrderTable, fanOutOrder]()
	product5 := EncodeOrderedUint(5, 6)
	for name, query := range map[string]*QueryBuilder[fanOutOrder]{
		"Contains": orders.Query().Eq(orders.T.StoreID, int32(7)).Contains(orders.T.ProductIDs, 5).Gt(orders.T.Updated, int32(10_000)),
		"Eq":       orders.Query().Eq(orders.T.StoreID, int32(7)).Eq(orders.T.ProductIDs, 5).Gt(orders.T.Updated, int32(10_000)),
	} {
		plan := onlyPlan(t, query)
		if plan.keyCond != "#pk = :pk AND #sk BETWEEN :lo AND :hi" {
			t.Fatalf("%s: keyCond = %q", name, plan.keyCond)
		}
		if s(plan.values[":pk"]) != "23456789"+"00007"+"005" {
			t.Fatalf("%s: pk = %q", name, s(plan.values[":pk"]))
		}
		if s(plan.values[":lo"]) != product5+"#"+EncodeOrderedUint(10_000, 6)+"$" || s(plan.values[":hi"]) != product5+"$" {
			t.Fatalf("%s: range = [%q, %q]", name, s(plan.values[":lo"]), s(plan.values[":hi"]))
		}
		if len(plan.postFilter) != 0 || len(plan.keyFilter) != 0 {
			t.Fatalf("%s: the range must be exact, got post %+v key %+v", name, plan.postFilter, plan.keyFilter)
		}
	}

	// Every index column pinned: the range goes on to the base Keys.
	plan := onlyPlan(t, orders.Query().Eq(orders.T.StoreID, int32(7)).Contains(orders.T.ProductIDs, 5).Eq(orders.T.Updated, int32(100)).Gte(orders.T.ID, int32(3)))
	if s(plan.values[":lo"]) != product5+"#"+EncodeOrderedUint(100, 6)+"#"+EncodeOrderedUint(3, 6) {
		t.Fatalf("lo = %q", s(plan.values[":lo"]))
	}
}

func TestPlanCompositeFanOutNeedsTheColumnsBeforeTheSlice(t *testing.T) {
	orders := NewRepo[fanOutOrderTable, fanOutOrder]()
	if _, err := orders.Query().Eq(orders.T.StoreID, int32(7)).Contains(orders.T.Tags, "gift").plans(); err == nil {
		t.Fatal("Contains(Tags) without Eq(Channel) must fail")
	}
	plan := onlyPlan(t, orders.Query().Eq(orders.T.StoreID, int32(7)).Eq(orders.T.Channel, "web").Contains(orders.T.Tags, "gift"))
	if plan.keyCond != "#pk = :pk AND begins_with(#sk, :sk)" || s(plan.values[":sk"]) != "web#gift#" {
		t.Fatalf("keyCond = %q, sk = %q", plan.keyCond, s(plan.values[":sk"]))
	}
}

type badFanOutRecord struct {
	ID     int32   `cb:"1"`
	Price  int32   `cb:"2"`
	TagIDs []int32 `cb:"3"`
	Other  []int32 `cb:"4"`
}

type badFanOutTable struct {
	Model[badFanOutTable, badFanOutRecord]
	ID     Col[*badFanOutTable, int32]
	Price  Col[*badFanOutTable, int32]
	TagIDs ColSlice[*badFanOutTable, int32]
	Other  ColSlice[*badFanOutTable, int32]
}

func TestFanOutIndexDeclarationRules(t *testing.T) {
	for name, index := range map[string]func(t badFanOutTable) Index{
		"a slice with a Slot": func(t badFanOutTable) Index { return Index{Slot: S1, Keys: Cols(t.TagIDs.Size(32))} },
		"two slices":          func(t badFanOutTable) Index { return Index{Keys: Cols(t.TagIDs.Size(32), t.Other.Size(32))} },
		"FullCopy on a GSI":   func(t badFanOutTable) Index { return Index{Slot: N1, Keys: Cols(t.Price.Size(32)), FullCopy: true} },
		"no Slot, no slice":   func(t badFanOutTable) Index { return Index{Keys: Cols(t.Price.Size(32))} },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected a panic", name)
				}
			}()
			tablePtr := new(badFanOutTable)
			populateColumnNames(tablePtr)
			schema := Schema{Entity: "bad_fan_out", TableID: 34567890, Keys: Cols(tablePtr.ID.Size(32)), Indexes: []Index{index(*tablePtr)}}
			buildTableMeta(schema, reflect.TypeFor[badFanOutRecord]())
		}()
	}
}

// ── ColSlice element type must match the record field ────────────────────────

type mismatchedSliceRecord struct {
	ID     int32   `cb:"1"`
	TagIDs []int32 `cb:"2"`
}

type mismatchedSliceTable struct {
	Model[mismatchedSliceTable, mismatchedSliceRecord]
	ID     Col[*mismatchedSliceTable, int32]
	TagIDs ColSlice[*mismatchedSliceTable, int64] // the field holds int32
}

func (t mismatchedSliceTable) GetSchema() Schema {
	return Schema{Entity: "mismatched_slice", Keys: Cols(t.ID.Size(32)), Indexes: []Index{{Keys: Cols(t.TagIDs.Size(32))}}}
}

func TestColSliceElementTypeMustMatchTheField(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a ColSlice[int64] on a []int32 field")
		}
	}()
	NewRepo[mismatchedSliceTable, mismatchedSliceRecord]()
}

// ── TableID collision guard ──────────────────────────────────────────────────

type clashingRecord struct {
	ID int32
}

type clashingTable struct {
	Model[clashingTable, clashingRecord]
	ID Col[*clashingTable, int32]
}

// Same explicit TableID as ProductTable, different entity.
func (t clashingTable) GetSchema() Schema {
	return Schema{Entity: "clash", TableID: 12345678, Keys: Cols(t.ID.Size(32))}
}

func TestTableIDCollisionPanics(t *testing.T) {
	newProducts(t)
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for two entities on one TableID")
		}
	}()
	NewRepo[clashingTable, clashingRecord]()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
