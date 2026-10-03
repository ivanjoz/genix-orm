package scylla

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

type textSearchRecord struct {
	TableStruct[textSearchSchema, textSearchRecord]
	CompanyID int32  `db:"company_id"`
	ID        int32  `db:"id"`
	Name      string `db:"name"`
	Brand     string `db:"brand"`
	Price     int32  `db:"price"`
	Status    int8   `db:"status"`
}

type textSearchSchema struct {
	TableStruct[textSearchSchema, textSearchRecord]
	CompanyID Col[*textSearchSchema, int32]
	ID        Col[*textSearchSchema, int32]
	Name      Col[*textSearchSchema, string]
	Brand     Col[*textSearchSchema, string]
	Price     Col[*textSearchSchema, int32]
	Status    Col[*textSearchSchema, int8]
}

func (e textSearchSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:         10101,
		Name:       "text_search_records",
		Partition:  e.CompanyID,
		Keys:       Cols(e.ID),
		TextSearch: &TextSearch{Columns: Cols(e.Name, e.Brand), Status: e.Status},
	}
}

// fakeTextSearch records what the ORM sends to the backend.
type fakeTextSearch struct {
	mu      sync.Mutex
	applies []fakeApply
}

type fakeApply struct {
	table, partition string
	puts             []db.TextDoc
	dels             []int32
}

func (f *fakeTextSearch) Apply(_ context.Context, table, partition string, puts []db.TextDoc, dels []int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applies = append(f.applies, fakeApply{table, partition, puts, dels})
	return nil
}
func (f *fakeTextSearch) Search(context.Context, string, string, string, int, int) ([]db.TextHit, error) {
	return nil, nil
}
func (f *fakeTextSearch) Compact(context.Context, string, string) error { return nil }
func (f *fakeTextSearch) Rebuild(context.Context, string, string, db.TextSource) error {
	return nil
}
func (f *fakeTextSearch) Drop(context.Context, string, string) error { return nil }

func installFakeTextSearch(t *testing.T) *fakeTextSearch {
	fake := &fakeTextSearch{}
	db.SetTextSearchBackend(fake)
	t.Cleanup(func() { db.SetTextSearchBackend(nil) })
	return fake
}

func compiledTextSearchTable(t *testing.T) ScyllaTable {
	t.Helper()
	resetORMTableCachesForTesting()
	return MakeScyllaTable[textSearchRecord, textSearchSchema]()
}

func columnsNamed(scyllaTable *ScyllaTable, names ...string) []IColInfo {
	columns := []IColInfo{}
	for _, name := range names {
		columns = append(columns, scyllaTable.ColumnsMap[name])
	}
	return columns
}

func TestTextSearchSyncInsert(t *testing.T) {
	fake := installFakeTextSearch(t)
	scyllaTable := compiledTextSearchTable(t)

	records := []textSearchRecord{
		{CompanyID: 1, ID: 10, Name: "Polo", Brand: "Nike", Status: 1},
		{CompanyID: 1, ID: 11, Name: "Gorra", Status: 0}, // inactive: deleted, in case it existed
		{CompanyID: 2, ID: 20, Name: " Casaca ", Brand: "", Status: 3},
	}

	err := syncTextSearchAfterWrite(&scyllaTable, textSearchWrite{
		records: makeRecordSlice(&records),
		columns: collectAffectedColumnsForInsert(&scyllaTable, nil),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(fake.applies) != 2 {
		t.Fatalf("one Apply per partition: got %d", len(fake.applies))
	}

	first, second := fake.applies[0], fake.applies[1]
	if first.table != "text_search_records" || first.partition != "p1" ||
		!slices.Equal(first.puts, []db.TextDoc{{ID: 10, Text: "Polo Nike"}}) || !slices.Equal(first.dels, []int32{11}) {
		t.Errorf("partition 1: %+v", first)
	}
	if second.partition != "p2" || !slices.Equal(second.puts, []db.TextDoc{{ID: 20, Text: "Casaca"}}) || len(second.dels) != 0 {
		t.Errorf("partition 2: %+v", second)
	}
}

func TestTextSearchSyncUpdate(t *testing.T) {
	fake := installFakeTextSearch(t)
	scyllaTable := compiledTextSearchTable(t)

	records := []textSearchRecord{
		{CompanyID: 1, ID: 10, Name: "Polo", Brand: "Nike", Status: 1},
		{CompanyID: 1, ID: 11, Name: "Gorra", Brand: "Puma", Status: 0},
		{CompanyID: 1, ID: 12, Name: "Casaca", Brand: "Adidas", Status: 1},
	}
	slice := makeRecordSlice(&records)

	// An update that does not touch the text or the status changes nothing.
	write := textSearchWrite{records: slice, columns: columnsNamed(&scyllaTable, "price", "updated")}
	if err := syncTextSearchAfterWrite(&scyllaTable, write); err != nil {
		t.Fatal(err)
	}
	if len(fake.applies) != 0 {
		t.Fatalf("an update of price wrote to the index: %+v", fake.applies)
	}

	// Writing every indexed column: the records in memory are the stored state. 12 is unchanged
	// (Merge compared it), so it is skipped.
	write = textSearchWrite{
		records: slice,
		columns: columnsNamed(&scyllaTable, "name", "brand", "status"),
		skip:    map[int64]bool{12: true},
	}
	if err := syncTextSearchAfterWrite(&scyllaTable, write); err != nil {
		t.Fatal(err)
	}
	if len(fake.applies) != 1 {
		t.Fatalf("applies: %+v", fake.applies)
	}
	got := fake.applies[0]
	if !slices.Equal(got.puts, []db.TextDoc{{ID: 10, Text: "Polo Nike"}}) || !slices.Equal(got.dels, []int32{11}) {
		t.Errorf("full update: %+v", got)
	}
}

func TestTextSearchManualAndMissingBackend(t *testing.T) {
	scyllaTable := compiledTextSearchTable(t)
	records := []textSearchRecord{{CompanyID: 1, ID: 10, Name: "Polo", Status: 1}}
	write := textSearchWrite{records: makeRecordSlice(&records), columns: collectAffectedColumnsForInsert(&scyllaTable, nil)}

	db.SetTextSearchBackend(nil)
	err := syncTextSearchAfterWrite(&scyllaTable, write)
	if err == nil || !strings.Contains(err.Error(), "no text search backend") {
		t.Errorf("without a backend: %v", err)
	}

	fake := installFakeTextSearch(t)
	scyllaTable.textSearchIndex.manual = true
	if err := syncTextSearchAfterWrite(&scyllaTable, write); err != nil || len(fake.applies) != 0 {
		t.Errorf("a Manual table synced on write: %+v %v", fake.applies, err)
	}
}

type textSearchVirtualRecord struct {
	TableStruct[textSearchVirtualSchema, textSearchVirtualRecord]
	CompanyID int32  `db:"company_id"`
	ID        int32  `db:"id"`
	Name      string `db:"name"`
	Code      int64  `db:"code"`
}

type textSearchVirtualSchema struct {
	TableStruct[textSearchVirtualSchema, textSearchVirtualRecord]
	CompanyID Col[*textSearchVirtualSchema, int32]
	ID        Col[*textSearchVirtualSchema, int32]
	Name      Col[*textSearchVirtualSchema, string]
	Code      Col[*textSearchVirtualSchema, int64]
}

func (e textSearchVirtualSchema) GetSchema() TableSchema {
	return TableSchema{
		ID:         10102,
		Name:       "text_search_bad_records",
		Partition:  e.CompanyID,
		Keys:       Cols(e.ID),
		TextSearch: &TextSearch{Columns: Cols(e.Name, e.Code)},
	}
}

func TestTextSearchRejectsANonStringColumn(t *testing.T) {
	resetORMTableCachesForTesting()

	defer func() {
		recovered := recover()
		if recovered == nil || !strings.Contains(toString(recovered), `column "code" must be string`) {
			t.Fatalf("expected a panic naming the int column, got: %v", recovered)
		}
	}()

	MakeScyllaTable[textSearchVirtualRecord, textSearchVirtualSchema]()
}

func TestTextSearchPartitionNames(t *testing.T) {
	for value, want := range map[int64]string{0: "p0", 12: "p12", -3: "n3"} {
		if got := db.TextSearchPartition(value); got != want {
			t.Errorf("TextSearchPartition(%d) = %q; want %q", value, got, want)
		}
	}

	if got := db.JoinSearchText(" Polo ", "", "Nike"); got != "Polo Nike" {
		t.Errorf("JoinSearchText = %q", got)
	}
}
