package scylla

import (
	"context"
	"maps"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// GENIX_ORM_SCYLLA_TEST is the host:port of a throwaway Scylla node for the tests that need one,
// e.g. podman run -d -p 127.0.0.1:19042:9042 scylladb/scylla --smp 1 --developer-mode 1. They
// write to the genix_orm_it keyspace.
func scyllaForTest(t *testing.T) {
	t.Helper()

	address := os.Getenv("GENIX_ORM_SCYLLA_TEST")
	if address == "" {
		t.Skip("GENIX_ORM_SCYLLA_TEST is not set")
	}

	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}

	MakeScyllaConnection(ConnParams{Host: host, Port: port, Keyspace: "genix_orm_it"})
	if err := Init(); err != nil {
		t.Fatal(err)
	}
}

// memTextSearch is an in-memory backend: the index state a real one would hold, and a search
// that matches every record containing the query.
type memTextSearch struct {
	mu    sync.Mutex
	parts map[string]map[int32]string
}

func (m *memTextSearch) part(table, partition string) map[int32]string {
	key := table + "/" + partition
	if m.parts[key] == nil {
		m.parts[key] = map[int32]string{}
	}
	return m.parts[key]
}

func (m *memTextSearch) Apply(_ context.Context, table, partition string, puts []db.TextDoc, dels []int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	part := m.part(table, partition)
	for _, doc := range puts {
		part[doc.ID] = doc.Text
	}
	for _, id := range dels {
		delete(part, id)
	}
	return nil
}

func (m *memTextSearch) Search(_ context.Context, table, partition, query string, limit, offset int) ([]db.TextHit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var hits []db.TextHit
	part := m.part(table, partition)
	for _, id := range slices.Sorted(maps.Keys(part)) {
		if strings.Contains(strings.ToLower(part[id]), strings.ToLower(query)) {
			hits = append(hits, db.TextHit{ID: id, Score: 1})
		}
	}
	return hits, nil
}

func (m *memTextSearch) Compact(context.Context, string, string) error { return nil }

func (m *memTextSearch) Rebuild(ctx context.Context, table, partition string, source db.TextSource) error {
	docs := map[int32]string{}
	err := source(ctx, func(doc db.TextDoc) error {
		docs[doc.ID] = doc.Text
		return nil
	})
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.parts[table+"/"+partition] = docs
	return nil
}

func (m *memTextSearch) Drop(_ context.Context, table, partition string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.parts, table+"/"+partition)
	return nil
}

func (m *memTextSearch) state(partition string) map[int32]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.part("text_search_records", partition))
}

// TestTextSearchOnScylla runs the writes of a table with TextSearch against a real node: inserts,
// an update of one indexed column (the ORM re-reads the other), a deactivation, Merge, the search
// with hydration and a reindex of every partition.
func TestTextSearchOnScylla(t *testing.T) {
	scyllaForTest(t)
	resetORMTableCachesForTesting()

	backend := &memTextSearch{parts: map[string]map[int32]string{}}
	db.SetTextSearchBackend(backend)
	t.Cleanup(func() { db.SetTextSearchBackend(nil) })

	if err := QueryExec("DROP TABLE IF EXISTS genix_orm_it.text_search_records"); err != nil {
		t.Fatal(err)
	}
	controller := &ScyllaController[textSearchRecord, textSearchSchema]{
		Table:  MakeScyllaTable[textSearchRecord, textSearchSchema](),
		Schema: MakeSchema[textSearchRecord, textSearchSchema](),
	}
	DeployScylla(0, controller)

	table := TableOf[textSearchRecord]()

	records := []textSearchRecord{
		{CompanyID: 1, ID: 10, Name: "Polo", Brand: "Nike", Status: 1},
		{CompanyID: 1, ID: 11, Name: "Gorra", Brand: "Puma", Status: 1},
		{CompanyID: 1, ID: 12, Name: "Casaca", Brand: "Adidas", Status: 0},
		{CompanyID: 2, ID: 20, Name: "Polo", Brand: "Lacoste", Status: 1},
	}
	if err := Insert(&records); err != nil {
		t.Fatal(err)
	}

	want := map[int32]string{10: "Polo Nike", 11: "Gorra Puma"}
	if got := backend.state("p1"); !maps.Equal(got, want) {
		t.Errorf("after the insert, p1 = %v; want %v", got, want)
	}

	// Only name is written: the ORM reads brand and status back to build the text.
	if err := Update(&[]textSearchRecord{{CompanyID: 1, ID: 10, Name: "Polo Pique"}}, table.Name); err != nil {
		t.Fatal(err)
	}
	if got := backend.state("p1")[10]; got != "Polo Pique Nike" {
		t.Errorf("after the update of name, 10 = %q", got)
	}

	// Price is not indexed: no change. Status 0 deletes from the index.
	if err := Update(&[]textSearchRecord{{CompanyID: 1, ID: 11, Price: 50}}, table.Price); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.state("p1")[11]; !ok {
		t.Error("an update of price removed 11 from the index")
	}
	if err := Update(&[]textSearchRecord{{CompanyID: 1, ID: 11}}, table.Status); err != nil {
		t.Fatal(err)
	}
	if _, ok := backend.state("p1")[11]; ok {
		t.Error("11 is still indexed after status 0")
	}

	// Reactivating through Merge puts it back.
	merged := []textSearchRecord{{CompanyID: 1, ID: 12, Name: "Casaca", Brand: "Adidas", Status: 1}}
	err := Merge(&merged, nil, func(_, _ *textSearchRecord) bool { return true }, func(*textSearchRecord) {})
	if err != nil {
		t.Fatal(err)
	}
	if got := backend.state("p1")[12]; got != "Casaca Adidas" {
		t.Errorf("after reactivating 12 with Merge, 12 = %q", got)
	}

	var found []textSearchRecord
	hits, err := SearchText(&found, 1, "polo", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || len(found) != 1 || found[0].ID != 10 || found[0].Brand != "Nike" {
		t.Errorf("SearchText polo: hits %+v, records %+v", hits, found)
	}

	// The index lost everything: a reindex of every partition restores it from the table.
	backend.parts = map[string]map[int32]string{}
	if err := controller.ReindexTextSearchAll(); err != nil {
		t.Fatal(err)
	}
	want = map[int32]string{10: "Polo Pique Nike", 12: "Casaca Adidas"}
	if got := backend.state("p1"); !maps.Equal(got, want) {
		t.Errorf("after ReindexTextSearchAll, p1 = %v; want %v", got, want)
	}
	if got := backend.state("p2"); !maps.Equal(got, map[int32]string{20: "Polo Lacoste"}) {
		t.Errorf("after ReindexTextSearchAll, p2 = %v", got)
	}
}
