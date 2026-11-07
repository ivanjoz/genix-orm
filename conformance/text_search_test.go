package conformance

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
)

// Seeded from TestTextSearchOnScylla.

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
	CompanyID db.Col[*textSearchSchema, int32]
	ID        db.Col[*textSearchSchema, int32]
	Name      db.Col[*textSearchSchema, string]
	Brand     db.Col[*textSearchSchema, string]
	Price     db.Col[*textSearchSchema, int32]
	Status    db.Col[*textSearchSchema, int8]
}

func (e textSearchSchema) GetSchema() db.TableSchema {
	return db.TableSchema{
		ID:         12003,
		Name:       "conformance_text_search_records",
		Partition:  e.CompanyID,
		Keys:       db.Cols(e.ID),
		TextSearch: &db.TextSearch{Columns: db.Cols(e.Name, e.Brand), Status: e.Status},
	}
}

// memoryTextSearch is an in-memory backend: the documents a real one would hold per partition, and
// a search that matches every document containing the query.
type memoryTextSearch struct {
	mutex      sync.Mutex
	partitions map[string]map[int32]string
}

func (backend *memoryTextSearch) partition(table, partition string) map[int32]string {
	key := table + "/" + partition
	if backend.partitions[key] == nil {
		backend.partitions[key] = map[int32]string{}
	}
	return backend.partitions[key]
}

func (backend *memoryTextSearch) Apply(_ context.Context, table, partition string, puts []db.TextDoc, deletes []int32) error {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	documents := backend.partition(table, partition)
	for _, document := range puts {
		documents[document.ID] = document.Text
	}
	for _, id := range deletes {
		delete(documents, id)
	}
	return nil
}

func (backend *memoryTextSearch) Search(_ context.Context, table, partition, query string, limit, offset int) ([]db.TextHit, error) {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	hits := []db.TextHit{}
	documents := backend.partition(table, partition)
	for _, id := range slices.Sorted(maps.Keys(documents)) {
		if strings.Contains(strings.ToLower(documents[id]), strings.ToLower(query)) {
			hits = append(hits, db.TextHit{ID: id, Score: 1})
		}
	}
	return hits, nil
}

func (backend *memoryTextSearch) Compact(context.Context, string, string) error { return nil }

func (backend *memoryTextSearch) Rebuild(ctx context.Context, table, partition string, source db.TextSource) error {
	documents := map[int32]string{}
	err := source(ctx, func(document db.TextDoc) error {
		documents[document.ID] = document.Text
		return nil
	})
	if err != nil {
		return err
	}
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	backend.partitions[table+"/"+partition] = documents
	return nil
}

func (backend *memoryTextSearch) Drop(_ context.Context, table, partition string) error {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	delete(backend.partitions, table+"/"+partition)
	return nil
}

// documents is what the backend holds for every partition of the test table.
func (backend *memoryTextSearch) documents() map[string]map[int32]string {
	backend.mutex.Lock()
	defer backend.mutex.Unlock()
	held := map[string]map[int32]string{}
	for key, documents := range backend.partitions {
		if partition, isTestTable := strings.CutPrefix(key, "conformance_text_search_records/"); isTestTable {
			held[partition] = maps.Clone(documents)
		}
	}
	return held
}

// TestTextSearch: inserts, an update of one indexed column (the ORM re-reads the other), an update of
// an unindexed one, a deactivation, a reactivation through Merge, the search with hydration and a
// reindex of every partition.
func TestTextSearch(t *testing.T) {
	backend := &memoryTextSearch{partitions: map[string]map[int32]string{}}
	db.SetTextSearchBackend(backend)
	t.Cleanup(func() { db.SetTextSearchBackend(nil) })
	controller := makeController[textSearchRecord, textSearchSchema]()

	runConformance(t, []db.Controller{controller}, func(run *conformanceRun) {
		table := db.TableOf[textSearchRecord]()
		run.must("Insert", db.Upsert(&[]textSearchRecord{
			{CompanyID: 1, ID: 10, Name: "Polo", Brand: "Nike", Status: 1},
			{CompanyID: 1, ID: 11, Name: "Gorra", Brand: "Puma", Status: 1},
			{CompanyID: 1, ID: 12, Name: "Casaca", Brand: "Adidas", Status: 0},
			{CompanyID: 2, ID: 20, Name: "Polo", Brand: "Lacoste", Status: 1},
		}))
		run.observe("after the insert", backend.documents())

		run.must("Update name", db.Update(&[]textSearchRecord{{CompanyID: 1, ID: 10, Name: "Polo Pique"}}, table.Name))
		run.observe("after an update of name", backend.documents())
		run.must("Update price", db.Update(&[]textSearchRecord{{CompanyID: 1, ID: 11, Price: 50}}, table.Price))
		run.observe("after an update of price", backend.documents())
		run.must("Update status", db.Update(&[]textSearchRecord{{CompanyID: 1, ID: 11}}, table.Status))
		run.observe("after status 0", backend.documents())

		merged := []textSearchRecord{{CompanyID: 1, ID: 12, Name: "Casaca", Brand: "Adidas", Status: 1}}
		run.must("Merge", db.Merge(&merged, nil, func(_, _ *textSearchRecord) bool { return true }, func(*textSearchRecord) {}))
		run.observe("after reactivating 12 with Merge", backend.documents())

		run.read("SearchText polo", func() (any, error) {
			found := []textSearchRecord{}
			hits, err := db.SearchText(&found, 1, "polo", 10, 0)
			return map[string]any{"hits": hits, "records": found}, err
		})

		// The backend lost everything: a reindex of every partition restores it from the table.
		backend.mutex.Lock()
		backend.partitions = map[string]map[int32]string{}
		backend.mutex.Unlock()
		run.must("ReindexTextSearchAll", controller.ReindexTextSearchAll())
		run.observe("after ReindexTextSearchAll", backend.documents())
	})
}
