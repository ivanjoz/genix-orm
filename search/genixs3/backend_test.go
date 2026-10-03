package genixs3

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/db"
	genix "github.com/ivanjoz/genix-search/genix-s3"
)

func newTestBackend(t *testing.T, minIndexDocs int) *Backend {
	t.Helper()

	store, err := genix.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	backend, err := NewWithStore(store, Config{
		MinIndexDocs: minIndexDocs,
		Compact:      genix.CompactSync,
		OnCompactError: func(table, partition string, err error) {
			t.Errorf("compaction of %s/%s: %v", table, partition, err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	return backend
}

func searchIDs(t *testing.T, b *Backend, partition, query string) []int32 {
	t.Helper()

	hits, err := b.Search(context.Background(), "products", partition, query, 20, 0)
	if err != nil {
		t.Fatalf("Search %q: %v", query, err)
	}

	ids := make([]int32, len(hits))
	for i, hit := range hits {
		ids[i] = hit.ID
	}
	slices.Sort(ids)

	return ids
}

func TestApplySearchAndDelete(t *testing.T) {
	ctx := context.Background()

	// Small enough that the writes below cross it and build an index.
	b := newTestBackend(t, 4)

	puts := []db.TextDoc{
		{ID: 1, Text: "Piña Golden"},
		{ID: 2, Text: "Fresa Albion"},
		{ID: 3, Text: "Arándano Biloxi"},
	}
	if err := b.Apply(ctx, "products", "p7", puts, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(ctx, "products", "p7", []db.TextDoc{{ID: 4, Text: "Fresa San Andreas"}, {ID: 5, Text: "Piña MD2"}}, nil); err != nil {
		t.Fatal(err)
	}

	if got := searchIDs(t, b, "p7", "fresa"); !slices.Equal(got, []int32{2, 4}) {
		t.Errorf("fresa = %v, want [2 4]", got)
	}
	if got := searchIDs(t, b, "p7", "pina"); !slices.Equal(got, []int32{1, 5}) {
		t.Errorf("pina = %v, want [1 5]", got)
	}

	// Partitions are separate indexes.
	if got := searchIDs(t, b, "p8", "fresa"); len(got) != 0 {
		t.Errorf("fresa in an unwritten partition = %v", got)
	}

	// A rename is a put; a deactivation is a delete.
	if err := b.Apply(ctx, "products", "p7", []db.TextDoc{{ID: 2, Text: "Frambuesa Heritage"}}, []int32{4}); err != nil {
		t.Fatal(err)
	}
	if got := searchIDs(t, b, "p7", "fresa"); len(got) != 0 {
		t.Errorf("fresa after the rename and delete = %v", got)
	}
	if got := searchIDs(t, b, "p7", "frambuesa"); !slices.Equal(got, []int32{2}) {
		t.Errorf("frambuesa = %v, want [2]", got)
	}

	if err := b.Compact(ctx, "products", "p7"); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := searchIDs(t, b, "p7", "frambuesa"); !slices.Equal(got, []int32{2}) {
		t.Errorf("frambuesa after Compact = %v, want [2]", got)
	}
}

func TestScoresAndTypos(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t, 100)

	puts := []db.TextDoc{{ID: 1, Text: "manzana roja"}, {ID: 2, Text: "manzana verde grande"}}
	if err := b.Apply(ctx, "products", "p1", puts, nil); err != nil {
		t.Fatal(err)
	}

	hits, err := b.Search(ctx, "products", "p1", "manzana verde", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) == 0 || hits[0].ID != 2 || hits[0].Score == 0 {
		t.Errorf("manzana verde = %+v, want 2 first with a score", hits)
	}

	// Fuzzy by default.
	if got := searchIDs(t, b, "p1", "manzena"); !slices.Equal(got, []int32{1, 2}) {
		t.Errorf("manzena = %v, want [1 2]", got)
	}

	b.exact = true
	if got := searchIDs(t, b, "p1", "manzena"); len(got) != 0 {
		t.Errorf("manzena with Exact = %v, want none", got)
	}
}

func TestRebuildAndDrop(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t, 10)

	if err := b.Apply(ctx, "products", "n3", []db.TextDoc{{ID: 1, Text: "stale name"}}, nil); err != nil {
		t.Fatal(err)
	}

	source := func(ctx context.Context, yield func(db.TextDoc) error) error {
		for id := int32(1); id <= 50; id++ {
			if err := yield(db.TextDoc{ID: id, Text: fmt.Sprintf("producto %d", id)}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := b.Rebuild(ctx, "products", "n3", source); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	if got := searchIDs(t, b, "n3", "stale"); len(got) != 0 {
		t.Errorf("stale after Rebuild = %v", got)
	}
	// Hits are ranked, not filtered: 42 matches both words, the rest one.
	hits, err := b.Search(ctx, "products", "n3", "producto 42", 5, 0)
	if err != nil || len(hits) == 0 || hits[0].ID != 42 {
		t.Errorf("producto 42 = %+v %v, want 42 first", hits, err)
	}

	if err := b.Drop(ctx, "products", "n3"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if got := searchIDs(t, b, "n3", "producto"); len(got) != 0 {
		t.Errorf("producto after Drop = %v", got)
	}

	// A dropped partition takes writes again.
	if err := b.Apply(ctx, "products", "n3", []db.TextDoc{{ID: 7, Text: "producto nuevo"}}, nil); err != nil {
		t.Fatal(err)
	}
	if got := searchIDs(t, b, "n3", "nuevo"); !slices.Equal(got, []int32{7}) {
		t.Errorf("nuevo = %v, want [7]", got)
	}
}

func TestDeletesOnAMissingPartition(t *testing.T) {
	ctx := context.Background()
	b := newTestBackend(t, 100)

	if err := b.Apply(ctx, "products", "p9", nil, []int32{1, 2}); err != nil {
		t.Fatalf("deletes alone: %v", err)
	}
	if err := b.Compact(ctx, "products", "p9"); err != nil {
		t.Fatalf("Compact of a missing partition: %v", err)
	}
	if got := searchIDs(t, b, "p9", "anything"); len(got) != 0 {
		t.Errorf("search on a missing partition = %v", got)
	}
}

func TestNewNeedsABucket(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Error("New without a bucket succeeded")
	}
}
