package text_search

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/ivanjoz/genix-orm/db"
)

// Backend is the GenixSearch daemon as the ORM's text-search engine: one bucket per (table,
// partition), named after the partition. Install it with db.SetTextSearchBackend(NewBackend())
// once Configure was called.
type Backend struct{}

var _ db.TextSearchBackend = Backend{}

// NewBackend returns the daemon backend.
func NewBackend() db.TextSearchBackend { return Backend{} }

// rebuildChunk is how many documents a rebuild sends per PUSHI round.
const rebuildChunk = 500

// Apply pushes the puts and pops the dels. The daemon indexes normalized text, and a document
// whose text normalizes to nothing is popped, since PUSHI rejects an empty payload.
func (Backend) Apply(ctx context.Context, table, partition string, puts []db.TextDoc, dels []int32) error {
	if err := validateNames(table, partition); err != nil {
		return err
	}

	records, dels := normalizeDocs(puts, dels)
	if len(records) == 0 && len(dels) == 0 {
		return nil
	}

	initPools()
	conn, err := ingestMgr.acquire(ctx)
	if err != nil {
		return err
	}
	defer ingestMgr.release(conn)

	// Pushes first: an ID in both must end up deleted.
	if err := pushI(ctx, conn, table, partition, records); err != nil {
		return fmt.Errorf("text_search: pushI %s/%s: %w", table, partition, err)
	}
	if err := popIPipeline(ctx, conn, table, partition, dels); err != nil {
		return fmt.Errorf("text_search: popI %s/%s: %w", table, partition, err)
	}
	return nil
}

// Search queries one partition's bucket. The daemon's float weights become the scores, in the
// same order.
func (Backend) Search(ctx context.Context, table, partition, query string, limit, offset int) ([]db.TextHit, error) {
	if err := validateNames(table, partition); err != nil {
		return nil, err
	}

	normalized := NormalizeSearchText(query)
	if normalized == "" {
		return nil, nil
	}

	initPools()
	conn, err := searchMgr.acquire(ctx)
	if err != nil {
		return nil, err
	}

	line := fmt.Sprintf("QUERY %s %s %s", table, partition, quote(normalized))
	if limit > 0 {
		line += fmt.Sprintf(" LIMIT(%d)", limit)
	}
	if offset > 0 {
		line += fmt.Sprintf(" OFFSET(%d)", offset)
	}

	res, err := conn.execPending(ctx, line)
	searchMgr.release(conn) // release discards the conn when execPending marked it broken
	if err != nil {
		return nil, fmt.Errorf("text_search: query %s/%s: %w", table, partition, err)
	}

	matches, err := decodeMatches(res.payload)
	if err != nil {
		return nil, err
	}

	hits := make([]db.TextHit, len(matches))
	for i, match := range matches {
		hits[i] = db.TextHit{ID: match.ID, Score: weightToScore(match.Weight)}
	}
	return hits, nil
}

// Compact does nothing: the daemon has no write log to fold.
func (Backend) Compact(context.Context, string, string) error { return nil }

// Rebuild flushes the bucket and pushes every document of source. The daemon has no atomic swap,
// so a search during the rebuild sees a partial index.
func (b Backend) Rebuild(ctx context.Context, table, partition string, source db.TextSource) error {
	if err := b.Drop(ctx, table, partition); err != nil {
		return err
	}

	chunk := make([]db.TextDoc, 0, rebuildChunk)
	flush := func() error {
		err := b.Apply(ctx, table, partition, chunk, nil)
		chunk = chunk[:0]
		return err
	}

	err := source(ctx, func(doc db.TextDoc) error {
		chunk = append(chunk, doc)
		if len(chunk) < rebuildChunk {
			return nil
		}
		return flush()
	})
	if err != nil {
		return err
	}
	return flush()
}

// Drop flushes the partition's bucket.
func (Backend) Drop(ctx context.Context, table, partition string) error {
	if err := validateNames(table, partition); err != nil {
		return err
	}

	initPools()
	conn, err := ingestMgr.acquire(ctx)
	if err != nil {
		return err
	}
	defer ingestMgr.release(conn)

	if _, err := conn.exec(ctx, fmt.Sprintf("FLUSHB %s %s", table, partition)); err != nil {
		return fmt.Errorf("text_search: flushB %s/%s: %w", table, partition, err)
	}
	return nil
}

func validateNames(table, partition string) error {
	if err := validateIdentifier(table); err != nil {
		return err
	}
	return validateIdentifier(partition)
}

// normalizeDocs turns puts into daemon records, moving the ones with no indexable text to dels.
func normalizeDocs(puts []db.TextDoc, dels []int32) ([]Record, []int32) {
	records := make([]Record, 0, len(puts))
	for _, doc := range puts {
		text := strings.TrimSpace(NormalizeSearchText(doc.Text))
		if text == "" {
			dels = append(dels, doc.ID)
			continue
		}
		records = append(records, Record{ID: doc.ID, SearchText: text})
	}
	return records, dels
}

// weightToScore keeps three decimals of the daemon's weight.
func weightToScore(weight float32) uint32 {
	if weight <= 0 || math.IsNaN(float64(weight)) {
		return 0
	}
	return uint32(min(float64(weight)*1000, math.MaxUint32))
}
