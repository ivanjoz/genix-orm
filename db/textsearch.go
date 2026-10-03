package db

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
)

// TextSearch declares a table's text index. The indexed text of a record is
// its Columns joined by a space, so a write and a reindex from the database
// always produce the same text: every column must be persisted, never filled
// by the caller.
//
// Only active records are indexed: a record whose Status is 0 is removed from
// the index, and goes back in when it becomes active again. Without Status
// every record is active.
type TextSearch struct {
	// Columns are the string columns whose text is indexed, in order.
	Columns []Coln
	// Status is the optional int8 column that marks a record inactive (0).
	Status Coln
	// Manual turns off the sync on save: the index only changes through the
	// controller's ReindexTextSearch. By default every write that changes a
	// record's text or its status updates the index.
	Manual bool
}

// TextDoc is one document of a text index: a record's key and its text.
type TextDoc struct {
	ID   int32
	Text string
}

// TextHit is one search result: the record's key, its score (higher is
// better) and how many words of the query it matched. The short JSON names
// are what reaches the frontend.
type TextHit struct {
	ID       int32  `json:"id"`
	Score    uint32 `json:"s"`
	Coverage uint8  `json:"c,omitempty"`
}

// TextSource streams every active document of one partition from the
// database, for a reindex. It calls yield once per document and returns the
// first error yield returns. A backend may call it more than once.
type TextSource func(ctx context.Context, yield func(TextDoc) error) error

// TextSearchBackend is the engine that holds the text indexes: one index per
// (table, partition). The ORM keeps it in sync with the tables that declare
// TextSearch and queries it on SearchText.
type TextSearchBackend interface {
	// Apply writes puts (new or changed documents) and dels (records that
	// left the index: deleted or inactive) to one partition. An ID in both
	// ends up deleted. Deleting an ID the index does not hold is not an error.
	Apply(ctx context.Context, table, partition string, puts []TextDoc, dels []int32) error
	// Search returns the best matches of query in one partition, best first.
	Search(ctx context.Context, table, partition, query string, limit, offset int) ([]TextHit, error)
	// Compact folds the pending writes of a partition, for backends that keep
	// a write log; the others do nothing.
	Compact(ctx context.Context, table, partition string) error
	// Rebuild replaces a partition's index with the documents of source.
	Rebuild(ctx context.Context, table, partition string, source TextSource) error
	// Drop empties a partition's index.
	Drop(ctx context.Context, table, partition string) error
}

// ErrNoTextSearchBackend is returned by the text-search operations before
// SetTextSearchBackend was called.
var ErrNoTextSearchBackend = errors.New("db: no text search backend: call db.SetTextSearchBackend first")

var textSearchBackend atomic.Pointer[TextSearchBackend]

// SetTextSearchBackend installs the engine every table's text index uses.
// Call it once at startup, before the first write to a table with TextSearch.
func SetTextSearchBackend(backend TextSearchBackend) {
	textSearchBackend.Store(&backend)
}

// GetTextSearchBackend returns the installed engine.
func GetTextSearchBackend() (TextSearchBackend, error) {
	backend := textSearchBackend.Load()
	if backend == nil || *backend == nil {
		return nil, ErrNoTextSearchBackend
	}
	return *backend, nil
}

// TextSearchPartition names the index of one partition value: "p12", or
// "n12" for -12. It is a valid name for every backend.
func TextSearchPartition(partValue int64) string {
	if partValue < 0 {
		return "n" + strconv.FormatInt(-partValue, 10)
	}
	return "p" + strconv.FormatInt(partValue, 10)
}

// JoinSearchText is the indexed text of a record: the non-empty values of its
// TextSearch columns, joined by a space.
func JoinSearchText(values ...string) string {
	nonEmpty := values[:0:0]
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			nonEmpty = append(nonEmpty, value)
		}
	}
	return strings.Join(nonEmpty, " ")
}
