// Package genixs3 is genix-s3 as a text-search backend of the ORM
// (db.TextSearchBackend): the index lives in an S3 bucket, an S3 Express One
// Zone directory bucket being the fast choice.
//
//	backend, err := genixs3.New(ctx, genixs3.Config{Bucket: "genix--use1-az4--x-s3"})
//	db.SetTextSearchBackend(backend)
//
// Granularity: one genix-s3 partition per (table, partition), under
// <Root><table>/<partition>/. Every save appends to the partition's p.log; the
// compaction that folds it into the index runs as Config.Compact says.
package genixs3

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ivanjoz/genix-orm/db"
	genix "github.com/ivanjoz/genix-search/genix-s3"
	"github.com/ivanjoz/genix-search/genix-s3/s3blob"
)

// Config configures the backend.
type Config struct {
	// Bucket is the S3 bucket of the index. New needs it; NewWithStore
	// ignores it.
	Bucket string
	// S3 is the client of Bucket. Default: one from the default AWS config.
	S3 *s3.Client
	// Root is the key prefix of every index; default "genix/".
	Root string
	// Extra are the two letters, besides a-z, the words of every table may
	// use; changing them needs a ReindexTextSearch of every partition.
	Extra [2]rune
	// MinIndexDocs is the size under which a partition is only its p.log.
	// Default 100.
	MinIndexDocs int
	// Compact says who folds the log: CompactAuto is sync on AWS Lambda and
	// async elsewhere.
	Compact genix.CompactMode
	// OnCompact receives the compaction requests in CompactCallback mode.
	OnCompact func(table, partition string)
	// OnCompactError receives the errors of the compactions a write runs,
	// sync or async: the write itself succeeded.
	OnCompactError func(table, partition string, err error)
	// Exact turns off typo tolerance. Searches are fuzzy by default: a word
	// with no exact match also matches the keys one or two edits away.
	Exact bool
}

// Backend is genix-s3 behind db.TextSearchBackend.
type Backend struct {
	client         *genix.Client
	exact          bool
	onCompactError func(table, partition string, err error)
}

var _ db.TextSearchBackend = (*Backend)(nil)

// New returns the backend of cfg.Bucket.
func New(ctx context.Context, cfg Config) (*Backend, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("genixs3: Config.Bucket is empty")
	}

	client := cfg.S3
	if client == nil {
		awsConfig, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("genixs3: aws config: %w", err)
		}

		client = s3.NewFromConfig(awsConfig)
	}

	return NewWithStore(s3blob.New(client, cfg.Bucket), cfg)
}

// NewWithStore returns the backend of any genix-s3 store, e.g. a directory
// from genix.NewFSStore for tests and local runs.
func NewWithStore(store genix.Blob, cfg Config) (*Backend, error) {
	client, err := genix.NewClient(store, genix.ClientOptions{
		Root:           cfg.Root,
		Extra:          cfg.Extra,
		MinIndexDocs:   cfg.MinIndexDocs,
		Compact:        cfg.Compact,
		OnCompact:      cfg.OnCompact,
		OnCompactError: cfg.OnCompactError,
	})
	if err != nil {
		return nil, fmt.Errorf("genixs3: %w", err)
	}

	return &Backend{client: client, exact: cfg.Exact, onCompactError: cfg.OnCompactError}, nil
}

// Client is the genix-s3 client underneath, for what the ORM interface does
// not cover (stats, waiting).
func (b *Backend) Client() *genix.Client { return b.client }

// Wait blocks until the async compactions running now end. A process that
// exits right after a write calls it to not lose the compaction, which the
// next write would ask for again anyway.
func (b *Backend) Wait() { b.client.Wait() }

// Apply appends the puts and deletes to the partition's log as one batch.
func (b *Backend) Apply(ctx context.Context, table, partition string, puts []db.TextDoc, dels []int32) error {
	if len(puts) == 0 && len(dels) == 0 {
		return nil
	}

	docs := make([]genix.Doc, len(puts))
	for i, doc := range puts {
		docs[i] = genix.Doc{ID: uint32(doc.ID), Text: doc.Text}
	}

	// One collection per table: every document is in group 0.
	refs := make([]genix.Ref, len(dels))
	for i, id := range dels {
		refs[i] = genix.Ref{ID: uint32(id)}
	}

	res, err := b.client.Apply(ctx, table, partition, docs, refs)
	if err != nil {
		return fmt.Errorf("genixs3: apply %s/%s: %w", table, partition, err)
	}

	// The write landed; a failed sync compaction is reported, and the next
	// write asks for it again.
	if res.CompactErr != nil && b.onCompactError != nil {
		b.onCompactError(table, partition, res.CompactErr)
	}

	return nil
}

// Search queries one partition. A partition never written has no hits.
func (b *Backend) Search(ctx context.Context, table, partition, query string, limit, offset int) ([]db.TextHit, error) {
	resp, err := b.client.Search(ctx, table, partition, genix.Request{
		Query:  query,
		Limit:  limit,
		Offset: offset,
		Fuzzy:  !b.exact,
	})
	if err != nil {
		return nil, fmt.Errorf("genixs3: search %s/%s: %w", table, partition, err)
	}

	if len(resp.Hits) == 0 {
		return nil, nil
	}

	hits := make([]db.TextHit, len(resp.Hits))
	for i, hit := range resp.Hits {
		hits[i] = db.TextHit{
			ID:       int32(hit.ID),
			Score:    hit.Score,
			Coverage: uint8(min(hit.Coverage, math.MaxUint8)),
		}
	}

	return hits, nil
}

// Compact folds the partition's log into a new generation. A compaction
// already running elsewhere does the work, so losing the race is not an error.
func (b *Backend) Compact(ctx context.Context, table, partition string) error {
	_, err := b.client.Compact(ctx, table, partition)
	if err != nil && !errors.Is(err, genix.ErrConflict) {
		return fmt.Errorf("genixs3: compact %s/%s: %w", table, partition, err)
	}

	return nil
}

// Rebuild replaces the partition with the documents of source. Searches keep
// seeing the previous generation until it ends, and writes that land
// meanwhile are kept.
func (b *Backend) Rebuild(ctx context.Context, table, partition string, source db.TextSource) error {
	_, err := b.client.Rebuild(ctx, table, partition, func(ctx context.Context, yield func(genix.Doc) error) error {
		return source(ctx, func(doc db.TextDoc) error {
			return yield(genix.Doc{ID: uint32(doc.ID), Text: doc.Text})
		})
	})
	if err != nil {
		return fmt.Errorf("genixs3: rebuild %s/%s: %w", table, partition, err)
	}

	return nil
}

// Drop empties the partition and deletes its index.
func (b *Backend) Drop(ctx context.Context, table, partition string) error {
	if _, err := b.client.Drop(ctx, table, partition); err != nil {
		return fmt.Errorf("genixs3: drop %s/%s: %w", table, partition, err)
	}

	return nil
}
