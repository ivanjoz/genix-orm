package dataframe

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Store is where the DataFrame files live. The ORM names every object by a key relative to the
// store ("<table>/<frame>/..."): an implementation on S3 prefixes its own root. berryapps implements
// it on S3 (core/frames); MemoryStore is the one for tests. ctx bounds every write: a compaction's
// writes end at its lock's deadline (lock.go), a write's log append at the write's.
type Store interface {
	// Get returns the object and its ETag, or an error wrapping ErrObjectNotFound.
	Get(key string) (content []byte, etag string, err error)
	// Put writes the object, whatever is stored.
	Put(ctx context.Context, key string, content []byte) error
	// PutIfMatch writes the object only while its ETag is still etag, or while it doesn't exist when
	// etag is "", and returns its new ETag. Otherwise it returns an error wrapping
	// ErrPreconditionFailed.
	PutIfMatch(ctx context.Context, key string, content []byte, etag string) (newETag string, err error)
	// Append adds content at the end of the object, creating it when it is missing. Concurrent appends
	// must all land: the log's appends come from every API request at once.
	Append(ctx context.Context, key string, content []byte) error
	// List returns every key under prefix, in any order.
	List(prefix string) ([]string, error)
	// Delete removes the objects; a missing one is not an error.
	Delete(ctx context.Context, keys ...string) error
}

var (
	ErrObjectNotFound     = errors.New("db: frame object not found")
	ErrPreconditionFailed = errors.New("db: frame object changed since it was read")
	// ErrNotBuilt is what a frame read returns while the frame has no files to read: before its first
	// run, or while its files are rebuilt after a change of its shape.
	ErrNotBuilt = errors.New("db: the DataFrame is not built yet: its next run builds it")
)

// MemoryStore is a Store held in memory, for tests. It is safe for concurrent use.
type MemoryStore struct {
	mutex      sync.Mutex
	objects    map[string][]byte
	etags      map[string]string
	writeCount int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: map[string][]byte{}, etags: map[string]string{}}
}

func (store *MemoryStore) Get(key string) ([]byte, string, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	content, exists := store.objects[key]
	if !exists {
		return nil, "", fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	return slices.Clone(content), store.etags[key], nil
}

func (store *MemoryStore) Put(ctx context.Context, key string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.write(key, slices.Clone(content))
	return nil
}

func (store *MemoryStore) PutIfMatch(ctx context.Context, key string, content []byte, etag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.etags[key] != etag {
		return "", fmt.Errorf("%s: %w", key, ErrPreconditionFailed)
	}
	store.write(key, slices.Clone(content))
	return store.etags[key], nil
}

func (store *MemoryStore) Append(ctx context.Context, key string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.write(key, append(slices.Clone(store.objects[key]), content...))
	return nil
}

func (store *MemoryStore) List(prefix string) ([]string, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	var keys []string
	for key := range store.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (store *MemoryStore) Delete(ctx context.Context, keys ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	for _, key := range keys {
		delete(store.objects, key)
		delete(store.etags, key)
	}
	return nil
}

// write stores content under a new ETag; the caller holds the mutex.
func (store *MemoryStore) write(key string, content []byte) {
	store.writeCount++
	store.objects[key] = content
	store.etags[key] = strconv.Itoa(store.writeCount)
}
