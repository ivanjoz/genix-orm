package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// FileStore is the file store genix-orm keeps its files in, whatever the database: the
// by-IDs slot files and the delta logs (dynamo), and the DataFrames (the dataframe module). Keys are
// relative to the store (each feature prefixes its own folder, like "ids-cache/"); an implementation
// on S3 prefixes its own root. berryapps implements it on S3 (core/filestore); MemoryFileStore is the
// one for tests. ctx bounds every write.
type FileStore interface {
	// GetFile returns the file and its ETag, or an error wrapping ErrFileNotFound.
	GetFile(key string) (content []byte, etag string, err error)
	// PutFileIfMatch writes the file only while its ETag is still etag, or while it doesn't exist when
	// etag is "", and returns its new ETag. Otherwise it returns an error wrapping
	// ErrFilePreconditionFailed.
	PutFileIfMatch(ctx context.Context, key string, content []byte, etag string) (newETag string, err error)
	// AppendBytesToFile adds content at the end of the file, creating it when it is missing.
	// Concurrent appends must all land: they come from every API request at once.
	AppendBytesToFile(ctx context.Context, key string, content []byte) error
	// ListFiles returns every key under prefix, in any order.
	ListFiles(prefix string) ([]string, error)
	// DeleteFiles removes the files; a missing one is not an error.
	DeleteFiles(ctx context.Context, keys ...string) error
}

var (
	ErrFileNotFound           = errors.New("db: file not found")
	ErrFilePreconditionFailed = errors.New("db: file changed since it was read")
)

// MemoryFileStore is a FileStore held in memory, for tests. It is safe for concurrent use.
type MemoryFileStore struct {
	mutex  sync.Mutex
	files  map[string][]byte
	etags  map[string]string
	writes int // every write takes the next count as its ETag
}

func NewMemoryFileStore() *MemoryFileStore {
	return &MemoryFileStore{files: map[string][]byte{}, etags: map[string]string{}}
}

func (store *MemoryFileStore) GetFile(key string) ([]byte, string, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	content, exists := store.files[key]
	if !exists {
		return nil, "", fmt.Errorf("%s: %w", key, ErrFileNotFound)
	}
	return slices.Clone(content), store.etags[key], nil
}

func (store *MemoryFileStore) PutFileIfMatch(ctx context.Context, key string, content []byte, etag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.etags[key] != etag {
		return "", fmt.Errorf("%s: %w", key, ErrFilePreconditionFailed)
	}
	store.write(key, slices.Clone(content))
	return store.etags[key], nil
}

func (store *MemoryFileStore) AppendBytesToFile(ctx context.Context, key string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.write(key, append(slices.Clone(store.files[key]), content...))
	return nil
}

func (store *MemoryFileStore) ListFiles(prefix string) ([]string, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	var keys []string
	for key := range store.files {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (store *MemoryFileStore) DeleteFiles(ctx context.Context, keys ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	for _, key := range keys {
		delete(store.files, key)
		delete(store.etags, key)
	}
	return nil
}

// write stores content under a new ETag; the caller holds the mutex.
func (store *MemoryFileStore) write(key string, content []byte) {
	store.writes++
	store.files[key] = content
	store.etags[key] = strconv.Itoa(store.writes)
}
