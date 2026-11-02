//go:build js

package idb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"syscall/js"

	"github.com/ivanjoz/genix-orm/idb/indexeddb"
	"github.com/ivanjoz/genix-orm/utils"
)

// FileStore is utils.FileStore on one IndexedDB object store: the ORM's by-IDs slot files, delta
// logs and DataFrames, kept in the browser. Each file is {content: Blob, etag: string}.
//
// Every conditional write and every append is a single IndexedDB read-modify-write transaction, so
// two tabs writing the same file serialize instead of losing a write.
type FileStore struct {
	database  *indexeddb.Database
	storeName string
}

// NewFileStore creates the object store when it is missing.
func NewFileStore(database *indexeddb.Database, storeName string) (*FileStore, error) {
	if err := database.EnsureStores(indexeddb.StoreSpec{Name: storeName}); err != nil {
		return nil, err
	}
	return &FileStore{database: database, storeName: storeName}, nil
}

func (store *FileStore) GetFile(key string) ([]byte, string, error) {
	record, exists, err := store.database.Get(store.storeName, key)
	if err != nil {
		return nil, "", err
	}
	if !exists {
		return nil, "", fmt.Errorf("%s: %w", key, utils.ErrFileNotFound)
	}
	content, err := indexeddb.BlobBytes(record.Get("content"))
	if err != nil {
		return nil, "", err
	}
	return content, record.Get("etag").String(), nil
}

func (store *FileStore) PutFileIfMatch(ctx context.Context, key string, content []byte, etag string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	newETag := makeETag()
	err := store.database.Update(store.storeName, key, func(current js.Value, exists bool) (js.Value, error) {
		currentETag := ""
		if exists {
			currentETag = current.Get("etag").String()
		}
		if currentETag != etag {
			return js.Value{}, fmt.Errorf("%s: %w", key, utils.ErrFilePreconditionFailed)
		}
		return fileRecord(indexeddb.NewBlob("", content), newETag), nil
	})
	if err != nil {
		return "", err
	}
	return newETag, nil
}

func (store *FileStore) AppendBytesToFile(ctx context.Context, key string, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.database.Update(store.storeName, key, func(current js.Value, exists bool) (js.Value, error) {
		if !exists {
			return fileRecord(indexeddb.NewBlob("", content), makeETag()), nil
		}
		// The previous Blob is referenced, not read: reading it is asynchronous and would let the
		// transaction commit before the write.
		return fileRecord(indexeddb.NewBlob("", current.Get("content"), content), makeETag()), nil
	})
}

func (store *FileStore) ListFiles(prefix string) ([]string, error) {
	keys, err := store.database.GetAllKeys(store.storeName, indexeddb.KeyRangePrefix(prefix))
	if err != nil {
		return nil, err
	}
	fileKeys := make([]string, len(keys))
	for i, key := range keys {
		fileKeys[i] = key.String()
	}
	return fileKeys, nil
}

func (store *FileStore) DeleteFiles(ctx context.Context, keys ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	deleteKeys := make([]any, len(keys))
	for i, key := range keys {
		deleteKeys[i] = key
	}
	return store.database.Delete(store.storeName, deleteKeys...)
}

func fileRecord(content js.Value, etag string) js.Value {
	return js.ValueOf(map[string]any{"content": content, "etag": etag})
}

// makeETag is random rather than a counter: two tabs write the same database, and a per-process
// counter would hand both the same ETag.
func makeETag() string {
	return strconv.FormatUint(rand.Uint64(), 36)
}
