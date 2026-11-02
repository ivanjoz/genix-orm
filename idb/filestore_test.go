//go:build js

package idb

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ivanjoz/genix-orm/idb/indexeddb"
	"github.com/ivanjoz/genix-orm/utils"
)

// These tests need IndexedDB, so they run in Node with the fake-indexeddb polyfill preloaded:
//
//	NODE_OPTIONS="--import <dir>/setup.mjs" GOOS=js GOARCH=wasm \
//	  go test -exec="$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./idb/...
//
// where <dir>/setup.mjs is `import "fake-indexeddb/auto";` and <dir>/node_modules has fake-indexeddb.

func openTestFileStore(t *testing.T, databaseName string) *FileStore {
	t.Helper()
	database, err := indexeddb.Open(databaseName)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewFileStore(database, "files")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestFileStoreConditionalWrites(t *testing.T) {
	store := openTestFileStore(t, "filestore-conditional")
	ctx := context.Background()

	if _, _, err := store.GetFile("ids-cache/a"); !errors.Is(err, utils.ErrFileNotFound) {
		t.Fatalf("missing file: got %v, want ErrFileNotFound", err)
	}
	firstETag, err := store.PutFileIfMatch(ctx, "ids-cache/a", []byte("one"), "")
	if err != nil {
		t.Fatal(err)
	}
	// Creating again must fail: the file exists now.
	if _, err := store.PutFileIfMatch(ctx, "ids-cache/a", []byte("two"), ""); !errors.Is(err, utils.ErrFilePreconditionFailed) {
		t.Fatalf("create over existing: got %v, want ErrFilePreconditionFailed", err)
	}
	secondETag, err := store.PutFileIfMatch(ctx, "ids-cache/a", []byte("two"), firstETag)
	if err != nil {
		t.Fatal(err)
	}
	// The first ETag is stale now.
	if _, err := store.PutFileIfMatch(ctx, "ids-cache/a", []byte("three"), firstETag); !errors.Is(err, utils.ErrFilePreconditionFailed) {
		t.Fatalf("stale etag: got %v, want ErrFilePreconditionFailed", err)
	}
	content, etag, err := store.GetFile("ids-cache/a")
	if err != nil || string(content) != "two" || etag != secondETag {
		t.Fatalf("got %q %q %v, want %q %q", content, etag, err, "two", secondETag)
	}
}

func TestFileStoreAppendListDelete(t *testing.T) {
	store := openTestFileStore(t, "filestore-append")
	ctx := context.Background()

	for _, chunk := range []string{"ab", "cd", "ef"} {
		if err := store.AppendBytesToFile(ctx, "delta-log/1", []byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if content, _, err := store.GetFile("delta-log/1"); err != nil || string(content) != "abcdef" {
		t.Fatalf("append: got %q %v", content, err)
	}

	if err := store.AppendBytesToFile(ctx, "delta-log/2", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutFileIfMatch(ctx, "ids-cache/9", []byte("y"), ""); err != nil {
		t.Fatal(err)
	}
	keys, err := store.ListFiles("delta-log/")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"delta-log/1", "delta-log/2"}) {
		t.Fatalf("list: got %v", keys)
	}

	if err := store.DeleteFiles(ctx, "delta-log/1", "missing"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetFile("delta-log/1"); !errors.Is(err, utils.ErrFileNotFound) {
		t.Fatalf("after delete: got %v", err)
	}
}

// EnsureStores on an already open database must upgrade it in place, keeping existing data.
func TestEnsureStoresUpgradesKeepingData(t *testing.T) {
	store := openTestFileStore(t, "filestore-upgrade")
	if _, err := store.PutFileIfMatch(context.Background(), "k", []byte("kept"), ""); err != nil {
		t.Fatal(err)
	}
	err := store.database.EnsureStores(indexeddb.StoreSpec{
		Name:    "products",
		Indexes: []indexeddb.IndexSpec{{Name: "by_category", KeyPath: []string{"p", "c"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if content, _, err := store.GetFile("k"); err != nil || string(content) != "kept" {
		t.Fatalf("after upgrade: got %q %v", content, err)
	}
}
