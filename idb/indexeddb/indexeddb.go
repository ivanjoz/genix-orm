//go:build js

// Package indexeddb is a thin Go wrapper over the browser's IndexedDB, for code compiled with
// GOOS=js GOARCH=wasm. It holds no ORM logic: the idb driver and the file stores build on it.
//
// Two rules of IndexedDB shape every function here:
//
//   - A transaction commits by itself as soon as the event loop finds it with no pending request.
//     A Go goroutine that waits on a channel between two requests yields to the event loop, so every
//     request of one transaction is issued synchronously: all at once (Put, Delete), or from inside
//     the previous request's success callback (Update).
//   - Every function blocks until IndexedDB answers. Call them from a goroutine, never from inside a
//     js.FuncOf callback: blocking there deadlocks the Go runtime.
package indexeddb

import (
	"errors"
	"fmt"
	"sync"
	"syscall/js"
)

// ErrAbort is returned by an Update change function to abort its transaction without writing.
var ErrAbort = errors.New("indexeddb: transaction aborted by the caller")

// StoreSpec declares an object store. Keys are always out-of-line: callers pass the key on every
// write, so the stored value never needs a key field.
type StoreSpec struct {
	Name    string
	Indexes []IndexSpec
}

// IndexSpec declares an index over the stored values. KeyPath is a property name, or a []string of
// names for a compound index. MultiEntry indexes every element of an array property separately.
type IndexSpec struct {
	Name       string
	KeyPath    any
	MultiEntry bool
	Unique     bool
}

// Database is one open IndexedDB database. It reopens itself at a higher version when EnsureStores
// needs a store or index that does not exist yet, so tables can be declared lazily.
type Database struct {
	name   string
	mutex  sync.Mutex // serializes reopening against every other operation that reads handle
	handle js.Value
}

// Open opens (or creates) the database name at its current version.
func Open(name string) (*Database, error) {
	database := &Database{name: name}
	handle, err := openHandle(name, 0, nil)
	if err != nil {
		return nil, err
	}
	database.handle = handle
	return database, nil
}

// openHandle runs indexedDB.open; version 0 means "the current one". upgrade runs inside the
// versionchange transaction, the only place stores and indexes can be created.
func openHandle(name string, version int, upgrade func(handle, upgradeTransaction js.Value)) (js.Value, error) {
	factory := js.Global().Get("indexedDB")
	if factory.IsUndefined() {
		return js.Value{}, errors.New("indexeddb: this runtime has no indexedDB")
	}
	var request js.Value
	if version > 0 {
		request = factory.Call("open", name, version)
	} else {
		request = factory.Call("open", name)
	}

	onUpgrade := js.FuncOf(func(this js.Value, args []js.Value) any {
		if upgrade != nil {
			upgrade(request.Get("result"), request.Get("transaction"))
		}
		return nil
	})
	defer onUpgrade.Release()
	request.Set("onupgradeneeded", onUpgrade)

	// Another connection (an older tab) still holds the previous version open. The upgrade waits
	// for it, so say why instead of hanging silently.
	onBlocked := js.FuncOf(func(this js.Value, args []js.Value) any {
		js.Global().Get("console").Call("warn", "indexeddb: upgrade of "+name+" is blocked by another open connection")
		return nil
	})
	defer onBlocked.Release()
	request.Set("onblocked", onBlocked)

	if _, err := awaitRequest(request); err != nil {
		return js.Value{}, fmt.Errorf("indexeddb: opening %s: %w", name, err)
	}
	handle := request.Get("result")

	// When a newer connection asks for an upgrade, step aside so it is not blocked by this one.
	// The next operation here reopens (see current).
	var onVersionChange js.Func
	onVersionChange = js.FuncOf(func(this js.Value, args []js.Value) any {
		handle.Call("close")
		onVersionChange.Release()
		return nil
	})
	handle.Set("onversionchange", onVersionChange)
	return handle, nil
}

// EnsureStores creates every missing store and index, reopening the database one version up when
// anything is missing. Existing stores and indexes are left as they are.
func (database *Database) EnsureStores(specs ...StoreSpec) error {
	database.mutex.Lock()
	defer database.mutex.Unlock()

	if !database.missesAny(specs) {
		return nil
	}
	nextVersion := database.handle.Get("version").Int() + 1
	database.handle.Call("close")

	handle, err := openHandle(database.name, nextVersion, func(handle, upgradeTransaction js.Value) {
		for _, spec := range specs {
			var store js.Value
			if handle.Get("objectStoreNames").Call("contains", spec.Name).Bool() {
				store = upgradeTransaction.Call("objectStore", spec.Name)
			} else {
				store = handle.Call("createObjectStore", spec.Name)
			}
			for _, index := range spec.Indexes {
				if store.Get("indexNames").Call("contains", index.Name).Bool() {
					continue
				}
				store.Call("createIndex", index.Name, toJS(index.KeyPath), map[string]any{
					"multiEntry": index.MultiEntry, "unique": index.Unique,
				})
			}
		}
	})
	if err != nil {
		return err
	}
	database.handle = handle
	return nil
}

// missesAny reports whether a store or index of specs does not exist. The caller holds the mutex.
func (database *Database) missesAny(specs []StoreSpec) bool {
	storeNames := database.handle.Get("objectStoreNames")
	for _, spec := range specs {
		if !storeNames.Call("contains", spec.Name).Bool() {
			return true
		}
		if len(spec.Indexes) == 0 {
			continue
		}
		indexNames := database.handle.Call("transaction", spec.Name, "readonly").Call("objectStore", spec.Name).Get("indexNames")
		for _, index := range spec.Indexes {
			if !indexNames.Call("contains", index.Name).Bool() {
				return true
			}
		}
	}
	return false
}

// transaction opens a transaction over stores. It reopens the handle first when another connection
// closed it for an upgrade (onversionchange above).
func (database *Database) transaction(mode string, stores ...string) (js.Value, error) {
	database.mutex.Lock()
	defer database.mutex.Unlock()

	storeNames := make([]any, len(stores))
	for i, store := range stores {
		storeNames[i] = store
	}
	transaction, err := tryTransaction(database.handle, storeNames, mode)
	if err == nil {
		return transaction, nil
	}
	// A closed connection throws InvalidStateError; reopen at the current version and retry once.
	handle, openErr := openHandle(database.name, 0, nil)
	if openErr != nil {
		return js.Value{}, openErr
	}
	database.handle = handle
	return tryTransaction(database.handle, storeNames, mode)
}

// tryTransaction turns the exception IDBDatabase.transaction throws into an error.
func tryTransaction(handle js.Value, storeNames []any, mode string) (transaction js.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("indexeddb: opening a transaction: %v", recovered)
		}
	}()
	return handle.Call("transaction", storeNames, mode), nil
}

// Get returns the value stored under key, and whether it exists.
func (database *Database) Get(store string, key any) (js.Value, bool, error) {
	transaction, err := database.transaction("readonly", store)
	if err != nil {
		return js.Value{}, false, err
	}
	value, err := awaitRequest(transaction.Call("objectStore", store).Call("get", toJS(key)))
	if err != nil {
		return js.Value{}, false, fmt.Errorf("indexeddb: get from %s: %w", store, err)
	}
	return value, !value.IsUndefined(), nil
}

// GetAllKeys returns every key of store inside keyRange (js.Undefined() for all of them), in key
// order.
func (database *Database) GetAllKeys(store string, keyRange js.Value) ([]js.Value, error) {
	transaction, err := database.transaction("readonly", store)
	if err != nil {
		return nil, err
	}
	keys, err := awaitRequest(transaction.Call("objectStore", store).Call("getAllKeys", keyRange))
	if err != nil {
		return nil, fmt.Errorf("indexeddb: listing keys of %s: %w", store, err)
	}
	result := make([]js.Value, keys.Length())
	for i := range result {
		result[i] = keys.Index(i)
	}
	return result, nil
}

// GetAll returns every entry of store inside keyRange, in key order. Both requests are issued in
// one transaction, so keys and values describe the same snapshot.
func (database *Database) GetAll(store string, keyRange js.Value) ([]Entry, error) {
	transaction, err := database.transaction("readonly", store)
	if err != nil {
		return nil, err
	}
	objectStore := transaction.Call("objectStore", store)
	// Both are watched before either is awaited: a request's events fire whether or not a handler
	// is attached yet, and the first wait yields to the event loop.
	keysResult := watchRequest(objectStore.Call("getAllKeys", keyRange))
	valuesResult := watchRequest(objectStore.Call("getAll", keyRange))
	keys, err := keysResult()
	if err != nil {
		return nil, fmt.Errorf("indexeddb: listing %s: %w", store, err)
	}
	values, err := valuesResult()
	if err != nil {
		return nil, fmt.Errorf("indexeddb: listing %s: %w", store, err)
	}
	entries := make([]Entry, keys.Length())
	for i := range entries {
		entries[i] = Entry{Key: keys.Index(i), Value: values.Index(i)}
	}
	return entries, nil
}

// Put writes every entry in one transaction and returns once it has committed.
func (database *Database) Put(store string, entries ...Entry) error {
	transaction, err := database.transaction("readwrite", store)
	if err != nil {
		return err
	}
	objectStore := transaction.Call("objectStore", store)
	for _, entry := range entries {
		objectStore.Call("put", entry.Value, toJS(entry.Key))
	}
	if err := awaitTransaction(transaction); err != nil {
		return fmt.Errorf("indexeddb: put into %s: %w", store, err)
	}
	return nil
}

// Entry is one value and the out-of-line key it is stored under.
type Entry struct {
	Key   any
	Value js.Value
}

// Delete removes the keys in one transaction; a missing key is not an error.
func (database *Database) Delete(store string, keys ...any) error {
	transaction, err := database.transaction("readwrite", store)
	if err != nil {
		return err
	}
	objectStore := transaction.Call("objectStore", store)
	for _, key := range keys {
		objectStore.Call("delete", toJS(key))
	}
	if err := awaitTransaction(transaction); err != nil {
		return fmt.Errorf("indexeddb: delete from %s: %w", store, err)
	}
	return nil
}

// Update reads the value under key and writes what change returns, in one transaction, so no other
// writer can slip in between. change runs inside an IndexedDB callback: it must not block, and it
// may return ErrAbort (or any error) to abort without writing; that error is returned.
func (database *Database) Update(store string, key any, change func(current js.Value, exists bool) (js.Value, error)) error {
	transaction, err := database.transaction("readwrite", store)
	if err != nil {
		return err
	}
	objectStore := transaction.Call("objectStore", store)
	jsKey := toJS(key)
	getRequest := objectStore.Call("get", jsKey)

	var changeErr error
	onRead := js.FuncOf(func(this js.Value, args []js.Value) any {
		current := getRequest.Get("result")
		next, err := change(current, !current.IsUndefined())
		if err != nil {
			changeErr = err
			transaction.Call("abort")
			return nil
		}
		// Issued from inside the read's callback: the transaction is still active here.
		objectStore.Call("put", next, jsKey)
		return nil
	})
	defer onRead.Release()
	getRequest.Set("onsuccess", onRead)

	transactionErr := awaitTransaction(transaction)
	if changeErr != nil {
		return changeErr
	}
	if transactionErr != nil {
		return fmt.Errorf("indexeddb: update in %s: %w", store, transactionErr)
	}
	return nil
}

// KeyRangePrefix is the range of string keys starting with prefix. "￿" sorts after every
// character a key can hold in practice, so it closes the range.
func KeyRangePrefix(prefix string) js.Value {
	return js.Global().Get("IDBKeyRange").Call("bound", prefix, prefix+"￿")
}

// NewBlob builds a Blob from parts: []byte values are copied into the browser, js.Value parts
// (other Blobs) are referenced without reading them. Building a Blob is synchronous, which is what
// lets an append run inside one transaction without ever reading the file it extends.
func NewBlob(contentType string, parts ...any) js.Value {
	jsParts := make([]any, 0, len(parts))
	for _, part := range parts {
		switch typedPart := part.(type) {
		case []byte:
			jsParts = append(jsParts, BytesToJS(typedPart))
		case js.Value:
			jsParts = append(jsParts, typedPart)
		default:
			panic(fmt.Sprintf("indexeddb: NewBlob part of type %T", part))
		}
	}
	return js.Global().Get("Blob").New(jsParts, map[string]any{"type": contentType})
}

// BlobBytes reads a Blob's content into Go memory.
func BlobBytes(blob js.Value) ([]byte, error) {
	buffer, err := Await(blob.Call("arrayBuffer"))
	if err != nil {
		return nil, fmt.Errorf("indexeddb: reading a blob: %w", err)
	}
	array := js.Global().Get("Uint8Array").New(buffer)
	content := make([]byte, array.Length())
	js.CopyBytesToGo(content, array)
	return content, nil
}

// BytesToJS copies content into a new Uint8Array.
func BytesToJS(content []byte) js.Value {
	array := js.Global().Get("Uint8Array").New(len(content))
	js.CopyBytesToJS(array, content)
	return array
}

// toJS converts a Go key or key path to its JS value. []string and []any become arrays (compound
// keys and key paths); js.ValueOf handles strings and numbers.
func toJS(value any) any {
	switch typedValue := value.(type) {
	case js.Value:
		return typedValue
	case []string:
		array := make([]any, len(typedValue))
		for i, element := range typedValue {
			array[i] = element
		}
		return array
	default:
		return value
	}
}

// Await blocks until promise settles and returns its value, or its rejection as an error.
func Await(promise js.Value) (js.Value, error) {
	type settlement struct {
		value js.Value
		err   error
	}
	settled := make(chan settlement, 1)
	onResolve := js.FuncOf(func(this js.Value, args []js.Value) any {
		settled <- settlement{value: args[0]}
		return nil
	})
	defer onResolve.Release()
	onReject := js.FuncOf(func(this js.Value, args []js.Value) any {
		settled <- settlement{err: jsError(args[0])}
		return nil
	})
	defer onReject.Release()
	promise.Call("then", onResolve, onReject)
	result := <-settled
	return result.value, result.err
}

// awaitRequest blocks until an IDBRequest succeeds or fails. Call it right after issuing the
// request, before anything else can yield to the event loop.
func awaitRequest(request js.Value) (js.Value, error) {
	return watchRequest(request)()
}

// watchRequest attaches the handlers now and returns a function that blocks for the outcome, so
// several requests can be watched before the first one is awaited.
func watchRequest(request js.Value) func() (js.Value, error) {
	done := make(chan error, 1)
	var onSuccess, onError js.Func
	onSuccess = js.FuncOf(func(this js.Value, args []js.Value) any {
		done <- nil
		return nil
	})
	onError = js.FuncOf(func(this js.Value, args []js.Value) any {
		done <- jsError(request.Get("error"))
		return nil
	})
	request.Set("onsuccess", onSuccess)
	request.Set("onerror", onError)
	return func() (js.Value, error) {
		err := <-done
		onSuccess.Release()
		onError.Release()
		if err != nil {
			return js.Value{}, err
		}
		return request.Get("result"), nil
	}
}

// awaitTransaction blocks until a transaction commits, or fails or aborts.
func awaitTransaction(transaction js.Value) error {
	done := make(chan error, 1)
	// A failing transaction fires onerror and then onabort. Only the first outcome is kept: a second
	// blocking send would hang inside a JS callback and deadlock the runtime.
	settle := func(err error) {
		select {
		case done <- err:
		default:
		}
	}
	onComplete := js.FuncOf(func(this js.Value, args []js.Value) any {
		settle(nil)
		return nil
	})
	defer onComplete.Release()
	onFailure := js.FuncOf(func(this js.Value, args []js.Value) any {
		if transactionError := transaction.Get("error"); !transactionError.IsNull() && !transactionError.IsUndefined() {
			settle(jsError(transactionError))
		} else {
			settle(errors.New("transaction aborted"))
		}
		return nil
	})
	defer onFailure.Release()
	transaction.Set("oncomplete", onComplete)
	transaction.Set("onerror", onFailure)
	transaction.Set("onabort", onFailure)
	return <-done
}

// jsError turns a DOMException (or anything thrown) into a Go error.
func jsError(value js.Value) error {
	if value.IsNull() || value.IsUndefined() {
		return errors.New("unknown IndexedDB error")
	}
	if name := value.Get("name"); name.Type() == js.TypeString {
		return fmt.Errorf("%s: %s", name.String(), value.Get("message").String())
	}
	return errors.New(value.Call("toString").String())
}
