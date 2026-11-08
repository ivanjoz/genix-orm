package scylla

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/ivanjoz/genix-orm/db"
)

type scyllaTableCacheEntry struct {
	once  sync.Once
	table ScyllaTable
}

var scyllaTableCache sync.Map

// getOrCompileScyllaTable takes the *T table struct as an interface rather than a type parameter:
// the body only reflects on it and calls GetSchema, so one compiled copy serves every table. It is
// `any` because Go gives a *T of a type parameter no methods, so callers could not pass a TableHandle.
func getOrCompileScyllaTable(schemaStruct any) ScyllaTable {
	schemaType := reflect.TypeOf(schemaStruct).Elem()
	cacheKey := schemaType.PkgPath() + "." + schemaType.Name()

	cacheEntryAny, _ := scyllaTableCache.LoadOrStore(cacheKey, &scyllaTableCacheEntry{})
	cacheEntry := cacheEntryAny.(*scyllaTableCacheEntry)
	cacheEntry.once.Do(func() {
		if ShouldLogFull() {
			fmt.Printf("Compiling ScyllaTable metadata once for %s\n", cacheKey)
		}
		cacheEntry.table = makeTable(schemaStruct.(TableHandle).GetSchema(), reflect.ValueOf(schemaStruct).Elem())
	})
	return cacheEntry.table
}

// resetORMTableCachesForTesting clears ORM metadata caches for deterministic benchmarks/tests.
func resetORMTableCachesForTesting() {
	db.ResetMetadataCacheForTesting()
	scyllaTableCache = sync.Map{}
}
