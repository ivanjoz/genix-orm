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

func getOrCompileScyllaTable[T TableInterface[T]](schemaStruct *T) ScyllaTable {
	cacheKey := reflect.TypeOf(schemaStruct).Elem().PkgPath() + "." + reflect.TypeOf(schemaStruct).Elem().Name()

	cacheEntryAny, _ := scyllaTableCache.LoadOrStore(cacheKey, &scyllaTableCacheEntry{})
	cacheEntry := cacheEntryAny.(*scyllaTableCacheEntry)
	cacheEntry.once.Do(func() {
		if ShouldLogFull() {
			fmt.Printf("Compiling ScyllaTable metadata once for %s\n", cacheKey)
		}
		cacheEntry.table = makeTable(schemaStruct)
	})
	return cacheEntry.table
}

// resetORMTableCachesForTesting clears ORM metadata caches for deterministic benchmarks/tests.
func resetORMTableCachesForTesting() {
	db.ResetMetadataCacheForTesting()
	scyllaTableCache = sync.Map{}
}
