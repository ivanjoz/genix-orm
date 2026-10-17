package dynamo

import (
	"github.com/ivanjoz/genix-orm/db/cache"
	"github.com/ivanjoz/genix-orm/utils"
)

// SetFileStore installs the ORM's one file store (db/cache): the by-IDs slot files, the delta logs
// and the DataFrames. Call it once at boot, before the first write to a table that needs it
// (CacheByIDs, a delta index, DataFrames).
func SetFileStore(store utils.FileStore) { cache.SetFileStore(store) }
