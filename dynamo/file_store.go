package dynamo

import "github.com/ivanjoz/genix-orm/utils"

// fileStore is where the ORM keeps its files (utils.FileStore): the by-IDs slot files, the delta logs
// and the DataFrames.
var fileStore utils.FileStore

// SetFileStore installs the ORM's one file store. Call it once at boot, before the first write to a
// table that needs it (CacheByIDs, a delta index, DataFrames).
func SetFileStore(store utils.FileStore) { fileStore = store }
