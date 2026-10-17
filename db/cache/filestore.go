// Package cache holds the two client cache protocols every driver serves, whatever the database:
// the by-IDs year-tick slot files (by_ids.go) and the TypeDelta change logs (delta_log.go), both
// kept in the ORM's utils.FileStore. A driver brings its keys, its record reads and its write hooks;
// the files, their formats and the rules over them live here, so genix-ui reads one protocol.
package cache

import "github.com/ivanjoz/genix-orm/utils"

// fileStore is where the ORM keeps its files: the by-IDs slot files, the delta logs and the
// DataFrames.
var fileStore utils.FileStore

// SetFileStore installs the ORM's one file store. Call it once at boot, before the first write to a
// table that needs it (CacheByIDs, a delta index, DataFrames).
func SetFileStore(store utils.FileStore) { fileStore = store }

// FileStore is the installed file store, nil before SetFileStore.
func FileStore() utils.FileStore { return fileStore }
