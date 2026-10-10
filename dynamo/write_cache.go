package dynamo

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Write cache: the stored blobs of records read for update, kept per process so
// a version-checked write can diff its hidden rows without reading them again.
//
// PutManyIfVersion diffs a record's hidden rows (fan-out, delta) against the
// record as stored. GetManyForUpdate and Modify keep the blob they read, keyed by
// the record's pk#sk and tagged with its "upd". A write uses an entry only when
// that version equals the one it is conditioned on, and the condition then
// proves the entry: if the write succeeds, the stored item was exactly that
// version, so the cached blob was the stored state. A stale entry is just a miss.
//
// Only version-checked writes may use it: a plain Put has no condition, so a
// stale entry would diff wrong rows. The blob is cached, not the decoded record,
// because callers edit the records they read in place.
//
// Entries live writeCacheTTL. A full cache drops its expired entries, and when
// none has expired yet it stops taking new ones: a miss only costs the read.
// ─────────────────────────────────────────────────────────────────────────────

const (
	writeCacheTTL        = 15 * time.Second
	writeCacheMaxEntries = 10_000
)

type writeCacheEntry struct {
	version  int64
	blob     []byte
	cachedAt time.Time
}

var writeCache = struct {
	sync.Mutex
	entriesByKey map[string]writeCacheEntry
}{entriesByKey: map[string]writeCacheEntry{}}

// rememberStoredItems keeps the blob of every versioned item read for update.
func rememberStoredItems(items []map[string]types.AttributeValue) {
	writeCache.Lock()
	defer writeCache.Unlock()
	for _, item := range items {
		versionAttr, hasVersion := item[updatedColumn].(*types.AttributeValueMemberN)
		blobAttr, hasBlob := item[dataColumn].(*types.AttributeValueMemberB)
		if !hasVersion || !hasBlob {
			continue
		}
		version, err := strconv.ParseInt(versionAttr.Value, 10, 64)
		if err != nil {
			continue
		}
		storeWriteCacheEntry(writeCacheKeyOf(item), version, blobAttr.Value)
	}
}

// refreshStoredItem replaces the entry of a record just written, so a second
// round in the same process starts from a hit. Keys never read for update stay out.
func refreshStoredItem(item map[string]types.AttributeValue, version int64) {
	writeCache.Lock()
	defer writeCache.Unlock()
	cacheKey := writeCacheKeyOf(item)
	if _, isCached := writeCache.entriesByKey[cacheKey]; isCached {
		storeWriteCacheEntry(cacheKey, version, item[dataColumn].(*types.AttributeValueMemberB).Value)
	}
}

// cachedStoredBlob returns the stored blob of a record at exactly expectedVersion, and when it was read.
func cachedStoredBlob(cacheKey string, expectedVersion int64) ([]byte, time.Time, bool) {
	writeCache.Lock()
	defer writeCache.Unlock()
	entry, isCached := writeCache.entriesByKey[cacheKey]
	if !isCached || entry.version != expectedVersion || Now().Sub(entry.cachedAt) > writeCacheTTL {
		return nil, time.Time{}, false
	}
	return entry.blob, entry.cachedAt, true
}

// storeWriteCacheEntry must run under the lock. The blob is copied: an SDK
// response buffer must not be kept alive, nor edited under the cache.
func storeWriteCacheEntry(cacheKey string, version int64, blob []byte) {
	if _, isCached := writeCache.entriesByKey[cacheKey]; !isCached && len(writeCache.entriesByKey) >= writeCacheMaxEntries {
		now := Now()
		for expiredKey, entry := range writeCache.entriesByKey {
			if now.Sub(entry.cachedAt) > writeCacheTTL {
				delete(writeCache.entriesByKey, expiredKey)
			}
		}
		if len(writeCache.entriesByKey) >= writeCacheMaxEntries {
			return
		}
	}
	writeCache.entriesByKey[cacheKey] = writeCacheEntry{version: version, blob: bytes.Clone(blob), cachedAt: Now()}
}

// writeCacheKeyOf is the item's pk#sk, the same string recordKey builds from a record.
func writeCacheKeyOf(item map[string]types.AttributeValue) string {
	return item["pk"].(*types.AttributeValueMemberN).Value + keySeparator + item["sk"].(*types.AttributeValueMemberS).Value
}
