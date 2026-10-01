package dynamo

import (
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// withFreshWriteCache empties the process cache and pins the clock for one test.
func withFreshWriteCache(t *testing.T, now time.Time) *time.Time {
	t.Helper()
	clock := now
	previousNow := Now
	Now = func() time.Time { return clock }
	writeCache.Lock()
	writeCache.entriesByKey = map[string]writeCacheEntry{}
	writeCache.Unlock()
	t.Cleanup(func() { Now = previousNow })
	return &clock
}

func storedItemOf(t *testing.T, members *Repo[deltaMemberTable, deltaMember], member deltaMember) map[string]types.AttributeValue {
	t.Helper()
	item, err := members.meta.marshalItem(unsafe.Pointer(&member), &member)
	if err != nil {
		t.Fatal(err)
	}
	return item
}

// An entry serves only its own version, and only within the TTL.
func TestWriteCacheServesTheExactVersionWithinTheTTL(t *testing.T) {
	clock := withFreshWriteCache(t, time.Unix(2_000_000_000, 0))
	members := NewRepo[deltaMemberTable, deltaMember]()
	member := deltaMember{ID: 9, TeamIDs: []int16{1, 2}, Status: 1, UpdatedVersion: 4}
	rememberStoredItems([]map[string]types.AttributeValue{storedItemOf(t, members, member)})
	recordKey := members.meta.recordKey(unsafe.Pointer(&member))

	if _, isCached := cachedStoredBlob(recordKey, 4); !isCached {
		t.Fatal("version 4 was read for update but is not cached")
	}
	if _, isCached := cachedStoredBlob(recordKey, 5); isCached {
		t.Fatal("an entry served a version it does not hold")
	}
	*clock = clock.Add(writeCacheTTL + time.Second)
	if _, isCached := cachedStoredBlob(recordKey, 4); isCached {
		t.Fatal("an expired entry was served")
	}
}

// A versioned write diffs against the cached blob with no read: storedVersions
// is never reached (it would need a client). A new record needs no stored version.
func TestStoredVersionsForWriteDecodesCachedBlobs(t *testing.T) {
	withFreshWriteCache(t, time.Unix(2_000_000_000, 0))
	members := NewRepo[deltaMemberTable, deltaMember]()
	stored := deltaMember{ID: 9, TeamIDs: []int16{1, 2}, Status: 1, UpdatedVersion: 4}
	rememberStoredItems([]map[string]types.AttributeValue{storedItemOf(t, members, stored)})

	edited := deltaMember{ID: 9, TeamIDs: []int16{2}, Status: 1}
	created := deltaMember{ID: 10, TeamIDs: []int16{3}, Status: 1}
	ptrs := []unsafe.Pointer{unsafe.Pointer(&edited), unsafe.Pointer(&created)}
	storedByKey, err := members.storedVersionsForWrite(nil, ptrs, []int64{4, 0})
	if err != nil {
		t.Fatal(err)
	}
	storedPtr := storedByKey[members.meta.recordKey(unsafe.Pointer(&edited))]
	if storedPtr == nil || len(storedByKey) != 1 {
		t.Fatalf("stored versions = %v, want only the cached member", storedByKey)
	}
	if decoded := (*deltaMember)(storedPtr); len(decoded.TeamIDs) != 2 || decoded.UpdatedVersion != 4 {
		t.Fatalf("decoded stored member = %+v", *decoded)
	}
}

// A successful write moves a cached entry to the new version; keys never read stay out.
func TestRefreshKeepsOnlyKeysReadForUpdate(t *testing.T) {
	withFreshWriteCache(t, time.Unix(2_000_000_000, 0))
	members := NewRepo[deltaMemberTable, deltaMember]()
	readMember := deltaMember{ID: 9, Status: 1, UpdatedVersion: 4}
	rememberStoredItems([]map[string]types.AttributeValue{storedItemOf(t, members, readMember)})

	readMember.UpdatedVersion = 7
	refreshStoredItem(storedItemOf(t, members, readMember), 7)
	if _, isCached := cachedStoredBlob(members.meta.recordKey(unsafe.Pointer(&readMember)), 7); !isCached {
		t.Fatal("the written version did not replace the cached one")
	}
	otherMember := deltaMember{ID: 11, Status: 1, UpdatedVersion: 7}
	refreshStoredItem(storedItemOf(t, members, otherMember), 7)
	if _, isCached := cachedStoredBlob(members.meta.recordKey(unsafe.Pointer(&otherMember)), 7); isCached {
		t.Fatal("a record never read for update entered the cache")
	}
}

// A full cache drops its expired entries; with none expired it refuses new keys.
func TestFullWriteCacheEvictsExpiredEntriesFirst(t *testing.T) {
	clock := withFreshWriteCache(t, time.Unix(2_000_000_000, 0))
	writeCache.Lock()
	for i := 0; i < writeCacheMaxEntries; i++ {
		storeWriteCacheEntry("old#"+strconv.Itoa(i), 1, []byte{1})
	}
	storeWriteCacheEntry("refused", 1, []byte{1})
	_, refusedIsCached := writeCache.entriesByKey["refused"]
	*clock = clock.Add(writeCacheTTL + time.Second)
	storeWriteCacheEntry("accepted", 1, []byte{1})
	_, acceptedIsCached := writeCache.entriesByKey["accepted"]
	remainingEntries := len(writeCache.entriesByKey)
	writeCache.Unlock()

	if refusedIsCached {
		t.Fatal("a full cache with nothing expired took a new key")
	}
	if !acceptedIsCached || remainingEntries != 1 {
		t.Fatalf("after expiry the cache holds %d entries (accepted: %v), want only the new one", remainingEntries, acceptedIsCached)
	}
}
