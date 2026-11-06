package db

import "testing"

func TestFilterIndexGroupFetchesSkipsMissingAndFreshHashes(t *testing.T) {
	hashGroups := []IndexGroupHash{
		{HashValue: 11, IndexGroupValues: []int64{1, 11}},
		{HashValue: 12, IndexGroupValues: []int64{2, 12}},
		{HashValue: 13, IndexGroupValues: []int64{3, 13}},
		{HashValue: 14, IndexGroupValues: []int64{4, 14}},
	}
	serverFreshnessByHash := map[int32]int32{
		11: 100,
		12: 200,
		14: 400,
	}
	cachedIndexGroups := map[int32]int32{
		11: 100,
		12: 150,
		13: 999,
	}

	fetchStates, _ := SplitIndexGroupFetches(hashGroups, serverFreshnessByHash, cachedIndexGroups, 10000)
	if len(fetchStates) != 2 {
		t.Fatalf("expected two changed hashes, got %+v", fetchStates)
	}
	if fetchStates[0].HashValue != 12 || fetchStates[0].LastWrite != 200 {
		t.Fatalf("unexpected first fetch state: %+v", fetchStates[0])
	}
	if len(fetchStates[0].IndexGroupValues) != 2 || fetchStates[0].IndexGroupValues[0] != 2 || fetchStates[0].IndexGroupValues[1] != 12 {
		t.Fatalf("unexpected first fetch state values: %+v", fetchStates[0])
	}
	if fetchStates[1].HashValue != 14 || fetchStates[1].LastWrite != 400 {
		t.Fatalf("unexpected second fetch state: %+v", fetchStates[1])
	}
	if len(fetchStates[1].IndexGroupValues) != 2 || fetchStates[1].IndexGroupValues[0] != 4 || fetchStates[1].IndexGroupValues[1] != 14 {
		t.Fatalf("unexpected second fetch state values: %+v", fetchStates[1])
	}
}

func TestSplitIndexGroupFetchesReturnsCachedMetadataStates(t *testing.T) {
	hashGroups := []IndexGroupHash{
		{HashValue: 11, IndexGroupValues: []int64{1, 11}},
		{HashValue: 12, IndexGroupValues: []int64{2, 12}},
	}
	serverFreshnessByHash := map[int32]int32{
		11: 100,
		12: 200,
	}
	cachedIndexGroups := map[int32]int32{
		11: 100,
		12: 150,
	}

	fetchStates, cachedStates := SplitIndexGroupFetches(hashGroups, serverFreshnessByHash, cachedIndexGroups, 10000)
	if len(fetchStates) != 1 || fetchStates[0].HashValue != 12 || fetchStates[0].LastWrite != 200 {
		t.Fatalf("unexpected fetch states: %+v", fetchStates)
	}
	if len(cachedStates) != 1 || cachedStates[0].HashValue != 11 || cachedStates[0].LastWrite != 100 {
		t.Fatalf("unexpected cached states: %+v", cachedStates)
	}
	if len(cachedStates[0].IndexGroupValues) != 2 || cachedStates[0].IndexGroupValues[0] != 1 || cachedStates[0].IndexGroupValues[1] != 11 {
		t.Fatalf("unexpected cached state values: %+v", cachedStates[0])
	}
}

// A group written less than IndexGroupSettleTicks ago is fetched even when the client holds its
// last_write, and goes out labelled 0; at the margin it is settled and served from the cache.
func TestSplitIndexGroupFetchesSendsUnsettledGroupsWhole(t *testing.T) {
	hashGroups := []IndexGroupHash{{HashValue: 11, IndexGroupValues: []int64{1, 11}}}
	serverFreshnessByHash := map[int32]int32{11: 1000}
	cachedIndexGroups := map[int32]int32{11: 1000}

	fetchStates, cachedStates := SplitIndexGroupFetches(hashGroups, serverFreshnessByHash, cachedIndexGroups, 1000+IndexGroupSettleTicks-1)
	if len(cachedStates) != 0 || len(fetchStates) != 1 || fetchStates[0].LastWrite != 0 {
		t.Fatalf("unsettled: fetched %+v, cached %+v", fetchStates, cachedStates)
	}
	// The label 0 a client stored while unsettled never matches a settled group.
	fetchStates, cachedStates = SplitIndexGroupFetches(hashGroups, serverFreshnessByHash, map[int32]int32{11: 0}, 1000+IndexGroupSettleTicks)
	if len(cachedStates) != 0 || len(fetchStates) != 1 || fetchStates[0].LastWrite != 1000 {
		t.Fatalf("settled, client held 0: fetched %+v, cached %+v", fetchStates, cachedStates)
	}
	fetchStates, cachedStates = SplitIndexGroupFetches(hashGroups, serverFreshnessByHash, cachedIndexGroups, 1000+IndexGroupSettleTicks)
	if len(fetchStates) != 0 || len(cachedStates) != 1 || cachedStates[0].LastWrite != 1000 {
		t.Fatalf("settled: fetched %+v, cached %+v", fetchStates, cachedStates)
	}
}
