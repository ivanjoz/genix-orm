package dataframe

import (
	"slices"
	"testing"
)

func TestValuesAt(t *testing.T) {
	A := &Values{Row: 1, Sums: []int64{5}}
	B := &Values{Row: 1, Sums: []int64{3}}
	C := &Values{Row: 2, Sums: []int64{7}}
	entry := func(newVersion int64, oldValues *Values) LogEntry {
		return LogEntry{NewVersion: newVersion, CreatedVersion: 40, SK: "s", OldValues: oldValues}
	}
	// The last run reached W = 100; this one targets X' = 110.
	for _, testCase := range []struct {
		name          string
		life          incarnation
		before, after *Values
	}{
		{"inserted after W", incarnation{createdVersion: 101, current: A}, nil, A},
		{"inserted after W, then updated", incarnation{createdVersion: 101, entries: []LogEntry{entry(103, A)}, current: B}, nil, B},
		{"updated after W, no frame column changed", incarnation{createdVersion: 40, current: A}, A, A},
		{"updated after W, frame columns changed", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, A)}, current: B}, A, B},
		{"Status 1 → 0 after W", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, A)}}, A, nil},
		{"Status 0 → 1 after W", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, nil)}, current: A}, nil, A},
		{"deleted after W", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, A)}}, A, nil},
		{"deleted and inserted again after W: the new incarnation", incarnation{createdVersion: 105, current: B}, nil, B},
		{"changed again after X'", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, A), entry(112, B)}, current: C}, A, B},
		{"entry written, then its base write failed", incarnation{createdVersion: 40, entries: []LogEntry{entry(103, A)}, current: A}, A, A},
	} {
		if before := testCase.life.valuesAt(100); !EqualValues(before, testCase.before) {
			t.Errorf("%s: valuesAt(W) = %v, want %v", testCase.name, before, testCase.before)
		}
		if after := testCase.life.valuesAt(110); !EqualValues(after, testCase.after) {
			t.Errorf("%s: valuesAt(X') = %v, want %v", testCase.name, after, testCase.after)
		}
	}
}

func TestIncarnationsGroupAndCancel(t *testing.T) {
	A := &Values{Row: 1, Sums: []int64{5}}
	B := &Values{Row: 2, Sums: []int64{3}}
	records := []RecordState{{SK: "s", CreatedVersion: 105, Values: B}}
	entries := []LogEntry{
		{NewVersion: 90, CreatedVersion: 40, SK: "s", OldValues: B},     // at or below the base: ignored
		{NewVersion: 103, CreatedVersion: 40, SK: "s", OldValues: A},    // the delete of the first incarnation
		{NewVersion: 108, CreatedVersion: 105, SK: "s", OldValues: A},   // a write that lost its condition...
		{NewVersion: 108, CreatedVersion: 105, SK: "s", IsCancel: true}, // ...and its cancel marker
	}
	lives := incarnations(records, entries, 100)
	slices.SortFunc(lives, func(a, b *incarnation) int { return int(a.createdVersion - b.createdVersion) })
	if len(lives) != 2 {
		t.Fatalf("got %d incarnations, want 2", len(lives))
	}
	if first := lives[0]; first.createdVersion != 40 || first.current != nil || len(first.entries) != 1 || first.entries[0].NewVersion != 103 {
		t.Errorf("deleted incarnation: %+v", first)
	}
	if second := lives[1]; second.createdVersion != 105 || second.current != B || len(second.entries) != 0 {
		t.Errorf("current incarnation: %+v", second)
	}
}
