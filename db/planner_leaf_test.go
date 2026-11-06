package db

import (
	"slices"
	"testing"
)

// HashInt is persisted in hash-index columns: these values are frozen, a change means every stored
// hash in every deployed table is wrong. int, int32 and a named int8 must all hash like their
// plain counterparts.
func TestHashIntLayoutIsFrozen(t *testing.T) {
	type namedInt8 int8
	// Golden values taken from the implementation as it was before it moved out of the Scylla driver.
	if got := HashInt(int32(7), "abc"); got != -34985898 {
		t.Fatalf("HashInt(int32(7), \"abc\") changed: %d", got)
	}
	if got := HashInt(int8(3)); got != 930681398 {
		t.Fatalf("HashInt(int8(3)) changed: %d", got)
	}
	if got := HashInt64(1, 2, 3); got != 1488209227 {
		t.Fatalf("HashInt64(1, 2, 3) changed: %d", got)
	}
	if HashInt(int8(3)) != HashInt(namedInt8(3)) {
		t.Error("a named numeric type must hash like its plain type")
	}
	if HashInt(int(5)) != HashInt(int64(5)) {
		t.Error("int is written as int64")
	}
	if HashInt(int32(5)) == HashInt(int64(5)) {
		t.Error("int32 and int64 write different widths, they must not collide")
	}
	if got := HashInt64(1, 2, 3); got != HashInt64(1, 2, 3) || got == HashInt64(3, 2, 1) {
		t.Errorf("HashInt64 must be order-sensitive and deterministic, got %d", got)
	}
}

func TestNormalizeCompositeRange(t *testing.T) {
	if from, to := NormalizeCompositeRange(9, 3, false); from != 3 || to != 9 {
		t.Errorf("an inverted range is reordered, got %d..%d", from, to)
	}
	// Week codes are YYWW; consecutive weeks must be consecutive indexes, across a year boundary too.
	weekA, _ := weekIndexOfWeekCode(2552)
	weekB, _ := weekIndexOfWeekCode(2601)
	if weekB-weekA != 1 {
		t.Errorf("week 2552 -> 2601 must be adjacent, got %d -> %d", weekA, weekB)
	}
	if from, to := NormalizeCompositeRange(2601, 2552, true); to-from != 1 {
		t.Errorf("week range must be ordered and contiguous, got %d..%d", from, to)
	}
	if _, ok := weekIndexOfWeekCode(2560); ok {
		t.Error("week 60 does not exist")
	}
}

func TestCompositeBucketID(t *testing.T) {
	if got := CompositeBucketID(25, 10, false); got != 2 {
		t.Errorf("25 in buckets of 10 is bucket 2, got %d", got)
	}
}

func TestPlanCompositeBucketsCoversRangeWithFewestStatements(t *testing.T) {
	selections := PlanCompositeBuckets(10, 29, []int8{10})
	if !slices.Equal(selections, []CompositeBucketSelection{{BucketSize: 10, BucketID: 1}, {BucketSize: 10, BucketID: 2}}) {
		t.Errorf("exact two-bucket cover expected, got %+v", selections)
	}
	if PlanCompositeBuckets(5, 1, []int8{10}) != nil || PlanCompositeBuckets(1, 5, nil) != nil {
		t.Error("an empty or inverted request has no plan")
	}
	// A mixed set prefers one wide bucket over many narrow ones when overfetch allows.
	if got := PlanCompositeBuckets(0, 29, []int8{30, 10}); len(got) != 1 || got[0].BucketSize != 30 {
		t.Errorf("one 30-wide bucket expected, got %+v", got)
	}
}

func TestStatementValuesForHash(t *testing.T) {
	if values, ok := StatementValuesForHash(ColumnStatement{Operator: "IN", Values: []any{int32(1), int8(2)}}); !ok || !slices.Equal(values, []int64{1, 2}) {
		t.Errorf("IN values, got %v %v", values, ok)
	}
	if _, ok := StatementValuesForHash(ColumnStatement{Operator: ">", Value: 1}); ok {
		t.Error("a range operator cannot feed a hash")
	}
	if _, ok := StatementValuesForHash(ColumnStatement{Operator: "IN"}); ok {
		t.Error("an empty IN cannot feed a hash")
	}
}

func TestFlattenInt64Values(t *testing.T) {
	five := int32(5)
	for name, testCase := range map[string]struct {
		input any
		want  []int64
	}{
		"nil":     {nil, nil},
		"scalar":  {int16(3), []int64{3}},
		"pointer": {&five, []int64{5}},
		"slice":   {[]int32{1, 2}, []int64{1, 2}},
	} {
		if got := FlattenInt64Values(testCase.input); !slices.Equal(got, testCase.want) {
			t.Errorf("%s: got %v want %v", name, got, testCase.want)
		}
	}
}

func TestEnumerateFixedValues(t *testing.T) {
	if got := EnumerateFixedValues(FixedValues{Values: []int64{4, 9}}, 0, 100); !slices.Equal(got, []int64{4, 9}) {
		t.Errorf("explicit values win over the span, got %v", got)
	}
	if got := EnumerateFixedValues(FixedValues{}, 1, 3); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Errorf("span enumerates, got %v", got)
	}
	if got := EnumerateFixedValues(FixedValues{}, 0, MaxEnumerableFixedValues); got != nil {
		t.Errorf("a span wider than %d is range-bounded only, got %v", MaxEnumerableFixedValues, got)
	}
}

func TestChunkValuesClonesAndSplits(t *testing.T) {
	source := []any{1, 2, 3, 4, 5}
	chunks := ChunkValues(source, 2)
	if len(chunks) != 3 || len(chunks[2]) != 1 {
		t.Fatalf("expected chunks of 2,2,1, got %v", chunks)
	}
	chunks[0][0] = 99
	if source[0] != 1 {
		t.Error("chunks must be copies")
	}
	if ChunkValues(nil, 2) != nil || len(ChunkValues(source, 0)) != 5 {
		t.Error("empty input has no chunks and a non-positive size falls back to 1")
	}
}

func TestSumSlotBitsAndBudget(t *testing.T) {
	if SumSlotBits([]int64{8, 16, 4}, 1) != 20 {
		t.Error("sums from the given slot to the end")
	}
	if PackedVirtualBudgetBits(true) != 32 || PackedVirtualBudgetBits(false) != 64 {
		t.Error("budget is the full width of the stored integer")
	}
}
