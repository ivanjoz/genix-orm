package scylla

import (
	"bytes"
	"testing"
)

// namedInt8 stands in for a domain enum such as finance/types.CashMovementType.
type namedInt8 int8

// The bug this guards: valueToCSVBase64's default branch used %v, so a named int8
// encoded as its decimal TEXT while every reader expects the base64 integer encoding —
// a corrupt value, not a missing one.
func TestValueToCSVBase64MatchesUnderlyingType(t *testing.T) {
	for _, value := range []int8{0, 1, 6, 10, -10, 127, -128} {
		named := valueToCSVBase64(namedInt8(value))
		plain := valueToCSVBase64(value)
		if !bytes.Equal(named, plain) {
			t.Fatalf("value %d: named encoded as %q, plain as %q", value, named, plain)
		}
	}
}

// makeNumericSlice's constraint is written with ~ so []namedInt8 satisfies it, but the
// inner type switch missed the named type and appended nothing — silently dropping
// every element.
func TestMakeNumericSliceMatchesUnderlyingType(t *testing.T) {
	named := makeNumericSlice([]namedInt8{6, 8, 9, 10})
	plain := makeNumericSlice([]int8{6, 8, 9, 10})
	if !bytes.Equal(named, plain) {
		t.Fatalf("named slice encoded as %q, plain as %q", named, plain)
	}
	if len(named) == 0 {
		t.Fatal("named slice encoded to nothing")
	}
}

func TestConvertToIntMatchesUnderlyingType(t *testing.T) {
	if got := convertToInt64(namedInt8(10)); got != 10 {
		t.Fatalf("convertToInt64 = %d, want 10", got)
	}
	if got := convertToInt32(namedInt8(-10)); got != -10 {
		t.Fatalf("convertToInt32 = %d, want -10", got)
	}
}

// A named type must hash to the same bytes as the type it wraps, otherwise retyping a
// column from int8 to a named int8 silently invalidates every persisted hash.
func TestHashIntStableAcrossNamedType(t *testing.T) {
	if HashInt(namedInt8(10)) != HashInt(int8(10)) {
		t.Fatal("named int8 must hash identically to plain int8")
	}
	if HashInt("company", namedInt8(6), int32(42)) != HashInt("company", int8(6), int32(42)) {
		t.Fatal("mixed-value hash must be stable across named types")
	}
}

func TestIsNonPositiveNumericValueHandlesNamedType(t *testing.T) {
	if !isNonPositiveNumericValue(namedInt8(0)) {
		t.Fatal("named zero must count as non-positive (a temporary key)")
	}
	if isNonPositiveNumericValue(namedInt8(5)) {
		t.Fatal("named positive must not count as non-positive")
	}
}
