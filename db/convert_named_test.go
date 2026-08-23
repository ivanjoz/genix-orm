package db

import "testing"

// namedInt8 stands in for a domain enum such as finance/types.CashMovementType: a named
// type whose underlying kind is int8. Every converter must treat it exactly like int8.
type namedInt8 int8

type namedInt32 int32
type namedFloat32 float32
type namedUint8 uint8

func TestNormalizeNamedNumericUnwrapsNamedTypes(t *testing.T) {
	cases := []struct {
		name  string
		input any
		want  any
	}{
		{"named int8", namedInt8(10), int8(10)},
		{"named int32", namedInt32(-7), int32(-7)},
		{"named float32", namedFloat32(1.5), float32(1.5)},
		{"named uint8", namedUint8(200), uint8(200)},
		{"pointer to named", func() any { v := namedInt8(6); return &v }(), int8(6)},
	}
	for _, testCase := range cases {
		got, ok := NormalizeNamedNumeric(testCase.input)
		if !ok {
			t.Fatalf("%s: expected normalization to apply", testCase.name)
		}
		if got != testCase.want {
			t.Fatalf("%s: got %#v, want %#v", testCase.name, got, testCase.want)
		}
	}
}

// The recursion at every call site relies on this: once normalized, a value must NOT
// normalize again, or the fallbacks loop forever.
func TestNormalizeNamedNumericRejectsPredeclaredAndNonNumeric(t *testing.T) {
	for _, input := range []any{int8(3), int64(3), float64(3), uint8(3), "text", nil, []int8{1}, (*int8)(nil)} {
		if _, ok := NormalizeNamedNumeric(input); ok {
			t.Fatalf("expected %#v (%T) to be left alone", input, input)
		}
	}
}

func TestConvertersMatchUnderlyingType(t *testing.T) {
	if got := ToInt64(namedInt8(10)); got != 10 {
		t.Fatalf("ToInt64(namedInt8(10)) = %d, want 10", got)
	}
	if got := ToInt64(namedInt32(-42)); got != -42 {
		t.Fatalf("ToInt64(namedInt32(-42)) = %d, want -42", got)
	}
	if got := ToInt64(namedUint8(200)); got != 200 {
		t.Fatalf("ToInt64(namedUint8(200)) = %d, want 200", got)
	}
	if got := ToFloat64(namedFloat32(1.5)); got != 1.5 {
		t.Fatalf("ToFloat64(namedFloat32(1.5)) = %v, want 1.5", got)
	}
	if got := ToFloat32(namedFloat32(1.5)); got != 1.5 {
		t.Fatalf("ToFloat32(namedFloat32(1.5)) = %v, want 1.5", got)
	}
	// The regression this whole change exists to prevent: before the fallback these
	// returned 0, so a named enum column persisted and compared as zero.
	if ToInt64(namedInt8(10)) != ToInt64(int8(10)) {
		t.Fatal("named int8 must widen identically to plain int8")
	}
}
