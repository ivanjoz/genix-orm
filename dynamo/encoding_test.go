package dynamo

import (
	"math/rand"
	"sort"
	"testing"
)

// TestOrderedAlphabetIsMonotonic is the property the whole scheme rests on: the
// alphabet must be 64 unique characters in strictly ascending ASCII order.
func TestOrderedAlphabetIsMonotonic(t *testing.T) {
	if len(orderedAlphabet) != 64 {
		t.Fatalf("alphabet must be 64 chars, got %d", len(orderedAlphabet))
	}
	for i := 1; i < len(orderedAlphabet); i++ {
		if orderedAlphabet[i] <= orderedAlphabet[i-1] {
			t.Fatalf("alphabet not strictly ascending at %d: %q >= %q",
				i, orderedAlphabet[i-1], orderedAlphabet[i])
		}
	}
}

// TestEncodeRoundTrip verifies decode(encode(v)) == v across widths.
func TestEncodeRoundTrip(t *testing.T) {
	values := []uint64{0, 1, 63, 64, 65, 4095, 4096, 1_000_000, 1 << 40, ^uint64(0)}
	for _, v := range values {
		width := maxBase64Width
		enc := EncodeOrderedUint(v, width)
		if len(enc) != width {
			t.Fatalf("width mismatch: got %d want %d", len(enc), width)
		}
		got, err := DecodeOrderedUint(enc)
		if err != nil {
			t.Fatalf("decode(%q): %v", enc, err)
		}
		if got != v {
			t.Fatalf("round trip: got %d want %d", got, v)
		}
	}
}

// TestOrderPreservedRandom is the core guarantee: for fixed width, lexicographic
// order of encodings matches numeric order of the values.
func TestOrderPreservedRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const width = 8 // 48 bits of range
	max := capacityForWidth(width)

	nums := make([]uint64, 2000)
	for i := range nums {
		nums[i] = uint64(rng.Int63n(int64(max)))
	}

	encoded := make([]string, len(nums))
	for i, v := range nums {
		encoded[i] = EncodeOrderedUint(v, width)
	}

	byNum := append([]uint64(nil), nums...)
	sort.Slice(byNum, func(i, j int) bool { return byNum[i] < byNum[j] })

	byStr := append([]string(nil), encoded...)
	sort.Strings(byStr)

	for i := range byNum {
		want := EncodeOrderedUint(byNum[i], width)
		if byStr[i] != want {
			t.Fatalf("order mismatch at %d: sorted-by-string=%q sorted-by-number=%q",
				i, byStr[i], want)
		}
	}
}

// TestOverflowPanics ensures a value too big for the width fails loudly.
func TestOverflowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on overflow")
		}
	}()
	EncodeOrderedUint(64*64, 1) // width 1 holds 0..63
}

// TestCompositeKeyRangeOrdering checks the real use case: an equality prefix
// plus a numeric suffix keeps numeric order within the same prefix.
func TestCompositeKeyRangeOrdering(t *testing.T) {
	mk := func(brand string, created uint64) string {
		return buildCompositeKey([]keyPart{stringPart(brand), numberPart(created, 8)})
	}
	a := mk("acme", 1700000000)
	b := mk("acme", 1700000500)
	c := mk("acme", 1800000000)
	if !(a < b && b < c) {
		t.Fatalf("expected a<b<c, got %q %q %q", a, b, c)
	}
	// Different prefixes must not interleave with the numeric suffix.
	if !("acme#"[0] < "acmf#"[0] || mk("acme", 9) < mk("acmf", 0)) {
		t.Fatalf("prefix ordering broken")
	}
}
