package dynamo

import (
	"math/rand"
	"sort"
	"testing"
)

// TestEncodeIsBigEndian pins the layout: most significant byte first, left-padded with zeros.
func TestEncodeIsBigEndian(t *testing.T) {
	if got := EncodeOrderedUint(0x0102, 3); got != "\x00\x01\x02" {
		t.Fatalf("EncodeOrderedUint(0x0102, 3) = %q", got)
	}
	if got := EncodeOrderedUint(^uint64(0), maxNumberWidth); got != "\xff\xff\xff\xff\xff\xff\xff\xff" {
		t.Fatalf("EncodeOrderedUint(max uint64, 8) = %q", got)
	}
}

// TestOrderPreservedRandom is the core guarantee: for fixed width, bytewise order
// of encodings matches numeric order of the values.
func TestOrderPreservedRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	const width = 6 // 48 bits of range

	nums := make([]uint64, 2000)
	for i := range nums {
		nums[i] = uint64(rng.Int63n(1 << 48))
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
			t.Fatalf("order mismatch at %d: sorted-by-bytes=%q sorted-by-number=%q",
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
	EncodeOrderedUint(256, 1) // width 1 holds 0..255
}

// TestSizeCapIsTheDeclaredBits: Size(20) stores 3 bytes (room for 24 bits), but
// the declared 20 bits are the limit, and Size(64) takes every uint64.
func TestSizeCapIsTheDeclaredBits(t *testing.T) {
	if got := numberPart(1<<20-1, 20).width; got != 3 {
		t.Fatalf("Size(20) must use 3 bytes, got %d", got)
	}
	if got := numberPart(^uint64(0), 64).width; got != maxNumberWidth {
		t.Fatalf("Size(64) must use %d bytes, got %d", maxNumberWidth, got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic: 2^20 overflows Size(20)")
		}
	}()
	numberPart(1<<20, 20)
}

// TestStringPartRejectsSeparatorBytes: 0x00 and 0x01 bound the key ranges, so a string part
// holding either would let a range bound land inside it.
func TestStringPartRejectsSeparatorBytes(t *testing.T) {
	for _, badString := range []string{"a\x00b", "a\x01"} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for string key part %q", badString)
				}
			}()
			buildCompositeKey([]keyPart{stringPart(badString)})
		}()
	}
}

// TestCompositeKeyRangeOrdering checks the real use case: an equality prefix
// plus a numeric suffix keeps numeric order within the same prefix.
func TestCompositeKeyRangeOrdering(t *testing.T) {
	mk := func(brand string, created uint64) string {
		return buildCompositeKey([]keyPart{stringPart(brand), numberPart(created, 48)})
	}
	a := mk("acme", 1700000000)
	b := mk("acme", 1700000500)
	c := mk("acme", 1800000000)
	if !(a < b && b < c) {
		t.Fatalf("expected a<b<c, got %q %q %q", a, b, c)
	}
	// Different prefixes must not interleave with the numeric suffix.
	if !(mk("acme", 9) < mk("acmf", 0)) {
		t.Fatalf("prefix ordering broken")
	}
}
