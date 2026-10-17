package utils

import (
	"bytes"
	"math"
	"slices"
	"sort"
	"testing"
)

func TestBase64AlphabetIsInASCIIOrder(t *testing.T) {
	if len(Base64Alphabet) != 64 || !sort.SliceIsSorted([]byte(Base64Alphabet), func(i, j int) bool { return Base64Alphabet[i] < Base64Alphabet[j] }) {
		t.Fatalf("Base64Alphabet must be 64 characters in ASCII order: %q", Base64Alphabet)
	}
}

func TestEncodeUintBase64(t *testing.T) {
	for value, want := range map[uint64]string{0: "-", 63: "z", 64: "0-", 78901377: "3gz10", math.MaxUint64: "Ezzzzzzzzzz"} {
		if got := EncodeUintBase64(value); got != want {
			t.Errorf("EncodeUintBase64(%d) = %q, want %q", value, got, want)
		}
		if decoded, ok := DecodeUintBase64(want); !ok || decoded != value {
			t.Errorf("DecodeUintBase64(%q) = %d, %t, want %d", want, decoded, ok, value)
		}
	}
	for _, token := range []string{"", "+", "zzzzzzzzzzz", "000000000000"} {
		if _, ok := DecodeUintBase64(token); ok {
			t.Errorf("DecodeUintBase64(%q) should fail", token)
		}
	}
}

// Same-length encodings sort like their values: numbers and byte strings.
func TestBase64KeepsOrder(t *testing.T) {
	numbers := []uint64{4096, 4097, 100000, 262143}
	numberTokens := make([]string, len(numbers))
	for i, number := range numbers {
		numberTokens[i] = EncodeUintBase64(number)
	}
	if !slices.IsSorted(numberTokens) {
		t.Errorf("number tokens out of order: %v", numberTokens)
	}
	byteStrings := [][]byte{{0, 0, 1}, {0, 255, 0}, {1, 0, 0}, {254, 3, 9}, {255, 255, 255}}
	for i := 1; i < len(byteStrings); i++ {
		previous, current := Base64.EncodeToString(byteStrings[i-1]), Base64.EncodeToString(byteStrings[i])
		if bytes.Compare(byteStrings[i-1], byteStrings[i]) < 0 != (previous < current) {
			t.Errorf("%q and %q out of order", previous, current)
		}
	}
}
