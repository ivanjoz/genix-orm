package dynamo

import (
	"fmt"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// Order-preserving Base64
//
// DynamoDB sort keys (and the string GSI partition attributes) are plain
// strings, compared lexicographically (byte by byte). To make numeric range
// queries — BETWEEN / > / < — work on a string sort key that concatenates
// several columns, every number must be encoded so that:
//
//	numeric   a < b   ⇔   lexicographic  encode(a) < encode(b)
//
// Two things guarantee that:
//
//  1. A *fixed width*: every value of a given column encodes to the same number
//     of characters (left-padded), so the comparison never runs off the end of
//     the shorter string early.
//  2. An alphabet whose 64 characters are already in ascending ASCII order, so a
//     bigger base-64 digit is also a bigger byte.
//
// This mirrors genix's packed-integer indexes (`DecimalSize` per column packed
// into one sortable int64), but instead of packing into a 19-digit int64 we pack
// into an arbitrary-length Base64 string — no 2^63 ceiling, and it lives happily
// inside a DynamoDB string key.
// ─────────────────────────────────────────────────────────────────────────────

// orderedAlphabet holds 64 characters in strictly ascending ASCII order:
//
//	'-'(45) < '0'..'9'(48..57) < 'A'..'Z'(65..90) < '_'(95) < 'a'..'z'(97..122)
//
// Because the alphabet is monotonic, digit value and byte value rank the same,
// which is what makes the encoding order-preserving. (These are exactly the
// URL-safe Base64 characters, re-ordered.)
const orderedAlphabet = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

// orderedReverse maps a byte back to its 0..63 digit value; 0xFF means invalid.
var orderedReverse = func() [256]byte {
	var table [256]byte
	for i := range table {
		table[i] = 0xFF
	}
	for i := 0; i < len(orderedAlphabet); i++ {
		table[orderedAlphabet[i]] = byte(i)
	}
	return table
}()

// maxBase64Width is the number of digits needed to hold a full uint64
// (ceil(64/6) = 11). A width of 11 covers every uint64; smaller widths cap the
// representable range at 64^width - 1.
const maxBase64Width = 11

// EncodeOrderedUint renders v as exactly `width` order-preserving Base64
// characters, most-significant digit first, left-padded with the lowest
// character ('-'). It panics if v does not fit in `width` digits, because a
// silent overflow would corrupt key ordering.
func EncodeOrderedUint(v uint64, width int) string {
	if width <= 0 || width > maxBase64Width {
		panic(fmt.Sprintf("db: base64 width must be 1..%d, got %d", maxBase64Width, width))
	}
	buf := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		buf[i] = orderedAlphabet[v&0x3F]
		v >>= 6
	}
	if v != 0 {
		panic(fmt.Sprintf("db: value overflows base64 width %d (max %d)", width, capacityForWidth(width)-1))
	}
	return string(buf)
}

// EncodeOrderedInt is EncodeOrderedUint for non-negative signed integers. Like
// genix's packed indexes it rejects negatives: an index/key component that can
// go negative would need a sign-bias that a small fixed width cannot hold, so we
// fail loudly instead of silently misordering.
func EncodeOrderedInt(v int64, width int) string {
	if v < 0 {
		panic(fmt.Sprintf("db: cannot order-encode negative value %d (index/key numbers must be >= 0)", v))
	}
	return EncodeOrderedUint(uint64(v), width)
}

// DecodeOrderedUint is the inverse of EncodeOrderedUint.
func DecodeOrderedUint(s string) (uint64, error) {
	if len(s) == 0 || len(s) > maxBase64Width {
		return 0, fmt.Errorf("db: invalid ordered-base64 length %d", len(s))
	}
	var v uint64
	for i := 0; i < len(s); i++ {
		d := orderedReverse[s[i]]
		if d == 0xFF {
			return 0, fmt.Errorf("db: invalid ordered-base64 character %q", s[i])
		}
		v = v<<6 | uint64(d)
	}
	return v, nil
}

// capacityForWidth returns 64^width (the count of representable values) as a
// uint64, saturating at the uint64 max for width >= 11.
func capacityForWidth(width int) uint64 {
	if width >= maxBase64Width {
		return ^uint64(0)
	}
	cap := uint64(1)
	for i := 0; i < width; i++ {
		cap *= 64
	}
	return cap
}

// ─────────────────────────────────────────────────────────────────────────────
// Composite keys
//
// A pk / sk / GSI-slot value is the concatenation of one or more column parts.
// String parts go in verbatim; number parts are order-preserving Base64 of the
// declared width. Parts are joined with keySeparator, which is lower in ASCII
// than every alphabet character, so a shorter prefix always sorts before a
// longer one that extends it ("a#..." < "ab#..."). String key parts therefore
// must not contain keySeparator.
// ─────────────────────────────────────────────────────────────────────────────

// keySeparator ('#', 0x23) sorts below every orderedAlphabet character.
const keySeparator = "#"

// keyPart is one resolved component of a composite key: either a raw string or a
// number to be order-encoded into `width` Base64 digits.
type keyPart struct {
	str      string
	num      uint64
	isNumber bool
	width    int
}

func stringPart(s string) keyPart        { return keyPart{str: s} }
func numberPart(v uint64, w int) keyPart { return keyPart{num: v, isNumber: true, width: w} }

// buildCompositeKey renders the parts into a single order-preserving string.
func buildCompositeKey(parts []keyPart) string {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString(keySeparator)
		}
		if p.isNumber {
			b.WriteString(EncodeOrderedUint(p.num, p.width))
		} else {
			if strings.Contains(p.str, keySeparator) {
				panic(fmt.Sprintf("db: string key part %q must not contain the %q separator", p.str, keySeparator))
			}
			b.WriteString(p.str)
		}
	}
	return b.String()
}
