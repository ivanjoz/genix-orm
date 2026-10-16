package dynamo

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// Order-preserving big-endian numbers
//
// DynamoDB sort keys (sk and the GSI rN slots) are binary, compared byte by byte
// as unsigned values. To make numeric range queries — BETWEEN / > / < — work on a
// binary sort key that concatenates several columns, every number must be encoded
// so that:
//
//	numeric   a < b   ⇔   bytewise  encode(a) < encode(b)
//
// A fixed-width big-endian unsigned integer does exactly that: the most
// significant byte comes first, and every value of a column takes the same number
// of bytes, so the comparison never runs off the end of the shorter one early.
// A Size(bits) column takes ceil(bits/8) bytes.
// ─────────────────────────────────────────────────────────────────────────────

// maxNumberWidth is the bytes of a full uint64.
const maxNumberWidth = 8

// EncodeOrderedUint renders v as exactly `width` big-endian bytes. It panics if v
// does not fit in `width` bytes, because a silent overflow would corrupt key
// ordering.
func EncodeOrderedUint(v uint64, width int) string {
	if width <= 0 || width > maxNumberWidth {
		panic(fmt.Sprintf("db: key number width must be 1..%d bytes, got %d", maxNumberWidth, width))
	}
	if width < maxNumberWidth && v>>(8*width) != 0 {
		panic(fmt.Sprintf("db: value %d overflows %d key bytes", v, width))
	}
	buf := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		buf[i] = byte(v)
		v >>= 8
	}
	return string(buf)
}

// numberWidth is the bytes a Size(bits) column takes in a sort key.
func numberWidth(bits int8) int { return (int(bits) + 7) / 8 }

// ─────────────────────────────────────────────────────────────────────────────
// Decimal numeric keys
//
// pk and the numeric GSI slots are DynamoDB numbers built as a decimal string:
// the 8-digit TableID followed by each integer column zero-padded to the decimal
// width of its Size(bits). That is TableID * 10^w + value, generalized to several
// columns. Because the TableID never has a leading zero and every width is fixed
// per schema, two tables can never produce the same number. DynamoDB numbers keep
// 38 significant digits, which caps the total width (checked at compile).
// ─────────────────────────────────────────────────────────────────────────────

// maxNumericKeyDigits is DynamoDB's number precision.
const maxNumericKeyDigits = 38

// maxValueForBits is the largest value a Size(bits) column accepts.
func maxValueForBits(bits int8) uint64 { return ^uint64(0) >> (64 - int(bits)) }

// decimalWidth is how many decimal digits a Size(bits) column takes in a numeric key.
func decimalWidth(bits int8) int { return len(strconv.FormatUint(maxValueForBits(bits), 10)) }

// decimalDigits renders v zero-padded to the decimal width of Size(bits), panicking on
// overflow for the same reason as EncodeOrderedUint: a silent overflow corrupts the key.
func decimalDigits(v uint64, bits int8) string {
	if v > maxValueForBits(bits) {
		panic(fmt.Sprintf("db: value %d overflows Size(%d) (max %d)", v, bits, maxValueForBits(bits)))
	}
	return fmt.Sprintf("%0*d", decimalWidth(bits), v)
}

// HashTableID is the TableID an entity gets when its schema leaves TableID at 0:
// FNV-1a 32 of the entity name, folded into 10_000_000..99_999_999 so it always
// has exactly 8 digits (a plain modulo could yield 7, and break the fixed-width
// prefix that keeps tables apart).
func HashTableID(entity string) int32 {
	hasher := fnv.New32a()
	hasher.Write([]byte(entity))
	return int32(10_000_000 + hasher.Sum32()%90_000_000)
}

// ─────────────────────────────────────────────────────────────────────────────
// Composite keys
//
// An sk / rN value (a DynamoDB binary) is the concatenation of one or more column
// parts. String parts go in verbatim as UTF-8; number parts are big-endian bytes
// of the declared width. Parts are joined with keySeparator, the lowest byte, so a
// shorter string prefix always sorts before a longer one that extends it
// ("a\x00..." < "ab\x00..."). A number part may hold any byte: its fixed width,
// not the separator, delimits it. String key parts must not contain keySeparator
// nor keySeparatorSuccessor, or a range bound could land inside them.
//
// The Go side keeps these keys as strings (a string holds any bytes); they become
// []byte only in the AttributeValue (binaryKeyAttr / binaryKeyText in client.go).
// ─────────────────────────────────────────────────────────────────────────────

// keySeparator (0x00) sorts below every other byte.
const keySeparator = "\x00"

// keySeparatorSuccessor (0x01) is the byte right after keySeparator, so every
// key starting with prefix+"\x00" sorts below prefix+"\x01": the exclusive upper
// bound of a prefix range.
const keySeparatorSuccessor = "\x01"

// keyPart is one resolved component of a composite key: either a raw string or a
// number to be order-encoded into `width` big-endian bytes.
type keyPart struct {
	str      string
	num      uint64
	isNumber bool
	width    int
}

func stringPart(s string) keyPart { return keyPart{str: s} }

// numberPart encodes v for a column declared .Size(bits): ceil(bits/8) bytes. It
// checks v against the declared bits, not the rounded-up width, so Size(20)
// rejects 2^20 even though its 3 bytes could hold it.
func numberPart(v uint64, bits int8) keyPart {
	if bits < 64 && v>>bits != 0 {
		panic(fmt.Sprintf("db: value %d overflows Size(%d) (max %d)", v, bits, uint64(1)<<bits-1))
	}
	return keyPart{num: v, isNumber: true, width: numberWidth(bits)}
}

// buildCompositeKey renders the parts into a single order-preserving binary key.
func buildCompositeKey(parts []keyPart) string {
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString(keySeparator)
		}
		if p.isNumber {
			b.WriteString(EncodeOrderedUint(p.num, p.width))
		} else {
			if strings.ContainsAny(p.str, keySeparator+keySeparatorSuccessor) {
				panic(fmt.Sprintf("db: string key part %q must not contain the bytes 0x00 or 0x01", p.str))
			}
			b.WriteString(p.str)
		}
	}
	return b.String()
}
