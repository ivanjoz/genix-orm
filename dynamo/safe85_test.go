package dynamo

import (
	"bytes"
	"errors"
	"testing"
)

// The vectors are the examples of safe85-specification.md: they pin this implementation to the spec,
// and so to any other Safe85 implementation (the browser's).
func TestSafe85MatchesTheSpecExamples(t *testing.T) {
	for _, specExample := range []struct {
		data    []byte
		encoded string
	}{
		{[]byte{0x39, 0x12, 0x82, 0xe1, 0x81, 0x39, 0xd9, 0x8b, 0x39, 0x4c, 0x63, 0x9d, 0x04, 0x8c}, "9F3{+RVCLI9LDzZ!4e"},
		{[]byte{0xe6, 0x12, 0xa6, 0x9f, 0xf8, 0x38, 0x6d, 0x7b, 0x01, 0x99, 0x3e, 0x6c, 0x53, 0x7b, 0x60}, "szEXiyl02C!Tc2o.w;X"},
		{[]byte{0x21, 0xd1, 0x7d, 0x3f, 0x21, 0xc1, 0x88, 0x99, 0x71, 0x45, 0x96, 0xad, 0xcc, 0x96, 0x79, 0xd8}, "1stg+1r5~+MKP7zkj0X2"},
	} {
		if encoded := EncodeSafe85(specExample.data); encoded != specExample.encoded {
			t.Errorf("EncodeSafe85(% x) = %q, want %q", specExample.data, encoded, specExample.encoded)
		}
		decoded, err := DecodeSafe85(specExample.encoded)
		if err != nil || !bytes.Equal(decoded, specExample.data) {
			t.Errorf("DecodeSafe85(%q) = % x, %v; want % x", specExample.encoded, decoded, err, specExample.data)
		}
	}
}

// Every length exercises a different final group, and the extreme byte values the group bounds.
func TestSafe85RoundTripsEveryFinalGroup(t *testing.T) {
	for _, fillByte := range []byte{0x00, 0x7f, 0xff} {
		for dataLength := 0; dataLength <= 9; dataLength++ {
			data := bytes.Repeat([]byte{fillByte}, dataLength)
			encoded := EncodeSafe85(data)
			if wantLength := dataLength/4*5 + map[int]int{0: 0, 1: 2, 2: 3, 3: 4}[dataLength%4]; len(encoded) != wantLength {
				t.Errorf("% x encodes to %d characters, want %d", data, len(encoded), wantLength)
			}
			decoded, err := DecodeSafe85(encoded)
			if err != nil || !bytes.Equal(decoded, data) {
				t.Errorf("% x round trips to % x, %v", data, decoded, err)
			}
		}
	}
}

func TestDecodeSafe85SkipsWhitespaceAndRejectsDamage(t *testing.T) {
	if decoded, err := DecodeSafe85(" 9F3{+\tRVCLI\r\n9LDzZ!4e "); err != nil || len(decoded) != 14 {
		t.Errorf("whitespace was not skipped: % x, %v", decoded, err)
	}
	if _, err := DecodeSafe85("9F3{+R"); !errors.Is(err, ErrSafe85Truncated) {
		t.Errorf("a lone final character was accepted: %v", err)
	}
	for _, damagedText := range []string{
		"9F3\"{",             // `"` is one of the characters Safe85 leaves out
		"~~~~~",              // above 0xffffffff
		"~~",                 // above one byte
		"9F3{+RVCLI9LDzZ!4/", // `/` is left out too
	} {
		if decoded, err := DecodeSafe85(damagedText); err == nil {
			t.Errorf("DecodeSafe85(%q) accepted: % x", damagedText, decoded)
		}
	}
}
