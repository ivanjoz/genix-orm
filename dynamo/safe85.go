package dynamo

import (
	"errors"
	"fmt"
	"strings"
)

// Safe85 is the one base85 variant of the ORM and of berryapps (https://github.com/kstenerud/safe-encoding,
// safe85-specification.md): the delta Metadata records carry their log bytes in it, and genix-ui's
// safe85.ts decodes them. Its alphabet leaves out `"` `#` `%` `&` `'` `/` `<` `?` `\`, so the text
// fits in JSON strings, HTML, URLs and file names unescaped, and it is in ASCII order, so encoded
// text sorts like the bytes it holds. Every 4 bytes, read big-endian, become 5 characters; a final
// group of 1 to 3 bytes becomes 2 to 4 characters. `$` and the backtick are in the alphabet: never
// interpolate a value unquoted in a shell.
const safe85Alphabet = "!$()*+,-.0123456789:;=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"

// safe85ValueOf maps a character to its chunk value, -1 for a character outside the alphabet.
var safe85ValueOf = func() (valueOf [256]int8) {
	for char := range valueOf {
		valueOf[char] = -1
	}
	for chunkValue := range len(safe85Alphabet) {
		valueOf[safe85Alphabet[chunkValue]] = int8(chunkValue)
	}
	return valueOf
}()

var ErrSafe85Truncated = errors.New("safe85: the text ends with a lone character: it was cut")

// EncodeSafe85 encodes data as Safe85 text.
func EncodeSafe85(data []byte) string {
	var encoded strings.Builder
	encoded.Grow((len(data)*5 + 3) / 4)
	for groupStart := 0; groupStart < len(data); groupStart += 4 {
		group := data[groupStart:min(groupStart+4, len(data))]
		accumulator := uint64(0)
		for _, groupByte := range group {
			accumulator = accumulator<<8 | uint64(groupByte)
		}
		// A group of n bytes takes n+1 characters, the most significant first.
		var chunks [5]byte
		chunkCount := len(group) + 1
		for chunkIndex := chunkCount - 1; chunkIndex >= 0; chunkIndex-- {
			chunks[chunkIndex] = safe85Alphabet[accumulator%85]
			accumulator /= 85
		}
		encoded.Write(chunks[:chunkCount])
	}
	return encoded.String()
}

// DecodeSafe85 decodes Safe85 text. As the spec requires, it skips tab, line feed, carriage return
// and space; a caller for whom whitespace means a damaged value must reject it before.
func DecodeSafe85(encoded string) ([]byte, error) {
	decoded := make([]byte, 0, len(encoded)*4/5)
	var chunkValues [5]uint64
	chunkCount := 0
	for charIndex := 0; charIndex < len(encoded); charIndex++ {
		char := encoded[charIndex]
		if char == '\t' || char == '\n' || char == '\r' || char == ' ' {
			continue
		}
		chunkValue := safe85ValueOf[char]
		if chunkValue < 0 {
			return nil, fmt.Errorf("safe85: %q at position %d is not in the alphabet", char, charIndex)
		}
		chunkValues[chunkCount] = uint64(chunkValue)
		if chunkCount++; chunkCount == 5 {
			var err error
			if decoded, err = appendSafe85Group(decoded, chunkValues[:5]); err != nil {
				return nil, err
			}
			chunkCount = 0
		}
	}
	switch chunkCount {
	case 0:
		return decoded, nil
	case 1:
		return nil, ErrSafe85Truncated
	default:
		return appendSafe85Group(decoded, chunkValues[:chunkCount])
	}
}

// appendSafe85Group decodes one group of 2 to 5 chunks into 1 to 4 bytes. A group whose value does
// not fit its bytes (above 0xffffffff for a full group) is invalid: no encoder writes it.
func appendSafe85Group(decoded []byte, chunkValues []uint64) ([]byte, error) {
	accumulator := uint64(0)
	for _, chunkValue := range chunkValues {
		accumulator = accumulator*85 + chunkValue
	}
	byteCount := len(chunkValues) - 1
	if accumulator>>(8*byteCount) != 0 {
		return nil, fmt.Errorf("safe85: a group of %d characters holds more than %d bytes", len(chunkValues), byteCount)
	}
	for shift := 8 * (byteCount - 1); shift >= 0; shift -= 8 {
		decoded = append(decoded, byte(accumulator>>shift))
	}
	return decoded, nil
}
