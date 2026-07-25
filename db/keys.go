package db

import (
	"fmt"
	"strings"
)

// Key encoding is part of the ORM's storage contract, not a driver detail: a
// concatenated key written by one driver must read back identically under another,
// so these live in the shared layer.

// base62Alphabet is the character set for compact, order-agnostic key tokens.
const base62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// MakeKeyConcat joins values into the deterministic string a KeyConcatenated column
// stores. Integers are base62-encoded to keep the key short, and non-positive
// integers collapse to empty so absent components do not pad the key.
func MakeKeyConcat(values ...any) string {
	valuesStrings := []string{}
	for _, value := range values {
		token := ""
		switch typedValue := value.(type) {
		case string:
			token = typedValue
		case int32:
			if typedValue > 0 {
				token = EncodeToBase62(int64(typedValue))
			}
		case int64:
			if typedValue > 0 {
				token = EncodeToBase62(typedValue)
			}
		case int:
			if typedValue > 0 {
				token = EncodeToBase62(int64(typedValue))
			}
		case int16:
			if typedValue > 0 {
				token = EncodeToBase62(int64(typedValue))
			}
		default:
			token = fmt.Sprintf("%v", typedValue)
		}
		valuesStrings = append(valuesStrings, token)
	}
	return strings.TrimRight(strings.Join(valuesStrings, "_"), "_")
}

// EncodeToBase62 encodes an int64 as a compact base62 token.
func EncodeToBase62(number int64) string {
	return encodeToBase62(uint64(number))
}

// DecodeFromBase62 reverses EncodeToBase62.
func DecodeFromBase62(token string) int64 {
	return int64(decodeFromBase62(token))
}

func encodeToBase62(number uint64) string {
	if number == 0 {
		return string(base62Alphabet[0])
	}

	chars := make([]byte, 0)
	length := uint64(len(base62Alphabet))

	for number > 0 {
		result := number / length
		remainder := number % length
		chars = append(chars, base62Alphabet[remainder])
		number = result
	}

	for i, j := 0, len(chars)-1; i < j; i, j = i+1, j-1 {
		chars[i], chars[j] = chars[j], chars[i]
	}

	return string(chars)
}

func decodeFromBase62(token string) uint64 {
	// Decode incrementally to avoid float math and repeated exponentiation.
	number := uint64(0)
	baseLength := uint64(len(base62Alphabet))

	for currentIndex := 0; currentIndex < len(token); currentIndex++ {
		digitValue := uint64(strings.IndexByte(base62Alphabet, token[currentIndex]))
		number = number*baseLength + digitValue
	}

	return number
}

// KeyParser reads back the components of a string built by MakeKeyConcat.
type KeyParser struct {
	Key        string
	keySplited []string
}

func (e *KeyParser) GetNumber(index int) int64 {
	if len(e.keySplited) == 0 {
		e.keySplited = strings.Split(e.Key, "_")
	}

	if len(e.keySplited) > index {
		return DecodeFromBase62(e.keySplited[index])
	}
	return 0
}

func (e *KeyParser) GetString(index int) string {
	if len(e.keySplited) == 0 {
		e.keySplited = strings.Split(e.Key, "_")
	}

	if len(e.keySplited) > index {
		return e.keySplited[index]
	}
	return ""
}
