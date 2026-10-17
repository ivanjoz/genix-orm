package utils

import (
	"encoding/base64"
	"strings"
)

// Base64Alphabet is the one base64 alphabet of everything genix-orm and its apps encode themselves
// (keys, tokens, file names, query parameters): URL-safe like base64url, but in ASCII order, so two
// encodings of the same length sort like the values they encode. External formats (JWT, data URLs,
// JSON []byte, PHC hashes) keep their standard base64.
const Base64Alphabet = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"

// Base64 encodes bytes in Base64Alphabet, unpadded.
var Base64 = base64.NewEncoding(Base64Alphabet).WithPadding(base64.NoPadding)

// EncodeUintBase64 writes value as base-64 digits of Base64Alphabet, most significant first, no
// leading zero digits: 0 → "-", 63 → "z", 64 → "0-". Numbers of the same digit count sort as text.
func EncodeUintBase64(value uint64) string { return string(AppendUintBase64(nil, value)) }

// AppendUintBase64 appends EncodeUintBase64(value) to out.
func AppendUintBase64(out []byte, value uint64) []byte {
	var digits [11]byte // 64 bits take at most 11 digits of 6 bits
	position := len(digits)
	for {
		position--
		digits[position] = Base64Alphabet[value&63]
		value >>= 6
		if value == 0 {
			break
		}
	}
	return append(out, digits[position:]...)
}

// DecodeUintBase64 reverses EncodeUintBase64; false for an empty token, a character outside
// Base64Alphabet or more than 64 bits.
func DecodeUintBase64(token string) (uint64, bool) {
	if token == "" || len(token) > 11 {
		return 0, false
	}
	var value uint64
	for i := 0; i < len(token); i++ {
		digit := strings.IndexByte(Base64Alphabet, token[i])
		if digit < 0 || value>>58 != 0 {
			return 0, false
		}
		value = value<<6 | uint64(digit)
	}
	return value, true
}
