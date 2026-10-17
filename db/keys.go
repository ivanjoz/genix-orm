package db

import (
	"fmt"
	"strings"

	"github.com/ivanjoz/genix-orm/utils"
)

// Key encoding is part of the ORM's storage contract, not a driver detail: a
// concatenated key written by one driver must read back identically under another,
// so these live in the shared layer.

// KeyConcatSeparator joins the components of a KeyConcatenated key: outside utils.Base64Alphabet,
// so it never appears inside an integer component.
const KeyConcatSeparator = "."

// MakeKeyConcat joins values into the deterministic string a KeyConcatenated column
// stores. Integers are written in utils.EncodeUintBase64 to keep the key short, and non-positive
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
				token = utils.EncodeUintBase64(uint64(typedValue))
			}
		case int64:
			if typedValue > 0 {
				token = utils.EncodeUintBase64(uint64(typedValue))
			}
		case int:
			if typedValue > 0 {
				token = utils.EncodeUintBase64(uint64(typedValue))
			}
		case int16:
			if typedValue > 0 {
				token = utils.EncodeUintBase64(uint64(typedValue))
			}
		default:
			token = fmt.Sprintf("%v", typedValue)
		}
		valuesStrings = append(valuesStrings, token)
	}
	return strings.TrimRight(strings.Join(valuesStrings, KeyConcatSeparator), KeyConcatSeparator)
}

// KeyParser reads back the components of a string built by MakeKeyConcat.
type KeyParser struct {
	Key        string
	keySplited []string
}

func (e *KeyParser) GetNumber(index int) int64 {
	if len(e.keySplited) == 0 {
		e.keySplited = strings.Split(e.Key, KeyConcatSeparator)
	}

	if len(e.keySplited) > index {
		number, _ := utils.DecodeUintBase64(e.keySplited[index])
		return int64(number)
	}
	return 0
}

func (e *KeyParser) GetString(index int) string {
	if len(e.keySplited) == 0 {
		e.keySplited = strings.Split(e.Key, KeyConcatSeparator)
	}

	if len(e.keySplited) > index {
		return e.keySplited[index]
	}
	return ""
}
