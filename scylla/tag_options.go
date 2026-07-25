package scylla

import (
	"fmt"
	"github.com/ivanjoz/genix-orm/db"
	"strings"
)

func applyCollectionTagOptions(recordTypeName string, recordFieldName string, inferredColType colType, tagConfig db.DBTag) colType {
	if !tagConfig.HasCollectionOptions() {
		return inferredColType
	}

	if !inferredColType.IsSlice {
		panic(fmt.Sprintf(`Record "%v": field "%v" uses db collection options on non-slice type "%v".`,
			recordTypeName, recordFieldName, inferredColType.FieldType))
	}

	// Reject ambiguous collection kind options so tag behavior is explicit and deterministic.
	if tagConfig.IsList && tagConfig.IsSet {
		panic(fmt.Sprintf(`Record "%v": field "%v" cannot declare both "list" and "set" db options.`,
			recordTypeName, recordFieldName))
	}

	innerCollectionType := unwrapFrozenCollectionType(inferredColType.DBType)
	if tagConfig.IsSet {
		innerCollectionType = swapCollectionKind(innerCollectionType, "set")
	} else {
		innerCollectionType = swapCollectionKind(innerCollectionType, "list")
	}

	if tagConfig.IsFrozen {
		inferredColType.DBType = "frozen<" + innerCollectionType + ">"
		return inferredColType
	}

	collectionColType := innerCollectionType
	inferredColType.DBType = collectionColType
	return inferredColType
}

func applyFrozenCollectionDefault(baseColType string, shouldBeFrozen bool) string {
	innerCollectionType := unwrapFrozenCollectionType(baseColType)
	if shouldBeFrozen {
		return "frozen<" + innerCollectionType + ">"
	}
	return innerCollectionType
}

func unwrapFrozenCollectionType(collectionColType string) string {
	const frozenPrefix = "frozen<"
	if strings.HasPrefix(collectionColType, frozenPrefix) && strings.HasSuffix(collectionColType, ">") {
		return collectionColType[len(frozenPrefix) : len(collectionColType)-1]
	}
	return collectionColType
}

func swapCollectionKind(collectionColType string, targetKind string) string {
	if targetKind != "list" && targetKind != "set" {
		return collectionColType
	}

	openBracketIndex := strings.IndexByte(collectionColType, '<')
	closeBracketIndex := strings.LastIndexByte(collectionColType, '>')
	if openBracketIndex < 0 || closeBracketIndex < 0 || closeBracketIndex <= openBracketIndex {
		return collectionColType
	}

	elementType := collectionColType[openBracketIndex+1 : closeBracketIndex]
	return targetKind + "<" + elementType + ">"
}
