package dynamo

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Array indexes: query a slice field by element
//
// DynamoDB cannot index inside a list, so each ArrayIndexes field is fanned out
// into hidden rows, one per distinct element, next to the record:
//
//	pk = base pk ‖ the field's cb id (3 digits)     (DynamoDB number)
//	sk = composite(element) # base sk               (order-preserving string)
//	d  = the record blob, only with FullCopy
//
// The base sk is part of the row sk because many records share an element (two
// users with profile 5); it also lets Contains extend its begins_with with the
// base sort predicates. A row pk is always 3 digits longer than any base pk of
// its table, so the two can never meet. The rows carry no nN/sN attributes, so
// they never show up in a GSI query.
//
// Sync on write: the stored version of the record is read first (consistent),
// its element set is diffed against the one being written, and the writes run
// as new rows → base item → stale rows. A crash at any point therefore leaves
// extra rows, never a missing one; Contains re-checks every record it returns
// against the element it asked for, so an extra row never becomes a result.
// ─────────────────────────────────────────────────────────────────────────────

// arrayIndexColumnIDDigits is the width of the cb id appended to the base pk.
const arrayIndexColumnIDDigits = 3

// arrayIndexMeta is one resolved ArrayIndexes entry.
type arrayIndexMeta struct {
	// element describes one slice element as a key column: the slice's field name,
	// the element's kind and the column's declared Size(bits).
	element  keyCol
	columnID string // the field's cb id, zero-padded to arrayIndexColumnIDDigits
	fullCopy bool
	// elements reads the slice straight from the record and returns one key part
	// per element (duplicates included).
	elements func(ptr unsafe.Pointer) []keyPart
}

func resolveArrayIndex(recordType reflect.Type, arrayIndex ArrayIndex) arrayIndexMeta {
	if arrayIndex.Column == nil {
		panic(fmt.Sprintf("db: %s has an ArrayIndexes entry with no Column", recordType.Name()))
	}
	column := arrayIndex.Column.col()
	field, ok := recordType.FieldByName(column.fieldName)
	if !ok {
		panic(fmt.Sprintf("db: array index column %q is not a field of %s", column.fieldName, recordType.Name()))
	}
	if field.Type.Kind() != reflect.Slice {
		panic(fmt.Sprintf("db: array index column %s.%s must be a slice", recordType.Name(), field.Name))
	}
	if declaredElement := arrayIndex.Column.elementType(); declaredElement != field.Type.Elem() {
		panic(fmt.Sprintf("db: array index column %s.%s is %s but its ColSlice declares %s elements",
			recordType.Name(), field.Name, field.Type, declaredElement))
	}
	elementKind := classifyKind(field.Type.Elem())
	if elementKind != kindString && !elementKind.isInteger() {
		panic(fmt.Sprintf("db: array index column %s.%s must hold integers or strings", recordType.Name(), field.Name))
	}
	if elementKind.isInteger() && column.bits <= 0 {
		panic(fmt.Sprintf("db: array index column %s.%s holds integers and must declare .Size(bits)", recordType.Name(), field.Name))
	}
	// The cb id, unlike the Go name, survives a field rename, so the rows stay findable.
	columnID, err := strconv.Atoi(field.Tag.Get("cb"))
	if err != nil || columnID < 1 || columnID > 999 {
		panic(fmt.Sprintf("db: array index column %s.%s needs a `cb:\"N\"` tag with N in 1..999", recordType.Name(), field.Name))
	}

	element := keyCol{fieldName: field.Name, kind: elementKind, bits: column.bits}
	fieldType, fieldOffset := field.Type, field.Offset
	return arrayIndexMeta{
		element:  element,
		columnID: fmt.Sprintf("%0*d", arrayIndexColumnIDDigits, columnID),
		fullCopy: arrayIndex.FullCopy,
		elements: func(ptr unsafe.Pointer) []keyPart {
			slice := reflect.NewAt(fieldType, unsafe.Add(ptr, fieldOffset)).Elem()
			parts := make([]keyPart, slice.Len())
			for i := range parts {
				parts[i] = keyPartFromValue(element, slice.Index(i).Interface())
			}
			return parts
		},
	}
}

// arrayRowSK is the sk of the row for one element of the record whose sk is baseSK.
func arrayRowSK(element keyPart, baseSK string) string {
	return buildCompositeKey([]keyPart{element}) + keySeparator + baseSK
}

// baseSKOfArrayRow recovers the base sk from a row sk. The element part never
// contains the separator (Base64 digits, or a string key part), so the first
// separator ends it.
func baseSKOfArrayRow(rowSK string) string {
	return rowSK[strings.Index(rowSK, keySeparator)+1:]
}

// holdsElement reports whether the record's slice still contains element — the
// read-side check that drops rows left behind by a crash or a concurrent writer.
func (arrayIndex *arrayIndexMeta) holdsElement(ptr unsafe.Pointer, element keyPart) bool {
	for _, part := range arrayIndex.elements(ptr) {
		if part == element {
			return true
		}
	}
	return false
}

// arrayIndexWrites diffs the array index rows of a record's stored version
// (storedPtr, nil when there is none) against the version being written
// (writtenPtr, nil for a delete) and returns the rows to put and to delete. Both
// versions share the same key. Keys-only rows are put only for new elements;
// FullCopy rows are put for every element, because the blob they copy changed.
func (m *tableMeta) arrayIndexWrites(storedPtr, writtenPtr unsafe.Pointer, blob []byte) (puts, deletes []types.WriteRequest) {
	keyPtr := writtenPtr
	if keyPtr == nil {
		keyPtr = storedPtr
	}
	basePK, baseSK := m.pkValue(keyPtr), m.skValue(keyPtr)

	rowSKsOf := func(ptr unsafe.Pointer, arrayIndex *arrayIndexMeta) []string {
		if ptr == nil {
			return nil
		}
		var rowSKs []string
		seen := map[string]bool{}
		for _, element := range arrayIndex.elements(ptr) {
			rowSK := arrayRowSK(element, baseSK)
			if !seen[rowSK] {
				seen[rowSK] = true
				rowSKs = append(rowSKs, rowSK)
			}
		}
		return rowSKs
	}

	for i := range m.arrayIndexes {
		arrayIndex := &m.arrayIndexes[i]
		rowPK := basePK + arrayIndex.columnID
		storedRowSKs := rowSKsOf(storedPtr, arrayIndex)
		writtenRowSKs := rowSKsOf(writtenPtr, arrayIndex)

		isStored := map[string]bool{}
		for _, rowSK := range storedRowSKs {
			isStored[rowSK] = true
		}
		isWritten := map[string]bool{}
		for _, rowSK := range writtenRowSKs {
			isWritten[rowSK] = true
			if isStored[rowSK] && !arrayIndex.fullCopy {
				continue
			}
			row := itemKey(rowPK, rowSK)
			if arrayIndex.fullCopy {
				row[dataColumn] = &types.AttributeValueMemberB{Value: blob}
			}
			puts = append(puts, types.WriteRequest{PutRequest: &types.PutRequest{Item: row}})
		}
		for _, rowSK := range storedRowSKs {
			if !isWritten[rowSK] {
				deletes = append(deletes, types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: itemKey(rowPK, rowSK)}})
			}
		}
	}
	return puts, deletes
}

// baseItemsOfArrayRows reads the base records a page of keys-only array rows
// points to, in row order. A row whose record is gone yields nothing.
func baseItemsOfArrayRows(client *dynamodb.Client, basePK string, rows []map[string]types.AttributeValue) ([]map[string]types.AttributeValue, error) {
	baseSKs := make([]string, len(rows))
	keys := make([]map[string]types.AttributeValue, len(rows))
	for i, row := range rows {
		baseSKs[i] = baseSKOfArrayRow(row["sk"].(*types.AttributeValueMemberS).Value)
		keys[i] = itemKey(basePK, baseSKs[i])
	}
	items, err := batchGet(client, keys, false)
	if err != nil {
		return nil, err
	}
	// BatchGetItem does not keep the request order; restore the row order.
	itemBySK := make(map[string]map[string]types.AttributeValue, len(items))
	for _, item := range items {
		itemBySK[item["sk"].(*types.AttributeValueMemberS).Value] = item
	}
	orderedItems := make([]map[string]types.AttributeValue, 0, len(items))
	for _, baseSK := range baseSKs {
		if item, ok := itemBySK[baseSK]; ok {
			orderedItems = append(orderedItems, item)
		}
	}
	return orderedItems, nil
}

// batchGet reads items by key in BatchGetItem chunks of 100 (its limit), retrying
// unprocessed keys. The response order is not the request order.
func batchGet(client *dynamodb.Client, keys []map[string]types.AttributeValue, consistentRead bool) ([]map[string]types.AttributeValue, error) {
	table := tableName()
	var items []map[string]types.AttributeValue
	for start := 0; start < len(keys); start += 100 {
		request := map[string]types.KeysAndAttributes{
			table: {Keys: keys[start:min(start+100, len(keys))], ConsistentRead: aws.Bool(consistentRead)},
		}
		for attempt := 0; len(request) > 0; attempt++ {
			if attempt == 8 {
				return nil, fmt.Errorf("db: BatchGetItem left keys unprocessed after retries")
			}
			out, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{RequestItems: request})
			if err != nil {
				return nil, err
			}
			items = append(items, out.Responses[table]...)
			request = out.UnprocessedKeys
		}
	}
	return items, nil
}
