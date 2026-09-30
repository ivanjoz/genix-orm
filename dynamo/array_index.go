package dynamo

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fan-out indexes: query a slice field by element
//
// DynamoDB cannot index inside a list, so an Index whose Keys hold a ColSlice
// fans the record out into hidden rows, one per distinct element, next to it:
//
//	pk = base pk ‖ the slice field's cb id (3 digits)            (DynamoDB number)
//	sk = composite(index Keys, slice → element) # base sk        (order-preserving string)
//	d  = the record blob, only with FullCopy
//
// With Keys(ProductIDs.Size(32), Created.Size(32)) a row sk is
// <product>#<created>#<base sk>, so Contains(ProductIDs, 5).Gt(Created, 1000) is
// one exact sk range. The base sk closes the row sk because many records share an
// element (two users with profile 5); it also lets a query that pins every index
// column range on the base Keys. A row pk is always 3 digits longer than any base
// pk of its table, so the two can never meet. The rows carry no nN/sN attributes,
// so they never show up in a GSI query.
//
// Sync on write: the stored version of the record is read first (consistent),
// its row set is diffed against the one being written, and the writes run as new
// rows → base item → stale rows. A crash at any point therefore leaves extra
// rows, never a missing one; Contains re-checks every record it returns against
// the row that led to it, so an extra row never becomes a result.
// ─────────────────────────────────────────────────────────────────────────────

// arrayIndexColumnIDDigits is the width of the cb id appended to the base pk.
const arrayIndexColumnIDDigits = 3

// arrayIndexMeta is one resolved fan-out Index.
type arrayIndexMeta struct {
	// element describes one slice element as a key column: the slice's field name,
	// the element's kind and the column's declared Size(bits).
	element keyCol
	// keys are the index Keys in declared order, with element at elementPosition;
	// the other columns are scalars read from the record.
	keys            []keyCol
	elementPosition int
	columnID        string // the slice field's cb id, zero-padded to arrayIndexColumnIDDigits
	fullCopy        bool
	// elements reads the slice straight from the record and returns one key part
	// per element (duplicates included).
	elements func(ptr unsafe.Pointer) []keyPart
}

func resolveArrayIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index) arrayIndexMeta {
	if index.Slot.attr != "" {
		panic(fmt.Sprintf("db: %s index %s holds a ColSlice: a fan-out index lives in the base table and takes no Slot",
			recordType.Name(), index.Slot.attr))
	}
	elementPosition := -1
	for i, column := range index.Keys {
		if _, isSlice := column.(SliceColn); !isSlice {
			continue
		}
		if elementPosition >= 0 {
			panic(fmt.Sprintf("db: %s has an Index holding two ColSlice columns: a fan-out index takes one", recordType.Name()))
		}
		elementPosition = i
	}

	sliceColumn := index.Keys[elementPosition].(SliceColn)
	column := sliceColumn.col()
	field, ok := recordType.FieldByName(column.fieldName)
	if !ok {
		panic(fmt.Sprintf("db: fan-out index column %q is not a field of %s", column.fieldName, recordType.Name()))
	}
	if field.Type.Kind() != reflect.Slice {
		panic(fmt.Sprintf("db: fan-out index column %s.%s must be a slice", recordType.Name(), field.Name))
	}
	if declaredElement := sliceColumn.elementType(); declaredElement != field.Type.Elem() {
		panic(fmt.Sprintf("db: fan-out index column %s.%s is %s but its ColSlice declares %s elements",
			recordType.Name(), field.Name, field.Type, declaredElement))
	}
	elementKind := classifyKind(field.Type.Elem())
	if elementKind != kindString && !elementKind.isInteger() {
		panic(fmt.Sprintf("db: fan-out index column %s.%s must hold integers or strings", recordType.Name(), field.Name))
	}
	if elementKind.isInteger() && column.bits <= 0 {
		panic(fmt.Sprintf("db: fan-out index column %s.%s holds integers and must declare .Size(bits)", recordType.Name(), field.Name))
	}
	// The cb id, unlike the Go name, survives a field rename, so the rows stay findable.
	columnID, err := strconv.Atoi(field.Tag.Get("cb"))
	if err != nil || columnID < 1 || columnID > 999 {
		panic(fmt.Sprintf("db: fan-out index column %s.%s needs a `cb:\"N\"` tag with N in 1..999", recordType.Name(), field.Name))
	}

	element := keyCol{fieldName: field.Name, kind: elementKind, bits: column.bits}
	keys := resolveKeyCols(recordType, accessors, index.Keys)
	keys[elementPosition] = element
	fieldType, fieldOffset := field.Type, field.Offset
	return arrayIndexMeta{
		element:         element,
		keys:            keys,
		elementPosition: elementPosition,
		columnID:        fmt.Sprintf("%0*d", arrayIndexColumnIDDigits, columnID),
		fullCopy:        index.FullCopy,
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

// arrayRowSKs returns the distinct sks of the rows the record at ptr fans out
// into (nil for a nil ptr): the index Keys with the slice replaced by each
// element, then the base sk.
func (m *tableMeta) arrayRowSKs(arrayIndex *arrayIndexMeta, ptr unsafe.Pointer) []string {
	if ptr == nil {
		return nil
	}
	baseSK := m.skValue(ptr)
	parts := make([]keyPart, len(arrayIndex.keys))
	for i, kc := range arrayIndex.keys {
		if i != arrayIndex.elementPosition {
			parts[i] = m.keyPartsFor(ptr, []keyCol{kc})[0]
		}
	}
	var rowSKs []string
	seen := map[string]bool{}
	for _, element := range arrayIndex.elements(ptr) {
		parts[arrayIndex.elementPosition] = element
		rowSK := buildCompositeKey(parts) + keySeparator + baseSK
		if !seen[rowSK] {
			seen[rowSK] = true
			rowSKs = append(rowSKs, rowSK)
		}
	}
	return rowSKs
}

// baseSKOfArrayRow recovers the base sk from a row sk. No index key part
// contains the separator (Base64 digits, or a string key part), so the base sk
// starts after the keyCount-th one.
func baseSKOfArrayRow(rowSK string, keyCount int) string {
	start := 0
	for range keyCount {
		start += strings.Index(rowSK[start:], keySeparator) + 1
	}
	return rowSK[start:]
}

// writesArrayRow reports whether the record at ptr, as it is now, still fans out
// into the row rowSK — the read-side check that drops rows left behind by a crash
// or a concurrent writer: a removed element, or a scalar index column that
// changed since the row was written.
func (m *tableMeta) writesArrayRow(arrayIndex *arrayIndexMeta, ptr unsafe.Pointer, rowSK string) bool {
	return slices.Contains(m.arrayRowSKs(arrayIndex, ptr), rowSK)
}

// arrayIndexWrites diffs the fan-out rows of a record's stored version
// (storedPtr, nil when there is none) against the version being written
// (writtenPtr, nil for a delete) and returns the rows to put and to delete. Both
// versions share the same key. Keys-only rows are put only when their sk is new
// (a new element, or a changed scalar index column); FullCopy rows are put for
// every element, because the blob they copy changed.
func (m *tableMeta) arrayIndexWrites(storedPtr, writtenPtr unsafe.Pointer, blob []byte) (puts, deletes []types.WriteRequest) {
	keyPtr := writtenPtr
	if keyPtr == nil {
		keyPtr = storedPtr
	}
	basePK := m.pkValue(keyPtr)

	for i := range m.arrayIndexes {
		arrayIndex := &m.arrayIndexes[i]
		rowPK := basePK + arrayIndex.columnID
		storedRowSKs := m.arrayRowSKs(arrayIndex, storedPtr)
		writtenRowSKs := m.arrayRowSKs(arrayIndex, writtenPtr)

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

// recordItemsOfArrayRows turns a page of fan-out rows into the record items they
// point to, in row order, with the sk of the row each came from (for
// writesArrayRow) and the read units spent. A FullCopy row is its own record
// item; keys-only rows read their base records in a BatchGetItem, and a row whose
// record is gone yields nothing.
func recordItemsOfArrayRows(client *dynamodb.Client, basePK string, arrayIndex *arrayIndexMeta, rows []map[string]types.AttributeValue) ([]map[string]types.AttributeValue, []string, float64, error) {
	rowSKs := make([]string, len(rows))
	for i, row := range rows {
		rowSKs[i] = row["sk"].(*types.AttributeValueMemberS).Value
	}
	if arrayIndex.fullCopy {
		return rows, rowSKs, 0, nil
	}

	// A stale row can point at the same record as a live one (a scalar index column
	// changed), and BatchGetItem rejects duplicate keys: each record is read once.
	var keys []map[string]types.AttributeValue
	isRequested := map[string]bool{}
	for _, rowSK := range rowSKs {
		baseSK := baseSKOfArrayRow(rowSK, len(arrayIndex.keys))
		if !isRequested[baseSK] {
			isRequested[baseSK] = true
			keys = append(keys, itemKey(basePK, baseSK))
		}
	}
	items, readUnits, err := batchGet(client, keys, false)
	if err != nil {
		return nil, nil, 0, err
	}
	// BatchGetItem does not keep the request order; restore the row order.
	itemBySK := make(map[string]map[string]types.AttributeValue, len(items))
	for _, item := range items {
		itemBySK[item["sk"].(*types.AttributeValueMemberS).Value] = item
	}
	orderedItems := make([]map[string]types.AttributeValue, 0, len(rows))
	orderedRowSKs := make([]string, 0, len(rows))
	for _, rowSK := range rowSKs {
		if item, ok := itemBySK[baseSKOfArrayRow(rowSK, len(arrayIndex.keys))]; ok {
			orderedItems = append(orderedItems, item)
			orderedRowSKs = append(orderedRowSKs, rowSK)
		}
	}
	return orderedItems, orderedRowSKs, readUnits, nil
}

// batchGet reads items by key in BatchGetItem chunks of 100 (its limit), retrying
// unprocessed keys, and returns the read units spent (a QueryScan budgets them).
// The response order is not the request order.
func batchGet(client *dynamodb.Client, keys []map[string]types.AttributeValue, consistentRead bool) ([]map[string]types.AttributeValue, float64, error) {
	table := tableName()
	var items []map[string]types.AttributeValue
	var readUnits float64
	for start := 0; start < len(keys); start += 100 {
		request := map[string]types.KeysAndAttributes{
			table: {Keys: keys[start:min(start+100, len(keys))], ConsistentRead: aws.Bool(consistentRead)},
		}
		for attempt := 0; len(request) > 0; attempt++ {
			if attempt == 8 {
				return nil, 0, fmt.Errorf("db: BatchGetItem left keys unprocessed after retries")
			}
			out, err := client.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems:           request,
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
			})
			if err != nil {
				return nil, 0, err
			}
			items = append(items, out.Responses[table]...)
			for _, consumed := range out.ConsumedCapacity {
				readUnits += aws.ToFloat64(consumed.CapacityUnits)
			}
			request = out.UnprocessedKeys
		}
	}
	return items, readUnits, nil
}
