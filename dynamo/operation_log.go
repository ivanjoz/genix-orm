package dynamo

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
)

// ─────────────────────────────────────────────────────────────────────────────
// Operation log
//
// LogOperations prints one line per DynamoDB call made through the shared client:
//
//	dynamo PutItem admin_user · 1 rec · 412 B · 1 WCU · 38ms
//	dynamo Query admin_user gsi-s1 · 3 rec (scanned 3) · 1.2 KB · 0.5 RCU · 41ms
//
// It is a development aid: every call then asks DynamoDB for its consumed capacity. The entity is
// resolved from the TableID every pk starts with; fan-out and delta rows share their entity's
// TableID, so they log under it.
// ─────────────────────────────────────────────────────────────────────────────

// LogOperations turns the operation log on. It is read on every call, so it can be set at any time.
var LogOperations bool

// operationLogOption is installed on the shared client by Client(); it is a no-op while
// LogOperations is false.
func operationLogOption(options *dynamodb.Options) {
	options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("dynamoOperationLog", logOperation), middleware.After)
	})
}

// readOperations spend read units; every other call spends write units.
var readOperations = []string{"GetItem", "Query", "Scan", "BatchGetItem"}

func logOperation(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
	if !LogOperations {
		return next.HandleInitialize(ctx, in)
	}
	// Every input that can report capacity has a ReturnConsumedCapacity field.
	if input := reflect.ValueOf(in.Parameters); input.Kind() == reflect.Pointer {
		if field := input.Elem().FieldByName("ReturnConsumedCapacity"); field.IsValid() {
			field.Set(reflect.ValueOf(types.ReturnConsumedCapacityTotal))
		}
	}
	startedAt := time.Now()
	out, metadata, err := next.HandleInitialize(ctx, in)
	elapsed := time.Since(startedAt).Round(time.Millisecond)

	operation := awsmiddleware.GetOperationName(ctx)
	header := "dynamo " + operation + " " + describeTarget(in.Parameters)
	if err != nil {
		log.Printf("%s · failed: %v · %s", header, err, elapsed)
		return out, metadata, err
	}
	records, size, capacityUnits := measureOperation(in.Parameters, out.Result)
	unitsName := "WCU"
	if slices.Contains(readOperations, operation) {
		unitsName = "RCU"
	}
	log.Printf("%s · %s · %s · %s %s · %s", header, records, formatItemBytes(size),
		strconv.FormatFloat(capacityUnits, 'f', -1, 64), unitsName, elapsed)
	return out, metadata, err
}

// describeTarget names the entity a call touches (plus the GSI of a Query), from the first pk it
// carries: the key of a single-item call, the :pk of a Query, the :lo bound of a Scan.
func describeTarget(input any) string {
	var key map[string]types.AttributeValue
	switch typed := input.(type) {
	case *dynamodb.PutItemInput:
		key = typed.Item
	case *dynamodb.GetItemInput:
		key = typed.Key
	case *dynamodb.UpdateItemInput:
		key = typed.Key
	case *dynamodb.DeleteItemInput:
		key = typed.Key
	case *dynamodb.QueryInput:
		target := entityOfKeyValue(typed.ExpressionAttributeValues[":pk"])
		if typed.IndexName != nil {
			target += " " + *typed.IndexName
		}
		return target
	case *dynamodb.ScanInput:
		return entityOfKeyValue(typed.ExpressionAttributeValues[":lo"])
	case *dynamodb.BatchWriteItemInput:
		for _, requests := range typed.RequestItems {
			if len(requests) > 0 && requests[0].PutRequest != nil {
				key = requests[0].PutRequest.Item
			} else if len(requests) > 0 && requests[0].DeleteRequest != nil {
				key = requests[0].DeleteRequest.Key
			}
		}
	case *dynamodb.BatchGetItemInput:
		for _, keysAndAttributes := range typed.RequestItems {
			if len(keysAndAttributes.Keys) > 0 {
				key = keysAndAttributes.Keys[0]
			}
		}
	}
	// A sequence row lives under pk 0 with the owning table's TableID as its sk.
	if pk, isNumber := key["pk"].(*types.AttributeValueMemberN); isNumber && pk.Value == sequencePartitionKey {
		return "sequence:" + entityOfKeyValue(key["sk"])
	}
	return entityOfKeyValue(key["pk"])
}

// entityOfKeyValue maps a pk, a numeric slot or a string slot back to its entity: all of them
// start with the 8-digit TableID.
func entityOfKeyValue(value types.AttributeValue) string {
	var raw string
	switch typed := value.(type) {
	case *types.AttributeValueMemberN:
		raw = typed.Value
	case *types.AttributeValueMemberS:
		raw = typed.Value
	}
	if len(raw) < 8 {
		return "?"
	}
	tableID, err := strconv.ParseInt(raw[:8], 10, 32)
	if err != nil {
		return "?"
	}
	if entity, found := entityByTableID.Load(int32(tableID)); found {
		return entity.(string)
	}
	return "?"
}

// measureOperation returns the record count, the bytes of the items the call carried (written
// items, or returned items for reads) and the capacity units it consumed. UpdateItem and
// DeleteItem only carry a key, so their size is the request's, not the stored item's.
func measureOperation(input any, result any) (records string, size int, capacityUnits float64) {
	switch typed := result.(type) {
	case *dynamodb.PutItemOutput:
		return "1 rec", itemBytes(input.(*dynamodb.PutItemInput).Item), totalUnits(typed.ConsumedCapacity)
	case *dynamodb.UpdateItemOutput:
		updateInput := input.(*dynamodb.UpdateItemInput)
		return "1 rec", itemBytes(updateInput.Key) + itemBytes(updateInput.ExpressionAttributeValues), totalUnits(typed.ConsumedCapacity)
	case *dynamodb.DeleteItemOutput:
		return "1 rec", itemBytes(input.(*dynamodb.DeleteItemInput).Key), totalUnits(typed.ConsumedCapacity)
	case *dynamodb.GetItemOutput:
		if typed.Item == nil {
			return "0 rec", 0, totalUnits(typed.ConsumedCapacity)
		}
		return "1 rec", itemBytes(typed.Item), totalUnits(typed.ConsumedCapacity)
	case *dynamodb.QueryOutput:
		for _, item := range typed.Items {
			size += itemBytes(item)
		}
		return fmt.Sprintf("%d rec (scanned %d)", typed.Count, typed.ScannedCount), size, totalUnits(typed.ConsumedCapacity)
	case *dynamodb.ScanOutput:
		for _, item := range typed.Items {
			size += itemBytes(item)
		}
		return fmt.Sprintf("%d rec (scanned %d)", typed.Count, typed.ScannedCount), size, totalUnits(typed.ConsumedCapacity)
	case *dynamodb.BatchWriteItemOutput:
		count := 0
		for _, requests := range input.(*dynamodb.BatchWriteItemInput).RequestItems {
			for _, request := range requests {
				count++
				if request.PutRequest != nil {
					size += itemBytes(request.PutRequest.Item)
				} else if request.DeleteRequest != nil {
					size += itemBytes(request.DeleteRequest.Key)
				}
			}
		}
		for _, tableConsumed := range typed.ConsumedCapacity {
			capacityUnits += totalUnits(&tableConsumed)
		}
		return fmt.Sprintf("%d rec", count), size, capacityUnits
	case *dynamodb.BatchGetItemOutput:
		count := 0
		for _, items := range typed.Responses {
			for _, item := range items {
				count++
				size += itemBytes(item)
			}
		}
		for _, tableConsumed := range typed.ConsumedCapacity {
			capacityUnits += totalUnits(&tableConsumed)
		}
		return fmt.Sprintf("%d rec", count), size, capacityUnits
	}
	return "? rec", 0, 0
}

func totalUnits(consumed *types.ConsumedCapacity) float64 {
	if consumed == nil {
		return 0
	}
	return aws.ToFloat64(consumed.CapacityUnits)
}

// itemBytes approximates DynamoDB's item size, the figure capacity is billed on: each attribute
// name plus its value, a number counting about one byte per two digits plus one.
func itemBytes(item map[string]types.AttributeValue) int {
	size := 0
	for name, value := range item {
		size += len(name)
		switch typed := value.(type) {
		case *types.AttributeValueMemberS:
			size += len(typed.Value)
		case *types.AttributeValueMemberB:
			size += len(typed.Value)
		case *types.AttributeValueMemberN:
			size += (len(strings.TrimLeft(typed.Value, "-0"))+1)/2 + 1
		default:
			size++
		}
	}
	return size
}

func formatItemBytes(size int) string {
	if size < 1024 {
		return strconv.Itoa(size) + " B"
	}
	return strconv.FormatFloat(float64(size)/1024, 'f', 1, 64) + " KB"
}
