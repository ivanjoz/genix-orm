package ormcheck

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/ivanjoz/genix-orm/dynamo"
)

// The meter is installed on the ORM's shared client at import time: dynamo.ClientOptions only
// applies to a client built after it is set, and an init() runs before any call can build one.
func init() {
	dynamo.ClientOptions = append(dynamo.ClientOptions, func(options *dynamodb.Options) {
		options.APIOptions = append(options.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("ormcheckConsumedCapacity", measureConsumedCapacity), middleware.After)
		})
	})
}

// readOperations are the calls whose capacity is read units; every other call spends write units.
var readOperations = []string{"GetItem", "Query", "Scan", "BatchGetItem"}

type capacityCall struct {
	operation string
	units     float64
}

// capacityMeter records the calls made since its last reset.
type capacityMeter struct {
	mutex sync.Mutex
	calls []capacityCall
}

var meter = &capacityMeter{}

// measureConsumedCapacity asks every call for its TOTAL consumed capacity and records it. Every
// DynamoDB input that can report capacity has a ReturnConsumedCapacity field and its output a
// ConsumedCapacity field (one struct, or a slice for batch calls), so reflection covers them all.
func measureConsumedCapacity(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
	if input := reflect.ValueOf(in.Parameters); input.Kind() == reflect.Pointer {
		if field := input.Elem().FieldByName("ReturnConsumedCapacity"); field.IsValid() {
			field.Set(reflect.ValueOf(types.ReturnConsumedCapacityTotal))
		}
	}
	out, metadata, err := next.HandleInitialize(ctx, in)
	if err != nil {
		return out, metadata, err
	}
	call := capacityCall{operation: awsmiddleware.GetOperationName(ctx)}
	if output := reflect.ValueOf(out.Result); output.Kind() == reflect.Pointer && !output.IsNil() {
		if field := output.Elem().FieldByName("ConsumedCapacity"); field.IsValid() {
			switch consumed := field.Interface().(type) {
			case *types.ConsumedCapacity:
				if consumed != nil {
					call.units = aws.ToFloat64(consumed.CapacityUnits)
				}
			case []types.ConsumedCapacity:
				for _, tableConsumed := range consumed {
					call.units += aws.ToFloat64(tableConsumed.CapacityUnits)
				}
			}
		}
	}
	meter.mutex.Lock()
	meter.calls = append(meter.calls, call)
	meter.mutex.Unlock()
	return out, metadata, err
}

func (meter *capacityMeter) reset() {
	meter.mutex.Lock()
	meter.calls = nil
	meter.mutex.Unlock()
}

// summary renders the calls since the last reset as "Query×2 BatchGetItem×1 · 1.5 RCU · 3 WCU".
func (meter *capacityMeter) summary() string {
	meter.mutex.Lock()
	defer meter.mutex.Unlock()
	var operationOrder []string
	callsByOperation := map[string]int{}
	var readUnits, writeUnits float64
	for _, call := range meter.calls {
		if callsByOperation[call.operation] == 0 {
			operationOrder = append(operationOrder, call.operation)
		}
		callsByOperation[call.operation]++
		if slices.Contains(readOperations, call.operation) {
			readUnits += call.units
		} else {
			writeUnits += call.units
		}
	}
	var parts []string
	for _, operation := range operationOrder {
		parts = append(parts, fmt.Sprintf("%s×%d", operation, callsByOperation[operation]))
	}
	summary := strings.Join(parts, " ") + " · " + formatUnits(readUnits) + " RCU"
	if writeUnits > 0 {
		summary += " · " + formatUnits(writeUnits) + " WCU"
	}
	return summary
}

func formatUnits(units float64) string { return strconv.FormatFloat(units, 'f', -1, 64) }
