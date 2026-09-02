package dynamo

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"unsafe"

	"github.com/ivanjoz/colbin"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// DynamoDB client
//
// One lazily-initialized client shared process-wide (the AWS SDK client is
// goroutine-safe). Configuration comes from the standard AWS chain; set
// DYNAMO_ENDPOINT to point at a local DynamoDB for tests/dev.
// ─────────────────────────────────────────────────────────────────────────────

var (
	clientOnce sync.Once
	clientRef  *dynamodb.Client
	clientErr  error
)

// Client returns the shared DynamoDB client.
func Client() (*dynamodb.Client, error) {
	clientOnce.Do(func() {
		var opts []func(*awsconfig.LoadOptions) error
		if region := os.Getenv("AWS_REGION"); region != "" {
			opts = append(opts, awsconfig.WithRegion(region))
		}
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
		if err != nil {
			clientErr = fmt.Errorf("db: loading AWS config: %w", err)
			return
		}
		var dynOpts []func(*dynamodb.Options)
		if endpoint := os.Getenv("DYNAMO_ENDPOINT"); endpoint != "" {
			dynOpts = append(dynOpts, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String(endpoint)
			})
		}
		clientRef = dynamodb.NewFromConfig(cfg, dynOpts...)
	})
	return clientRef, clientErr
}

// TableName is the single DynamoDB table this ORM operates on. Set it once at
// startup, before the first query:
//
//	dynamo.TableName = "my-table"
//
// If left empty, the DYNAMO_TABLE environment variable is used; if that is also
// empty, it falls back to "demo-app".
var TableName string

// tableName resolves the single-table name: explicit config, then environment,
// then the built-in default.
func tableName() string {
	if TableName != "" {
		return TableName
	}
	if t := os.Getenv("DYNAMO_TABLE"); t != "" {
		return t
	}
	return "demo-app"
}

// ─────────────────────────────────────────────────────────────────────────────
// Marshaling
//
// The whole record is serialized once with colbin into a single binary column
// "d"; the item otherwise carries only the derived key/index attributes
// (pk/sk/nN/sN). So an item is exactly: the keys DynamoDB needs to find it, plus
// one opaque blob holding everything else. This mirrors genix persisting complex
// types as a blob via colbin, and keeps the table schemaless — new record fields
// never change the item shape. The trade-off: DynamoDB cannot see inside "d", so
// predicates on non-key fields are applied in memory after decode (see query.go).
// ─────────────────────────────────────────────────────────────────────────────

// dataColumn is the attribute name of the binary record blob.
const dataColumn = "d"

// Omit-empty colbin encoding. It matters more here than anywhere else in the ORM:
// the whole record is one blob, so every field a record leaves untouched is paid
// for in that blob. A column of nothing but empty values becomes its type byte
// alone instead of a slot per record. colbin's flag is process-global, so it is
// set on import rather than left to a caller. Decoding is unaffected — both forms
// are self-describing — at the price of a *T pointing at T's zero value decoding
// back as nil.
func init() { colbin.SetOmitEmpty(true) }

// marshalItem produces the full DynamoDB item for a record. ptr is the struct
// pointer (used by the precompiled key accessors); record is the same value (a
// *E) handed to colbin for the "d" blob.
func (m *tableMeta) marshalItem(ptr unsafe.Pointer, record any) (map[string]types.AttributeValue, error) {
	blob, err := colbin.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("db: colbin marshaling %s: %w", m.recordType.Name(), err)
	}
	item := map[string]types.AttributeValue{
		"pk":       &types.AttributeValueMemberS{Value: m.pkValue(ptr)},
		"sk":       &types.AttributeValueMemberS{Value: m.skValue(ptr)},
		dataColumn: &types.AttributeValueMemberB{Value: blob},
	}
	for _, idx := range m.indexes {
		item[idx.slot.attr] = attributeForSlot(m.slotValue(ptr, idx), idx.slot.isNumber)
	}
	return item, nil
}

// unmarshalItem decodes the "d" blob of an item into dst (a *E).
func (m *tableMeta) unmarshalItem(item map[string]types.AttributeValue, dst any) error {
	blob, ok := item[dataColumn].(*types.AttributeValueMemberB)
	if !ok {
		return fmt.Errorf("db: %s item is missing binary column %q", m.recordType.Name(), dataColumn)
	}
	if err := colbin.Unmarshal(blob.Value, dst); err != nil {
		return fmt.Errorf("db: colbin unmarshaling %s: %w", m.recordType.Name(), err)
	}
	return nil
}

func attributeForSlot(v any, isNumber bool) types.AttributeValue {
	if isNumber {
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(v.(int64), 10)}
	}
	return &types.AttributeValueMemberS{Value: v.(string)}
}

// keyOnly builds just the {pk, sk} key map for Get/Delete.
func (m *tableMeta) keyOnly(ptr unsafe.Pointer) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: m.pkValue(ptr)},
		"sk": &types.AttributeValueMemberS{Value: m.skValue(ptr)},
	}
}
