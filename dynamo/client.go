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

// ClientOptions are applied to the shared client when it is built, so they must be set before the
// first call (an init() is the safe place). They exist for instrumentation, e.g. the middleware in
// ormcheck that asks every call for its consumed capacity.
var ClientOptions []func(*dynamodb.Options)

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
		dynOpts := []func(*dynamodb.Options){operationLogOption}
		if endpoint := os.Getenv("DYNAMO_ENDPOINT"); endpoint != "" {
			dynOpts = append(dynOpts, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String(endpoint)
			})
		}
		clientRef = dynamodb.NewFromConfig(cfg, append(dynOpts, ClientOptions...)...)
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

// marshalItem produces the full DynamoDB item for a record. ptr is the struct
// pointer (used by the precompiled key accessors); record is the same value (a
// *E) handed to colbin for the "d" blob.
//
// A field left at its zero value is not written at all — colbin omits it and the
// decoder restores the zero from the missing key. That is what keeps a whole
// record in one blob affordable, and it is why a *T pointing at T's zero value
// comes back nil rather than pointing at a zero.
func (m *tableMeta) marshalItem(ptr unsafe.Pointer, record any) (map[string]types.AttributeValue, error) {
	blob, err := colbin.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("db: colbin marshaling %s: %w", m.recordType.Name(), err)
	}
	item := m.keyOnly(ptr)
	item[dataColumn] = &types.AttributeValueMemberB{Value: blob}
	// A versioned table exposes Updated outside the blob: Modify's conditional write compares it.
	if m.isVersioned {
		item[updatedColumn] = &types.AttributeValueMemberN{Value: strconv.FormatInt(m.updated.acc.getI64(ptr), 10)}
	}
	for _, idx := range m.indexes {
		item[idx.slot.hashAttr] = &types.AttributeValueMemberN{Value: m.partitionValue(ptr, idx.partition)}
		item[idx.slot.rangeAttr] = &types.AttributeValueMemberS{Value: buildCompositeKey(m.keyPartsFor(ptr, idx.sortColumns))}
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

// keyOnly builds just the {pk, sk} key map for Get/Delete.
func (m *tableMeta) keyOnly(ptr unsafe.Pointer) map[string]types.AttributeValue {
	return itemKey(m.pkValue(ptr), m.skValue(ptr))
}

// itemKey is the {pk, sk} map of any row: pk is a number, sk a string.
func itemKey(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberN{Value: pk},
		"sk": &types.AttributeValueMemberS{Value: sk},
	}
}
