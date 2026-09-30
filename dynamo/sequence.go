package dynamo

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Auto-increment sequences
//
// Each entity with UseAutoincrement gets its own counter, stored as a single
// item in the shared table:
//
//	pk = 0   sk = "<TableID>" (or the ReserveIDs name)   cv = <current value, native number>
//
// This mirrors genix's `sequences` table (name -> current_value counter), but
// where genix issues a Scylla `UPDATE ... SET current_value = current_value + ?`
// we use one DynamoDB UpdateItem with an atomic `ADD` and RETURN UPDATED_NEW:
// the increment and the read of the reserved high-water mark happen in a single
// round-trip, so there is no read-modify-write window for two writers to race.
// DynamoDB treats a missing attribute in ADD as 0, so the first reservation of a
// fresh sequence naturally yields 1 — no separate "seed to 1" step is needed.
//
// Every counter shares pk 0, which no entity can produce (a TableID has 8 digits),
// so no Repo.Scan (which filters by its own TableID range) ever returns them, and
// they never carry a colbin "d" blob.
// ─────────────────────────────────────────────────────────────────────────────

const (
	// sequencePartitionKey is the pk of every sequence item.
	sequencePartitionKey = "0"
	// seqValueAttr is the native numeric attribute holding the current value.
	seqValueAttr = "cv"
)

// reserveSequence atomically reserves `count` consecutive values for the named
// sequence and returns the FIRST reserved value (>= 1). Reserving in a single
// ADD means a batch of N records costs one write, and the returned range never
// overlaps another writer's range.
func reserveSequence(name string, count int) (int64, error) {
	if count < 1 {
		count = 1
	}
	client, err := Client()
	if err != nil {
		return 0, err
	}
	out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName()),
		Key:                      sequenceKey(name),
		UpdateExpression:         aws.String("ADD #cv :inc"),
		ExpressionAttributeNames: map[string]string{"#cv": seqValueAttr},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: strconv.Itoa(count)},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, fmt.Errorf("db: reserving %d id(s) for sequence %q: %w", count, name, err)
	}
	attr, ok := out.Attributes[seqValueAttr].(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("db: sequence %q returned no counter value", name)
	}
	high, err := strconv.ParseInt(attr.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("db: sequence %q counter %q is not an integer: %w", name, attr.Value, err)
	}
	return rangeStart(high, count), nil
}

// ReserveIDs atomically reserves `count` consecutive raw sequence values for an
// arbitrarily-named sequence and returns the first. It is the manual analogue of
// UseAutoincrement (genix's GetAutoincrementID): use it when you need IDs before
// building the records, or a sequence not tied to an entity. The returned values
// are raw counters with no random padding applied.
func ReserveIDs(name string, count int) (int64, error) { return reserveSequence(name, count) }

// GetAutoincrementValue returns the entity's sequence counter: the last value
// handed out, 0 for a sequence that never reserved one.
func (r *Repo[T, E]) GetAutoincrementValue() (int64, error) {
	if r.meta.autoinc == nil {
		return 0, fmt.Errorf("db: entity %q does not use autoincrement", r.meta.entity)
	}
	client, err := Client()
	if err != nil {
		return 0, err
	}
	out, err := client.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(tableName()),
		Key:            sequenceKey(r.meta.autoinc.seqName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return 0, fmt.Errorf("db: reading sequence %q: %w", r.meta.autoinc.seqName, err)
	}
	return sequenceCounterValue(r.meta.autoinc.seqName, out.Item)
}

// SetAutoincrementValue moves the entity's sequence counter to an absolute value
// and returns what it held before (genix's SetCounterValue). The next generated
// ID uses value+1. Moving it below an ID already in use makes the ORM hand that
// ID out again, and Put is an upsert: the caller owns that check.
func (r *Repo[T, E]) SetAutoincrementValue(value int64) (int64, error) {
	if r.meta.autoinc == nil {
		return 0, fmt.Errorf("db: entity %q does not use autoincrement", r.meta.entity)
	}
	client, err := Client()
	if err != nil {
		return 0, err
	}
	out, err := client.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:                aws.String(tableName()),
		Key:                      sequenceKey(r.meta.autoinc.seqName),
		UpdateExpression:         aws.String("SET #cv = :value"),
		ExpressionAttributeNames: map[string]string{"#cv": seqValueAttr},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":value": &types.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)},
		},
		ReturnValues: types.ReturnValueUpdatedOld,
	})
	if err != nil {
		return 0, fmt.Errorf("db: setting sequence %q to %d: %w", r.meta.autoinc.seqName, value, err)
	}
	return sequenceCounterValue(r.meta.autoinc.seqName, out.Attributes)
}

func sequenceKey(name string) map[string]types.AttributeValue {
	return itemKey(sequencePartitionKey, name)
}

// sequenceCounterValue reads cv from a sequence item; a missing item or
// attribute is a sequence that never reserved a value, so 0.
func sequenceCounterValue(name string, item map[string]types.AttributeValue) (int64, error) {
	attr, ok := item[seqValueAttr].(*types.AttributeValueMemberN)
	if !ok {
		return 0, nil
	}
	value, err := strconv.ParseInt(attr.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("db: sequence %q counter %q is not an integer: %w", name, attr.Value, err)
	}
	return value, nil
}

// rangeStart converts the post-increment high-water mark into the first value of
// the reserved [start, high] range. Kept separate so it is unit-testable without
// touching DynamoDB.
func rangeStart(high int64, count int) int64 { return high - int64(count) + 1 }

// ─────────────────────────────────────────────────────────────────────────────
// ID composition (sequence + random low digits)
// ─────────────────────────────────────────────────────────────────────────────

// pow10 returns 10^n for n in 0..18 (fits int64). n<=0 returns 1.
func pow10(n int) int64 {
	p := int64(1)
	for i := 0; i < n; i++ {
		p *= 10
	}
	return p
}

// autoincConfig is the resolved per-entity autoincrement setup, precompiled once
// and hung off tableMeta. get/set read and write the record's integer ID field
// through its xunsafe accessor (no per-call reflection).
type autoincConfig struct {
	seqName string // sequence key (the entity's TableID)
	padding int    // number of random low decimal digits
	factor  int64  // 10^padding
	get     func(ptr unsafe.Pointer) int64
	set     func(ptr unsafe.Pointer, v int64)
}

// composeID lays the sequence value into the high digits and a random value into
// the low `padding` digits: seq*10^padding + randDigits. randDigits must be in
// [0, factor). Pure (random supplied by the caller) so it is unit-testable.
func (a *autoincConfig) composeID(seq, randDigits int64) int64 {
	return seq*a.factor + randDigits
}

// randDigits draws the low-digit random component for one ID (0 when padding=0).
func (a *autoincConfig) randDigits() int64 {
	if a.factor <= 1 {
		return 0
	}
	return rand.Int63n(a.factor)
}
