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
// Query builder + planner
//
// Predicates are collected fluently, then planned against the physical schema:
//
//   - A partition source is chosen from the equality predicates: the base table
//     (all Partition columns have "=") or a GSI slot (all of its key columns
//     have "="). Base table wins when both are available.
//   - Predicates on the Sort columns become the shared sk key condition
//     (=, begins_with, range, between). Because the physical table shares one sk
//     across the base table and every GSI, the sort dimension is uniform.
//   - Any remaining predicates are on fields that live inside the "d" blob, which
//     DynamoDB cannot see, so they are evaluated in memory after each row decodes.
// ─────────────────────────────────────────────────────────────────────────────

type op int8

const (
	opEq op = iota
	opGt
	opGte
	opLt
	opLte
	opBetween
	opBeginsWith
)

type predicate struct {
	field  string
	op     op
	v1, v2 any
}

// ─────────────────────────────────────────────────────────────────────────────
// Dynamic (type-erased) query
//
// QueryRecords is the non-generic entry point behind db.Controller: it lets a
// caller that only holds a Controller (e.g. an admin/table-inspector handler)
// query an entity chosen at runtime, passing predicates by field name instead of
// by the compile-time Col handles.
//
// It is deliberately STRICT about what is queryable, because DynamoDB only
// supports the physical access paths this store declares:
//
//   - The predicate set MUST resolve to a partition source: either an equality on
//     every base-table Partition column, or an equality on every key column of one
//     GSI. Otherwise it errors ("no usable partition") — this covers a missing
//     index or a partial (missing-column) index combination.
//   - Only the shared sort key supports ranges (>, >=, <, <=, between,
//     begins_with). A range on a hash column (the base pk or a numeric GSI, which
//     store a single value, not an order-preserving concatenation) or a filter on
//     a non-indexed field is rejected: such predicates would fall to the in-memory
//     post-filter, and QueryRecords refuses to run a query that isn't fully served
//     by an index.
// ─────────────────────────────────────────────────────────────────────────────

// queryRecordsMaxLimit caps how many records a dynamic query returns.
const queryRecordsMaxLimit = 400

// QueryPredicate is one field/operator/value constraint for QueryRecords. Values
// arrive as decoded JSON (string, number, bool) and are coerced to the column's
// Go kind before the query is planned.
type QueryPredicate struct {
	Field  string `json:"field"`
	Op     string `json:"op"`               // "=", ">", ">=", "<", "<=", "between", "begins_with"
	Value  any    `json:"value"`            // the (first) operand
	Value2 any    `json:"value2,omitempty"` // the upper bound for "between"
}

// QueryRecords runs a dynamic query for this entity and returns up to limit
// records (capped at 400) as JSON-serializable values. See the package comment
// above for the strict key rules it enforces.
func (r *Repo[T, E]) QueryRecords(preds []QueryPredicate, limit int32) ([]any, error) {
	if limit <= 0 || limit > queryRecordsMaxLimit {
		limit = queryRecordsMaxLimit
	}

	q := r.Query()
	for _, p := range preds {
		o, err := parseOp(p.Op)
		if err != nil {
			return nil, err
		}
		acc, ok := r.meta.accessors[p.Field]
		if !ok {
			return nil, fmt.Errorf("db: %s has no field %q", r.meta.recordType.Name(), p.Field)
		}
		v1, err := coerceValue(acc.kind, p.Value, p.Field)
		if err != nil {
			return nil, err
		}
		var v2 any
		if o == opBetween {
			if v2, err = coerceValue(acc.kind, p.Value2, p.Field); err != nil {
				return nil, err
			}
		}
		q.preds = append(q.preds, predicate{field: p.Field, op: o, v1: v1, v2: v2})
	}

	// Plan up front so we can reject anything not served by an index before we
	// ever hit DynamoDB. A missing partition/index surfaces here as an error.
	plan, err := q.plan()
	if err != nil {
		return nil, err
	}
	if len(plan.postFilter) > 0 {
		fields := make([]string, 0, len(plan.postFilter))
		for _, p := range plan.postFilter {
			fields = append(fields, p.field)
		}
		return nil, fmt.Errorf("db: cannot query %s by %v: only the partition key, a full GSI key (equality), "+
			"or the sort key (equality/range) are queryable — a range on a hash/partition column or a filter on a "+
			"non-indexed field is not supported", r.meta.recordType.Name(), fields)
	}

	q.Limit(limit)
	var out []E
	if err := q.Exec(&out); err != nil {
		return nil, err
	}
	res := make([]any, len(out))
	for i := range out {
		res[i] = out[i]
	}
	return res, nil
}

// parseOp maps a wire operator token to an internal op. Both symbol ("=", ">=")
// and word ("eq", "gte", "between", "begins_with") forms are accepted.
func parseOp(s string) (op, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "=", "==", "eq":
		return opEq, nil
	case ">", "gt":
		return opGt, nil
	case ">=", "gte":
		return opGte, nil
	case "<", "lt":
		return opLt, nil
	case "<=", "lte":
		return opLte, nil
	case "between":
		return opBetween, nil
	case "begins_with", "beginswith", "prefix":
		return opBeginsWith, nil
	default:
		return 0, fmt.Errorf("db: unknown query operator %q", s)
	}
}

// coerceValue converts a decoded-JSON value into the Go type the column's kind
// expects, so downstream key encoding and comparisons see a well-typed operand.
func coerceValue(kind valueKind, v any, field string) (any, error) {
	if v == nil {
		return nil, fmt.Errorf("db: missing value for field %q", field)
	}
	switch kind {
	case kindString:
		if s, ok := v.(string); ok {
			return s, nil
		}
		return fmt.Sprintf("%v", v), nil
	case kindInt, kindUint:
		switch t := v.(type) {
		case float64:
			return int64(t), nil
		case int64:
			return t, nil
		case int:
			return int64(t), nil
		case string:
			n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("db: field %q expects an integer, got %q", field, t)
			}
			return n, nil
		default:
			return nil, fmt.Errorf("db: field %q expects an integer, got %T", field, v)
		}
	case kindFloat:
		switch t := v.(type) {
		case float64:
			return t, nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
			if err != nil {
				return nil, fmt.Errorf("db: field %q expects a number, got %q", field, t)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("db: field %q expects a number, got %T", field, v)
		}
	case kindBool:
		switch t := v.(type) {
		case bool:
			return t, nil
		case string:
			b, err := strconv.ParseBool(strings.TrimSpace(t))
			if err != nil {
				return nil, fmt.Errorf("db: field %q expects a boolean, got %q", field, t)
			}
			return b, nil
		default:
			return nil, fmt.Errorf("db: field %q expects a boolean, got %T", field, v)
		}
	default:
		return nil, fmt.Errorf("db: field %q has a non-scalar type and cannot be queried", field)
	}
}

// QueryBuilder accumulates predicates for one entity. E is the record type.
type QueryBuilder[E any] struct {
	meta    *tableMeta
	preds   []predicate
	limit   int32
	desc    bool
	planErr error
}

func (q *QueryBuilder[E]) add(c Coln, o op, v1, v2 any) *QueryBuilder[E] {
	q.preds = append(q.preds, predicate{field: c.col().fieldName, op: o, v1: v1, v2: v2})
	return q
}

func (q *QueryBuilder[E]) Eq(c Coln, v any) *QueryBuilder[E]  { return q.add(c, opEq, v, nil) }
func (q *QueryBuilder[E]) Gt(c Coln, v any) *QueryBuilder[E]  { return q.add(c, opGt, v, nil) }
func (q *QueryBuilder[E]) Gte(c Coln, v any) *QueryBuilder[E] { return q.add(c, opGte, v, nil) }
func (q *QueryBuilder[E]) Lt(c Coln, v any) *QueryBuilder[E]  { return q.add(c, opLt, v, nil) }
func (q *QueryBuilder[E]) Lte(c Coln, v any) *QueryBuilder[E] { return q.add(c, opLte, v, nil) }
func (q *QueryBuilder[E]) Between(c Coln, a, b any) *QueryBuilder[E] {
	return q.add(c, opBetween, a, b)
}
func (q *QueryBuilder[E]) BeginsWith(c Coln, prefix string) *QueryBuilder[E] {
	return q.add(c, opBeginsWith, prefix, nil)
}

// Limit caps the number of returned items.
func (q *QueryBuilder[E]) Limit(n int32) *QueryBuilder[E] { q.limit = n; return q }

// Desc returns items in descending sort-key order.
func (q *QueryBuilder[E]) Desc() *QueryBuilder[E] { q.desc = true; return q }

// queryPlan is the resolved DynamoDB query input pieces.
type queryPlan struct {
	indexName  string // empty => base table
	keyCond    string
	names      map[string]string
	values     map[string]types.AttributeValue
	postFilter []predicate // evaluated in memory against the decoded record
}

// Exec runs the query and appends results into *dst.
func (q *QueryBuilder[E]) Exec(dst *[]E) error {
	plan, err := q.plan()
	if err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}

	input := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName()),
		KeyConditionExpression:    aws.String(plan.keyCond),
		ExpressionAttributeNames:  plan.names,
		ExpressionAttributeValues: plan.values,
		ScanIndexForward:          aws.Bool(!q.desc),
	}
	if plan.indexName != "" {
		input.IndexName = aws.String(plan.indexName)
	}
	// With an in-memory post-filter, page sizes no longer map 1:1 to results, so
	// only push Limit to DynamoDB when there is nothing to filter out.
	if q.limit > 0 && len(plan.postFilter) == 0 {
		input.Limit = aws.Int32(q.limit)
	}

	for {
		out, err := client.Query(context.Background(), input)
		if err != nil {
			return err
		}
		for _, item := range out.Items {
			var record E
			if err := q.meta.unmarshalItem(item, &record); err != nil {
				return err
			}
			if !q.meta.matchesFilter(unsafe.Pointer(&record), plan.postFilter) {
				continue
			}
			*dst = append(*dst, record)
			if q.limit > 0 && int32(len(*dst)) >= q.limit {
				return nil
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			return nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// First runs the query with limit 1 and unmarshals the single result, if any.
func (q *QueryBuilder[E]) First(dst *E) (bool, error) {
	var out []E
	if err := q.Limit(1).Exec(&out); err != nil {
		return false, err
	}
	if len(out) == 0 {
		return false, nil
	}
	*dst = out[0]
	return true, nil
}

// plan resolves predicates into a queryPlan.
func (q *QueryBuilder[E]) plan() (*queryPlan, error) {
	if q.planErr != nil {
		return nil, q.planErr
	}
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}

	plan := &queryPlan{
		names:  map[string]string{},
		values: map[string]types.AttributeValue{},
	}

	// 1. Choose the partition source.
	usedPartitionFields, err := q.resolvePartition(byField, plan)
	if err != nil {
		return nil, err
	}

	// 2. Build the shared sk condition from sort-column predicates.
	usedSortFields, err := q.resolveSort(byField, plan)
	if err != nil {
		return nil, err
	}

	// 3. Remaining predicates are evaluated in memory after decode.
	for _, p := range q.preds {
		if usedPartitionFields[p.field] || usedSortFields[p.field] {
			continue
		}
		if _, ok := q.meta.accessors[p.field]; !ok {
			continue
		}
		plan.postFilter = append(plan.postFilter, p)
	}

	return plan, nil
}

// resolvePartition picks the base table or a GSI slot and appends its key
// condition; it returns the set of fields it consumed.
func (q *QueryBuilder[E]) resolvePartition(byField map[string]predicate, plan *queryPlan) (map[string]bool, error) {
	m := q.meta
	used := map[string]bool{}

	// Base table: every partition column must have an equality predicate.
	baseOK := true
	for _, kc := range m.partition {
		if p, ok := byField[kc.fieldName]; !ok || p.op != opEq {
			baseOK = false
			break
		}
	}
	if baseOK {
		parts := []keyPart{stringPart(m.entity)}
		for _, kc := range m.partition {
			parts = append(parts, keyPartFromValue(kc, byField[kc.fieldName].v1))
			used[kc.fieldName] = true
		}
		plan.names["#pk"] = "pk"
		plan.values[":pk"] = &types.AttributeValueMemberS{Value: buildCompositeKey(parts)}
		plan.keyCond = "#pk = :pk"
		return used, nil
	}

	// Otherwise try each GSI slot whose key columns all have equality predicates.
	for _, idx := range m.indexes {
		ok := true
		for _, kc := range idx.keys {
			if p, has := byField[kc.fieldName]; !has || p.op != opEq {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		plan.indexName = idx.slot.index
		plan.names["#pk"] = idx.slot.attr
		if idx.slot.isNumber {
			kc := idx.keys[0]
			plan.values[":pk"] = &types.AttributeValueMemberN{
				Value: strconv.FormatInt(valueToInt64(byField[kc.fieldName].v1, kc.fieldName), 10),
			}
			used[kc.fieldName] = true
		} else {
			parts := []keyPart{stringPart(m.entity)}
			for _, kc := range idx.keys {
				parts = append(parts, keyPartFromValue(kc, byField[kc.fieldName].v1))
				used[kc.fieldName] = true
			}
			plan.values[":pk"] = &types.AttributeValueMemberS{Value: buildCompositeKey(parts)}
		}
		plan.keyCond = "#pk = :pk"
		return used, nil
	}

	return nil, fmt.Errorf("db: query on %s has no usable partition: give an equality on the Partition column(s) or on a full index key", m.recordType.Name())
}

// resolveSort builds the sk key condition from predicates on the sort columns.
func (q *QueryBuilder[E]) resolveSort(byField map[string]predicate, plan *queryPlan) (map[string]bool, error) {
	m := q.meta
	used := map[string]bool{}

	// Longest leading run of sort columns constrained by equality.
	prefix := []keyPart{}
	i := 0
	for ; i < len(m.sort); i++ {
		kc := m.sort[i]
		p, ok := byField[kc.fieldName]
		if !ok || p.op != opEq {
			break
		}
		prefix = append(prefix, keyPartFromValue(kc, p.v1))
		used[kc.fieldName] = true
	}

	// No sort predicates at all → whole partition.
	if i == 0 && (i >= len(m.sort) || byFieldHasNoSortPred(byField, m.sort)) {
		if len(prefix) == 0 {
			return used, nil
		}
	}

	// Case A: all sort columns pinned by equality → sk = <exact>.
	if i == len(m.sort) {
		plan.names["#sk"] = "sk"
		plan.values[":sk"] = &types.AttributeValueMemberS{Value: buildCompositeKey(prefix)}
		plan.keyCond += " AND #sk = :sk"
		return used, nil
	}

	// Case B: a range / begins_with on the next sort column.
	next := m.sort[i]
	if p, ok := byField[next.fieldName]; ok {
		plan.names["#sk"] = "sk"
		used[next.fieldName] = true
		switch p.op {
		case opBetween:
			lo := buildCompositeKey(append(clone(prefix), keyPartFromValue(next, p.v1)))
			hi := buildCompositeKey(append(clone(prefix), keyPartFromValue(next, p.v2)))
			plan.values[":lo"] = &types.AttributeValueMemberS{Value: lo}
			plan.values[":hi"] = &types.AttributeValueMemberS{Value: hi}
			plan.keyCond += " AND #sk BETWEEN :lo AND :hi"
		case opGt, opGte:
			v := buildCompositeKey(append(clone(prefix), keyPartFromValue(next, p.v1)))
			plan.values[":sk"] = &types.AttributeValueMemberS{Value: v}
			cmp := ">="
			if p.op == opGt && i == len(m.sort)-1 {
				cmp = ">"
			}
			plan.keyCond += " AND #sk " + cmp + " :sk"
		case opLt, opLte:
			v := buildCompositeKey(append(clone(prefix), keyPartFromValue(next, p.v1)))
			plan.values[":sk"] = &types.AttributeValueMemberS{Value: v}
			cmp := "<="
			if p.op == opLt && i == len(m.sort)-1 {
				cmp = "<"
			}
			plan.keyCond += " AND #sk " + cmp + " :sk"
		case opBeginsWith:
			v := buildCompositeKey(append(clone(prefix), stringPart(fmt.Sprintf("%v", p.v1))))
			plan.values[":sk"] = &types.AttributeValueMemberS{Value: v}
			plan.keyCond += " AND begins_with(#sk, :sk)"
		default:
			return nil, fmt.Errorf("db: unsupported operator on sort column %q", next.fieldName)
		}
		return used, nil
	}

	// Case C: only a leading equality prefix, nothing on the next column →
	// begins_with on the prefix boundary.
	if len(prefix) > 0 {
		plan.names["#sk"] = "sk"
		plan.values[":sk"] = &types.AttributeValueMemberS{Value: buildCompositeKey(prefix) + keySeparator}
		plan.keyCond += " AND begins_with(#sk, :sk)"
	}
	return used, nil
}

// matchesFilter evaluates the in-memory post-filter predicates against a decoded
// record via its precompiled accessors. All predicates must pass (AND semantics).
func (m *tableMeta) matchesFilter(ptr unsafe.Pointer, preds []predicate) bool {
	for _, p := range preds {
		acc, ok := m.accessors[p.field]
		if !ok {
			continue
		}
		if !evalPredicate(acc, ptr, p) {
			return false
		}
	}
	return true
}

func evalPredicate(acc *colAccessor, ptr unsafe.Pointer, p predicate) bool {
	switch acc.kind {
	case kindString:
		s := acc.getStr(ptr)
		want, _ := p.v1.(string)
		switch p.op {
		case opEq:
			return s == want
		case opGt:
			return s > want
		case opGte:
			return s >= want
		case opLt:
			return s < want
		case opLte:
			return s <= want
		case opBetween:
			hi, _ := p.v2.(string)
			return s >= want && s <= hi
		case opBeginsWith:
			return strings.HasPrefix(s, want)
		}
	case kindBool:
		if p.op == opEq {
			want, _ := p.v1.(bool)
			return acc.getBool(ptr) == want
		}
	default:
		if acc.getF64 == nil {
			return false // non-scalar field: not comparable
		}
		a := acc.getF64(ptr)
		b, okB := asFloat(p.v1)
		if !okB {
			return false
		}
		switch p.op {
		case opEq:
			return a == b
		case opGt:
			return a > b
		case opGte:
			return a >= b
		case opLt:
			return a < b
		case opLte:
			return a <= b
		case opBetween:
			c, okC := asFloat(p.v2)
			return okC && a >= b && a <= c
		}
	}
	return false
}

func asFloat(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	default:
		return 0, false
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func clone(parts []keyPart) []keyPart { return append([]keyPart(nil), parts...) }

func byFieldHasNoSortPred(byField map[string]predicate, sort []keyCol) bool {
	for _, kc := range sort {
		if _, ok := byField[kc.fieldName]; ok {
			return false
		}
	}
	return true
}

// keyPartFromValue turns a predicate value into a key part for the given column.
func keyPartFromValue(kc keyCol, v any) keyPart {
	switch kc.kind {
	case kindString:
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprintf("%v", v)
		}
		return stringPart(s)
	case kindInt, kindUint:
		return numberPart(uint64(valueToInt64(v, kc.fieldName)), kc.base)
	default:
		panic(fmt.Sprintf("db: cannot build key from column %q value", kc.fieldName))
	}
}

// valueToInt64 coerces a numeric interface value to int64 (>= 0 enforced for keys).
func valueToInt64(v any, field string) int64 {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := rv.Int()
		if i < 0 {
			panic(fmt.Sprintf("db: key value for %q is negative (%d)", field, i))
		}
		return i
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint())
	default:
		panic(fmt.Sprintf("db: key value for %q must be an integer, got %T", field, v))
	}
}
