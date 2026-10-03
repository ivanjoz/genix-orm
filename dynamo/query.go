package dynamo

import (
	"context"
	"fmt"
	"maps"
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
//   - Every access path is a hash (a number: TableID ‖ partition columns, each
//     needing "=") and a sorted range (a composite string): the base table is
//     pk + sk(Keys); a GSI is hN + rN(its Keys, then the base Keys left out).
//   - On the range, an "=" on a leading run of its columns and then one range
//     (=, begins_with, range, between) become the key condition. The usable path
//     serving the most predicates wins; on a tie, the base table, then the GSIs
//     in declared order.
//   - Any remaining predicates are on fields that live inside the "d" blob (or on
//     key columns in a shape the key condition cannot express), which DynamoDB
//     cannot see. Query rejects them; QueryScan evaluates them in memory after
//     each row decodes, under a read budget.
// ─────────────────────────────────────────────────────────────────────────────

// queryScanMaxReadUnits is what a QueryScan may read before it fails: 5 MB of
// eventually consistent reads (0.5 RCU per 4 KB), about five 1 MB Query pages.
// It is measured in read units, not pages, because a keys-only Contains also
// reads its base records in a BatchGetItem.
const queryScanMaxReadUnits = 5 * 1024 / 4 * 0.5

type op int8

const (
	opEq op = iota
	opGt
	opGte
	opLt
	opLte
	opBetween
	opBeginsWith
	opContains // v1 holds the []any of values; served by a fan-out Index
	opIn       // v1 holds the []any of values; in-memory only (Delta's sync filter)
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
//   - The predicate set MUST resolve to an access path: an equality on every
//     Partition column of the base table or of one GSI. Otherwise it errors ("no
//     usable partition").
//   - Only a range key supports ranges (>, >=, <, <=, between, begins_with). A
//     range on a hash column or a filter on a non-indexed field is rejected: such
//     predicates would fall to the in-memory post-filter, and QueryRecords
//     refuses to run a query that isn't fully served by an index.
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
// records (capped at 400) as JSON-serializable values, in descending sort-key
// order when desc is set (newest first on an autoincrement ID). See the package
// comment above for the strict key rules it enforces.
func (r *Repo[T, E]) QueryRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error) {
	return r.queryRecords(r.Query(), preds, limit, desc)
}

// QueryScanRecords is QueryRecords over QueryScan: a partition or full GSI key
// must still serve the read, and the predicates no key serves are filtered in
// memory after it, within QueryScan's 5 MB read budget.
func (r *Repo[T, E]) QueryScanRecords(preds []QueryPredicate, limit int32, desc bool) ([]any, error) {
	return r.queryRecords(r.QueryScan(), preds, limit, desc)
}

func (r *Repo[T, E]) queryRecords(q *QueryBuilder[E], preds []QueryPredicate, limit int32, desc bool) ([]any, error) {
	if limit <= 0 || limit > queryRecordsMaxLimit {
		limit = queryRecordsMaxLimit
	}

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

	// Exec plans before any DynamoDB call and rejects a missing partition/index; a
	// strict Query also rejects a predicate no index serves.
	q.Limit(limit)
	if desc {
		q.Desc()
	}
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
	// allowsMemoryFilter is set by Repo.QueryScan: predicates no key serves are
	// filtered in memory instead of rejected.
	allowsMemoryFilter bool
	// Set by Delta (delta.go): the TypeDelta index it reads, and the first-sync
	// filter on its sync filter column.
	deltaIndex *arrayIndexMeta
	syncFilter *predicate
	// consistentRead is set by Consistent.
	consistentRead bool
}

func (q *QueryBuilder[E]) add(c Coln, o op, v1, v2 any) *QueryBuilder[E] {
	q.preds = append(q.preds, predicate{field: c.col().fieldName, op: o, v1: v1, v2: v2})
	return q
}

// Eq on a ColSlice is Contains with that one value: records whose slice holds v.
func (q *QueryBuilder[E]) Eq(c Coln, v any) *QueryBuilder[E] {
	if _, isSlice := c.(SliceColn); isSlice {
		return q.add(c, opContains, []any{v}, nil)
	}
	return q.add(c, opEq, v, nil)
}
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

// Contains matches records whose slice field holds ANY of the values. The field
// must be in a fan-out Index, and the query needs an equality on every Partition
// column and on every index Keys column before the slice. It runs one Query per
// value; a record matching several is returned once, and results come value by
// value (each in row sk order).
func (q *QueryBuilder[E]) Contains(c SliceColn, values ...any) *QueryBuilder[E] {
	return q.add(c, opContains, values, nil)
}

// Limit caps the number of returned items.
func (q *QueryBuilder[E]) Limit(n int32) *QueryBuilder[E] { q.limit = n; return q }

// Desc returns items in descending sort-key order.
func (q *QueryBuilder[E]) Desc() *QueryBuilder[E] { q.desc = true; return q }

// Consistent reads with strong consistency: the query sees every write that
// finished before it, at twice the read units. Only base-table reads can (a
// partition or Keys query, Contains, Delta); a query planned on a GSI fails.
// Read inside a Modify change (modify.go), it is what keeps derived data fresh.
func (q *QueryBuilder[E]) Consistent() *QueryBuilder[E] { q.consistentRead = true; return q }

// queryPlan is the resolved DynamoDB query input pieces.
type queryPlan struct {
	indexName  string // empty => base table
	keyCond    string
	names      map[string]string
	values     map[string]types.AttributeValue
	postFilter []predicate // predicates no key serves: QueryScan only, evaluated in memory
	// keyFilter holds key predicates the key condition can only approximate (a
	// strict < under an equality prefix), checked in memory so the key read stays
	// exact. They are not user filters, so a strict Query allows them.
	keyFilter []predicate

	// Set when the plan serves one value of a Contains: it queries the fan-out
	// rows of arrayIndex for that element, under the base pk basePK.
	arrayIndex *arrayIndexMeta
	basePK     string
}

// Exec runs the query and appends results into *dst.
func (q *QueryBuilder[E]) Exec(dst *[]E) error {
	plans, err := q.plans()
	if err != nil {
		return err
	}
	client, err := Client()
	if err != nil {
		return err
	}

	// A record holding several of the Contains values is returned once.
	returnedSKs := map[string]bool{}
	// A QueryScan counts what it reads (every page of every plan, and the base
	// records of a keys-only Contains) against queryScanMaxReadUnits.
	var readUnits float64
	for _, plan := range plans {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(tableName()),
			KeyConditionExpression:    aws.String(plan.keyCond),
			ExpressionAttributeNames:  plan.names,
			ExpressionAttributeValues: plan.values,
			ScanIndexForward:          aws.Bool(!q.desc),
			ConsistentRead:            aws.Bool(q.consistentRead),
		}
		if plan.indexName != "" {
			if q.consistentRead {
				return fmt.Errorf("db: %s Consistent() query is planned on %s, and a GSI cannot read consistently", q.meta.recordType.Name(), plan.indexName)
			}
			input.IndexName = aws.String(plan.indexName)
		}
		if q.allowsMemoryFilter {
			input.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal
		}
		// With an in-memory post-filter, page sizes no longer map 1:1 to results, so
		// only push Limit to DynamoDB when there is nothing to filter out.
		if q.limit > 0 && len(plan.postFilter) == 0 {
			input.Limit = aws.Int32(q.limit)
		}

		for {
			// Checked before every call (the next page, or the next Contains value):
			// only a read that would go on past the budget fails; one that finished
			// within a page of it returns what it found.
			if q.allowsMemoryFilter && readUnits > queryScanMaxReadUnits {
				return fmt.Errorf("db: %s QueryScan read more than 5 MB (%.1f RCU) without finishing: narrow its index predicates",
					q.meta.recordType.Name(), readUnits)
			}
			out, err := client.Query(context.Background(), input)
			if err != nil {
				return err
			}
			if out.ConsumedCapacity != nil {
				readUnits += aws.ToFloat64(out.ConsumedCapacity.CapacityUnits)
			}
			items, rowSKs := out.Items, []string(nil)
			if plan.arrayIndex != nil {
				var baseReadUnits float64
				if items, rowSKs, baseReadUnits, err = recordItemsOfArrayRows(client, plan.basePK, plan.arrayIndex, out.Items, q.consistentRead); err != nil {
					return err
				}
				readUnits += baseReadUnits
			}
			for i, item := range items {
				var record E
				if err := q.meta.unmarshalItem(item, &record); err != nil {
					return err
				}
				ptr := unsafe.Pointer(&record)
				if plan.arrayIndex != nil {
					recordSK := q.meta.skValue(ptr)
					// A row left behind by a crash or a concurrent writer points at a
					// record that would no longer write it: it is not a result.
					if returnedSKs[recordSK] || !q.meta.writesArrayRow(plan.arrayIndex, ptr, rowSKs[i]) {
						continue
					}
					returnedSKs[recordSK] = true
				}
				if !q.meta.matchesFilter(ptr, plan.keyFilter) || !q.meta.matchesFilter(ptr, plan.postFilter) {
					continue
				}
				*dst = append(*dst, record)
				if q.limit > 0 && int32(len(*dst)) >= q.limit {
					return nil
				}
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			input.ExclusiveStartKey = out.LastEvaluatedKey
		}
	}
	return nil
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

// plans resolves the predicates into one queryPlan, or into one per value when
// the query has a Contains (each value is its own begins_with on the array rows).
func (q *QueryBuilder[E]) plans() ([]*queryPlan, error) {
	if q.planErr != nil {
		return nil, q.planErr
	}
	var containsPredicate *predicate
	for i, p := range q.preds {
		if p.op != opContains {
			continue
		}
		if containsPredicate != nil {
			return nil, fmt.Errorf("db: a query on %s takes one Contains", q.meta.recordType.Name())
		}
		containsPredicate = &q.preds[i]
	}
	if containsPredicate == nil {
		// A Delta read without a Contains reads its delta index's single rows.
		plan, err := q.plan(q.deltaIndex, nil)
		if err != nil {
			return nil, err
		}
		return []*queryPlan{plan}, nil
	}

	var arrayIndex *arrayIndexMeta
	for i := range q.meta.arrayIndexes {
		if q.meta.arrayIndexes[i].elementPosition >= 0 && q.meta.arrayIndexes[i].element.fieldName == containsPredicate.field {
			arrayIndex = &q.meta.arrayIndexes[i]
		}
	}
	if arrayIndex == nil {
		return nil, fmt.Errorf("db: Contains on %s.%s needs an Index whose Keys hold that ColSlice",
			q.meta.recordType.Name(), containsPredicate.field)
	}
	if q.deltaIndex != nil && q.deltaIndex != arrayIndex {
		return nil, fmt.Errorf("db: %s Delta() with a Contains on %s needs a TypeDelta index holding that ColSlice",
			q.meta.recordType.Name(), containsPredicate.field)
	}
	values := containsPredicate.v1.([]any)
	plans := make([]*queryPlan, 0, len(values))
	for _, value := range values {
		plan, err := q.plan(arrayIndex, value)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// plan resolves predicates into a queryPlan. With an arrayIndex it targets that
// index's rows for one element instead of the base table or a GSI.
func (q *QueryBuilder[E]) plan(arrayIndex *arrayIndexMeta, element any) (*queryPlan, error) {
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}

	// 1. The key condition: the fan-out rows of a Contains or a Delta, else the
	// access path (base table or GSI) that serves the most predicates.
	var plan *queryPlan
	var usedFields map[string]bool
	var err error
	if arrayIndex != nil {
		plan, usedFields, err = q.planArrayRows(byField, arrayIndex, element)
	} else {
		plan, usedFields, err = q.planBestAccessPath(byField)
	}
	if err != nil {
		return nil, err
	}
	// Delta's first-sync filter is not a user filter, so even a strict Query takes it.
	if q.syncFilter != nil {
		plan.keyFilter = append(plan.keyFilter, *q.syncFilter)
	}

	// 2. Remaining predicates no key serves: a QueryScan evaluates them in memory
	// after decode, a strict Query rejects them.
	var unindexedFields []string
	for _, p := range q.preds {
		if usedFields[p.field] {
			continue
		}
		if _, ok := q.meta.accessors[p.field]; !ok {
			continue
		}
		plan.postFilter = append(plan.postFilter, p)
		unindexedFields = append(unindexedFields, p.field)
	}
	if len(unindexedFields) > 0 && !q.allowsMemoryFilter {
		return nil, fmt.Errorf("db: %s Query() cannot filter %s through an index: use QueryScan() to filter it in memory after the index read",
			q.meta.recordType.Name(), strings.Join(unindexedFields, ", "))
	}

	return plan, nil
}

// pinPartition reads the Eq values of every partition column, and reports false
// when one lacks an Eq. It marks the columns it consumed in used.
func pinPartition(byField map[string]predicate, partition []keyCol, used map[string]bool) ([]uint64, bool) {
	values := make([]uint64, len(partition))
	for i, kc := range partition {
		p, ok := byField[kc.fieldName]
		if !ok || p.op != opEq {
			return nil, false
		}
		values[i] = uint64(valueToInt64(p.v1, kc.fieldName))
		used[kc.fieldName] = true
	}
	return values, true
}

// planArrayRows targets the fan-out (or delta) rows of arrayIndex: they live
// under the base pk ‖ the index cb id, and their sk is the index Keys then the
// base Keys. A Contains element pins the slice column like an Eq.
func (q *QueryBuilder[E]) planArrayRows(byField map[string]predicate, arrayIndex *arrayIndexMeta, element any) (*queryPlan, map[string]bool, error) {
	m := q.meta
	used := map[string]bool{}
	partitionValues, pinned := pinPartition(byField, m.partition, used)
	if !pinned {
		return nil, nil, fmt.Errorf("db: Contains or Delta on %s needs an equality on every Partition column", m.recordType.Name())
	}
	plan := &queryPlan{names: map[string]string{"#pk": "pk"}, values: map[string]types.AttributeValue{}, keyCond: "#pk = :pk", arrayIndex: arrayIndex}
	plan.basePK = m.numericKey(partitionValues, m.partition)
	plan.values[":pk"] = &types.AttributeValueMemberN{Value: plan.basePK + arrayIndex.columnID}

	sortColumns := append(append([]keyCol(nil), arrayIndex.keys...), m.keys...)
	hasElement := arrayIndex.elementPosition >= 0
	if hasElement {
		byField[arrayIndex.element.fieldName] = predicate{field: arrayIndex.element.fieldName, op: opEq, v1: element}
	}
	usedKeys, err := resolveKeys(byField, plan, sortColumns, "sk")
	if err != nil {
		return nil, nil, err
	}
	if hasElement && !usedKeys[arrayIndex.element.fieldName] {
		return nil, nil, fmt.Errorf("db: Contains on %s.%s needs an Eq on every index Keys column before it",
			m.recordType.Name(), arrayIndex.element.fieldName)
	}
	maps.Copy(used, usedKeys)
	return plan, used, nil
}

// planBestAccessPath tries the base table, then every GSI in declared order: a
// path is usable when its partition columns all have an Eq, and the one whose
// key condition serves the most predicates wins (the earlier on a tie, so the
// base table before any GSI).
func (q *QueryBuilder[E]) planBestAccessPath(byField map[string]predicate) (*queryPlan, map[string]bool, error) {
	m := q.meta
	type accessPath struct {
		gsi         *indexMeta // nil: the base table
		partition   []keyCol
		sortColumns []keyCol
	}
	paths := []accessPath{{partition: m.partition, sortColumns: m.keys}}
	for i := range m.indexes {
		paths = append(paths, accessPath{gsi: &m.indexes[i], partition: m.indexes[i].partition, sortColumns: m.indexes[i].sortColumns})
	}

	var bestPlan *queryPlan
	var bestUsed map[string]bool
	var keysErr error
	for _, path := range paths {
		used := map[string]bool{}
		partitionValues, pinned := pinPartition(byField, path.partition, used)
		if !pinned {
			continue
		}
		hashAttr, rangeAttr := "pk", "sk"
		plan := &queryPlan{names: map[string]string{}, values: map[string]types.AttributeValue{}, keyCond: "#pk = :pk"}
		if path.gsi != nil {
			hashAttr, rangeAttr = path.gsi.slot.hashAttr, path.gsi.slot.rangeAttr
			plan.indexName = path.gsi.slot.index
		}
		plan.names["#pk"] = hashAttr
		plan.values[":pk"] = &types.AttributeValueMemberN{Value: m.numericKey(partitionValues, path.partition)}
		usedKeys, err := resolveKeys(byField, plan, path.sortColumns, rangeAttr)
		if err != nil {
			keysErr = err
			continue
		}
		maps.Copy(used, usedKeys)
		if bestPlan == nil || len(used) > len(bestUsed) {
			bestPlan, bestUsed = plan, used
		}
	}
	if bestPlan != nil {
		return bestPlan, bestUsed, nil
	}
	if keysErr != nil {
		return nil, nil, keysErr
	}
	return nil, nil, fmt.Errorf("db: query on %s has no usable partition: give an equality on the Partition column(s) of the table or of an index", m.recordType.Name())
}

// resolveKeys builds the range key condition (on rangeAttr: "sk", or a GSI's rN)
// from predicates on sortColumns, the columns that range is composed of: the
// Keys, a GSI's sort columns, a fan-out row's index Keys + Keys, or a GroupBy
// counter's tag + group Keys (group_by.go).
func resolveKeys(byField map[string]predicate, plan *queryPlan, sortColumns []keyCol, rangeAttr string) (map[string]bool, error) {
	used := map[string]bool{}

	// Longest leading run of sort columns constrained by equality.
	var prefix []keyPart
	i := 0
	for ; i < len(sortColumns); i++ {
		kc := sortColumns[i]
		p, ok := byField[kc.fieldName]
		if !ok || p.op != opEq {
			break
		}
		prefix = append(prefix, keyPartFromValue(kc, p.v1))
		used[kc.fieldName] = true
	}

	// No sort predicates → whole partition.
	if len(prefix) == 0 && byFieldHasNoKeysPred(byField, sortColumns) {
		return used, nil
	}

	// Case A: all sort columns pinned by equality → sk = <exact>.
	if i == len(sortColumns) {
		plan.names["#sk"] = rangeAttr
		plan.values[":sk"] = &types.AttributeValueMemberS{Value: buildCompositeKey(prefix)}
		plan.keyCond += " AND #sk = :sk"
		return used, nil
	}

	// Case B: a range / begins_with on the next sort column.
	next := sortColumns[i]
	if p, ok := byField[next.fieldName]; ok {
		plan.names["#sk"] = rangeAttr
		used[next.fieldName] = true
		// Every sk whose next column equals v sorts in [v, v$): v itself when that column is the
		// last one, v#<suffix> otherwise. So "<= v" is "< v$" and "> v" is ">= v$", which keeps
		// every bound exact at any position of a composite sort key.
		boundOf := func(value any) string {
			return buildCompositeKey(append(clone(prefix), keyPartFromValue(next, value)))
		}
		switch p.op {
		case opBetween:
			plan.values[":lo"] = &types.AttributeValueMemberS{Value: boundOf(p.v1)}
			plan.values[":hi"] = &types.AttributeValueMemberS{Value: boundOf(p.v2) + keySeparatorSuccessor}
			plan.keyCond += " AND #sk BETWEEN :lo AND :hi"
		case opGt, opGte, opLt, opLte:
			v := boundOf(p.v1)
			if p.op == opGt || p.op == opLte {
				v += keySeparatorSuccessor
			}
			isLowerBound := p.op == opGt || p.op == opGte
			if len(prefix) == 0 {
				cmp := "<"
				if isLowerBound {
					cmp = ">="
				}
				plan.values[":sk"] = &types.AttributeValueMemberS{Value: v}
				plan.keyCond += " AND #sk " + cmp + " :sk"
				break
			}
			// With an equality prefix a one-sided range must stay inside it, or it runs into the
			// rows of the next prefix value: every sk under the prefix sorts in [prefix#, prefix$).
			lo, hi := v, buildCompositeKey(prefix)+keySeparatorSuccessor
			if !isLowerBound {
				lo, hi = buildCompositeKey(prefix)+keySeparator, v
			}
			plan.values[":lo"] = &types.AttributeValueMemberS{Value: lo}
			plan.values[":hi"] = &types.AttributeValueMemberS{Value: hi}
			plan.keyCond += " AND #sk BETWEEN :lo AND :hi"
			// BETWEEN includes its upper bound, which for "< v" can be a real key: the
			// key filter drops it, so even a strict Query takes this range.
			if p.op == opLt {
				plan.keyFilter = append(plan.keyFilter, p)
			}
		case opBeginsWith:
			v := buildCompositeKey(append(clone(prefix), stringPart(fmt.Sprintf("%v", p.v1))))
			plan.values[":sk"] = &types.AttributeValueMemberS{Value: v}
			plan.keyCond += " AND begins_with(#sk, :sk)"
		default:
			return nil, fmt.Errorf("db: unsupported operator on Keys column %q", next.fieldName)
		}
		return used, nil
	}

	// Case C: only a leading equality prefix, nothing on the next column →
	// begins_with on the prefix boundary.
	if len(prefix) > 0 {
		plan.names["#sk"] = rangeAttr
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
	if p.op == opIn {
		for _, value := range p.v1.([]any) {
			if evalPredicate(acc, ptr, predicate{field: p.field, op: opEq, v1: value}) {
				return true
			}
		}
		return false
	}
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

func byFieldHasNoKeysPred(byField map[string]predicate, sort []keyCol) bool {
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
		return numberPart(uint64(valueToInt64(v, kc.fieldName)), kc.bits)
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
