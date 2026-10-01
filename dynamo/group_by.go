package dynamo

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ivanjoz/colbin"
)

// ─────────────────────────────────────────────────────────────────────────────
// GroupBy counters: an Index declaring GroupBy keeps, per base partition and per
// distinct value of its Keys (the group), one counter item with the number of
// records in the group and the sum of each GroupBy column:
//
//	pk   = base pk ‖ 000                      (the bookkeeping pk of the slot versions, sk "v")
//	sk   = g<cb ids of the Keys>#<group key>  e.g. g005.006#web#<Status b64>
//	c    = record count                       (native number)
//	sNNN = sum of the column with cb id NNN   (native number; floats as round(v * 1e6))
//	d    = colbin blob of a record holding only the group Keys, for Group.Key
//	upv, upd = the last write that touched it (GroupDelta only)
//
// A record with Status 0 is deleted and counts in no group. On a fan-out Index each
// distinct element is a group, and the record's whole values count in each one.
//
// Write path: every write already reads the stored version of its records; the
// group sets of the stored and the written version are diffed into signed deltas
// (groupCounterDeltas), merged per counter over the whole call, and ADDed after
// the base items land, one UpdateItem per counter. It is best-effort: two plain
// Puts racing on one record, a crash between the base write and the ADD, or a
// retried ADD drift a counter, and RebuildGroups recomputes them from the records.
// A counter that reaches 0 is kept, so a GroupDelta client sees its group empty out.
// ─────────────────────────────────────────────────────────────────────────────

const (
	groupCountersSKPrefix = "g"
	groupCountAttr        = "c"
	groupUpdatedAttr      = "upd"
	// floatSumScale turns a float into the int64 its sums are kept in: 6 decimals, and adding
	// then subtracting the same value lands back on the exact previous sum.
	floatSumScale = 1e6
	// groupTagKeyField names the GroupBy tag as the first sort column of a group read. It is
	// not a Go identifier, so it can never be a record field.
	groupTagKeyField = "\x00groupBy"
	statusFieldName  = "Status"
)

// groupSumColumn is one GroupBy column and the counter attribute holding its sum.
type groupSumColumn struct {
	fieldName string
	attr      string // "s" + the column's cb id
	isFloat   bool
	acc       *colAccessor
}

// groupIndexMeta is one resolved GroupBy. keys, elementPosition and elements are
// resolved as for a fan-out index (resolveArrayIndex).
type groupIndexMeta struct {
	tag             string // "g" + the cb ids of the Keys, dot-joined: the sk prefix of its counters
	keys            []keyCol
	elementPosition int
	elements        func(ptr unsafe.Pointer) []keyPart
	sums            []groupSumColumn
	isDelta         bool
}

// compileGroupIndex resolves the GroupBy of an Index.
func compileGroupIndex(recordType reflect.Type, accessors map[string]*colAccessor, index Index) groupIndexMeta {
	if index.Type == TypeDelta {
		panic(fmt.Sprintf("db: %s declares GroupBy on a TypeDelta index: declare it on a GSI, a fan-out or a slot-less Index", recordType.Name()))
	}
	resolved := resolveArrayIndex(recordType, accessors, Index{Keys: index.Keys})
	keyColumnIDs := make([]string, len(index.Keys))
	for i, column := range index.Keys {
		keyColumnIDs[i] = cbColumnID(recordType, column.col().fieldName)
	}
	groupIndex := groupIndexMeta{
		tag:             groupCountersSKPrefix + strings.Join(keyColumnIDs, "."),
		keys:            resolved.keys,
		elementPosition: resolved.elementPosition,
		elements:        resolved.elements,
		isDelta:         index.GroupDelta,
	}
	isSummed := map[string]bool{}
	for _, column := range index.GroupBy {
		fieldName := column.col().fieldName
		accessor := accessors[fieldName]
		if _, isSlice := column.(SliceColn); isSlice || accessor == nil || !(accessor.kind.isInteger() || accessor.kind == kindFloat) {
			panic(fmt.Sprintf("db: %s GroupBy column %q must be an integer or float Col", recordType.Name(), fieldName))
		}
		if isSummed[fieldName] {
			panic(fmt.Sprintf("db: %s GroupBy lists %q twice", recordType.Name(), fieldName))
		}
		isSummed[fieldName] = true
		groupIndex.sums = append(groupIndex.sums, groupSumColumn{
			fieldName: fieldName,
			attr:      "s" + cbColumnID(recordType, fieldName),
			isFloat:   accessor.kind == kindFloat,
			acc:       accessor,
		})
	}
	return groupIndex
}

// recordGroup is one group a record counts in: its counter sk, and for a fan-out
// GroupBy the position of the element that made it (-1 otherwise).
type recordGroup struct {
	sk           string
	elementIndex int
}

// recordGroups returns the distinct groups the record at ptr counts in: none for a
// nil ptr or a deleted record (Status 0).
func (m *tableMeta) recordGroups(groupIndex *groupIndexMeta, ptr unsafe.Pointer) []recordGroup {
	if ptr == nil || (m.status != nil && m.status.getI64(ptr) == 0) {
		return nil
	}
	parts := make([]keyPart, len(groupIndex.keys)+1)
	parts[0] = stringPart(groupIndex.tag)
	for i, kc := range groupIndex.keys {
		if i != groupIndex.elementPosition {
			parts[i+1] = m.keyPartsFor(ptr, []keyCol{kc})[0]
		}
	}
	if groupIndex.elementPosition < 0 {
		return []recordGroup{{sk: buildCompositeKey(parts), elementIndex: -1}}
	}
	var groups []recordGroup
	isSeen := map[string]bool{}
	for elementIndex, element := range groupIndex.elements(ptr) {
		parts[groupIndex.elementPosition+1] = element
		groupSK := buildCompositeKey(parts)
		if !isSeen[groupSK] {
			isSeen[groupSK] = true
			groups = append(groups, recordGroup{sk: groupSK, elementIndex: elementIndex})
		}
	}
	return groups
}

// groupSumValues reads the GroupBy columns of the record at ptr as the int64s they
// are summed as. A float goes through math.Round, since v * 1e6 can land a hair
// under the integer it stands for and a plain cast would truncate it.
func (m *tableMeta) groupSumValues(groupIndex *groupIndexMeta, ptr unsafe.Pointer) ([]int64, error) {
	values := make([]int64, len(groupIndex.sums))
	for i, sum := range groupIndex.sums {
		if !sum.isFloat {
			values[i] = sum.acc.getI64(ptr)
			continue
		}
		scaled := math.Round(sum.acc.getF64(ptr) * floatSumScale)
		if math.IsNaN(scaled) || math.Abs(scaled) >= math.MaxInt64 {
			return nil, fmt.Errorf("db: %s GroupBy column %s holds %v, outside what a sum keeps (±9.2e12, 6 decimals)",
				m.recordType.Name(), sum.fieldName, sum.acc.getF64(ptr))
		}
		values[i] = int64(scaled)
	}
	return values, nil
}

// groupCounterDelta is the signed change one write call makes to one counter item.
type groupCounterDelta struct {
	groupIndex   *groupIndexMeta
	basePK, sk   string
	count        int64
	sums         []int64 // parallel to groupIndex.sums
	writeVersion int64   // the highest UpdatedVersion written into the group; 0 for a delete
	// keyRecordPtr is a record of the group (stored or written) the counter's key blob is
	// built from, and elementIndex the element that put it there on a fan-out GroupBy.
	keyRecordPtr unsafe.Pointer
	elementIndex int
}

func (delta *groupCounterDelta) isZero() bool {
	if delta.count != 0 {
		return false
	}
	for _, sum := range delta.sums {
		if sum != 0 {
			return false
		}
	}
	return true
}

// addGroupCounterDeltas adds to deltas (keyed by base pk + counter sk) the changes
// that turn the stored record (nil for a new one) into the written one (nil for a
// delete): a group in both moves its sums by the difference, a group only written
// gains the record, a group only stored loses it. Moving up or down is one signed
// delta. Counters whose deltas cancel out stay in the map and are skipped on apply.
func (m *tableMeta) addGroupCounterDeltas(deltas map[string]*groupCounterDelta, storedPtr, writtenPtr unsafe.Pointer) error {
	keyPtr := writtenPtr
	if keyPtr == nil {
		keyPtr = storedPtr
	}
	writeVersion := int64(0)
	if m.writeVersion != nil && writtenPtr != nil {
		writeVersion = m.writeVersion.acc.getI64(writtenPtr)
	}
	for i := range m.groupIndexes {
		groupIndex := &m.groupIndexes[i]
		storedGroups := m.recordGroups(groupIndex, storedPtr)
		writtenGroups := m.recordGroups(groupIndex, writtenPtr)
		if len(storedGroups) == 0 && len(writtenGroups) == 0 {
			continue
		}
		basePK := m.pkValue(keyPtr)
		var storedSums, writtenSums []int64
		var err error
		if len(storedGroups) > 0 {
			if storedSums, err = m.groupSumValues(groupIndex, storedPtr); err != nil {
				return err
			}
		}
		if len(writtenGroups) > 0 {
			if writtenSums, err = m.groupSumValues(groupIndex, writtenPtr); err != nil {
				return err
			}
		}

		isStoredGroup := map[string]bool{}
		for _, group := range storedGroups {
			isStoredGroup[group.sk] = true
		}
		isWrittenGroup := map[string]bool{}
		for _, group := range writtenGroups {
			isWrittenGroup[group.sk] = true
			delta := groupCounterDeltaOf(deltas, groupIndex, basePK, group, writtenPtr)
			delta.writeVersion = max(delta.writeVersion, writeVersion)
			if isStoredGroup[group.sk] {
				for j := range delta.sums {
					delta.sums[j] += writtenSums[j] - storedSums[j]
				}
				continue
			}
			delta.count++
			for j := range delta.sums {
				delta.sums[j] += writtenSums[j]
			}
		}
		for _, group := range storedGroups {
			if isWrittenGroup[group.sk] {
				continue
			}
			delta := groupCounterDeltaOf(deltas, groupIndex, basePK, group, storedPtr)
			delta.writeVersion = max(delta.writeVersion, writeVersion)
			delta.count--
			for j := range delta.sums {
				delta.sums[j] -= storedSums[j]
			}
		}
	}
	return nil
}

// groupCounterDeltaOf returns the delta of a group's counter, creating it at zero.
func groupCounterDeltaOf(deltas map[string]*groupCounterDelta, groupIndex *groupIndexMeta, basePK string, group recordGroup, recordPtr unsafe.Pointer) *groupCounterDelta {
	counterKey := basePK + keySeparator + group.sk
	delta, exists := deltas[counterKey]
	if !exists {
		delta = &groupCounterDelta{
			groupIndex: groupIndex, basePK: basePK, sk: group.sk, sums: make([]int64, len(groupIndex.sums)),
			keyRecordPtr: recordPtr, elementIndex: group.elementIndex,
		}
		deltas[counterKey] = delta
	}
	return delta
}

// mergeGroupCounterDeltas adds the deltas of one record (from) into those of the call (into).
func mergeGroupCounterDeltas(into, from map[string]*groupCounterDelta) {
	for counterKey, delta := range from {
		merged, exists := into[counterKey]
		if !exists {
			into[counterKey] = delta
			continue
		}
		merged.count += delta.count
		for j := range merged.sums {
			merged.sums[j] += delta.sums[j]
		}
		merged.writeVersion = max(merged.writeVersion, delta.writeVersion)
	}
}

// groupKeyBlob is the colbin blob of a record holding only the group Keys of the
// record at recordPtr: for a fan-out GroupBy, its slice holds just the one element.
func (m *tableMeta) groupKeyBlob(groupIndex *groupIndexMeta, recordPtr unsafe.Pointer, elementIndex int) ([]byte, error) {
	sourceRecord := reflect.NewAt(m.recordType, recordPtr).Elem()
	keyRecord := reflect.New(m.recordType)
	for i, kc := range groupIndex.keys {
		keyField, sourceField := keyRecord.Elem().FieldByName(kc.fieldName), sourceRecord.FieldByName(kc.fieldName)
		if i != groupIndex.elementPosition {
			keyField.Set(sourceField)
			continue
		}
		keyField.Set(reflect.MakeSlice(keyField.Type(), 1, 1))
		keyField.Index(0).Set(sourceField.Index(elementIndex))
	}
	blob, err := colbin.Marshal(keyRecord.Interface())
	if err != nil {
		return nil, fmt.Errorf("db: colbin marshaling the %s group key: %w", m.recordType.Name(), err)
	}
	return blob, nil
}

// groupCountersPK is the pk of a base partition's counters (and of its slot versions).
func groupCountersPK(basePK string) string { return basePK + slotVersionsColumnID }

// applyGroupCounterDeltas ADDs every non-zero delta to its counter, one UpdateItem
// each, and on a GroupDelta stamps upv/upd. It must run after the base items land.
// A delete carries no write version, so it reserves one per base pk. The updates
// run in parallel: each one touches its own counter item and an ADD commutes, so
// their order never matters. The version reservations happen before, in the
// sequential loop that builds them, so one base pk never reserves twice.
func (m *tableMeta) applyGroupCounterDeltas(client *dynamodb.Client, deltas map[string]*groupCounterDelta) error {
	writeTime := (Now().Unix() - 1e9) / 2
	reservedVersionByPK := map[string]int64{}
	counterUpdates := make([]*dynamodb.UpdateItemInput, 0, len(deltas))
	counterSKs := make([]string, 0, len(deltas))
	for _, delta := range deltas {
		if delta.isZero() {
			continue
		}
		keyBlob, err := m.groupKeyBlob(delta.groupIndex, delta.keyRecordPtr, delta.elementIndex)
		if err != nil {
			return err
		}
		names := map[string]string{"#c": groupCountAttr, "#d": dataColumn}
		values := map[string]types.AttributeValue{
			":c": &types.AttributeValueMemberN{Value: strconv.FormatInt(delta.count, 10)},
			":d": &types.AttributeValueMemberB{Value: keyBlob},
		}
		addClauses := []string{"#c :c"}
		for j, sum := range delta.groupIndex.sums {
			names["#"+sum.attr] = sum.attr
			values[":"+sum.attr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(delta.sums[j], 10)}
			addClauses = append(addClauses, "#"+sum.attr+" :"+sum.attr)
		}
		setClauses := []string{"#d = if_not_exists(#d, :d)"}
		if delta.groupIndex.isDelta {
			version := delta.writeVersion
			if version == 0 {
				if version = reservedVersionByPK[delta.basePK]; version == 0 {
					if version, err = reserveSequence(delta.basePK+updatedVersionSeqSuffix, 1); err != nil {
						return err
					}
					reservedVersionByPK[delta.basePK] = version
				}
			}
			names["#upv"], names["#upd"] = versionColumn, groupUpdatedAttr
			values[":upv"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(version, 10)}
			values[":upd"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(writeTime, 10)}
			setClauses = append(setClauses, "#upv = :upv", "#upd = :upd")
		}
		counterUpdates = append(counterUpdates, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(tableName()),
			Key:                       itemKey(groupCountersPK(delta.basePK), delta.sk),
			UpdateExpression:          aws.String("SET " + strings.Join(setClauses, ", ") + " ADD " + strings.Join(addClauses, ", ")),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		})
		counterSKs = append(counterSKs, delta.sk)
	}
	return runInParallel(len(counterUpdates), func(updateIndex int) error {
		if _, err := client.UpdateItem(context.Background(), counterUpdates[updateIndex]); err != nil {
			return fmt.Errorf("db: %s updating the GroupBy counter %s: %w", m.recordType.Name(), counterSKs[updateIndex], err)
		}
		return nil
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Read: QueryGroups
// ─────────────────────────────────────────────────────────────────────────────

// Group is one counter of a GroupBy: its group Keys, the number of records in it
// and, through Sum / SumFloat, the sum of each GroupBy column.
type Group[E any] struct {
	// Key is a record with only the group Keys set; on a fan-out GroupBy its slice holds the one element.
	Key   E
	Count int64
	// Updated and UpdatedVersion are the last write that touched the group (GroupDelta only).
	Updated        int32
	UpdatedVersion int32
	groupIndex     *groupIndexMeta
	sums           []int64
}

// Sum returns the sum of an integer GroupBy column.
func (g Group[E]) Sum(column Coln) int64 { return g.sums[g.sumPosition(column, false)] }

// SumFloat returns the sum of a float GroupBy column, exact to 6 decimals.
func (g Group[E]) SumFloat(column Coln) float64 {
	return float64(g.sums[g.sumPosition(column, true)]) / floatSumScale
}

// sumPosition panics on a column that is not summed, or on the wrong accessor for its
// kind: both are programming errors, like the ORM's other schema misuses.
func (g Group[E]) sumPosition(column Coln, isFloat bool) int {
	fieldName := column.col().fieldName
	for i, sum := range g.groupIndex.sums {
		if sum.fieldName != fieldName {
			continue
		}
		if sum.isFloat != isFloat {
			panic(fmt.Sprintf("db: GroupBy column %q is read with Sum when it is an integer and SumFloat when it is a float", fieldName))
		}
		return i
	}
	panic(fmt.Sprintf("db: %q is not a column of this GroupBy", fieldName))
}

// GroupQuery reads the counters of one GroupBy in one base partition.
type GroupQuery[E any] struct {
	meta         *tableMeta
	groupIndex   *groupIndexMeta
	preds        []predicate
	updatedSince int32
	readsDelta   bool
	planErr      error
}

// QueryGroups reads the counters of the GroupBy whose Keys are exactly groupKeys,
// in order. It needs an Eq on every Partition column (counters are per partition);
// the group Keys take an Eq on a leading run, then one range, as Keys do in Query.
// Without Since, groups whose count fell to 0 are left out.
func (r *Repo[T, E]) QueryGroups(groupKeys ...Coln) *GroupQuery[E] {
	query := &GroupQuery[E]{meta: r.meta}
	for i := range r.meta.groupIndexes {
		groupIndex := &r.meta.groupIndexes[i]
		isMatch := len(groupIndex.keys) == len(groupKeys)
		for j := 0; isMatch && j < len(groupKeys); j++ {
			isMatch = groupIndex.keys[j].fieldName == groupKeys[j].col().fieldName
		}
		if isMatch {
			query.groupIndex = groupIndex
		}
	}
	if query.groupIndex == nil {
		query.planErr = fmt.Errorf("db: %s has no GroupBy on an Index with exactly those Keys", r.meta.recordType.Name())
	}
	return query
}

func (q *GroupQuery[E]) add(column Coln, o op, v1, v2 any) *GroupQuery[E] {
	q.preds = append(q.preds, predicate{field: column.col().fieldName, op: o, v1: v1, v2: v2})
	return q
}

// Eq on a ColSlice group Key pins its element: the one group of that element.
func (q *GroupQuery[E]) Eq(column Coln, v any) *GroupQuery[E]  { return q.add(column, opEq, v, nil) }
func (q *GroupQuery[E]) Gt(column Coln, v any) *GroupQuery[E]  { return q.add(column, opGt, v, nil) }
func (q *GroupQuery[E]) Gte(column Coln, v any) *GroupQuery[E] { return q.add(column, opGte, v, nil) }
func (q *GroupQuery[E]) Lt(column Coln, v any) *GroupQuery[E]  { return q.add(column, opLt, v, nil) }
func (q *GroupQuery[E]) Lte(column Coln, v any) *GroupQuery[E] { return q.add(column, opLte, v, nil) }
func (q *GroupQuery[E]) Between(column Coln, a, b any) *GroupQuery[E] {
	return q.add(column, opBetween, a, b)
}

// Since turns the read into a delta read on a GroupDelta: only the groups written
// after updatedSince, the highest UpdatedVersion the client holds, emptied groups
// included so the client drops them. A first sync (0) leaves the empty ones out.
func (q *GroupQuery[E]) Since(updatedSince int32) *GroupQuery[E] {
	if q.planErr == nil && !q.groupIndex.isDelta {
		q.planErr = fmt.Errorf("db: %s Since() needs GroupDelta on its GroupBy", q.meta.recordType.Name())
	}
	q.updatedSince, q.readsDelta = updatedSince, true
	return q
}

// Exec reads the groups in group key order. A GroupBy holds few groups, so Since
// filters them in memory after reading the range.
func (q *GroupQuery[E]) Exec() ([]Group[E], error) {
	if q.planErr != nil {
		return nil, q.planErr
	}
	m := q.meta
	recordName := m.recordType.Name()
	byField := map[string]predicate{}
	for _, p := range q.preds {
		byField[p.field] = p
	}
	plan := &queryPlan{names: map[string]string{"#pk": "pk"}, values: map[string]types.AttributeValue{}, keyCond: "#pk = :pk"}
	partitionValues := make([]uint64, len(m.partition))
	for i, kc := range m.partition {
		p, ok := byField[kc.fieldName]
		if !ok || p.op != opEq {
			return nil, fmt.Errorf("db: %s QueryGroups needs an Eq on every Partition column: counters are kept per partition", recordName)
		}
		partitionValues[i] = uint64(valueToInt64(p.v1, kc.fieldName))
		delete(byField, kc.fieldName)
	}
	plan.values[":pk"] = &types.AttributeValueMemberN{Value: groupCountersPK(m.numericKey(partitionValues, m.partition))}

	// The tag is pinned as the first sort column, so the read stays inside this GroupBy's counters.
	sortColumns := append([]keyCol{{fieldName: groupTagKeyField, kind: kindString}}, q.groupIndex.keys...)
	byField[groupTagKeyField] = predicate{field: groupTagKeyField, op: opEq, v1: q.groupIndex.tag}
	usedFields, err := resolveKeys(byField, plan, sortColumns)
	if err != nil {
		return nil, err
	}
	for field := range byField {
		if !usedFields[field] {
			return nil, fmt.Errorf("db: %s QueryGroups cannot filter %s: it takes an Eq on a leading run of the group Keys, then one range", recordName, field)
		}
	}
	// A strict < under a prefix is a BETWEEN whose upper bound is the one sk to drop.
	excludedSK := ""
	if len(plan.keyFilter) > 0 {
		excludedSK = plan.values[":hi"].(*types.AttributeValueMemberS).Value
	}

	client, err := Client()
	if err != nil {
		return nil, err
	}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName()),
		KeyConditionExpression:    aws.String(plan.keyCond),
		ExpressionAttributeNames:  plan.names,
		ExpressionAttributeValues: plan.values,
	}
	var groups []Group[E]
	for {
		out, err := client.Query(context.Background(), input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			if item["sk"].(*types.AttributeValueMemberS).Value == excludedSK {
				continue
			}
			group := Group[E]{
				Count:          numberAttrValue(item, groupCountAttr),
				Updated:        int32(numberAttrValue(item, groupUpdatedAttr)),
				UpdatedVersion: int32(numberAttrValue(item, versionColumn)),
				groupIndex:     q.groupIndex,
				sums:           make([]int64, len(q.groupIndex.sums)),
			}
			// A later delta sync sends every changed group, emptied ones included; any other read skips them.
			if q.readsDelta && q.updatedSince > 0 {
				if group.UpdatedVersion <= q.updatedSince {
					continue
				}
			} else if group.Count == 0 {
				continue
			}
			if err := m.unmarshalItem(item, &group.Key); err != nil {
				return nil, err
			}
			// A column added to the GroupBy after the counter was written reads 0 until RebuildGroups.
			for j, sum := range q.groupIndex.sums {
				group.sums[j] = numberAttrValue(item, sum.attr)
			}
			groups = append(groups, group)
		}
		if len(out.LastEvaluatedKey) == 0 {
			return groups, nil
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// numberAttrValue reads an integer number attribute, 0 when it is missing.
func numberAttrValue(item map[string]types.AttributeValue, attr string) int64 {
	number, ok := item[attr].(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	value, _ := strconv.ParseInt(number.Value, 10, 64)
	return value
}

// ─────────────────────────────────────────────────────────────────────────────
// Backfill: RebuildGroups / RebuildGroupsAll
// ─────────────────────────────────────────────────────────────────────────────

// RebuildGroups recomputes every GroupBy counter of one base partition from its
// records (pass one value per Partition column, none without Partition) and
// returns how many counters it rewrote. It is the fix for drift, and the backfill
// after adding a GroupBy, a GroupBy column or changing its Keys. Writes landing
// while it runs can be lost from it or counted twice: run it when the partition
// is quiet, and a second run converges.
func (r *Repo[T, E]) RebuildGroups(partitionValues ...any) (int, error) {
	m := r.meta
	if len(m.groupIndexes) == 0 {
		return 0, fmt.Errorf("db: %s declares no GroupBy", m.recordType.Name())
	}
	if len(partitionValues) != len(m.partition) {
		return 0, fmt.Errorf("db: %s has %d partition column(s), got %d value(s)", m.recordType.Name(), len(m.partition), len(partitionValues))
	}
	query := r.Query().Consistent()
	values := make([]uint64, len(m.partition))
	for i, kc := range m.partition {
		query.preds = append(query.preds, predicate{field: kc.fieldName, op: opEq, v1: partitionValues[i]})
		values[i] = uint64(valueToInt64(partitionValues[i], kc.fieldName))
	}
	var records []E
	if err := query.Exec(&records); err != nil {
		return 0, err
	}
	client, err := Client()
	if err != nil {
		return 0, err
	}
	return r.rebuildPartitionGroups(client, m.numericKey(values, m.partition), records)
}

// RebuildGroupsAll is RebuildGroups for every base partition of the entity: it
// scans its records, and its counters too, so a partition left without records
// gets its counters zeroed. It returns how many counters it rewrote.
func (r *Repo[T, E]) RebuildGroupsAll() (int, error) {
	m := r.meta
	if len(m.groupIndexes) == 0 {
		return 0, fmt.Errorf("db: %s declares no GroupBy", m.recordType.Name())
	}
	records, err := r.Scan(0)
	if err != nil {
		return 0, err
	}
	recordsByPK := map[string][]E{}
	for i := range records {
		basePK := m.pkValue(unsafe.Pointer(&records[i]))
		recordsByPK[basePK] = append(recordsByPK[basePK], records[i])
	}
	client, err := Client()
	if err != nil {
		return 0, err
	}

	lowestPK, highestPK := m.partitionRange(arrayIndexColumnIDDigits)
	input := &dynamodb.ScanInput{
		TableName:                aws.String(tableName()),
		FilterExpression:         aws.String("#pk BETWEEN :lo AND :hi AND begins_with(#sk, :g)"),
		ProjectionExpression:     aws.String("#pk"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":lo": &types.AttributeValueMemberN{Value: lowestPK},
			":hi": &types.AttributeValueMemberN{Value: highestPK},
			":g":  &types.AttributeValueMemberS{Value: groupCountersSKPrefix},
		},
	}
	for {
		out, err := client.Scan(context.Background(), input)
		if err != nil {
			return 0, err
		}
		for _, item := range out.Items {
			if counterPK := item["pk"].(*types.AttributeValueMemberN).Value; isSlotVersionsPK(counterPK) {
				basePK := strings.TrimSuffix(counterPK, slotVersionsColumnID)
				recordsByPK[basePK] = recordsByPK[basePK] // a partition with counters and no records
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	rewrittenCount := 0
	for basePK, partitionRecords := range recordsByPK {
		rewritten, err := r.rebuildPartitionGroups(client, basePK, partitionRecords)
		rewrittenCount += rewritten
		if err != nil {
			return rewrittenCount, err
		}
	}
	return rewrittenCount, nil
}

// rebuildPartitionGroups computes the counters of one partition from its records,
// compares them with the stored ones and Puts only those that differ: a stored
// counter no record produces anymore is zeroed, never deleted. Every rewritten
// counter of a GroupDelta gets one version freshly reserved for the partition, so
// clients past their watermark still refetch the corrected groups.
func (r *Repo[T, E]) rebuildPartitionGroups(client *dynamodb.Client, basePK string, records []E) (int, error) {
	m := r.meta
	expectedDeltas := map[string]*groupCounterDelta{}
	for i := range records {
		if err := m.addGroupCounterDeltas(expectedDeltas, nil, unsafe.Pointer(&records[i])); err != nil {
			return 0, err
		}
	}

	counterPK := groupCountersPK(basePK)
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(tableName()),
		KeyConditionExpression:    aws.String("#pk = :pk AND begins_with(#sk, :g)"),
		ExpressionAttributeNames:  map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberN{Value: counterPK}, ":g": &types.AttributeValueMemberS{Value: groupCountersSKPrefix}},
		ConsistentRead:            aws.Bool(true),
	}
	storedCounterBySK := map[string]map[string]types.AttributeValue{}
	for {
		out, err := client.Query(context.Background(), input)
		if err != nil {
			return 0, err
		}
		for _, item := range out.Items {
			storedCounterBySK[item["sk"].(*types.AttributeValueMemberS).Value] = item
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}

	writeTime := (Now().Unix() - 1e9) / 2
	rebuildVersion := int64(0)
	stampVersion := func(counter map[string]types.AttributeValue, groupIndex *groupIndexMeta) error {
		if groupIndex == nil || !groupIndex.isDelta {
			return nil
		}
		if rebuildVersion == 0 {
			var err error
			if rebuildVersion, err = reserveSequence(basePK+updatedVersionSeqSuffix, 1); err != nil {
				return err
			}
		}
		counter[versionColumn] = &types.AttributeValueMemberN{Value: strconv.FormatInt(rebuildVersion, 10)}
		counter[groupUpdatedAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(writeTime, 10)}
		return nil
	}

	var counterPuts []types.WriteRequest
	for _, expected := range expectedDeltas {
		storedCounter := storedCounterBySK[expected.sk]
		delete(storedCounterBySK, expected.sk)
		if storedCounter != nil && storedCounterMatches(storedCounter, expected) {
			continue
		}
		keyBlob, err := m.groupKeyBlob(expected.groupIndex, expected.keyRecordPtr, expected.elementIndex)
		if err != nil {
			return 0, err
		}
		counter := itemKey(counterPK, expected.sk)
		counter[groupCountAttr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expected.count, 10)}
		counter[dataColumn] = &types.AttributeValueMemberB{Value: keyBlob}
		for j, sum := range expected.groupIndex.sums {
			counter[sum.attr] = &types.AttributeValueMemberN{Value: strconv.FormatInt(expected.sums[j], 10)}
		}
		if err := stampVersion(counter, expected.groupIndex); err != nil {
			return 0, err
		}
		counterPuts = append(counterPuts, types.WriteRequest{PutRequest: &types.PutRequest{Item: counter}})
	}
	// What is left was produced by no record: an emptied group, or a GroupBy that changed its Keys.
	for groupSK, storedCounter := range storedCounterBySK {
		if numberAttrValue(storedCounter, groupCountAttr) == 0 && !holdsNonZeroSum(storedCounter) {
			continue
		}
		counter := itemKey(counterPK, groupSK)
		counter[groupCountAttr] = &types.AttributeValueMemberN{Value: "0"}
		if keyBlob, hasKey := storedCounter[dataColumn]; hasKey {
			counter[dataColumn] = keyBlob
		}
		if err := stampVersion(counter, m.groupIndexOfSK(groupSK)); err != nil {
			return 0, err
		}
		counterPuts = append(counterPuts, types.WriteRequest{PutRequest: &types.PutRequest{Item: counter}})
	}
	return len(counterPuts), r.batchWriteAll(client, counterPuts)
}

// storedCounterMatches reports whether a stored counter already holds the expected count and sums.
func storedCounterMatches(storedCounter map[string]types.AttributeValue, expected *groupCounterDelta) bool {
	if numberAttrValue(storedCounter, groupCountAttr) != expected.count {
		return false
	}
	for j, sum := range expected.groupIndex.sums {
		if numberAttrValue(storedCounter, sum.attr) != expected.sums[j] {
			return false
		}
	}
	return true
}

// holdsNonZeroSum reports whether any sum attribute (sNNN) of a counter is not 0.
func holdsNonZeroSum(counter map[string]types.AttributeValue) bool {
	for attr := range counter {
		if strings.HasPrefix(attr, "s") && attr != "sk" && numberAttrValue(counter, attr) != 0 {
			return true
		}
	}
	return false
}

// groupIndexOfSK returns the GroupBy a counter sk belongs to, nil when its tag is no longer declared.
func (m *tableMeta) groupIndexOfSK(groupSK string) *groupIndexMeta {
	tag, _, _ := strings.Cut(groupSK, keySeparator)
	for i := range m.groupIndexes {
		if m.groupIndexes[i].tag == tag {
			return &m.groupIndexes[i]
		}
	}
	return nil
}
