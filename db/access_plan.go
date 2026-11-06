package db

import (
	"fmt"
	"reflect"
	"slices"
)

// AccessPlan is what a query resolves to before any storage syntax exists: which source answers
// it, the reads against that source, and what is left to filter. Each driver renders the steps
// in its own syntax (CQL, FDB key ranges, YQL); none of them re-derives the range math.
type AccessPlan struct {
	Source *IndexSource // nil = the table's primary key
	// Steps are read independently and their rows unioned (deduplicated when Source can repeat a row).
	Steps []AccessStep
	// Filter holds predicates the source key cannot serve. A driver may push them to the server
	// (Scylla ALLOW FILTERING, a YQL WHERE) or apply them in memory: either answer is exact.
	Filter []ColumnStatement
	// PostFilter holds predicates that must be re-checked in memory even after the read, because the
	// source overfetches (hash collisions, a one-sided packed bound spilling into the next prefix).
	PostFilter []ColumnStatement
	Limit      int32
	Descending bool
}

// AccessStep is one read against the source. Exactly one of Range / Element is set.
type AccessStep struct {
	Range   *KeyRange
	Element *ElementLookup
}

// KeyRange is a span of the source key: equalities (or value lists) on the leading key columns,
// partition first, then an optional bound on the next column. With no bound it is a set of point
// or prefix lookups, the cartesian product of the value lists.
type KeyRange struct {
	EqualPrefix []KeyValues
	// BoundColumn is the column From / To apply to; "" when the range is the prefix alone.
	BoundColumn                string
	From, To                   any // nil = open on that side
	FromInclusive, ToInclusive bool
}

// KeyValues pins one key column to one value (equality) or several (IN).
type KeyValues struct {
	Column string
	Values []any
}

// ElementLookup reads the rows whose collection column holds Value (a set CONTAINS), within the
// partition EqualPrefix pins when the collection index is partition-scoped (a composite bucket).
type ElementLookup struct {
	EqualPrefix []KeyValues
	Column      string
	Value       any
}

// Values in a step use the neutral layout the IndexSource describes: unsigned packed values
// (uint64) and int32 hashes. A driver applies its own storage encoding while rendering (Scylla
// flips the top bit of packed values, storeVirtualPacked).

// HasAccessPlanner tells whether PlanSourceSteps can bind predicates to this source. Single-column
// and composite-bucket indexes are read through the base statements instead.
func HasAccessPlanner(source *IndexSource) bool {
	switch {
	case source == nil:
		return false
	case source.Kind == IndexKindPacked, source.Kind == IndexKindGroupHash:
		return true
	case source.Type == 6, source.Type == 7, source.Type == 8, source.Type == 9:
		return true
	}
	return false
}

// PlanSourceSteps binds the statements a source was chosen for to reads against its key. It returns
// the steps and the statements the key could not serve (Filter). An error means the source cannot
// serve these predicates and the planner must not use it.
func PlanSourceSteps(source *IndexSource, statements []ColumnStatement) ([]AccessStep, []ColumnStatement, error) {
	switch {
	case source.Kind == IndexKindPacked:
		steps, err := planPackedIndexSteps(source, statements)
		return steps, nil, err
	case source.Kind == IndexKindGroupHash:
		steps, err := planGroupHashSteps(source, statements)
		return steps, nil, err
	case source.Type == 7:
		steps, err := planHashViewSteps(source, statements)
		return steps, nil, err
	case source.Type == 8:
		steps, err := planPackedRangeSteps(source, statements)
		return steps, nil, err
	case source.Type == 6, source.Type == 9:
		step, filter := planKeyPrefixStep(source.Columns, source.FanoutColumnName, statements)
		return []AccessStep{step}, filter, nil
	}
	return nil, nil, fmt.Errorf(`index "%v" has no access planner`, source.Name)
}

func rangeStep(keyRange KeyRange) AccessStep { return AccessStep{Range: &keyRange} }

func isRangeOperator(operator string) bool {
	return operator == ">" || operator == ">=" || operator == "<" || operator == "<="
}

// statementValues is the value list of an equality or IN statement.
func statementValues(statement ColumnStatement) []any {
	if statement.Operator == "IN" {
		return slices.Clone(statement.Values)
	}
	return []any{statement.Value}
}

// boundFromOperator turns a one-sided range operator into a KeyRange bound.
func boundFromOperator(keyRange *KeyRange, column, operator string, value any) {
	keyRange.BoundColumn = column
	switch operator {
	case ">", ">=":
		keyRange.From, keyRange.FromInclusive = value, operator == ">="
	case "<", "<=":
		keyRange.To, keyRange.ToInclusive = value, operator == "<="
	}
}

// planKeyPrefixStep serves a source whose key holds the query columns themselves (a simple view or
// a view table): equality / IN on the leading key columns, then one range on the next. A CONTAINS
// on the fan-out column is an equality, since the view table holds one row per element. Whatever
// does not fit that shape stays a Filter; the source still narrows the read.
func planKeyPrefixStep(keyColumns []string, fanoutColumnName string, statements []ColumnStatement) (AccessStep, []ColumnStatement) {
	statements = slices.Clone(statements)
	for statementIndex := range statements {
		if statements[statementIndex].Col == fanoutColumnName && statements[statementIndex].Operator == "CONTAINS" {
			statements[statementIndex].Operator = "="
		}
	}
	keyRange := KeyRange{}
	consumed := make([]bool, len(statements))
	findStatement := func(column string) int {
		for statementIndex, statement := range statements {
			if !consumed[statementIndex] && statement.Col == column && len(statement.From) == 0 {
				return statementIndex
			}
			if !consumed[statementIndex] && len(statement.From) == 1 && statement.From[0].Col == column {
				return statementIndex
			}
		}
		return -1
	}

	for _, keyColumn := range keyColumns {
		statementIndex := findStatement(keyColumn)
		if statementIndex < 0 {
			break
		}
		statement := statements[statementIndex]
		operator := statement.Operator
		if operator == "=" || operator == "IN" {
			consumed[statementIndex] = true
			keyRange.EqualPrefix = append(keyRange.EqualPrefix, KeyValues{Column: keyColumn, Values: statementValues(statement)})
			continue
		}
		if len(statement.From) == 1 && len(statement.To) == 1 {
			consumed[statementIndex] = true
			keyRange.BoundColumn = keyColumn
			keyRange.From, keyRange.FromInclusive = statement.From[0].Value, true
			keyRange.To, keyRange.ToInclusive = statement.To[0].Value, true
		} else if isRangeOperator(operator) {
			consumed[statementIndex] = true
			boundFromOperator(&keyRange, keyColumn, operator, statement.Value)
		}
		break
	}

	filter := []ColumnStatement{}
	for statementIndex, statement := range statements {
		if !consumed[statementIndex] {
			filter = append(filter, statement)
		}
	}
	return rangeStep(keyRange), filter
}

// expandValueGroups extends every value group with each of the values: the cartesian product of IN
// lists across the columns of a packed or hashed key.
func expandValueGroups[T any](valueGroups [][]T, values []T) [][]T {
	expandedGroups := make([][]T, 0, len(valueGroups)*len(values))
	for _, valueGroup := range valueGroups {
		for _, value := range values {
			expandedGroups = append(expandedGroups, append(slices.Clone(valueGroup), value))
		}
	}
	return expandedGroups
}

// MaxHashViewHashes caps how many hashes one hash-view read sends: a BETWEEN on one of its columns
// is enumerated value by value, since a hash has no order, and every combination is one more value
// in the read's IN list.
const MaxHashViewHashes = 100

// planHashViewSteps serves a hash view (Type 7): the declared columns' values are hashed together,
// one hash per combination, read as one value list on the hash column. A BETWEEN is enumerated into
// its integer values; an open range (>, <) has no finite value set and is refused.
func planHashViewSteps(source *IndexSource, statements []ColumnStatement) ([]AccessStep, error) {
	valueGroups := [][]any{{}}
	for _, declaredColumn := range source.DeclaredColumns {
		for _, statement := range statements {
			if statement.Col != declaredColumn.GetName() {
				continue
			}
			columnValues, err := hashViewStatementValues(statement)
			if err != nil {
				return nil, fmt.Errorf(`hash view "%v": %w`, source.Name, err)
			}
			valueGroups = expandValueGroups(valueGroups, columnValues)
			if len(valueGroups) > MaxHashViewHashes {
				return nil, fmt.Errorf(`hash view "%v": the predicates select more than %d value combinations`, source.Name, MaxHashViewHashes)
			}
			break
		}
	}

	hashValues := make([]any, 0, len(valueGroups))
	for _, values := range valueGroups {
		hashValues = append(hashValues, HashInt(values...))
	}
	keyRange := KeyRange{}
	if source.PartitionColumn != nil && !source.PartitionColumn.IsNil() {
		for _, statement := range statements {
			if statement.Col == source.PartitionColumn.GetName() {
				keyRange.EqualPrefix = append(keyRange.EqualPrefix, KeyValues{Column: statement.Col, Values: []any{statement.Value}})
				break
			}
		}
	}
	keyRange.EqualPrefix = append(keyRange.EqualPrefix, KeyValues{Column: source.Column.GetName(), Values: hashValues})
	return []AccessStep{rangeStep(keyRange)}, nil
}

// hashViewStatementValues lists the values one statement selects on a hash view column, each of the
// bound's own type: the hash of an int16 and of an int32 with the same value differ.
func hashViewStatementValues(statement ColumnStatement) ([]any, error) {
	switch {
	case len(statement.Values) > 0:
		return statement.Values, nil
	case statement.Operator == "=":
		return []any{statement.Value}, nil
	case len(statement.From) == 1 && len(statement.To) == 1:
		fromValue, toValue := reflect.ValueOf(statement.From[0].Value), reflect.ValueOf(statement.To[0].Value)
		if !fromValue.CanInt() || fromValue.Type() != toValue.Type() {
			return nil, fmt.Errorf(`a BETWEEN on "%v" needs integer bounds of one type, got %T and %T`,
				statement.Col, statement.From[0].Value, statement.To[0].Value)
		}
		from, to := fromValue.Int(), toValue.Int()
		if to-from >= MaxHashViewHashes {
			return nil, fmt.Errorf(`a BETWEEN on "%v" spans more than %d values`, statement.Col, MaxHashViewHashes)
		}
		values := []any{}
		for value := from; value <= to; value++ {
			values = append(values, reflect.ValueOf(value).Convert(fromValue.Type()).Interface())
		}
		return values, nil
	}
	return nil, fmt.Errorf(`operator %v on "%v" has no finite value set to hash`, statement.Operator, statement.Col)
}

// statementRangePair is the lower statement of a column and, for a BETWEEN, its upper bound.
type statementRangePair struct {
	from      *ColumnStatement
	betweenTo *ColumnStatement
}

// planPackedRangeSteps serves a packed range view (Type 8, delta views included). A packed key is
// prefix-searchable: the leading columns pinned by equality / IN select a contiguous block, and
// everything from the first unconstrained or range-filtered column onwards spans that block's full
// width. Absent columns count as range columns, not as an equality on zero, which is what lets one
// packed view answer "Status = 1" as well as the full key.
func planPackedRangeSteps(source *IndexSource, statements []ColumnStatement) ([]AccessStep, error) {
	slotBits := source.PackedSlotBitsPerColumn
	packedColumnName := source.Column.GetName()
	partitionColumn := source.PartitionColumn
	if partitionColumn != nil && partitionColumn.IsNil() {
		partitionColumn = nil
	}

	statementsByColumn := map[string]statementRangePair{}
	usesBetween := false
	for statementIndex := range statements {
		statement := &statements[statementIndex]
		if statement.Operator == "BETWEEN" {
			usesBetween = true
			for boundIndex := range statement.From {
				statementsByColumn[statement.From[boundIndex].Col] = statementRangePair{
					from: &statement.From[boundIndex], betweenTo: &statement.To[boundIndex],
				}
			}
		} else {
			statementsByColumn[statement.Col] = statementRangePair{from: statement}
		}
	}

	// The partition is pinned by its own statement; the packed column holds only the source columns.
	partitionPrefix := []KeyValues{}
	if partitionColumn != nil {
		if partitionStatement := statementsByColumn[partitionColumn.GetName()].from; partitionStatement != nil {
			partitionPrefix = append(partitionPrefix, KeyValues{
				Column: partitionColumn.GetName(), Values: []any{ConvertToInt64(partitionStatement.Value)},
			})
		}
	}
	withPartition := func(keyRange KeyRange) AccessStep {
		keyRange.EqualPrefix = append(slices.Clone(partitionPrefix), keyRange.EqualPrefix...)
		return rangeStep(keyRange)
	}

	valueGroups := [][]int64{}
	rangeColumns := []IColInfo{}
	for _, column := range source.PackedSourceColumns {
		if partitionColumn != nil && column.GetName() == partitionColumn.GetName() {
			continue
		}
		statementRange, hasStatement := statementsByColumn[column.GetName()]
		isUnconstrained := !hasStatement || statementRange.from == nil
		if len(rangeColumns) > 0 || isUnconstrained || isRangeOperator(statementRange.from.Operator) {
			rangeColumns = append(rangeColumns, column)
			continue
		}
		pinnedValues := []int64{}
		if len(statementRange.from.Values) > 0 {
			for _, value := range statementRange.from.Values {
				pinnedValues = append(pinnedValues, ConvertToInt64(value))
			}
		} else {
			pinnedValues = append(pinnedValues, ConvertToInt64(statementRange.from.Value))
		}
		if len(valueGroups) == 0 {
			valueGroups = [][]int64{{}}
		}
		valueGroups = expandValueGroups(valueGroups, pinnedValues)
	}

	// Only the first range column can carry a bound; a filter on anything after it cannot be one
	// packed range and must not be silently dropped.
	for _, column := range rangeColumns[min(1, len(rangeColumns)):] {
		if statementRange, hasStatement := statementsByColumn[column.GetName()]; hasStatement && statementRange.from != nil {
			return nil, fmt.Errorf(`packed view "%v" cannot bind a filter on "%v" behind an unconstrained column`,
				source.Name, column.GetName())
		}
	}

	if usesBetween {
		fromValues, toValues := []int64{}, []int64{}
		for _, column := range source.PackedSourceColumns {
			statementRange, hasStatement := statementsByColumn[column.GetName()]
			// An unconstrained column spans its whole slot: it floors on the low bound and tops out
			// on the high one.
			if !hasStatement || statementRange.from == nil {
				fromValues = append(fromValues, 0)
				toValues = append(toValues, int64(SlotMaxValue(slotBits[len(toValues)])))
				continue
			}
			fromValues = append(fromValues, ConvertToInt64(statementRange.from.Value))
			if statementRange.betweenTo != nil {
				toValues = append(toValues, ConvertToInt64(statementRange.betweenTo.Value))
			} else {
				toValues = append(toValues, ConvertToInt64(statementRange.from.Value))
			}
		}
		return []AccessStep{withPartition(KeyRange{
			BoundColumn: packedColumnName,
			From:        PackSlotValues(fromValues, slotBits), FromInclusive: true,
			To: PackSlotValues(toValues, slotBits), ToInclusive: true,
		})}, nil
	}

	if len(rangeColumns) > 0 {
		if len(valueGroups) == 0 {
			// Nothing is pinned by equality, so the scan starts at the packed column's floor.
			valueGroups = [][]int64{{}}
		}
		steps := make([]AccessStep, 0, len(valueGroups))
		for _, prefixValues := range valueGroups {
			fromValues := slices.Clone(prefixValues)
			for _, column := range rangeColumns {
				// Only the first range column can carry a lower bound; the rest span their slot.
				rangeFrom := int64(0)
				if statementRange, hasStatement := statementsByColumn[column.GetName()]; hasStatement && statementRange.from != nil {
					rangeFrom = ConvertToInt64(statementRange.from.Value)
				}
				fromValues = append(fromValues, rangeFrom)
			}
			// Upper bounds are inclusive (every trailing slot at its max), so none can overflow.
			steps = append(steps, withPartition(KeyRange{
				BoundColumn: packedColumnName,
				From:        PackSlotValues(fromValues, slotBits), FromInclusive: true,
				To: PackSlotPrefixBound(prefixValues, slotBits, true), ToInclusive: true,
			}))
		}
		return steps, nil
	}

	packedValues := make([]any, 0, len(valueGroups))
	for _, values := range valueGroups {
		packedValues = append(packedValues, PackSlotValues(values, slotBits))
	}
	return []AccessStep{withPartition(KeyRange{
		EqualPrefix: []KeyValues{{Column: packedColumnName, Values: packedValues}},
	})}, nil
}

// planPackedIndexSteps serves a packed secondary index: every source column but the last pinned by
// equality / IN, the last one by equality or a range. The range column is the last slot, so a bound
// packs exactly; a one-sided bound still spills into the neighbouring prefixes, which the
// post-filter drops (RequiresPostFilter). One step per prefix combination: a secondary index answers
// one indexed value or range per read.
func planPackedIndexSteps(source *IndexSource, statements []ColumnStatement) ([]AccessStep, error) {
	packedIndex := source.PackedIndex
	sourceColumnNames := packedIndex.SourceColumnNames
	slotBits := packedIndex.SlotBitsPerColumn
	packedColumnName := packedIndex.PackedColumnName
	// A global index restricts IN to the first component, which bounds its fan-out.
	restrictsInToFirst := packedIndex.PartitionColumnName == ""
	refuse := func(reason string) ([]AccessStep, error) {
		return nil, fmt.Errorf(`packed index "%v" cannot serve this query: %v`, source.Name, reason)
	}

	statementByColumn := map[string]ColumnStatement{}
	for _, statement := range statements {
		statementByColumn[statement.Col] = statement
	}

	prefixValueGroups := [][]int64{{}}
	for columnIndex, columnName := range sourceColumnNames[:len(sourceColumnNames)-1] {
		statement, hasStatement := statementByColumn[columnName]
		if !hasStatement {
			return refuse(fmt.Sprintf(`no predicate on "%v"`, columnName))
		}
		if restrictsInToFirst && columnIndex > 0 && statement.Operator == "IN" {
			return refuse(fmt.Sprintf(`IN on "%v" behind the first column`, columnName))
		}
		values := []int64{}
		switch statement.Operator {
		case "=":
			values = append(values, ConvertToInt64(statement.Value))
		case "IN":
			for _, value := range statement.Values {
				values = append(values, ConvertToInt64(value))
			}
		default:
			return refuse(fmt.Sprintf(`operator %v on "%v"`, statement.Operator, columnName))
		}
		prefixValueGroups = expandValueGroups(prefixValueGroups, values)
	}

	lastColumnName := sourceColumnNames[len(sourceColumnNames)-1]
	lastStatement, hasStatement := statementByColumn[lastColumnName]
	if !hasStatement {
		return refuse(fmt.Sprintf(`no predicate on "%v"`, lastColumnName))
	}
	packWith := func(prefixValues []int64, value any) uint64 {
		return PackSlotValues(append(slices.Clone(prefixValues), ConvertToInt64(value)), slotBits)
	}

	steps := make([]AccessStep, 0, len(prefixValueGroups))
	for _, prefixValues := range prefixValueGroups {
		switch {
		case lastStatement.Operator == "=":
			steps = append(steps, rangeStep(KeyRange{
				EqualPrefix: []KeyValues{{Column: packedColumnName, Values: []any{packWith(prefixValues, lastStatement.Value)}}},
			}))
		case lastStatement.Operator == "BETWEEN":
			if len(lastStatement.From) == 0 || len(lastStatement.To) == 0 {
				return refuse(fmt.Sprintf(`incomplete BETWEEN on "%v"`, lastColumnName))
			}
			steps = append(steps, rangeStep(KeyRange{
				BoundColumn: packedColumnName,
				From:        packWith(prefixValues, lastStatement.From[0].Value), FromInclusive: true,
				To: packWith(prefixValues, lastStatement.To[0].Value), ToInclusive: true,
			}))
		case isRangeOperator(lastStatement.Operator):
			keyRange := KeyRange{}
			boundFromOperator(&keyRange, packedColumnName, lastStatement.Operator, packWith(prefixValues, lastStatement.Value))
			steps = append(steps, rangeStep(keyRange))
		default:
			return refuse(fmt.Sprintf(`operator %v on "%v"`, lastStatement.Operator, lastColumnName))
		}
	}
	return steps, nil
}

// planGroupHashSteps serves a cache group's hash column: every combination of the source columns'
// values is hashed, one read per distinct hash. A slice group stores its hashes in a collection,
// read by element.
func planGroupHashSteps(source *IndexSource, statements []ColumnStatement) ([]AccessStep, error) {
	indexGroup := source.IndexGroup
	valueGroups := [][]int64{{}}
	for _, sourceColumn := range indexGroup.SourceColumns {
		statement, err := FindIndexGroupStatement(sourceColumn.Column.GetName(), statements)
		if err != nil {
			return nil, err
		}
		columnValues, err := ResolveIndexGroupQueryValues(statement)
		if err != nil {
			return nil, err
		}
		if len(columnValues) == 0 {
			return nil, fmt.Errorf(`QueryIndexGroup has no values for column "%v"`, statement.Col)
		}
		valueGroups = expandValueGroups(valueGroups, columnValues)
	}

	hashColumnName := source.Column.GetName()
	seenHashes := map[int32]bool{}
	steps := make([]AccessStep, 0, len(valueGroups))
	for _, valueGroup := range valueGroups {
		hashValue := HashInt64(valueGroup...)
		if seenHashes[hashValue] {
			continue
		}
		seenHashes[hashValue] = true
		if indexGroup.UsesCollectionValues {
			steps = append(steps, AccessStep{Element: &ElementLookup{Column: hashColumnName, Value: hashValue}})
			continue
		}
		steps = append(steps, rangeStep(KeyRange{EqualPrefix: []KeyValues{{Column: hashColumnName, Values: []any{hashValue}}}}))
	}
	return steps, nil
}

// FindIndexGroupStatement returns the query statement on one of a cache group's source columns.
func FindIndexGroupStatement(columnName string, statements []ColumnStatement) (ColumnStatement, error) {
	for _, statement := range statements {
		if statement.Col == columnName {
			return statement, nil
		}
	}
	return ColumnStatement{}, fmt.Errorf(`QueryIndexGroup missing statement for column "%v"`, columnName)
}

// ResolveIndexGroupQueryValues lists the values a statement selects on a group source column: a
// BETWEEN is expanded into one value per step, since a group hash has no order.
func ResolveIndexGroupQueryValues(statement ColumnStatement) ([]int64, error) {
	switch statement.Operator {
	case "=", "CONTAINS":
		return FlattenInt64Values(statement.Value), nil
	case "IN":
		columnValues := make([]int64, 0, len(statement.Values))
		for _, rawValue := range statement.Values {
			columnValues = append(columnValues, FlattenInt64Values(rawValue)...)
		}
		return columnValues, nil
	case "BETWEEN":
		if len(statement.From) == 0 || len(statement.To) == 0 {
			return nil, fmt.Errorf(`QueryIndexGroup received an invalid BETWEEN for "%v"`, statement.Col)
		}
		fromValue := ConvertToInt64(statement.From[0].Value)
		toValue := ConvertToInt64(statement.To[0].Value)
		if toValue < fromValue {
			fromValue, toValue = toValue, fromValue
		}
		columnValues := make([]int64, 0, int(toValue-fromValue)+1)
		for currentValue := fromValue; currentValue <= toValue; currentValue++ {
			columnValues = append(columnValues, currentValue)
		}
		return columnValues, nil
	default:
		return nil, fmt.Errorf(`QueryIndexGroup does not support operator "%v" on "%v"`, statement.Operator, statement.Col)
	}
}
