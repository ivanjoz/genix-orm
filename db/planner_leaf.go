package db

// Storage-independent building blocks of the query planner: value coercion, index hashes,
// composite bucketing, FixedValues enumeration and packed-slot arithmetic. They were private to
// the Scylla driver; every driver's planner and index maintenance needs the exact same results, or
// two drivers would store and look up different index entries for the same record.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"reflect"
	"slices"
	"sync"
	"time"
)

// ConvertToInt64 coerces a statement or column value to int64. A non-integer prints a diagnostic
// and yields 0, which is what every planner caller already relied on.
func ConvertToInt64(val any) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		if plainValue, isNamed := NormalizeNamedNumeric(val); isNamed {
			return ConvertToInt64(plainValue)
		}
		// The value is not an integer
		fmt.Println("Error: Value is not an integer:", v)
		return 0
	}
}

// HashInt hashes values of mixed types. The result is persisted in hash-index columns, so the
// byte layout below must never change.
func HashInt(values ...any) int32 {
	buf := new(bytes.Buffer)

	for _, anyVal := range values {
		appendHashValue(buf, anyVal)
		buf.WriteByte(0)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return int32(h.Sum32())
}

// appendHashValue writes one value into the hash buffer. A named numeric type is
// normalized to its underlying plain type and re-dispatched, so it hashes to the SAME
// bytes as the plain type it wraps — retyping a column from int8 to a named int8 must not
// silently invalidate every hash already persisted from it. Without this it would fall to
// the %v branch and hash the decimal text instead of the binary value.
func appendHashValue(buf *bytes.Buffer, anyVal any) {
	switch val := anyVal.(type) {
	case int:
		binary.Write(buf, binary.LittleEndian, int64(val))
	case int32:
		binary.Write(buf, binary.LittleEndian, val)
	case int64:
		binary.Write(buf, binary.LittleEndian, val)
	case int16:
		binary.Write(buf, binary.LittleEndian, val)
	case int8:
		binary.Write(buf, binary.LittleEndian, val)
	case float32:
		binary.Write(buf, binary.LittleEndian, val)
	case float64:
		binary.Write(buf, binary.LittleEndian, val)
	case string:
		buf.WriteString(val)
	default:
		if plainValue, isNamed := NormalizeNamedNumeric(anyVal); isNamed {
			appendHashValue(buf, plainValue)
			return
		}
		buf.WriteString(fmt.Sprintf("%v", val))
	}
}

// HashInt64 is HashInt for a list of int64 (the group-hash and composite-bucket inputs).
func HashInt64(values ...int64) int32 {
	buf := new(bytes.Buffer)

	for _, value := range values {
		binary.Write(buf, binary.LittleEndian, value)
		buf.WriteByte(0)
	}

	h := fnv.New32a()
	h.Write(buf.Bytes())
	return int32(h.Sum32())
}

// mondayUnixDayByWeekCode memoizes the week-code -> Monday conversion composite bucketing uses to
// turn week-coded ranges into a contiguous timeline. A sync.Map because planning runs on many
// request goroutines at once.
var mondayUnixDayByWeekCode sync.Map // int16 -> int16

func mondayUnixDayOfWeekCode(weekCode int16) int16 {
	if mondayUnixDay, ok := mondayUnixDayByWeekCode.Load(weekCode); ok {
		return mondayUnixDay.(int16)
	}
	if weekCode == 0 {
		return 0
	}

	year := int(weekCode/100) + 2000
	week := int(weekCode % 100)
	if week < 1 || week > 53 {
		return 0
	}

	// ISO week 1 is the week containing January 4th. Move back to Monday from that anchor.
	januaryFourth := time.Date(year, time.January, 4, 8, 0, 0, 0, time.UTC)
	weekday := int(januaryFourth.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	firstMonday := januaryFourth.AddDate(0, 0, 1-weekday)
	targetMonday := firstMonday.AddDate(0, 0, (week-1)*7)
	mondayUnixDay := int16(targetMonday.Unix() / (24 * 60 * 60))
	mondayUnixDayByWeekCode.Store(weekCode, mondayUnixDay)
	return mondayUnixDay
}

func weekIndexOfWeekCode(weekCode int64) (int64, bool) {
	// Use the Monday unix-day as the real timeline, then collapse it into contiguous week units.
	mondayUnixDay := mondayUnixDayOfWeekCode(int16(weekCode))
	if mondayUnixDay == 0 {
		return 0, false
	}
	return int64(mondayUnixDay) / 7, true
}

// NormalizeCompositeRange orders a range and, for week-coded values, converts it to contiguous
// week indexes: range planning needs contiguous numbers.
func NormalizeCompositeRange(from, to int64, isWeek bool) (int64, int64) {
	if isWeek {
		fromWeekIndex, fromOk := weekIndexOfWeekCode(from)
		toWeekIndex, toOk := weekIndexOfWeekCode(to)
		if fromOk && toOk {
			if toWeekIndex < fromWeekIndex {
				return toWeekIndex, fromWeekIndex
			}
			return fromWeekIndex, toWeekIndex
		}
	}
	if to < from {
		return to, from
	}
	return from, to
}

// CompositeBucketID is the bucket a value falls in. Bucket IDs are computed on a contiguous
// domain, using date-derived week indexes when the column holds week codes.
func CompositeBucketID(value int64, bucketSize int8, isWeek bool) int64 {
	basisValue := value
	if isWeek {
		if weekIndex, ok := weekIndexOfWeekCode(value); ok {
			basisValue = weekIndex
		}
	}
	return basisValue / int64(bucketSize)
}

// CompositeBucketSelection is one bucket of a composite-bucket cover.
type CompositeBucketSelection struct {
	BucketSize int8
	BucketID   int64
}

// StatementValuesForHash lists the values of a statement that composite hash planning can turn
// into deterministic hash inputs; only =, CONTAINS and IN qualify.
func StatementValuesForHash(statement ColumnStatement) ([]int64, bool) {
	switch statement.Operator {
	case "=", "CONTAINS":
		return []int64{ConvertToInt64(statement.Value)}, true
	case "IN":
		if len(statement.Values) == 0 {
			return nil, false
		}
		values := make([]int64, 0, len(statement.Values))
		for _, value := range statement.Values {
			values = append(values, ConvertToInt64(value))
		}
		return values, true
	default:
		return nil, false
	}
}

// FlattenInt64Values normalizes a scalar/slice/pointer numeric value for in-memory predicate checks.
func FlattenInt64Values(rawValue any) []int64 {
	if rawValue == nil {
		return nil
	}
	rv := reflect.ValueOf(rawValue)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return []int64{ConvertToInt64(rv.Interface())}
	}
	values := make([]int64, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		values = append(values, ConvertToInt64(rv.Index(i).Interface()))
	}
	return values
}

// BetweenRange returns the ordered bounds of a BETWEEN statement. The composite planner targets
// BETWEEN ranges only.
func BetweenRange(statement ColumnStatement, isWeek bool) (int64, int64, bool) {
	if statement.Operator != "BETWEEN" || len(statement.From) == 0 || len(statement.To) == 0 {
		return 0, 0, false
	}
	from, to := NormalizeCompositeRange(ConvertToInt64(statement.From[0].Value), ConvertToInt64(statement.To[0].Value), isWeek)
	return from, to, true
}

// PlanCompositeBuckets chooses the minimum statement-count bucket cover while enforcing a bounded
// total overfetch.
func PlanCompositeBuckets(from, to int64, bucketSizes []int8) []CompositeBucketSelection {
	if to < from || len(bucketSizes) == 0 {
		return nil
	}

	type interval struct {
		selection CompositeBucketSelection
		start     int64
		end       int64
		overfetch int64
	}

	maxOverfetch := int64(0)
	for _, size := range bucketSizes {
		maxOverfetch += int64(size)
	}

	maxOverfetch = maxOverfetch / int64(len(bucketSizes))
	minCoverage := from - maxOverfetch
	maxCoverage := to + maxOverfetch

	intervals := []interval{}
	// Precompute candidate bucket intervals that intersect the target range and satisfy overfetch constraints.
	for _, bucketSize := range bucketSizes {
		size := int64(bucketSize)
		if size <= 0 {
			continue
		}

		minBucketID := minCoverage / size
		maxBucketID := maxCoverage / size
		for bucketID := minBucketID; bucketID <= maxBucketID; bucketID++ {
			start := bucketID * size
			end := start + size - 1
			if end < from || start > to {
				continue
			}
			overfetch := int64(0)
			if start < from {
				overfetch += from - start
			}
			if end > to {
				overfetch += end - to
			}
			if overfetch > maxOverfetch {
				continue
			}
			intervals = append(intervals, interval{
				selection: CompositeBucketSelection{BucketSize: bucketSize, BucketID: bucketID},
				start:     start,
				end:       end,
				overfetch: overfetch,
			})
		}
	}

	type planState struct {
		count      int
		overfetch  int64
		selections []CompositeBucketSelection
		valid      bool
	}

	memo := map[string]planState{}
	var solve func(currentWeek, usedOverfetch int64) planState
	solve = func(currentWeek, usedOverfetch int64) planState {
		// DP state represents the best plan from currentWeek to end, given accumulated overfetch.
		if currentWeek > to {
			return planState{valid: true}
		}

		memoKey := fmt.Sprintf("%d|%d", currentWeek, usedOverfetch)
		if state, ok := memo[memoKey]; ok {
			return state
		}

		best := planState{valid: false}
		for _, intervalDef := range intervals {
			if intervalDef.start > currentWeek || intervalDef.end < currentWeek {
				continue
			}
			nextOverfetch := usedOverfetch + intervalDef.overfetch
			if nextOverfetch > maxOverfetch {
				continue
			}

			nextState := solve(intervalDef.end+1, nextOverfetch)
			if !nextState.valid {
				continue
			}

			candidate := planState{
				count:      nextState.count + 1,
				overfetch:  nextState.overfetch + intervalDef.overfetch,
				selections: append([]CompositeBucketSelection{intervalDef.selection}, nextState.selections...),
				valid:      true,
			}

			if !best.valid || candidate.count < best.count ||
				(candidate.count == best.count && candidate.overfetch < best.overfetch) {
				best = candidate
			}
		}

		memo[memoKey] = best
		return best
	}

	plan := solve(from, 0)
	if !plan.valid {
		return nil
	}

	return plan.selections
}

// ColumnValueRange is the inclusive range a column was declared to hold via FixedValues.
type ColumnValueRange struct {
	MinValue int64
	MaxValue int64
	// DeclaredValues enumerates every value the column may hold. It is what lets the select planner
	// fan a query out over an unconstrained key column instead of falling back to a table scan. It
	// stays nil for a range too wide to be worth enumerating, which is exactly the case where the
	// fan-out would cost more than it saves.
	DeclaredValues []int64
}

// MaxEnumerableFixedValues caps how wide a Min/Max declaration may be before its values stop being
// enumerated. It sits above the planner's own fan-out ceiling so a range that just misses the cut
// is still visible to any future caller that can afford a wider fan-out.
const MaxEnumerableFixedValues = 32

// EnumerateFixedValues lists every value a FixedValues entry pins down, preferring the explicit
// list over the Min/Max span. A span wider than MaxEnumerableFixedValues yields nil: the column is
// still range-bounded for slot sizing, just not enumerable.
func EnumerateFixedValues(declared FixedValues, minValue, maxValue int64) []int64 {
	if len(declared.Values) > 0 {
		return slices.Clone(declared.Values)
	}
	if maxValue-minValue+1 > MaxEnumerableFixedValues {
		return nil
	}
	values := make([]int64, 0, maxValue-minValue+1)
	for value := minValue; value <= maxValue; value++ {
		values = append(values, value)
	}
	return values
}

// ChunkValues splits an IN list into chunks of at most chunkSize values.
func ChunkValues(values []any, chunkSize int) [][]any {
	if len(values) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = 1
	}

	valueChunks := make([][]any, 0, (len(values)+chunkSize-1)/chunkSize)
	for startIndex := 0; startIndex < len(values); startIndex += chunkSize {
		endIndex := min(startIndex+chunkSize, len(values))
		valueChunks = append(valueChunks, slices.Clone(values[startIndex:endIndex]))
	}
	return valueChunks
}

// NarrowInt64ToColumnWidth returns value as the column's own Go integer type. The synthesized
// values come from the schema as int64, but they end up bound straight into a query and, on hash
// views, fed to HashInt — which writes each width differently, so an int64 1 would not match the
// int8 1 the row was written with.
func NarrowInt64ToColumnWidth(value int64, column IColInfo) any {
	switch column.GetType().FieldType {
	case "int8":
		return int8(value)
	case "int16":
		return int16(value)
	case "int32":
		return int32(value)
	case "int":
		return int(value)
	}
	return value
}

// Virtual packed columns (range views, delta views, packed indexes) never reach the application, so
// they spend the sign bit as well: the whole 64 (or 32) bits hold the unsigned layout.
const (
	PackedVirtualInt64Bits = 64
	PackedVirtualInt32Bits = 32
)

// PackedVirtualBudgetBits is the bit budget of a virtual packed column.
func PackedVirtualBudgetBits(isInt32 bool) int64 {
	if isInt32 {
		return PackedVirtualInt32Bits
	}
	return PackedVirtualInt64Bits
}

// PackRowValues packs one row's source column values. It checks every slot first so a value that
// does not fit panics naming the table, the packed column and the offending source column, instead
// of the anonymous slot index PackSlotValues can report.
func PackRowValues(tableName, packedColumnName string, sourceColumns []IColInfo, values []int64, slotBits []int64) uint64 {
	for slotIndex, value := range values {
		if value < 0 || uint64(value) > SlotMaxValue(slotBits[slotIndex]) {
			panic(fmt.Sprintf(`Table "%v": packed column "%v" cannot hold %v in "%v": its %v-bit slot takes 0..%v`,
				tableName, packedColumnName, value, sourceColumns[slotIndex].GetName(), slotBits[slotIndex], SlotMaxValue(slotBits[slotIndex])))
		}
	}
	return PackSlotValues(values, slotBits)
}

// SumSlotBits adds the widths of the slots from fromIndex to the end.
func SumSlotBits(slotBits []int64, fromIndex int) int64 {
	totalBits := int64(0)
	for _, bits := range slotBits[fromIndex:] {
		totalBits += bits
	}
	return totalBits
}
