package framesql

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ivanjoz/genix-orm/dataframe"
)

// addFile stores a file of rows given as {rowID, sum0, sum1, ...}.
func (source *memorySource) addFile(keys [dataframe.MaxKeys]int64, rows ...[]int64) {
	file := dataframe.File{Sums: make([][]int64, len(rows[0])-1)}
	for _, row := range rows {
		file.RowIDs = append(file.RowIDs, row[0])
		for i, sum := range row[1:] {
			file.Sums[i] = append(file.Sums[i], sum)
		}
	}
	source.files[keys] = file
}

func runOn(t *testing.T, source *memorySource, frameName, statementText string, options Options) Result {
	t.Helper()
	result, err := Run(frameName, statementText, testFrames(source), testResolvers(), options)
	if err != nil {
		t.Fatalf("%s: %v", statementText, err)
	}
	return result
}

// formatResult writes a result as "name:kind=v1,v2 | …", NaN as "-".
func formatResult(result Result) string {
	columns := make([]string, len(result.Columns))
	for i, column := range result.Columns {
		values := make([]string, len(column.Values))
		for j, value := range column.Values {
			values[j] = "-"
			if !math.IsNaN(value) {
				values[j] = fmt.Sprint(value)
			}
		}
		columns[i] = fmt.Sprintf("%s:%d=%s", column.Name, column.Kind, strings.Join(values, ","))
	}
	return strings.Join(columns, " | ")
}

func TestRunAggregatesTheFiles(t *testing.T) {
	source := newMemorySource()
	// day-product: {product, quantity, amount} per day.
	source.addFile([dataframe.MaxKeys]int64{1}, []int64{10, 2, 1000}, []int64{11, 1, 500})
	source.addFile([dataframe.MaxKeys]int64{2}, []int64{10, 1, 700}, []int64{12, 5, 0})
	source.addFile([dataframe.MaxKeys]int64{3}, []int64{11, 3, 1500})

	cases := []struct{ statement, want string }{
		{
			// Top products: COUNT(*) is the days each one has a row, AVG the average over them.
			"SELECT product_id, SUM(amount) AS revenue, COUNT(*) AS days, AVG(amount) WHERE fecha BETWEEN 1 AND 3 GROUP BY product_id ORDER BY revenue DESC LIMIT 2",
			"product_id:3=11,10 | revenue:2=2000,1700 | days:0=2,2 | amount:2=1000,850",
		},
		{
			// Without GROUP BY, one row even when no file matched; AVG over no row has no value.
			"SELECT SUM(amount), COUNT(*), AVG(quantity) WHERE fecha = 9",
			"amount:2=0 | count:0=0 | quantity:4=-",
		},
		{
			// A day without the filtered product adds no group.
			"SELECT fecha, SUM(quantity) WHERE fecha BETWEEN 1 AND 3 AND product_id = 12 GROUP BY fecha",
			"fecha:1=2 | quantity:0=5",
		},
		{
			// A division by zero has no value, and sorts last either way.
			"SELECT product_id, SUM(quantity) / SUM(amount) AS ratio WHERE fecha BETWEEN 1 AND 3 GROUP BY 1 ORDER BY ratio DESC",
			"product_id:3=11,10,12 | ratio:4=0.002,0.0017647058823529412,-",
		},
		{
			// Without ORDER BY, the groups ascend; ORDER BY may name what SELECT doesn't return.
			"SELECT product_id WHERE fecha IN (1, 3) GROUP BY product_id",
			"product_id:3=10,11",
		},
		{
			"SELECT product_id WHERE fecha IN PERIOD('1..3') GROUP BY product_id ORDER BY SUM(quantity) DESC",
			"product_id:3=12,11,10",
		},
	}
	for _, testCase := range cases {
		if got := formatResult(runOn(t, source, "day_product", testCase.statement, Options{})); got != testCase.want {
			t.Errorf("%s\n got  %s\n want %s", testCase.statement, got, testCase.want)
		}
	}

	result := runOn(t, source, "day_product", cases[0].statement, Options{})
	if !result.Truncated || result.RowCount != 2 || result.FilesRead != 3 || result.Snapshot != 77 || result.FromKey != 1 || result.ToKey != 3 {
		t.Errorf("got Truncated %v, RowCount %d, FilesRead %d, Snapshot %d, keys %d..%d",
			result.Truncated, result.RowCount, result.FilesRead, result.Snapshot, result.FromKey, result.ToKey)
	}
	// The columns describe themselves to the caller that shows them.
	var described []string
	for _, column := range result.Columns {
		described = append(described, fmt.Sprintf("%s/%s/%v/%s", column.Label, column.Collection, column.IsGroup, column.Aggregate))
	}
	if want := []string{"Product/products/true/", "Amount//false/SUM", "//false/COUNT", "Amount//false/AVG"}; !slices.Equal(described, want) {
		t.Errorf("got  %v\nwant %v", described, want)
	}
	weekly := runOn(t, source, "day_product", "SELECT WEEK(fecha), fecha, SUM(amount) WHERE fecha BETWEEN 1 AND 3 GROUP BY WEEK(fecha), fecha", Options{})
	if weekly.Columns[0].Period != "week" || weekly.Columns[1].Period != "day" || weekly.Columns[2].Period != "" {
		t.Errorf("a group of the frame's days carries its period: %+v", weekly.Columns)
	}
}

func TestRunPrunesFilesAndCapsTheRead(t *testing.T) {
	source := newMemorySource()
	for clientID := int64(1); clientID <= 3; clientID++ {
		source.addFile([dataframe.MaxKeys]int64{1, clientID}, []int64{10, 1, 100})
	}
	frames := testFrames(source)

	result := runOn(t, source, "day_client_product", "SELECT SUM(amount) WHERE fecha = 1 AND client_id IN (1, 3)", Options{})
	if result.FilesRead != 2 || result.Columns[0].Values[0] != 200 {
		t.Errorf("IN on a later Key reads only its files: read %d, sum %v", result.FilesRead, result.Columns[0].Values)
	}
	runOn(t, source, "day_client_product", "SELECT SUM(amount) WHERE fecha = 1 AND client_id = 2", Options{})
	if !slices.Equal(source.readPinned, []int64{2}) {
		t.Errorf("= on the second Key is pinned in the read: %v", source.readPinned)
	}

	_, err := Run("day_client_product", "SELECT SUM(amount) WHERE fecha = 1", frames, Resolvers{}, Options{MaxFiles: 2})
	want := "`day_client_product` would read 3 files; the maximum is 2: narrow `fecha` or add `client_id = …`"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
}

func TestWeekAndMonthAreTheirFirstDay(t *testing.T) {
	unixDay := func(year int, month time.Month, day int) int64 {
		return time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / 86400
	}
	sunday := unixDay(2026, time.October, 4)
	if got := transformWeek.apply(sunday); got != unixDay(2026, time.September, 28) {
		t.Errorf("WEEK of a Sunday is the Monday before it: got %d", got)
	}
	if got := transformWeek.apply(unixDay(2026, time.September, 28)); got != unixDay(2026, time.September, 28) {
		t.Errorf("WEEK of a Monday is itself: got %d", got)
	}
	if got := transformMonth.apply(sunday); got != unixDay(2026, time.October, 1) {
		t.Errorf("MONTH is the 1st: got %d", got)
	}
	weeks := PeriodStarts("week", unixDay(2026, time.September, 30), sunday+1)
	if !slices.Equal(weeks, []int64{unixDay(2026, time.September, 28), sunday + 1}) {
		t.Errorf("PeriodStarts of weeks: %v", weeks)
	}
	months := PeriodStarts("month", unixDay(2026, time.August, 31), sunday)
	if !slices.Equal(months, []int64{unixDay(2026, time.August, 1), unixDay(2026, time.September, 1), unixDay(2026, time.October, 1)}) {
		t.Errorf("PeriodStarts of months: %v", months)
	}
	if days := PeriodStarts("day", 5, 7); !slices.Equal(days, []int64{5, 6, 7}) {
		t.Errorf("PeriodStarts of days: %v", days)
	}
}

// bruteForceCase is a random statement and the naive way to compute it: every row of every file,
// filtered, grouped and computed one by one.
type bruteForceCase struct {
	conditions []string
	// keepsKey and keepsRow are the same filters as functions; nil keeps all.
	keepsKey  [dataframe.MaxKeys]func(int64) bool
	keepsRow  func(int64) bool
	groups    []string
	groupOf   []func(keys [dataframe.MaxKeys]int64, rowID int64) int64
	items     []string
	itemOf    []func(sums []int64, count int64) float64
	orderDesc bool
	hasOrder  bool
	limit     int
}

func (testCase bruteForceCase) statement() string {
	// The items may repeat, or share a name (SUM(s0) and AVG(s0) are both s0): each gets an alias.
	items := slices.Clone(testCase.groups)
	for i, item := range testCase.items {
		items = append(items, fmt.Sprintf("%s AS i%d", item, i))
	}
	statementText := "SELECT " + strings.Join(items, ", ") + " WHERE " + strings.Join(testCase.conditions, " AND ")
	if len(testCase.groups) > 0 {
		statementText += " GROUP BY " + strings.Join(testCase.groups, ", ")
	}
	if testCase.hasOrder {
		statementText += " ORDER BY " + testCase.items[0] + map[bool]string{true: " DESC", false: ""}[testCase.orderDesc]
	}
	if testCase.limit > 0 {
		statementText += fmt.Sprintf(" LIMIT %d", testCase.limit)
	}
	return statementText
}

func (testCase bruteForceCase) bruteForce(source *memorySource) (rows [][]float64, isTruncated bool) {
	type group struct {
		key   []int64
		sums  []int64
		count int64
	}
	groupByKey := map[string]*group{}
	for keys, file := range source.files {
		if !keepsAll(testCase.keepsKey[:], keys[:]) {
			continue
		}
		for row, rowID := range file.RowIDs {
			if testCase.keepsRow != nil && !testCase.keepsRow(rowID) {
				continue
			}
			key := make([]int64, len(testCase.groupOf))
			for i, groupOf := range testCase.groupOf {
				key[i] = groupOf(keys, rowID)
			}
			current := groupByKey[fmt.Sprint(key)]
			if current == nil {
				current = &group{key: key, sums: make([]int64, len(file.Sums))}
				groupByKey[fmt.Sprint(key)] = current
			}
			for i := range file.Sums {
				current.sums[i] += file.Sums[i][row]
			}
			current.count++
		}
	}
	if len(testCase.groups) == 0 && len(groupByKey) == 0 {
		groupByKey[""] = &group{sums: make([]int64, 2)}
	}
	for _, current := range groupByKey {
		row := make([]float64, 0, len(current.key)+len(testCase.itemOf))
		for _, value := range current.key {
			row = append(row, float64(value))
		}
		for _, itemOf := range testCase.itemOf {
			row = append(row, itemOf(current.sums, current.count))
		}
		rows = append(rows, row)
	}
	groupCount := len(testCase.groups)
	slices.SortFunc(rows, func(a, b []float64) int {
		if testCase.hasOrder {
			if order := compareValues(a[groupCount], b[groupCount], testCase.orderDesc); order != 0 {
				return order
			}
		}
		return slices.Compare(a[:groupCount], b[:groupCount])
	})
	limit := testCase.limit
	if limit == 0 {
		limit = defaultMaxRows
	}
	return rows[:min(limit, len(rows))], len(rows) > limit
}

func keepsAll(filters []func(int64) bool, values []int64) bool {
	for i, keeps := range filters {
		if keeps != nil && !keeps(values[i]) {
			return false
		}
	}
	return true
}

// randomCondition is "= v", "IN (v, w)" or, half the time, "BETWEEN v AND w" on column, with its
// function.
func randomCondition(random *rand.Rand, column string, low, high int64) (string, func(int64) bool) {
	value := func() int64 { return low + random.Int64N(high-low+1) }
	switch random.IntN(4) {
	case 0:
		v := value()
		return fmt.Sprintf("%s = %d", column, v), func(x int64) bool { return x == v }
	case 1:
		v, w := value(), value()
		return fmt.Sprintf("%s IN (%d, %d)", column, v, w), func(x int64) bool { return x == v || x == w }
	}
	v := value()
	w := v + random.Int64N(25)
	return fmt.Sprintf("%s BETWEEN %d AND %d", column, v, w), func(x int64) bool { return x >= v && x <= w }
}

func TestEngineMatchesBruteForce(t *testing.T) {
	random := rand.New(rand.NewPCG(7, 11))
	mondayOf := func(day int64) int64 {
		for time.Unix(day*86400, 0).UTC().Weekday() != time.Monday {
			day--
		}
		return day
	}
	firstOfMonth := func(day int64) int64 {
		for time.Unix(day*86400, 0).UTC().Day() != 1 {
			day--
		}
		return day
	}
	sumItems := map[string]func(sums []int64, count int64) float64{
		"SUM(s0)":            func(sums []int64, _ int64) float64 { return float64(sums[0]) },
		"SUM(s1)":            func(sums []int64, _ int64) float64 { return float64(sums[1]) },
		"COUNT(*)":           func(_ []int64, count int64) float64 { return float64(count) },
		"AVG(s0)":            func(sums []int64, count int64) float64 { return divide(float64(sums[0]), float64(count)) },
		"AVG(s1)":            func(sums []int64, count int64) float64 { return math.Round(divide(float64(sums[1]), float64(count))) },
		"SUM(s1) / COUNT(*)": func(sums []int64, count int64) float64 { return math.Round(divide(float64(sums[1]), float64(count))) },
		"SUM(s0) / SUM(s1)":  func(sums []int64, _ int64) float64 { return divide(float64(sums[0]), float64(sums[1])) },
		"SUM(s1) * 3 - SUM(s1)": func(sums []int64, _ int64) float64 {
			return math.Round(float64(sums[1])*3 - float64(sums[1]))
		},
	}
	itemNames := slices.Sorted(maps.Keys(sumItems))

	roundsWithRows := 0
	for round := range 400 {
		keyCount := 1 + random.IntN(dataframe.MaxKeys)
		keyNames := []string{"day", "k1", "k2"}[:keyCount]
		source := newMemorySource()
		frame := Frame{Name: "f", Keys: []Column{{Name: "day", Kind: KindDay}}, Rows: Column{Name: "r"},
			Sums: []Column{{Name: "s0"}, {Name: "s1", Kind: KindCents}}, Source: source}
		for _, name := range keyNames[1:] {
			frame.Keys = append(frame.Keys, Column{Name: name})
		}
		// 40 days from a Wednesday at the end of a month, up to 3 × 3 files a day, rows 0..7.
		for day := int64(20480); day < 20520; day++ {
			for k1 := range int64(3) {
				for k2 := range int64(3) {
					if (keyCount < 2 && k1 > 0) || (keyCount < 3 && k2 > 0) || random.IntN(10) < 4 {
						continue
					}
					var rows [][]int64
					for rowID := range int64(8) {
						if random.IntN(2) == 0 {
							rows = append(rows, []int64{rowID, random.Int64N(1050) - 50, random.Int64N(100000)})
						}
					}
					if len(rows) > 0 {
						source.addFile([dataframe.MaxKeys]int64{day, k1, k2}, rows...)
					}
				}
			}
		}

		testCase := bruteForceCase{}
		firstCondition, keepsDay := randomCondition(random, "day", 20475, 20520)
		testCase.conditions, testCase.keepsKey[0] = []string{firstCondition}, keepsDay
		for i := 1; i < keyCount; i++ {
			if random.IntN(2) == 0 {
				var condition string
				condition, testCase.keepsKey[i] = randomCondition(random, keyNames[i], 0, 3)
				testCase.conditions = append(testCase.conditions, condition)
			}
		}
		if random.IntN(3) == 0 {
			var condition string
			condition, testCase.keepsRow = randomCondition(random, "r", 0, 8)
			testCase.conditions = append(testCase.conditions, condition)
		}

		groupChoices := map[string]func(keys [dataframe.MaxKeys]int64, rowID int64) int64{
			"r":          func(_ [dataframe.MaxKeys]int64, rowID int64) int64 { return rowID },
			"WEEK(day)":  func(keys [dataframe.MaxKeys]int64, _ int64) int64 { return mondayOf(keys[0]) },
			"MONTH(day)": func(keys [dataframe.MaxKeys]int64, _ int64) int64 { return firstOfMonth(keys[0]) },
		}
		for i, name := range keyNames {
			groupChoices[name] = func(keys [dataframe.MaxKeys]int64, _ int64) int64 { return keys[i] }
		}
		for _, name := range slices.Sorted(maps.Keys(groupChoices)) {
			if random.IntN(2) == 0 {
				testCase.groups = append(testCase.groups, name)
				testCase.groupOf = append(testCase.groupOf, groupChoices[name])
			}
		}
		random.Shuffle(len(testCase.groups), func(i, j int) {
			testCase.groups[i], testCase.groups[j] = testCase.groups[j], testCase.groups[i]
			testCase.groupOf[i], testCase.groupOf[j] = testCase.groupOf[j], testCase.groupOf[i]
		})
		for range 1 + random.IntN(3) {
			name := itemNames[random.IntN(len(itemNames))]
			testCase.items = append(testCase.items, name)
			testCase.itemOf = append(testCase.itemOf, sumItems[name])
		}
		testCase.hasOrder, testCase.orderDesc = random.IntN(2) == 0, random.IntN(2) == 0
		if random.IntN(2) == 0 {
			testCase.limit = 1 + random.IntN(10)
		}

		statementText := testCase.statement()
		result, err := Run("f", statementText, []Frame{frame}, Resolvers{}, Options{})
		if err != nil {
			t.Fatalf("round %d: %s: %v", round, statementText, err)
		}
		wantRows, wantTruncated := testCase.bruteForce(source)
		if len(wantRows) > 1 {
			roundsWithRows++
		}
		if result.RowCount != len(wantRows) || result.Truncated != wantTruncated {
			t.Fatalf("round %d: %s\n got %d rows (truncated %v), want %d (truncated %v)", round, statementText, result.RowCount, result.Truncated, len(wantRows), wantTruncated)
		}
		for row, wantRow := range wantRows {
			for i, column := range result.Columns {
				got, want := column.Values[row], wantRow[i]
				if got != want && !(math.IsNaN(got) && math.IsNaN(want)) {
					t.Fatalf("round %d: %s\n row %d column %s: got %v, want %v", round, statementText, row, column.Name, got, want)
				}
			}
		}
	}
	// Most statements must return several rows, or the comparison proves little.
	if roundsWithRows < 200 {
		t.Fatalf("only %d of 400 rounds returned more than one row", roundsWithRows)
	}
}

func divide(a, b float64) float64 {
	if b == 0 {
		return math.NaN()
	}
	return a / b
}
