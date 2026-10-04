package framesql

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ivanjoz/genix-orm/dynamo/dataframe"
)

// memorySource is a Source over files held in memory. It honours Scan's contract, files in a random
// order included, and records what the last read was asked for.
type memorySource struct {
	files    map[[dataframe.MaxKeys]int64]dataframe.File
	random   *rand.Rand
	snapshot int64

	readFrom, readTo int64
	readPinned       []int64
	readFiles        int
}

func newMemorySource() *memorySource {
	return &memorySource{files: map[[dataframe.MaxKeys]int64]dataframe.File{}, random: rand.New(rand.NewPCG(1, 2)), snapshot: 77}
}

func (source *memorySource) Scan(fromKey, toKey int64, pinnedKeys []int64,
	selectFiles func(fileKeys [][dataframe.MaxKeys]int64) ([][dataframe.MaxKeys]int64, error),
	fn func(keys [dataframe.MaxKeys]int64, file dataframe.File) error) (int64, error) {
	source.readFrom, source.readTo, source.readPinned = fromKey, toKey, slices.Clone(pinnedKeys)
	var listed [][dataframe.MaxKeys]int64
	for keys := range source.files {
		if keys[0] >= fromKey && keys[0] <= toKey && slices.Equal(keys[1:1+len(pinnedKeys)], pinnedKeys) {
			listed = append(listed, keys)
		}
	}
	selected, err := selectFiles(listed)
	if err != nil {
		return 0, err
	}
	source.random.Shuffle(len(selected), func(i, j int) { selected[i], selected[j] = selected[j], selected[i] })
	source.readFiles = len(selected)
	for _, keys := range selected {
		if err := fn(keys, source.files[keys]); err != nil {
			return 0, err
		}
	}
	return source.snapshot, nil
}

var (
	fechaColumn    = Column{Name: "fecha", Label: "Date", Kind: KindDay}
	productColumn  = Column{Name: "product_id", Label: "Product", Kind: KindRef, Collection: "products"}
	clientColumn   = Column{Name: "client_id", Kind: KindRef, Collection: "clients"}
	storeColumn    = Column{Name: "store_id"}
	quantityColumn = Column{Name: "quantity", Label: "Quantity"}
	amountColumn   = Column{Name: "amount", Label: "Amount", Kind: KindCents}
)

// testFrames are a 1-, a 2- and a 3-key frame, all on one source.
func testFrames(source Source) []Frame {
	sums := []Column{quantityColumn, amountColumn}
	return []Frame{
		{Name: "day-product", Keys: []Column{fechaColumn}, Rows: productColumn, Sums: sums, Source: source},
		{Name: "day-client-product", Keys: []Column{fechaColumn, clientColumn}, Rows: productColumn, Sums: sums, Source: source},
		{Name: "day-store-client", Keys: []Column{fechaColumn, storeColumn, clientColumn}, Rows: productColumn, Sums: sums, Source: source},
	}
}

var errAskedTheUser = errors.New("asked the user")

// testResolvers read PERIOD('a..b') as the days a to b, and resolve a name to the numbers among its
// words ('milk 1 2' is 1 and 2; 'none' is no record; 'ambiguous' asks the user).
func testResolvers() Resolvers {
	return Resolvers{
		Period: func(text string) (int64, int64, error) {
			fromText, toText, isRange := strings.Cut(text, "..")
			from, fromErr := strconv.ParseInt(fromText, 10, 64)
			to, toErr := strconv.ParseInt(toText, 10, 64)
			if !isRange || fromErr != nil || toErr != nil {
				return 0, 0, fmt.Errorf("the period %q is not a..b", text)
			}
			return from, to, nil
		},
		Names: func(column Column, names []string) ([]int64, error) {
			var ids []int64
			for _, name := range names {
				if name == "ambiguous" {
					return nil, errAskedTheUser
				}
				for _, word := range strings.Fields(name) {
					if id, err := strconv.ParseInt(word, 10, 64); err == nil {
						ids = append(ids, id)
					}
				}
			}
			return ids, nil
		},
	}
}

// planFor plans a statement on the frame its FROM names: the tables read shorter with it.
func planFor(t *testing.T, statementText string) (*queryPlan, error) {
	t.Helper()
	parsed, err := parse(statementText)
	if err != nil {
		t.Fatalf("parse(%q): %v", statementText, err)
	}
	statement, err := Prepare(parsed.frameName, statementText, testFrames(newMemorySource()), Options{})
	if err != nil {
		return nil, err
	}
	return statement.bindConditions(testResolvers())
}

func TestPlanNamesTheItems(t *testing.T) {
	statement, err := Prepare("day_product", "SELECT WEEK(fecha), product_id, SUM(amount), AVG(quantity), COUNT(*), SUM(amount) / COUNT(*) WHERE fecha = 1 GROUP BY WEEK(fecha), product_id SORT BY amount DESC",
		testFrames(newMemorySource()), Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan := statement.planner.plan
	var got []string
	for _, column := range statement.Columns() {
		got = append(got, fmt.Sprintf("%s/%s/%s/%s/%v", column.Name, column.Label, column.Aggregate, column.Period, column.IsGroup))
	}
	want := []string{"week/Date//week/true", "product_id/Product///true", "amount/Amount/SUM//false", "quantity/Quantity/AVG//false",
		"count//COUNT//false", "SUM(amount) / COUNT(*)////false"}
	if !slices.Equal(got, want) {
		t.Errorf("got  %v\nwant %v", got, want)
	}
	if len(plan.order) != 1 || plan.order[0].item != 2 || !plan.order[0].isDescending || len(plan.items) != plan.visibleItems {
		t.Errorf("SORT BY amount is the item SUM(amount): %+v", plan.order)
	}
}

func TestPlanPushdown(t *testing.T) {
	cases := []struct {
		where string
		// want is "from..to pinned=[…] files=<Keys filtered> rows=<filtered> unpinned=<key>".
		want string
	}{
		{"fecha BETWEEN 10 AND 20", "10..20 pinned=[] files=--- rows=false unpinned=store_id"},
		{"fecha = 10 AND store_id = 3 AND client_id = 4", "10..10 pinned=[3 4] files=--- rows=false unpinned="},
		{"fecha = 10 AND client_id = 4", "10..10 pinned=[] files=--k rows=false unpinned=store_id"},
		{"fecha = 10 AND store_id IN (3, 5) AND client_id = 4", "10..10 pinned=[] files=-kk rows=false unpinned="},
		{"fecha IN (10, 12) AND store_id = 3", "10..12 pinned=[3] files=k-- rows=false unpinned=client_id"},
		{"fecha IN PERIOD('5..9') AND store_id IN (3) AND product_id IN ('milk 1 2', 7)", "5..9 pinned=[3] files=--- rows=true unpinned=client_id"},
	}
	for _, testCase := range cases {
		plan, err := planFor(t, "SELECT SUM(amount) FROM day_store_client WHERE "+testCase.where)
		if err != nil {
			t.Fatalf("%s: %v", testCase.where, err)
		}
		filteredKeys := ""
		for _, filter := range plan.keyFilters {
			filteredKeys += map[bool]string{true: "k", false: "-"}[filter != nil]
		}
		got := fmt.Sprintf("%d..%d pinned=%v files=%s rows=%v unpinned=%s", plan.fromKey, plan.toKey, plan.pinnedKeys, filteredKeys, plan.rowFilter != nil, plan.unpinnedKey)
		if got != testCase.want {
			t.Errorf("%s\n got  %s\n want %s", testCase.where, got, testCase.want)
		}
	}

	plan, _ := planFor(t, "SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND product_id IN ('milk 1 2', 7)")
	if !plan.rowFilter.matches(1) || !plan.rowFilter.matches(2) || !plan.rowFilter.matches(7) || plan.rowFilter.matches(3) {
		t.Errorf("the names and numbers of an IN list are one filter: %v", plan.rowFilter.set)
	}
}

func TestPlanErrorsSayHowToRewrite(t *testing.T) {
	cases := []struct{ statement, want string }{
		{"SELECT SUM(amount) FROM sales WHERE fecha = 1", "there is no frame `sales`; the frames are: day_product, day_client_product, day_store_client"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND client_id = 4", "`day_product` has no column `client_id`; frames with it: `day_client_product`, `day_store_client`"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY region", "`day_product` has no column `region`; its columns are: fecha, product_id, quantity, amount"},
		{"SELECT SUM(amount) FROM day_product", "WHERE must bound `fecha` on `day_product`, such as fecha IN PERIOD(\"D-13..D\")"},
		{"SELECT SUM(amount) FROM day_product WHERE product_id = 3", "WHERE must bound `fecha` on `day_product`, such as fecha IN PERIOD(\"D-13..D\")"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha BETWEEN 1 AND 500", "the range of `fecha` covers 500 values; the maximum is 400"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha IN (1, 401)", "the range of `fecha` covers 401 values; the maximum is 400"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND amount = 5", "`amount` is a summed value: WHERE filters fecha, product_id"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND fecha = 2", "one condition per column: `fecha` has two; use IN (…) for several values"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND product_id IN PERIOD('1..2')", "PERIOD() takes a day column; `product_id` is not one"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 'monday'", "`fecha` takes numbers; names in quotes go on columns of records (product_id)"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha BETWEEN 'a' AND 2", "BETWEEN takes whole numbers; for days write fecha IN PERIOD(\"…\")"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha BETWEEN 5 AND 1", "`fecha` BETWEEN 5 AND 1: the first bound is above the second"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha IN PERIOD('last week')", "PERIOD(\"last week\"): the period \"last week\" is not a..b"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND product_id = 'none'", "`product_id`: no record matches none"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY amount", "`amount` is a summed value, not a dimension: GROUP BY fecha, product_id"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY WEEK(product_id)", "WEEK() takes a day column; `product_id` is not one"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY 1", "GROUP BY takes columns, WEEK(day) or MONTH(day): `SUM(amount)` is none of them"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY 3", "GROUP BY 3: a position is a whole number from 1 to 1, the SELECT items"},
		{"SELECT product_id, SUM(amount) FROM day_product WHERE fecha = 1", "`product_id` is in SELECT but not in GROUP BY"},
		{"SELECT WEEK(fecha), SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY fecha", "`WEEK(fecha)` is in SELECT but not in GROUP BY"},
		{"SELECT amount FROM day_product WHERE fecha = 1", "`amount` is a summed value: write SUM(amount) or AVG(amount)"},
		{"SELECT SUM(product_id) FROM day_product WHERE fecha = 1", "SUM() takes one summed column: quantity, amount"},
		{"SELECT AVG(SUM(amount)) FROM day_product WHERE fecha = 1", "AVG() takes one summed column: quantity, amount"},
		{"SELECT COUNT(product_id) FROM day_product WHERE fecha = 1", "write COUNT(*): it counts the frame's rows in each group"},
		{"SELECT MAX(amount) FROM day_product WHERE fecha = 1", "MAX() is not supported: use SUM(), COUNT(*) or AVG()"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 ORDER BY TODAY()", "TODAY() is not supported here: bound days in WHERE, column IN PERIOD(\"D-6..D\")"},
		{"SELECT FOO(amount) FROM day_product WHERE fecha = 1", "unknown function FOO(): use SUM(), COUNT(*), AVG(), and WEEK() or MONTH() in GROUP BY"},
		{"SELECT SUM(amount) + SUM(quantity) FROM day_product WHERE fecha = 1", "`SUM(amount) + SUM(quantity)` adds money to a value that is not money"},
		{"SELECT SUM(amount) * SUM(amount) FROM day_product WHERE fecha = 1", "`SUM(amount) * SUM(amount)` multiplies money by money"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 ORDER BY quantity", "`quantity` is a summed value: write SUM(quantity) or AVG(quantity)"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 LIMIT 5000", "LIMIT is at most 1000"},
		{"SELECT SUM(amount), AVG(amount) FROM day_product WHERE fecha = 1", "`SUM(amount)` and `AVG(amount)` are both named `amount`: name one with AS"},
		{"SELECT fecha, SUM(quantity) AS FECHA FROM day_product WHERE fecha = 1 GROUP BY fecha", "`fecha` and `SUM(quantity)` are both named `FECHA`: name one with AS"},
	}
	for _, testCase := range cases {
		_, err := planFor(t, testCase.statement)
		if err == nil || err.Error() != testCase.want {
			t.Errorf("%s\n got  %v\n want %s", testCase.statement, err, testCase.want)
		}
	}
}

func TestPlanRunsOnTheCallersFrame(t *testing.T) {
	_, err := Prepare("day-product", "SELECT SUM(amount) FROM day_client_product WHERE fecha = 1", testFrames(newMemorySource()), Options{})
	want := "FROM `day_client_product` is not the frame `day_product` the statement runs on: leave FROM out"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}
	statement, err := Prepare("Day_Client_Product", "SELECT SUM(amount) FROM day-client-product WHERE fecha = 1", testFrames(newMemorySource()), Options{})
	if err != nil || statement.planner.frame.Name != "day-client-product" {
		t.Errorf("a frame is named with '_' or '-', in any case: %v", err)
	}
}

// A name lookup may stop the run to ask the user: every other error comes before it.
func TestPlanLooksUpNamesLast(t *testing.T) {
	for _, statementText := range []string{
		"SELECT SUM(amount) FROM day_client_product WHERE client_id = 'ambiguous' AND fecha BETWEEN 1 AND 500",
		"SELECT SUM(amount) FROM day_client_product WHERE client_id = 'ambiguous' AND fecha IN PERIOD('x')",
		"SELECT SUM(amount) FROM day_client_product WHERE client_id = 'ambiguous' AND region = 1",
		"SELECT SUM(amount) FROM day_client_product WHERE client_id = 'ambiguous'",
	} {
		if _, err := planFor(t, statementText); err == nil || errors.Is(err, errAskedTheUser) {
			t.Errorf("%s: got %v, want the statement's own error", statementText, err)
		}
	}
}

func TestPlanKeepsTheResolversErrors(t *testing.T) {
	_, err := planFor(t, "SELECT SUM(amount) FROM day_product WHERE fecha = 1 AND product_id = 'ambiguous'")
	if !errors.Is(err, errAskedTheUser) {
		t.Fatalf("a resolver's error must stay recognizable: %v", err)
	}
}

func TestPlanTypesTheItems(t *testing.T) {
	cases := []struct {
		item string
		want Kind
	}{
		{"SUM(amount)", KindCents},
		{"SUM(quantity)", KindInteger},
		{"AVG(amount)", KindCents},
		{"AVG(quantity)", KindDecimal},
		{"COUNT(*)", KindInteger},
		{"SUM(amount) / COUNT(*)", KindCents},
		{"SUM(amount) / SUM(amount)", KindDecimal},
		{"SUM(amount) - SUM(amount)", KindCents},
		{"SUM(quantity) / COUNT(*)", KindDecimal},
		{"SUM(quantity) / SUM(amount)", KindDecimal},
		{"SUM(amount) * 2", KindCents},
		{"2 * SUM(amount)", KindCents},
		{"SUM(quantity) * 1.5", KindDecimal},
		{"SUM(quantity) + COUNT(*)", KindInteger},
		{"product_id", KindRef},
		{"fecha", KindDay},
		{"WEEK(fecha)", KindDay},
		{"MONTH(fecha)", KindDay},
	}
	for _, testCase := range cases {
		plan, err := planFor(t, "SELECT "+testCase.item+" FROM day_product WHERE fecha = 1 GROUP BY product_id, fecha, WEEK(fecha), MONTH(fecha)")
		if err != nil {
			t.Fatalf("%s: %v", testCase.item, err)
		}
		if got := plan.items[0].value.resultKind; got != testCase.want {
			t.Errorf("%s is kind %d, want %d", testCase.item, got, testCase.want)
		}
	}
	plan, _ := planFor(t, "SELECT product_id FROM day_product WHERE fecha = 1 GROUP BY product_id")
	if plan.items[0].value.collection != "products" {
		t.Errorf("a record column carries its collection to the result")
	}
}
