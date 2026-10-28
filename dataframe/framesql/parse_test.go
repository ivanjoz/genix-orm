package framesql

import (
	"fmt"
	"strings"
	"testing"
)

// formatStatement writes a parsed statement back in one canonical form, so a table of statements can
// state the tree it expects as text.
func formatStatement(parsed *statement) string {
	var out strings.Builder
	items := make([]string, len(parsed.items))
	for i, item := range parsed.items {
		items[i] = formatExpr(item.value)
		if item.alias != "" {
			items[i] += " AS " + item.alias
		}
	}
	fmt.Fprintf(&out, "SELECT %s", strings.Join(items, ", "))
	if parsed.frameName != "" {
		out.WriteString(" FROM " + parsed.frameName)
	}
	for i, condition := range parsed.conditions {
		out.WriteString([]string{" WHERE ", " AND "}[min(i, 1)])
		values := make([]string, len(condition.values))
		for j, value := range condition.values {
			values[j] = fmt.Sprint(value.integer)
			if value.isName {
				values[j] = "'" + value.name + "'"
			}
		}
		switch condition.op {
		case conditionEq:
			fmt.Fprintf(&out, "%s = %s", condition.column, values[0])
		case conditionIn:
			fmt.Fprintf(&out, "%s IN (%s)", condition.column, strings.Join(values, ", "))
		case conditionBetween:
			fmt.Fprintf(&out, "%s BETWEEN %s AND %s", condition.column, values[0], values[1])
		case conditionPeriod:
			fmt.Fprintf(&out, "%s IN PERIOD('%s')", condition.column, condition.period)
		}
	}
	for i, group := range parsed.groupBy {
		out.WriteString([]string{" GROUP BY ", ", "}[min(i, 1)] + formatExpr(group))
	}
	for i, term := range parsed.orderBy {
		out.WriteString([]string{" ORDER BY ", ", "}[min(i, 1)] + formatExpr(term.value))
		if term.isDescending {
			out.WriteString(" DESC")
		}
	}
	if parsed.limit > 0 {
		fmt.Fprintf(&out, " LIMIT %d", parsed.limit)
	}
	return out.String()
}

func TestParseValidStatements(t *testing.T) {
	cases := []struct{ statement, want string }{
		{
			"SELECT product_id, SUM(amount) AS revenue FROM day_product WHERE fecha IN PERIOD('D-89..D') GROUP BY product_id ORDER BY revenue DESC LIMIT 10",
			"SELECT product_id, SUM(amount) AS revenue FROM day_product WHERE fecha IN PERIOD('D-89..D') GROUP BY product_id ORDER BY revenue DESC LIMIT 10",
		},
		{
			"select Product_ID, sum(Amount) revenue from Day-Product where FECHA between 10 and 20 group by 1 order by 2 asc;",
			"SELECT product_id, SUM(amount) AS revenue FROM day-product WHERE fecha BETWEEN 10 AND 20 GROUP BY 1 ORDER BY 2",
		},
		{
			"SELECT WEEK(fecha), SUM(amount) / COUNT(*) AS ticket FROM day_client_product WHERE fecha = 20730 AND client_id IN ('Juan Pérez', 'O''Brien', 7) GROUP BY WEEK(fecha)",
			"SELECT WEEK(fecha), SUM(amount) / COUNT(*) AS ticket FROM day_client_product WHERE fecha = 20730 AND client_id IN ('Juan Pérez', 'O'Brien', 7) GROUP BY WEEK(fecha)",
		},
		{
			// FROM is optional (Run names the frame), SORT BY is ORDER BY, and names may be in double quotes.
			`SELECT fecha, product_id, SUM(amount) WHERE fecha IN PERIOD("M-1") AND product_id IN ("café moreno", "Juan ""JJ"" Pérez") GROUP BY fecha, product_id SORT BY amount DESC`,
			"SELECT fecha, product_id, SUM(amount) WHERE fecha IN PERIOD('M-1') AND product_id IN ('café moreno', 'Juan \"JJ\" Pérez') GROUP BY fecha, product_id ORDER BY amount DESC",
		},
		{
			// * and / bind tighter than + and -; parentheses group.
			"SELECT SUM(a) + SUM(b) * 2, (SUM(a) + SUM(b)) * 2.5, SUM(a) - SUM(b) - 1 AS 'x y' FROM f WHERE k = 1",
			"SELECT SUM(a) + (SUM(b) * 2), (SUM(a) + SUM(b)) * 2.5, (SUM(a) - SUM(b)) - 1 AS x y FROM f WHERE k = 1",
		},
	}
	for _, testCase := range cases {
		parsed, err := parse(testCase.statement)
		if err != nil {
			t.Fatalf("parse(%q): %v", testCase.statement, err)
		}
		if got := formatStatement(parsed); got != testCase.want {
			t.Errorf("parse(%q)\n got  %s\n want %s", testCase.statement, got, testCase.want)
		}
	}
}

func TestParseErrorsSayHowToRewrite(t *testing.T) {
	cases := []struct{ statement, want string }{
		{"FROM day_product", "expected SELECT at the start of the statement, found `FROM`"},
		{"SELECT * FROM day_product", "SELECT * is not supported: list the columns and aggregates, such as SELECT product_id, SUM(amount)"},
		{"SELECT DISTINCT product_id FROM day_product", "DISTINCT is not supported: GROUP BY the columns instead"},
		{"SELECT SUM(amount) FROM day_product d WHERE fecha = 1", "frames take no alias: write the columns without a prefix"},
		{"SELECT d.amount FROM day_product", "write columns without a prefix: `amount`, not `d.amount`"},
		{"SELECT SUM(amount) FROM day_product JOIN products", "a statement reads one frame: no joins"},
		{"SELECT SUM(amount) FROM day_product, products", "a statement reads one frame: no joins"},
		{"SELECT SUM(amount) FROM (SELECT 1)", "subqueries are not supported: FROM takes a frame's name"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 OR fecha = 2", "OR is not supported: use IN (…) for several values of one column"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha >= 1", "`>=` is not supported: use =, IN (…), BETWEEN a AND b, or IN PERIOD(\"…\") for days"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha IN PERIOD(D-1)", "write PERIOD with its notation in quotes: fecha IN PERIOD(\"D-6..D\")"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = PERIOD('D')", "write a period as column IN PERIOD(\"…\")"},
		{"SELECT SUM(amount) FROM day_product WHERE `product_id` = 1", "` quotes are not supported: write names of records in quotes (\"name\") and columns without quotes"},
		{"SELECT SUM(amount) FROM day_product WHERE product_id = 'milk", "a name in quotes is not closed: add the closing '"},
		{`SELECT SUM(amount) WHERE product_id = "milk`, "a name in quotes is not closed: add the closing \""},
		{"SELECT SUM(amount) WHERE fecha = 1 SORT amount", "expected BY after SORT, found `amount`"},
		{"SELECT SUM(amount) FROM day_product WHERE amount = 1.5", "conditions compare whole numbers: 1.5 is not one"},
		{"SELECT SUM(amount) FROM day_product WHERE WEEK(fecha) = 3", "conditions name a column, not a function: for a range of days write column IN PERIOD(\"…\")"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 GROUP BY product_id HAVING SUM(amount) > 0", "HAVING is not supported: filter Keys and Rows columns in WHERE, then use ORDER BY and LIMIT"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 LIMIT 10 OFFSET 10", "OFFSET is not supported: use ORDER BY and LIMIT"},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 LIMIT 0", "LIMIT takes a whole number above 0, found `0`"},
		{"SELECT COUNT(DISTINCT product_id) FROM day_product", "DISTINCT is not supported: GROUP BY the columns instead"},
		{"SELECT SUM(amount) * -1 FROM day_product", "negative numbers are not supported"},
		{"SELECT SUM(amount FROM day_product", "expected `)` after the arguments of SUM, found `FROM`"},
		{"SELECT 'milk' FROM day_product", "a name in quotes goes in WHERE, column = \"milk\""},
		{"SELECT SUM(amount) FROM day_product WHERE fecha = 1 extra", "expected the end of the statement, found `extra`"},
	}
	for _, testCase := range cases {
		_, err := parse(testCase.statement)
		if err == nil || err.Error() != testCase.want {
			t.Errorf("parse(%q)\n got  %v\n want %s", testCase.statement, err, testCase.want)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add("SELECT product_id, SUM(amount) AS revenue FROM day_product WHERE fecha IN PERIOD('D-89..D') GROUP BY product_id ORDER BY revenue DESC LIMIT 10")
	f.Add("SELECT WEEK(fecha), SUM(a) / COUNT(*) FROM f WHERE k IN (1, 'x') AND r BETWEEN 1 AND 2 GROUP BY 1")
	f.Add("SELECT ((1 + 2) * 3) FROM f-g-h WHERE a = 'O''Brien';")
	f.Fuzz(func(t *testing.T, statementText string) {
		parsed, err := parse(statementText)
		if err == nil {
			formatStatement(parsed)
		}
	})
}
