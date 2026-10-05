# FrameSQL — SQL over DataFrames, and the answers it draws (`dataframe/framesql`)

This is the reference of FrameSQL as it is:
- how a statement is read, checked and run against a DataFrame's files;
- how the files are folded in memory into groups;
- how berryapps' agent tool turns the result into the table and the chart the user sees.

It builds on `DATA_FRAMES.md` (what a frame is, its files, `QueryFrame`, `.Fresh()`), which it does
not repeat.
Where this document and the code disagree, the code wins.

**Status (2026-10-04).** Built and tested:
- the language, the planner and the engine;
- the ORM's read, `Repo.FrameSource`;
- the agent tool, with the example module's `example_frame_sql` over the four sale frames;
- the datasets lane of the chat's search (§7);
- the reply's tables and the chat's table.

## 1. What FrameSQL is

One SQL statement over one DataFrame, folded from the frame's files as they arrive, without a record
per row. The result is a set of typed columns. In berryapps the chat model writes the statement and
says how to show the result. The tool runs it, and the user gets a table, a chart or both, in the
same turn and with no other model call.

The model's call to the tool:

```json
{
  "dataset": "day_product",
  "sql": "SELECT product_id, SUM(amount) WHERE fecha IN PERIOD(\"D-29..D\") GROUP BY product_id SORT BY amount DESC LIMIT 10",
  "message": "Los 10 productos con más ventas de los últimos 30 días.",
  "table": { "columns": ["product_id", "amount"], "sort_by": [], "inline_bar": "amount" },
  "chart": { "type": "auto", "x": "", "y": [], "y2": [], "series_by": "" }
}
```

What the user sees: the message, then a table of the 10 products by name with their amount in
currency units, each row underlined by a bar as long as its share of the largest amount.

## 2. Map of the code

| Where | What |
|---|---|
| `dataframe/framesql/framesql.go` | The API: `Frame`, `Column`, the kinds, `Source`, `Resolvers`, `Options`, `Prepare` → `Statement.Columns` / `Statement.Run`, `Run`, `Result`, `ResultColumn` |
| `dataframe/framesql/parse.go` | The tokenizer, the recursive-descent parser, the words it refuses with a rewrite, `formatExpr` (the canonical text of an expression) |
| `dataframe/framesql/plan.go` | The statement bound to a frame: groups, items and their names, types, ORDER BY, then WHERE split into the read's bounds, the file filters and the row filter; `selectFiles` and the file cap |
| `dataframe/framesql/engine.go` | The fold of each file into one accumulator; items, ORDER BY, LIMIT; `PeriodStarts` |
| `backend/db/frame_sql.go` (berryapps) | Aliases: `db.SQLFrame`, `db.SQLColumn`, `db.SQLDay`…, `db.PrepareFrameSQL`, `db.FramePeriodStarts`, `db.FrameMaxDays` |
| `core/agent/agenttools/frame_sql.go` | `NewFrameSQLTool`: the tool's input, its schema text, the run, the table and the chart |
| `core/agent/agenttools/table.go`, `chart.go` | `Table` / `ValidateTables`, `Chart` / `ValidateCharts`: the reply's JSON and the renderer's rules |
| `core/agent/agenttools/tool.go` | Input schemas from structs, nested structs as strict objects; `Answer{Message, Summary, Charts, Tables}` |
| `core/agent/protocol.go`, `agent_tools.go`, `cli.go` | `ReplyFrame.Tables`, validation before the reply, `fn-agent-tool-run` output |
| `frontend/core/agent/agent-table.ts`, `domain-components/AgentTable.svelte` | Cell formats, inline bars, the table in the chat |
| `frontend/core/agent/agent-chart.ts`, `domain-components/AgentChart.svelte` | How a chart is drawn (unchanged by FrameSQL, but the title is now optional) |

## 3. Using it from Go

### Describing a frame

`framesql` knows nothing of the ORM's schema: the caller describes each frame it may read.

```go
type Frame struct {
	Name   string   // "day-product"; statements and the tool write it with '_' too
	Keys   []Column // 1 to dataframe.MaxKeys, in the frame's order
	Rows   Column
	Sums   []Column // in the order of dataframe.File.Sums (the DataFrame's Sums)
	Source Source
}

type Column struct {
	Name       string // snake_case, as statements write it: "product_id"
	Label      string // for people, "English|Spanish": "Product|Producto"
	Kind       Kind
	Collection string // KindRef only: the text-search collection its IDs point to
}
```

| Kind | The integers are | What it enables |
|---|---|---|
| `KindInteger` | plain numbers | |
| `KindDay` | UnixDays | `IN PERIOD("…")`, `WEEK()`, `MONTH()`; dates in tables, a date axis in charts |
| `KindCents` | money in integer cents | money typing (§4.9); formatted in currency units |
| `KindRef` | record IDs | names in quotes in WHERE, resolved by `Resolvers.Names`; record names in the answer |
| `KindDecimal` | — | a result's kind only (a division); a frame column can't have it |

`validateFrame` refuses, whenever a statement is prepared on the frame:
- a frame without 1–3 Keys, without Sums or without a Source;
- a `KindDecimal` column;
- a Sums column that is not `KindInteger` or `KindCents`.

### The read: `Source`

```go
type Source interface {
	Scan(fromKey, toKey int64, pinnedKeys []int64,
		selectFiles func(fileKeys [][dataframe.MaxKeys]int64) ([][dataframe.MaxKeys]int64, error),
		fn func(keys [dataframe.MaxKeys]int64, file dataframe.File) error) (snapshot int64, err error)
}
```

The contract every implementation keeps:
- **What it reads.** The files whose `Keys[0]` is in `[fromKey, toKey]` and whose later Keys start
  with `pinnedKeys`.
- **`selectFiles` first.** It gets every file the read would GET, before the first GET, and returns
  the ones to read, or an error that ends the read. `Scan` returns that error unwrapped.
- **`fn`, one file at a time.** Each file once, from one goroutine at a time, in any order.
- **Snapshot.** It returns the snapshot the files hold.

**The ORM's implementation**, `dataframe.FrameSource` (`dataframe/materialize.go`), built by a driver's
`Repo.FrameSource(frameName)` (dynamo: `data_frame_query.go`), which panics on an unknown frame:
- `Scan` is the fresh read of `QueryFrame(...).Fresh()` (`dataframe.Read`): the files as the records
  hold them now. A frame never built returns `ErrNotBuilt`.
- It hands `fn` the files only after the frame's state is re-read unchanged. A fresh read retries
  when a run commits meanwhile, so streaming would hand `fn` files that the retry reads again.
- A file with no change since its snapshot passes through as read.

### Prepare, check, run

```go
statement, err := framesql.Prepare("day_product", sqlText, frames, framesql.Options{MaxRows: 400})
// statement.Columns(): the result's columns, known before any name is looked up or any file read
result, err := statement.Run(framesql.Resolvers{
	Period: func(text string) (fromDay, toDay int64, err error) { … }, // the text of PERIOD("…")
	Names:  func(column framesql.Column, names []string) ([]int64, error) { … },
})
```

- `Prepare` parses and binds everything but WHERE. It errors on anything wrong with the statement
  itself.
- `Run` resolves WHERE through the resolvers and reads. A `Statement` runs once.
- `framesql.Run(frameName, sqlText, frames, resolvers, options)` does both in one call.
- The resolvers' errors come back wrapped (`fmt.Errorf("…: %w")`), so the caller still recognizes
  its own: `agenttools.ErrSuspended` when a name lookup asked the user.
- `Options` default to `MaxRows` 1,000 (the largest LIMIT, and the LIMIT of a statement without
  one) and `MaxFiles` 2,000 (the most files a statement reads).

### The result

```go
type Result struct {
	Columns        []ResultColumn // one per SELECT item
	RowCount       int
	FromKey, ToKey int64 // the range of Keys[0] read
	Snapshot       int64
	Truncated      bool // more groups matched than LIMIT returned
	FilesRead      int
}

type ResultColumn struct {
	Name       string  // §4.3
	Label      string  // the source column's Label, on a group or a lone aggregate
	Kind       Kind
	Collection string
	IsGroup    bool    // a GROUP BY value
	Aggregate  string  // "SUM", "AVG" or "COUNT" when the item is only that
	Period     string  // "day", "week" or "month" on a group of a day Keys[0]
	Values     []float64
}
```

- **Values are `float64` for every kind.** Integers are exact up to 2^53, cents are rounded to whole
  cents, and NaN is no value (a division by zero).
- **Money stays in cents:** dividing by 100 is the presentation's job (§6.6).
- **`PeriodStarts(period, fromDay, toDay)`** lists every day, Monday or 1st of month the range
  touches: the points of a series over a `Period` column, rows or not.

## 4. The language

```text
SELECT  item [, item]...                      item := expression [[AS] alias]
[FROM   frame]
WHERE   condition [AND condition]...
[GROUP BY expression [, ...]]                 a column, WEEK(day), MONTH(day), or an item's position
[ORDER BY | SORT BY expression [ASC|DESC] [, ...]]
[LIMIT n] [;]

condition  := column = value | column IN (value, ...) | column IN PERIOD("<period>")
            | column BETWEEN integer AND integer
value      := integer | "name in quotes"
expression := term {(+|-) term},  term := factor {(*|/) factor}
factor     := number | column | FUNCTION(* | expression) | (expression)
```

Keywords, functions and column names are case-insensitive.

### 4.1 The frame

`FROM` is optional:
- The frame is the one `Prepare` (the tool's `dataset`) names.
- A `FROM` must name that same frame, as `day-product` or `day_product`.
- Otherwise the statement fails with "FROM `x` is not the frame `y` the statement runs on: leave
  FROM out".
- One frame per statement: no joins, no aliases on the frame, no subqueries.

### 4.2 Columns

The snake_case names of the frame's Keys, Rows and Sums.
- An unknown column fails, naming the frames that have it, or else every column of this one.
- A prefix (`d.amount`) fails.

### 4.3 Items and their names

An item is a group, an aggregate, a number, or arithmetic over them (`+ - * /`, parentheses, integer
and decimal literals). Every item has a name. SORT BY, and the tool's table and chart, refer to items
by these names:

| Item | Its name |
|---|---|
| a column | the column: `fecha`, `product_id` |
| `SUM(amount)`, `AVG(amount)` | the column: `amount` |
| `COUNT(*)` | `count` |
| `WEEK(fecha)`, `MONTH(fecha)` | `week`, `month` |
| anything else | the expression as written canonically (`SUM(amount) / COUNT(*)`) |
| `… AS ticket`, `… ticket`, `… AS "the ticket"` | the alias |

- No AS is needed in the common case.
- Two items of one name (case-insensitive) fail, for example `SUM(amount), AVG(amount)`:
  "`SUM(amount)` and `AVG(amount)` are both named `amount`: name one with AS".

### 4.4 Aggregates

A frame row is one (file, Rows value) pair: with `Keys[0]` a day and `Rows` a product, one product
on one day. The aggregates are the ones a frame answers exactly:

- `SUM(col)` adds a Sums column over the group's frame rows.
- `COUNT(*)` counts the group's frame rows (the days each product has a row), never source records.
- `AVG(col)` is `SUM(col) / COUNT(*)`: per frame row, so a day without a row does not count.
- `MIN`, `MAX`, `MEDIAN`, `STDDEV`, `PERCENTILE`, `COUNT(col)` and `COUNT(DISTINCT …)` fail with
  the supported list.

### 4.5 Conditions

One condition per column, joined by `AND`.

| Column | Takes | Becomes |
|---|---|---|
| `Keys[0]`, required | `= n`, `IN (a, b, …)`, `BETWEEN a AND b`, or `IN PERIOD("…")` on a day | the read's range: min..max of the values, at most 400 values (`dataframe.MaxFirstKeys`); a list of several values also drops the files of the values not listed |
| a later Key | `= v`, `IN (…)`, `BETWEEN` | pinned in the read, or a filter on the listed files (§5.3) |
| `Rows` | `= v`, `IN (…)`, `BETWEEN` | a filter on each row, in the fold |
| a Sums column | nothing | "`amount` is a summed value: WHERE filters fecha, product_id" |

- A value is an integer or, on a `KindRef` column, a record's name in quotes, mixed freely:
  `product_id IN ("leche", 7)`.
- Names go through `Resolvers.Names`; a list where nothing matches fails with "no record matches".
- `PERIOD("…")` goes through `Resolvers.Period` and only takes a `KindDay` column. Its text is the
  caller's notation; the berryapps tool uses the period notation, `D-6..D`, `M-1`, `S=23,Y=2025`.
- No `OR`, `NOT`, `<`, `>`, `LIKE`, `IS NULL` or parentheses: each fails with the rewrite (`OR` →
  "use IN (…) for several values of one column").

### 4.6 GROUP BY

- A Keys or Rows column, `WEEK(day)` (the Monday of its ISO week) or `MONTH(day)` (its 1st), or the
  position of a SELECT item (`GROUP BY 1`). Never a Sums column.
- `WEEK` and `MONTH` give a UnixDay, not a week number: weeks of two years never merge, and the
  result keeps a date axis.
- At most 6 groups (every Key, Rows, WEEK and MONTH); a repeated group is dropped.
- A column in SELECT that is not an aggregate must be a group: SELECT `WEEK(fecha)` needs GROUP BY
  `WEEK(fecha)`, and `fecha` alone doesn't cover it.
- Without GROUP BY the statement returns exactly one row, even over no rows (SUM 0, COUNT 0, AVG no
  value).

### 4.7 ORDER BY / SORT BY

`SORT BY` is a synonym. Each term is resolved, in this order:
1. A number is an item's position.
2. A word is an item's name (§4.3): `SORT BY amount` is `SUM(amount)`.
3. An expression equal to an item's (`ORDER BY SUM(amount)`) is that item.
4. Anything else an item could be is computed as a hidden item, sorted on and not returned
   (`SELECT product_id … ORDER BY SUM(quantity) DESC`).

- No value (NaN) sorts last in both directions.
- Ties, and a statement without ORDER BY, go by the groups ascending, in GROUP BY order. The same
  statement always returns its rows in one order.

### 4.8 LIMIT and quotes

- **LIMIT** is a whole number from 1 to `MaxRows`; without one, `MaxRows` applies. `Truncated`
  tells that more groups matched.
- **Quotes.** Names and periods go in double quotes (`"café moreno"`, `PERIOD("M-1")`). The quote
  inside is written twice (`"Pantalla 24"""`).
  - Single quotes are accepted with the same meaning (`'O''Brien'`).
  - Backticks fail.
  - Everything the model reads (the grammar, the examples, the errors) uses double quotes.

### 4.9 Types

Each item gets a kind, which the presentation formats.

| Expression | Kind |
|---|---|
| `SUM` of a cents column / of an integer column | cents / integer |
| `AVG` of a cents column / of an integer column | cents / decimal |
| `COUNT(*)`, an integer literal | integer |
| a decimal literal | decimal |
| a group | its column's (day, ref, integer) |
| cents `±` cents, cents `×` or `/` a non-money value, a non-money value `×` cents | cents |
| cents `/` cents | decimal (a ratio) |
| any other `/`, or anything with a decimal | decimal |
| cents `±` a non-money value, cents `×` cents | an error |

- `SUM(amount) / COUNT(*)` (an average ticket) stays money.
- The engine can't tell `/ COUNT(*)` from `/ 100`, so the tool tells the model that money is in
  cents and must never be divided by 100.
- A cents result is rounded to whole cents.

## 5. How a statement runs

### 5.1 Parse (`parse.go`)

1. **Tokenize** into words, numbers, strings (single or double quotes, doubled quote inside) and
   symbols (`, ( ) * + - / = < > ;` and `<= >= <> !=`, kept only to refuse them by name).
2. **Parse** with one recursive-descent pass over the tokens. `*` and `/` bind tighter than `+` and
   `-`. Column names are lowercased, function names uppercased.
3. A word of SQL the subset doesn't take (`JOIN`, `HAVING`, `OR`, `DISTINCT`, `UNION`, `WITH`…)
   fails with its own message saying how to write it here.
4. Expressions get a canonical text (`formatExpr`, nested operations parenthesized). It matches a
   SELECT item with its GROUP BY or ORDER BY entry, and names unaliased expressions.

### 5.2 Prepare: bind to the frame (`planStatement`)

In this order:
1. **The frame.** Pick it by name (`_` or `-`, any case); check FROM; validate the declaration;
   check LIMIT.
2. **Groups.** Bind each GROUP BY entry (position → its item's expression) to a Key or to Rows.
   `WEEK`/`MONTH` record a transform. A group of a day `Keys[0]` records its period (`day`, `week`,
   `month`).
3. **Items.** Bind each one in two steps:
   - first by its text against the groups;
   - else as an aggregate or arithmetic, typed by §4.9.

   A Sums column used in `SUM` or `AVG` gets an accumulator slot (`sumColumns`); a Sums column the
   statement doesn't use is never read. Each item is then named and described (label, group,
   aggregate, period), and duplicate names fail.
4. **ORDER BY**, by §4.7.

`Statement.Columns()` exposes the described items. Nothing has been looked up or read yet.

### 5.3 Run: WHERE becomes the read (`planConditions`)

1. Every condition is checked first:
   - the column exists and is not a Sums column;
   - one condition per column;
   - `Keys[0]` is bounded.
2. **Conditions without names compile first, the ones with names last.** A name lookup may stop
   the run to ask the user (a choice card). The run then resumes by replaying the same input, so
   every other error must come before the first lookup.
3. Each condition becomes a `valueFilter`: a set of values, or a range.
4. They are split:
   - **`Keys[0]`** → the read's `fromKey..toKey` (at most 400 values). A list of several values is
     also a file filter.
   - **Later Keys** → walk them in order. While each holds a single value, it is **pinned**
     (`pinnedKeys`): the read names those files directly, or narrows its listing. From the first one
     that doesn't, every remaining condition is a **file filter**.
   - **Rows** → the **row filter**.
5. `Source.Scan` runs. Its `selectFiles` callback is `queryPlan.selectFiles`: it drops the listed
   files the file filters exclude, before any GET. If more than `MaxFiles` remain, it fails before
   any GET: "`day_client_product` would read 27000 files; the maximum is 2000: narrow `fecha` or add
   `client_id = …`" (the first unpinned Key, when there is one).

On a frame `fecha, store_id, client_id → product_id`:

| WHERE | Read range | Pinned | File filters | Row filter |
|---|---|---|---|---|
| `fecha BETWEEN 10 AND 20` | 10..20 | — | — | — |
| `fecha = 10 AND store_id = 3 AND client_id = 4` | 10..10 | 3, 4 | — | — |
| `fecha = 10 AND client_id = 4` | 10..10 | — | client_id | — |
| `fecha = 10 AND store_id IN (3, 5) AND client_id = 4` | 10..10 | — | store_id, client_id | — |
| `fecha IN (10, 12) AND store_id = 3` | 10..12 | 3 | fecha (drops 11) | — |
| `fecha IN PERIOD("5..9") AND product_id IN ("leche", 7)` | 5..9 | — | — | product_id |

### 5.4 The fold: how the files are iterated in memory (`engine.go`)

`Scan` hands the files to `fn` one at a time, from one goroutine. A single accumulator therefore
needs no lock and no merge. Each file is folded and dropped: memory holds the groups, not the files.

```go
type accumulator struct {
	slotByGroup map[[6]int64]int // the group key → its slot
	groupKeys   [][6]int64       // the key of each slot
	sums        [][]int64        // sums[i][slot]: the i-th used Sums column of the slot's group
	counts      []int64          // frame rows per slot (COUNT(*), AVG)
}
```

- **The group key** is a fixed array of the group values: integers, never strings. It is hashed by
  value, with no allocation per row.
- **Per file**, before its rows: the group parts that come from the Keys are computed once, with
  `WEEK`/`MONTH` applied. When no group comes from Rows, the whole key is the same for every row of
  the file.
- **Per row** (`file.RowIDs[row]`):
  1. The row filter: a set lookup or a range check. A dropped row costs nothing more.
  2. The slot: when a group comes from Rows, the key gets the row's value (transformed if needed)
     and the slot is looked up. Otherwise the file's slot is looked up once, at its first kept row,
     so a file whose rows are all filtered out adds no group.
  3. `sums[i][slot] += file.Sums[sumColumns[i]][row]` for each used Sums column, then
     `counts[slot]++`.
- **Integer adds** while folding: sums are exact `int64` until the items are computed.
- **Order doesn't matter.** Addition commutes, so files can arrive in any order; the sort below
  makes the output deterministic.

### 5.5 After the last file

1. Without GROUP BY and with no row, one empty slot is added (the one-row rule).
2. Each item is evaluated per slot. A group reads its key part, `SUM` its sum, `COUNT` its count,
   `AVG` sum / count, and arithmetic its operands, in `float64`. A division by zero, or an `AVG` over
   no rows, is NaN. Cents are rounded.
3. The slots are sorted by the ORDER BY items (hidden ones included), NaN last, then by group key.
4. LIMIT cuts the list; `Truncated` says whether it cut anything. Only the visible items become
   result columns.

### 5.6 A worked example

`day-product` (`fecha → product_id`, Sums `quantity, amount`), two files:

| File | product_id | quantity | amount |
|---|---|---|---|
| `fecha` 20727 (Thu 2026-10-01) | 10 | 2 | 1000 |
| | 11 | 1 | 500 |
| `fecha` 20728 (Fri 2026-10-02) | 10 | 1 | 700 |
| | 12 | 5 | 0 |

```sql
SELECT WEEK(fecha), product_id, SUM(amount), COUNT(*)
WHERE fecha BETWEEN 20727 AND 20728 AND product_id IN (10, 11)
GROUP BY WEEK(fecha), product_id
SORT BY amount DESC
```

- **Prepare:**
  - groups: `WEEK(fecha)` (Key 0, transform week, period `week`) and `product_id` (Rows);
  - one used Sums column: `amount` (`quantity` is never touched);
  - items: `week`, `product_id`, `amount` (SUM, cents), `count`;
  - order: item 3 descending.
- **Run:** read range 20727..20728, nothing pinned (a 1-key frame), row filter {10, 11}.
- **Fold.** Say 20728 arrives first. Its week part is 20724 (Mon 2026-09-28).
  - Row 10: slot 0 = key (20724, 10), amount 700, count 1.
  - Row 12: dropped by the filter.
- **Fold 20727.** Same week.
  - Row 10: slot 0, amount 1700, count 2.
  - Row 11: new slot 1 = (20724, 11), amount 500, count 1.
- **After:** sorted by amount descending, slot 0 then slot 1.

| week | product_id | amount | count |
|---|---|---|---|
| 20724 | 10 | 1700 | 2 |
| 20724 | 11 | 500 | 1 |

The tool layer then writes `2026-09-28`, the product names, and 17.00 / 5.00.

## 6. The agent tool (berryapps)

### 6.1 Declaring it

A module builds its tool from its frames and registers it from `init()`, like its other tools. Its
`RequiredAPIs` gate who may run it. The example module's is `backend/example/tools/frames.go`.

```go
agenttools.NewFrameSQLTool(agenttools.ToolCard{
	Name: "example_frame_sql", Title: "…", Description: "…",
	RequiredAPIs: []string{"GET.example-sales-groups"},
}, []agenttools.FrameSQLDataset{{
	Frame: db.SQLFrame{
		Name:   types.FrameDayProduct,
		Keys:   []db.SQLColumn{{Name: "fecha", Label: "Date|Fecha", Kind: db.SQLDay}},
		Rows:   db.SQLColumn{Name: "product_id", Label: "Product|Producto", Kind: db.SQLRef, Collection: "example_product"},
		Sums:   []db.SQLColumn{{Name: "quantity", Label: "Units|Unidades"}, {Name: "amount", Label: "Amount|Monto", Kind: db.SQLCents}},
		Source: types.SaleLines.FrameSource(types.FrameDayProduct),
	},
	Title:       "Sales per day and product",
	Description: "One row per product sold on a day: its units and money, cancelled sales left out.",
	Examples:    []string{"ventas por semana de cada producto", "…"},
}}, map[string]agenttools.FrameSQLRecords{
	"example_product": {Noun: "product|producto", NamesByID: func(_ *agenttools.Run, ids []int32) (map[int32]string, error) { … }},
})
```

- **`FrameSQLDataset`**: a frame, and the card the tools index finds it by. A dataset without a
  `Title`, a `Description` or `Examples` panics at boot.
  - `Description` says what one row is and what its numbers count. The model reads it too, so it
    also carries the rules the columns can't say: "payment_method is 1 cash, 2 card, 3 transfer".
  - `Examples` are questions as users type them. The tool's card gets them all, so leave the card's
    `Examples` empty.
- **`FrameSQLRecords`**, one per collection of a `Ref` column. A missing one panics at boot.
  - `Noun` names a record in the choice card's question.
  - `NamesByID(run, ids)` names the result's IDs, deleted records included: a past sale of a deleted
    product still shows its name. A record with no name shows as `#412`.
  - The run gives the user's language, for values the app names itself: the payment methods have no
    records, so their names come from the app, and their column is filtered by number.
- **The schema.** The `dataset` argument is an enum of the frames' names (`day_product`). The `sql`
  argument's description carries:
  - each dataset with its title, description, columns and kinds:
    `- day_product: Sales per day and product. One row per … \n  Keys fecha (day); Rows product_id (product); Sums quantity (number), amount (money)`;
  - the grammar;
  - two example calls built from the first frame (a ranking with an inline bar, and a weekly line
    per named record);
  - the period notation.
- **The index.** `Tool.Datasets` (title, description, column labels, examples) is what the tools
  index holds of the tool: one card per dataset, never the tool's own card (§7).

### 6.2 The input

Five arguments, all required by the strict schema; an empty value means none.

| Argument | What it is |
|---|---|
| `dataset` | the frame the statement reads: its FROM |
| `sql` | one statement, §4, usually without FROM |
| `message` | the sentence the user reads above the result, in the user's language. Empty fails. It is the table's and the chart's title: they carry none |
| `table` | `{columns, sort_by, inline_bar}` (§6.3) |
| `chart` | `{type, x, y, y2, series_by}` (§6.4) |

They refer to the result's columns by their names (§4.3), case-insensitive. Which part shows:

| Chart (`chart.x` set) | `table.columns` | Shows |
|---|---|---|
| no | empty | a table of every column |
| no | set | a table of those columns |
| yes | empty | the chart only |
| yes | set | the chart, then the table |

### 6.3 Table options

- **`columns`**: the columns to show, in order, at most 12. A record column shows the record's name;
  `product_id:id` shows its ID. `:id` on any other column fails.
- **`sort_by`**: `"column"`, `"column ASC"` or `"column DESC"`, in priority order. It may name a
  column the table doesn't show.
  - A record column sorts by its name (case-insensitive); everything else by its number.
  - No value sorts last.
  - Rows the keys leave tied keep the statement's order.
  - It reorders the rows the statement returned; the statement's ORDER BY and LIMIT chose them
    (top 10 by amount, shown by name).
- **`inline_bar`**: a value column (not a group) among the table's columns. Under each row it draws
  a thin bar whose length is the row's value over the column's largest. Negative values are drawn by
  their size. Decorative: it reads a ranking at a glance.

### 6.4 Chart options

- **`type`**: `auto`, `bar` or `line`.
  - `auto` leaves the shape to the chat's renderer (below).
  - `bar` and `line` set every series' type.
- **`x`**: a group column, the points along the chart: days, weeks, months or records.
- **`y`**: one or more value columns (not groups) drawn against x.
- **`y2`**: value columns on a second scale, when their size differs from y's (units beside money).
- **`series_by`**: a second group column whose values become one series each (a line per product
  over the days).
  - It takes exactly one `y` and no `y2`.
  - Its values give the series' names, in the result's order.
  - More than 12 values fail: "chart.series_by: `product_id` has 13 values, at most 12: filter it in
    WHERE".
- **Any other group column fails.** It would give x several points: "chart: the result has a row
  per `product_id` too: set series_by to it, or leave it out of GROUP BY". A group the statement
  groups by without selecting it is caught after the read, by a repeated point.
- At most 12 series in all.

**The points:**
- **Days, weeks and months** run ascending. When the result holds every group (`Truncated` false),
  they cover every period of the read (`PeriodStarts(Period, FromKey, ToKey)`): a day without sales
  is a point at 0.
- **Records** keep the result's order, so a ranking stays a ranking.
- **A point without a row** is 0 for a `SUM` or `COUNT` column, and no value (a gap) for anything
  else, such as an `AVG` or a ratio.

**How the chat draws it** (`planAgentChart`):
- **Ranked horizontal bars**, highest first, with each one's share of the total: one `y` series
  over at most 15 points that are not dates, unless its type is `line`.
- **Otherwise a canvas.**
  - Dates give a time axis.
  - A series without a type is a bar, or a line on a date axis of more than 31 points.
  - `y2` is a line on its own axis.
  - A legend shows from 2 series.

### 6.5 The run, step by step (`runFrameSQL`)

1. `message` not empty.
2. `db.PrepareFrameSQL(dataset, sql, frames, {MaxRows: 400})`.
3. The table and the chart are checked against `Statement.Columns()`. This happens before any name
   lookup, for the reason in §5.3, and before any GET.
4. `Statement.Run` with the resolvers:
   - **Period**: `run.ResolvePeriod(text, 400)`. It does the calendar math against `run.Today`, so a
     replayed run reads the same days.
   - **Names**: `ResolveRecords` on the column's collection.
     - A stored answer (`<collection>:<normalized words>`) resolves a name at once; that is how a
       resumed run gets the user's pick.
     - Otherwise a confident text search resolves it.
     - Otherwise the run is suspended with a choice card ("Include all" allowed).
     - A name with no match fails back to the model.
5. Errors return to the model, which fixes its call. `db.ErrFrameNotBuilt` becomes "the dataset is
   being rebuilt after a change and can't be read for about 20 minutes: tell the user to try again
   later".
6. No row: the message plus "La consulta no encontró datos." / "The query found no data."
7. The record names (`NamesByID`) of every `Ref` column of the result.
8. The chart, then the table.
9. A result cut at the 400-row cap adds "Se muestran los primeros 400 grupos." under the message.
10. **`Summary`** is what later turns read, since they never see the table:
    "Ran FrameSQL on day_product (3 rows): <the sql>; first product_id: [11] Pan, [10] Leche, [12] #12".
    A follow-up ("ahora por semana") edits that statement.

### 6.6 Headers and cells

**Headers**, in the user's language (`"English|Spanish"` labels):
- `Semana` / `Week` and `Mes` / `Month` for `WEEK` / `MONTH` groups.
- `Cantidad` / `Count` for `COUNT(*)`.
- `<label> promedio` / `Average <label>` for `AVG`.
- Otherwise the column's label, else the item's name (an expression, an alias).
- `column:id` adds ` ID`.

**Cells** (`frameSQLCell`, used by both the table and the chart):

| Column | Value sent | Table format |
|---|---|---|
| a day (also WEEK / MONTH) | `"2026-09-28"` | `date` → `28/09/2026` |
| a record | its name, or `"#id"` | `text` |
| a record as `:id`, another group | the integer as text (a code, not a quantity) | `text` |
| cents | the amount in currency units (`17.0`) | `money` → `17.00` |
| integer, decimal | the number | `number` → `1,234`, `12.50`, ratios under 1 with 4 decimals |
| no value | `null` | `—` |

### 6.7 Examples

A daily line per named product, no table:

```sql
SELECT fecha, product_id, SUM(amount)
WHERE fecha IN PERIOD("M-1") AND product_id IN ("café moreno", "café criollo")
GROUP BY fecha, product_id
```
```json
"table": {"columns": [], "sort_by": [], "inline_bar": ""},
"chart": {"type": "line", "x": "fecha", "y": ["amount"], "y2": [], "series_by": "product_id"}
```

Weekly amount as bars with the units on a second scale, and the same numbers as a table:

```sql
SELECT WEEK(fecha), SUM(amount), SUM(quantity) WHERE fecha IN PERIOD("M-2..M") GROUP BY WEEK(fecha)
```
```json
"table": {"columns": ["week", "amount", "quantity"], "sort_by": [], "inline_bar": ""},
"chart": {"type": "bar", "x": "week", "y": ["amount"], "y2": ["quantity"], "series_by": ""}
```

The average ticket per product, best first, with its ID:

```sql
SELECT product_id, SUM(amount) / COUNT(*) AS ticket WHERE fecha IN PERIOD("D-29..D") GROUP BY product_id SORT BY ticket DESC LIMIT 20
```
```json
"table": {"columns": ["product_id", "product_id:id", "ticket"], "sort_by": [], "inline_bar": "ticket"},
"chart": {"type": "auto", "x": "", "y": [], "y2": [], "series_by": ""}
```

From the command line, once a module registers its tool:

```bash
./app.sh fn-agent-tool-run example_frame_sql '{"dataset":"day_product","sql":"…","message":"…","table":{…},"chart":{…}}' es
```

It prints the message, the summary, the charts' and tables' validation, and the tables' JSON, or
the choice card's questions.

## 7. In the chat

- **How a turn finds it.** The tools index searches in two lanes (point `kind` in `agent/src/tools.rs`):
  - the report tools, ranked as always;
  - the datasets, as each dataset's card plus one dense-only point per example question.

  `discovery.SelectDatasets` keeps the datasets whose closest point has a dense cosine ≥
  `DatasetMinDenseScore` (0.40). Their tool joins the turn after the reports; with none, the turn
  has no SQL tool.
- **What the model reads.** A `[Datasets]` context block lists the retrieved datasets with their
  probabilities. It says to prefer a report that answers the request exactly and to use the dataset
  tool for the rest. The usage guide stays in the tool's schema.
- **Its quality.** `./app.sh fn-agent-tools-eval` checks the lane against the `"dataset"` of each
  golden line (`core/agent/agenttools/testdata/discovery-golden.jsonl`). The bars: offered ≥ 95%,
  never offered for a question none answers.
- **Validation.** `finishAgentTool` validates the answer's charts and tables (`ValidateCharts`,
  `ValidateTables`). An invalid one returns to the model as an error. Otherwise the reply frame
  carries `Message`, `Summary`, `Charts` and `Tables`.
- **Storage.** The browser stores the reply row with its `charts` and `tables` in IndexedDB. They
  are never sent back to the model; the summary is.
- **Order.** `AgentChat.svelte` shows the message, the charts (`AgentChart`), the tables
  (`AgentTable`), then any choice card.
- **`AgentTable`** (`planAgentTable` in `agent-table.ts`):
  - a grid with a sticky header, scrolling past 360 px;
  - numbers and money right-aligned in the monospace font;
  - text of 14 px;
  - the inline bar as a full-width row under each data row.
  - A table it can't lay out shows the reason instead.
- **Limits**, shared with the backend: 3 tables per answer, 12 columns, 400 rows; 3 charts, 12
  series, 400 points.

## 8. Limits

- One frame and one statement per call; no joins, `OR`, `HAVING`, `MIN` / `MAX` or `DISTINCT`.
- **Frame rows, not records.** `COUNT(*)` counts frame rows, and `AVG` averages over the rows that
  exist: an average over every day of the range, days without sales included, is not expressible.
- **No filters on values.** WHERE takes Keys and Rows only, never a Sums column or an aggregate.
- **Size caps.** A range of `Keys[0]` is at most 400 values, a read at most `MaxFiles` files (2,000
  until a measurement on S3 Express sets it), and a result at most 400 rows in the tool.
- **Exactness.** Results are `float64`: exact up to 2^53, 90 trillion currency units in cents.
- **Filled periods.** A chart over days fills every period between the read's bounds: with
  `fecha IN (1, 3)`, day 2 shows as 0.
- **Names in the answer** exist only for `Ref` columns; any other group shows its integer.
- **Quoted names** resolve only in a text-search collection. A `Ref` the app names itself (a payment
  method) is filtered by number.

## 9. Tests

`go test ./framesql/` in `genix-orm/dataframe`:
- **`parse_test.go`**:
  - valid statements, printed back canonically;
  - invalid ones, to their exact error text;
  - `FuzzParse`: arbitrary input never panics.
- **`plan_test.go`**:
  - the pushdown table of §5.3;
  - every validation error, to its exact text;
  - the resolvers' errors still recognizable through the wrap;
  - names looked up after every other check;
  - item names, labels and periods;
  - the frame chosen by the caller;
  - the typing of items.
- **`engine_test.go`**:
  - hand-checked results (NaN last, one row without GROUP BY, hidden ORDER BY items);
  - pruning and the file cap;
  - `WEEK` / `MONTH` and `PeriodStarts`.
  - **`TestEngineMatchesBruteForce`, the spec**: 400 random frames (1–3 Keys) and statements,
    files fed in shuffled order, compared with a naive aggregation row by row. At least half the
    rounds must return several rows, so the comparison proves something.

`go test ./core/agent/...` in `backend`:
- **`frame_sql_test.go`**:
  - a ranking table with `:id` and an inline bar;
  - every column by default and `sort_by`, names and NaN included;
  - a daily line of a named record, with the missing day filled;
  - `series_by`, and 13 series refused;
  - every table and chart error raised before the read (the test's source fails if reached);
  - errors and no rows;
  - the schema (datasets enum, examples, strict nested objects, the boot panic);
  - a name error returned to the model.
- **`TestValidateTables`**.

`bun test core/agent/` in `frontend`: `agent-table.test.ts` covers formats, bar widths and refused
tables.

## 10. Invariants to keep when changing this code

- **`framesql` stays pure.** It reads only through `Source` and resolves only through `Resolvers`.
  It knows no storage, no text search and no presentation.
- **Every check that can fail runs before the first name lookup.** That means `Prepare`, the tool's
  table and chart checks, and every WHERE check, since names compile last. A resumed run must not
  fail on its replay.
- **The fold is order-independent**, and `fn` is called serially (the `Source` contract): one
  accumulator, no locks.
- **The output is deterministic**: ties and unordered statements go by group key.
- **Item names are unique per statement**: they are how the table, the chart and SORT BY refer to
  columns.
- **Money stays integer cents until `frameSQLCell`** divides it by 100 for display.
- **Errors are written for the model**: each says what to write instead.
- **The renderer's limits match the backend's** (`table.go` / `agent-table.ts`, `chart.go` /
  `agent-chart.ts`).
- **The brute-force test is the engine's spec**: a change must keep it green.
