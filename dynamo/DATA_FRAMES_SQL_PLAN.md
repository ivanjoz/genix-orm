# Plan — FrameSQL: a small SQL for agents over DataFrames (`dynamo`)

Status: **partly implemented (2026-10-04, not committed).**
- Done: `framesql/` (parse, plan, engine; `Prepare` → `Statement.Columns` → `Statement.Run`, and
  `Run`) and its tests, against `framesql.Source`.
- Done in berryapps, tested on a fake `Source`:
  - `db/frame_sql.go` (aliases);
  - `agenttools.NewFrameSQLTool` with the input `{dataset, sql, message, table, chart}` (D10);
  - `agenttools.Table` in the reply frame;
  - the chat's `AgentTable.svelte`.
- Not built yet:
  - the read side (changes 1–3, and `FrameSource`, which implements `Source`);
  - the example module's tool;
  - the golden eval lines, the deploy, and a look at the table in the browser.

  The read side waits for the `.Fresh()` v2 work of `DATA_FRAMES_PLAN.md` (D5–D7), which changes
  the same files.

The reference of what is built is `DATA_FRAMES_SQL.md`. The decisions and their alternatives live in
this file: no `RATIONALE.md` entries yet.

This plan builds on `DATA_FRAMES_PLAN.md` (the frames, their files, the log and
`QueryFrame(...).Fresh()`). Where the two disagree about how frames are stored or kept live, that
plan wins.

## Goal

An agent answers "top 10 products by amount in the last 90 days" by writing one SQL statement over
a DataFrame. The user gets the rows as a table, a chart or both, in the same turn and with no
other model call. The engine aggregates the frame's files **as they arrive**, in columns, without
building a record per row. It **never reads files or logs itself**: it consumes the ORM's standard
live read, `QueryFrame(...).Fresh()`.

```json
{
  "dataset": "day_product",
  "sql": "SELECT product_id, SUM(amount) WHERE fecha IN PERIOD(\"D-89..D\") GROUP BY product_id SORT BY amount DESC LIMIT 10",
  "message": "Los 10 productos con más ventas de los últimos 90 días.",
  "table": { "columns": ["product_id", "amount"], "sort_by": [], "inline_bar": "amount" },
  "chart": { "type": "auto", "x": "", "y": [], "y2": [], "series_by": "" }
}
```

FrameSQL is generic: any module's frames, any Keys, Rows and Sums. The example module's sales frames
are test data; nothing below depends on their shape.

## The agent tool contract it must fit (berryapps)

Read from `backend/core/agent/`; these facts shape most decisions below:

- **A tool's successful result is the turn's reply** (`turn.go`, `agenttools/tool.go` `Answer`). The
  model never reads it, so it can't inspect a result and write a second statement.
- **Errors go back to the model**, which fixes its call. That is the only feedback loop.
- **The model passes record names as the user wrote them**; the tool resolves them
  (`ResolveRecords`, with a choice card when a name is ambiguous). The model knows no record IDs.
- **The model writes dates in the period notation** (`agenttools/period.go`); `ResolvePeriod` does
  the calendar math against `run.Today`, which keeps a replayed tool on the same dates.
- **Tools are `<module>_<thing>`, registered by their module**, and `RequiredAPIs` (per tool) is what
  the access guarantee rests on. `agenttools` imports no module.

## Decisions

**D1 — The language is SQL, not a JSON plan. Decided.** The optimization target is the agent: models
write SQL fluently, and one statement is easier to get right than a nested object. Cost: a parser.

**D2 — The parser is hand-written. Decided.** A closed subset (below), a few hundred lines, with
error messages written for an agent. No new dependency; same approach as the hand-written frame
codec.

**D3 — Aggregates: `SUM`, `COUNT(*)` and `AVG`, and arithmetic between them. Decided.** A frame
holds sums per row, and a frame only keeps rows with a non-zero sum, so these are the aggregates a
frame answers exactly:

- A **frame row** is one (file, Rows value) pair inside the filter. What it means depends on the
  frame: with `Keys[0]` a day and `Rows` a product, it is one product on one day.
- `SUM(col)` adds a Sums column over the frame rows of the group.
- `COUNT(*)` counts the frame rows of the group (in the example above, the days each product has a
  row). It is not a count of the source records.
- `AVG(col)` is `SUM(col) / COUNT(*)`: the average per frame row of the group. A value of
  `Keys[0]` with no row in the group does not count. An average over every value of the range
  (days without a row included) is not expressible in v1 (see Limits).
- `MIN`, `MAX`, `HAVING`, `COUNT(DISTINCT)` and percentiles are out of v1: a frame row is already
  an aggregate, so their answers mislead easily (`MAX(amount)` would be the best day, not the
  largest record), and no target question needs them.

The tool's description states these meanings, since the model can't see them in the result.

**D4 — The engine reads through `QueryFrame`. Decided.** Reading files, the `_idx`, the log and the
delta stays in one place. FrameSQL adds a columnar exit to that read (below) and consumes it.

**D5 — `Amount` goes in the example's `day-product`. Done** (`backend/example/types/sales.go`): the
test frame had only Quantity. A shape change: the next runs rebuild the frame.

**D6 — Ranges of `Keys[0]` use the period notation when `Keys[0]` is a day. Decided.**
`fecha IN PERIOD("D-89..D")`. The text inside `PERIOD()` is the notation the agent already writes
for every report tool; the statement holds no `TODAY()`, no date literal and no date arithmetic.
FrameSQL lives in the ORM and does not know the notation: the caller passes a resolver (the tool
passes `run.ResolvePeriod`). A `Keys[0]` that is not a day is bounded with integers (`=`,
`BETWEEN`).

**D7 — Column kinds are declared by the module, not by the ORM. Decided.** The schema knows only
integers. The module registering its frames with the tool tags their columns:

| Kind | Meaning | What it enables |
|---|---|---|
| `Day` | a UnixDay | `IN PERIOD(...)`, `WEEK()`, `MONTH()`; dates in the result and a date axis in charts |
| `Cents` | money, integer cents | formatted as money; typing of expressions over it (below) |
| `Ref(collection)` | a record ID of a text-search collection | a quoted name as a value (`product_id = "leche"`), resolved by `ResolveRecords`; names on the result's IDs, from the module's `FrameSQLRecords.NamesByID` |
| (none) | a plain integer | |

FrameSQL takes the kinds as input and the name resolution as a callback, so it stays pure and
knows nothing of text search. Each column also carries a bilingual `Label` ("Amount|Monto"), which
becomes the table's header and the chart's series name.

Names come from `NamesByID`, deleted records included, not from `RegisterRecordOptions`, which lists
only active records: a past sale of a deleted product must still show its name. A record with no
name shows as `#id`.

**D8 — One tool per module, built by a shared helper. Decided.** `agenttools.NewFrameSQLTool` builds
the tool from a module's frames, their column kinds and its `RequiredAPIs`; the module registers it
from `init()` as `<module>_frame_sql`. Because the result is the reply:

- No `DESCRIBE` or `SHOW FRAMES`: their result would reach the user, not the model. The `sql`
  argument's schema description lists the module's frames (datasets) with their columns and kinds,
  the grammar and two example calls built from the first frame. The tools index never sees them,
  as with the period notation.
- No "two statements joined in the tool layer": one call, one statement.
- `Summary` (what later turns read) is the dataset, the statement and the first records, so a
  follow-up ("now by week") edits it.

**D9 — Reads are bound by CPU as much as by I/O. Decided.** The frame store's target is S3 Express
One Zone (the general purpose bucket is a temporary fallback): a GET is a few milliseconds and the
read fetches 10 files at a time, so 90 files are about 9 rounds. Building a `FrameRow` per row (two
allocations each, 450 thousand rows for 90 files of 5,000) costs as much as the reads. Hence:
no `FrameRow` per row, and the fold runs while the remaining GETs are in flight (change 1).

- **Where the store is today.** berryapps' `core/frames` still writes a general purpose bucket
  (`[frames]` in `config.toml`): a measurement there is the fallback's, not production's.
- **Before moving it.** The Lambda role needs `s3express:CreateSession` on the directory bucket. As
  of 2026-10-02 only the deploy user had S3 Express, and the health check reports that grant as
  skipped while nothing uses it: check the role first.
- **A side benefit, outside FrameSQL.** A directory bucket appends natively, so the log append,
  today a GET and a PUT conditioned on the ETag in a retry loop (`core/frames/store.go`), becomes
  one request.

**D10 — The tool's input is five arguments; the model says how the result shows. Done.**
`{dataset, sql, message, table, chart}`, all required by the strict schema; empty means none.

- **`dataset`** is the frame, as an enum of the module's frames. It is the statement's FROM, so the
  statement leaves FROM out; a FROM that names another frame fails.
- **`sql`** is plain SQL. Presentation stays out of it: an early draft had
  `RETURN TABLE(...)` inside the statement, which made the model write SQL that isn't SQL.
- **`message`** is the sentence above the result, in the user's language; empty fails. It replaces
  a chart title: the table and the chart carry none.
- **`table`**: `{columns, sort_by, inline_bar}`.
  - `columns` lists the result's columns in order. A record column shows the name;
    `product_id:id` shows the ID.
  - `sort_by` is `"column"` or `"column DESC"`. A record column sorts by its name, and no value
    sorts last. It reorders the shown rows; the statement's ORDER BY and LIMIT picked them.
  - `inline_bar` names a value column of the table. It draws a thin bar under each row, scaled to
    the column's largest value: a decoration to read a ranking at a glance.
- **`chart`**: `{type: auto|bar|line, x, y, y2, series_by}`.
  - `x` is a group column; `y` and `y2` are value columns, `y2` on its own scale.
  - `series_by` turns a second group column into one series per value. It takes exactly one `y`
    and no `y2`, and at most 12 series. More is an error to the model, so it filters in WHERE.
  - Any other group column in the result fails ("set series_by to it"): it would give x several
    points.
  - Day points run ascending. When the result holds every group (not `Truncated`), they cover
    every period of the read (`framesql.PeriodStarts`). A missing point is 0 for SUM and COUNT,
    and no value otherwise. Record points keep the result's order (a ranking).
- **Which shows:**

  | Chart | `table.columns` | Shows |
  |---|---|---|
  | no | empty | a table of every column |
  | no | set | a table of those columns |
  | yes | empty | the chart only |
  | yes | set | the chart, then the table |

- **Headers** come from the column labels (D7) in the user's language: "Semana" / "Mes" for
  `WEEK` / `MONTH`, "Cantidad" for `COUNT(*)`, "<label> promedio" for `AVG`. An expression without
  a label shows its name.
- **One statement per call.**

**D11 — Items are named after their column; no AS needed. Done.**
- `SUM(amount)` and `AVG(amount)` are `amount`, `COUNT(*)` is `count`.
- `WEEK(fecha)` is `week`, `MONTH(fecha)` is `month`.
- Any other expression is named as written, or with AS (`SUM(amount) / COUNT(*) AS ticket`).

`SORT BY amount`, the table and the chart use these names. Two items of one name
(`SUM(amount), AVG(amount)`) fail with "name one with AS". The model writes shorter statements and
never misspells an alias it then refers to.

**D12 — The table and the chart are checked before any name is looked up. Done.**
- `framesql.Prepare` binds everything but WHERE and exposes `Statement.Columns` (name, label, kind,
  group, aggregate, period).
- The tool checks `table` and `chart` against them before `Statement.Run` resolves WHERE.
- WHERE's own checks run before its first name lookup: names compile last.

*Why:* a lookup may suspend the run with a choice card, and the resumed run replays the same input.
A mistake found after the user answered would fail the resumed turn, which the model never sees.
Checking first also spares the read.

**D13 — The table is structured, not markdown. Done.** `Answer.Tables` / `ReplyFrame.Tables` (`[{columns: [{label, format: text|number|money|date, values, inlineBar}]}]`), validated by `ValidateTables`, drawn by `AgentTable.svelte`.
- *Why:* the chat's markdown sanitizer allows no table tags, so a markdown table showed as text.
- Formats and bars belong to the renderer.
- Money travels in currency units, as in charts.
- At most 3 tables, 12 columns and 400 rows (the statement's `MaxRows`).
- A statement cut at 400 rows says so under the message.

## Architecture

```text
SQL text + the module's frames (column kinds) + resolvers (period, names)
  └ parse (hand-written)                          framesql/parse.go    pure
  └ plan: bind columns, resolve PERIOD() and      framesql/plan.go     pure, resolvers are callbacks
    names, split WHERE into pushdown + residual,
    type the items, count the files (cap)
  └ FrameSource.Scan(fn)                          dynamo               the standard live read, columnar
    10 GETs at a time, each file handed over                            exit, files as they arrive
    as soon as it is decoded and merged
  └ fold each file into one accumulator           framesql/engine.go   pure
  └ compute items, ORDER BY, LIMIT                framesql/engine.go
  └ typed columns + snapshot                      result
```

`dynamo/framesql/` is a subpackage, like `dataframe/`: parse, plan and engine are pure and
unit-testable without DynamoDB or S3. Only `Scan` touches storage, and it is the existing read.
`framesql` declares the small interface it reads through; `dynamo` implements it with
`Repo.FrameSource(name)` (the frame's columns and roles, and `Scan` by plain integer bounds), so
`dynamo` does not import `framesql` and a statement reaches a frame without the Repo's generics.

### Changes to the standard read (`dynamo/` and `dynamo/dataframe/`)

Useful to every caller of `QueryFrame`:

1. **A streaming columnar exit.** `FrameSource` implements `framesql.Source`:

   ```go
   Scan(fromKey, toKey int64, pinnedKeys []int64,
       selectFiles func(fileKeys [][dataframe.MaxKeys]int64) ([][dataframe.MaxKeys]int64, error),
       fn func(keys [dataframe.MaxKeys]int64, file dataframe.File) error) (snapshot int64, err error)
   ```

   - The 10 workers GET, decode and (when `Fresh`) merge the file's delta, as `ReadFiles` does now.
   - Each finished file goes through a channel to **one** goroutine, which calls `fn`. The caller
     folds without locks while the other GETs are in flight, and memory holds about 10 files, not
     all of them.
   - Files arrive in completion order: `Scan` promises no order. Aggregation doesn't need one;
     `Exec`, which returns rows sorted by Keys, sorts the files it receives by their keys.
   - No `FrameRow` per row. `Exec` is rewritten on top of `Scan`.
   - `selectFiles` gets every file the read would GET (the `_idx` listing plus the files the live
     delta names), before the first GET, and returns the ones to read or an error that ends the
     read. FrameSQL's key filters (`IN (...)` on a later Key reads only those files) and its file
     cap live there; the ORM knows neither. `Scan` returns that error as is.
   - *Why one callback:* it keeps the ORM ignorant of FrameSQL's conditions and cap, and it sees the
     final list, live-delta files included. Its error comes back unwrapped, so `Run` returns the
     read's errors as they are: the ORM's already name the frame.
2. **The delta touches only the files it changes.** In `ReadFreshFiles`, `rowSumsOf` / `addRowSums` /
   `fileOf` (a map per file) run only for files named by `deltasBetween`; the others pass through as
   decoded. *Why:* today every file is rebuilt as a map even when no change lands in it, and a
   90-day read has one or two touched days. A file passed through keeps its `Snapshot`; set it to 0
   like the merged ones (the doc of `ReadFreshFiles` says files come back without one).
3. **Overlap the fresh read's record and log reads with the file reads (optional).** With cheap
   GETs, `Fresh`'s fixed cost (the delta-index query, the log, `readBySK`) probably dominates. It
   can run while the `_idx` and the files are read; workers wait for it before merging, and files
   the delta names that the `_idx` doesn't list yet are read after. The order inside
   `readIncarnations` and the snapshot re-check are unchanged. Only if the measurement asks for it.
4. **A fused column scan (optional).** `ScanFile` unpacking block by block straight into the fold,
   skipping Sums columns the query doesn't use by walking block heads. Only worth it if profiling
   shows the decode dominates; it already runs inside the 10 workers.

What stays expanded, honestly: a file a delta changed is merged as a map. That is the live part of
the read and it is one or two files of many.

### The language

A closed subset. Anything outside it fails with a message that says how to rewrite.

```text
SELECT  item [, item]...
[FROM   frame]
WHERE   condition [AND condition]...
[GROUP BY group [, group]...]
[ORDER BY | SORT BY name [ASC|DESC] [, ...]]
[LIMIT n]
```

- **`FROM`** is optional: the caller names the frame (`Prepare(frameName, ...)`, the tool's
  `dataset`), and a FROM must name that same one (`day-product` or `day_product`). One frame per
  statement. No joins, subqueries, `DISTINCT`, `HAVING`, `OFFSET` or `UNION`.
- **Columns** are the snake_case names of the frame's Keys, Rows and Sums fields (`fecha`,
  `client_id`, `product_id`, `quantity`, `amount`), as the tool description lists them.
- **`item`** is a group, an aggregate (D3), or arithmetic over aggregates, named by D11 or by
  `AS alias`. Arithmetic: `+ - * /`, parentheses, integer and decimal literals. `/` is decimal
  division.
- **Quotes.** Names and periods go in double quotes (`"café moreno"`, `PERIOD("M-1")`): the tool's
  grammar, its examples and the errors all write them so. A double quote inside is doubled
  (`"Pantalla 24"""`). The parser also takes single quotes, with the same meaning
  (`'O''Brien'`). Backticks fail. *Why double:* an apostrophe in a name ("O'Brien", "D'Onofrio")
  needs no escaping, which models get wrong; a double quote inside a name (inches) is rarer. One
  style in everything the model reads, so it never thinks the two mean different things.
- **`condition`**:
  - On `Keys[0]`, required: `= n`, `BETWEEN a AND b`, or `IN PERIOD("<period notation>")` on a
    `Day` column (D6).
  - On later Keys and on Rows: `= v` or `IN (v, ...)`. A value is an integer, or a quoted name on a
    `Ref` column (D7).
  - None on Sums columns, none on aggregates.
- **`group`** is a Keys or Rows column, or `WEEK(col)` / `MONTH(col)` of a `Day` column: the Monday
  of the ISO week, or the 1st of the month, as a UnixDay. A day, not a week number, so weeks of
  different years never merge and the chart gets a date axis.
- **`ORDER BY`** (or `SORT BY`) names an item (D11), an item's position, or any expression an item
  could be. Without it, rows come by group ascending, so results and tests are deterministic.
- **`LIMIT`** is optional and at most `maxRows` (default 1,000). A result cut by it says so
  (`Truncated`).
- **Types of the result columns**: integer, decimal, day, cents, ref.
  - `SUM` and `AVG` of a `Cents` column are cents.
  - Cents `±` cents is cents; cents `×` or `/` a non-cents value is cents; cents `/` cents is a
    decimal.
  - A cents value is rounded to whole cents. `COUNT(*)` is an integer.
  - The engine can't tell `/ COUNT(*)` from `/ 100`, so `SUM(amount) / 100` stays cents: the tool's
    description says money is in cents, the tool formats it, and the statement never divides it by
    100. Money plus a non-money value, or money times money, fails. *Why:* `SUM(amount) / COUNT(*)`
    (an average ticket) must stay money; the cost is that one instruction to the model.
- **Result columns** carry their name (D11), the source column's `Label`, kind, collection,
  whether they are a group, their aggregate (`SUM`, `AVG`, `COUNT`), and, on a group of a day
  `Keys[0]`, their period (`day`, `week`, `month`). The result carries the read's
  `FromKey..ToKey`, so a chart can fill the periods without rows.

**Validation, before any file read:**

| Rule | Why |
|---|---|
| `WHERE` bounds `Keys[0]`, to at most 400 values | `QueryFrame`'s limit; also the cost bound |
| `=` on later Keys, in order, is pushed to the read; skipping one means the rest are not | `QueryFrame` takes only a leading run of Eq on the later Keys |
| Other conditions on later Keys filter the file keys before any GET | they name files, so they prune reads |
| Conditions on Rows are applied per row by the engine | the read takes no other predicate |
| The files to read are counted from each day's `_idx`; above a cap the statement fails, naming the Key to pin | a frame whose later Keys aren't pinned reads every file of each `Keys[0]` value; the cap comes from a measured GET |
| `PERIOD()`, `WEEK()`, `MONTH()` only on `Day` columns; a quoted name only on `Ref` columns | D6, D7 |
| `GROUP BY` takes Keys or Rows (or `WEEK`/`MONTH`), never Sums | a Sums column is a value, not a dimension |
| Every non-aggregate `SELECT` item is in `GROUP BY` | standard SQL |

**Errors an agent can act on** (all name the frame and the fix): *"`day_product` has no column
`client_id`; frames with it: `day_client_product`"*, *"`WHERE` must bound `fecha`: add
`fecha IN PERIOD("D-13..D")`"*, *"the range of `fecha` covers 612 values; the maximum is 400"*,
*"`day_client_product` would read 27,000 files; add `client_id = ...`"*, *"`MAX` is not supported:
use `SUM`, `COUNT(*)` or `AVG`"*. A name the text search can't match fails the same way
(`ResolveRecords`); an ambiguous one suspends the tool with a choice card.

### Engine

- **One accumulator.** `Scan` calls `fn` from one goroutine, so the fold needs no locks and no merge.
  A Go map from the group key (a fixed array of the group values: `int64`s, never strings) to a slot;
  each aggregate is a slice indexed by slot (sum per Sums column used, one row count).
- **Per row** of a file: the Rows conditions (a set lookup), the group key (Keys from the file's
  keys, Rows from the row, `WEEK`/`MONTH` computed once per file), then the sums and the count.
- **After the last file:** compute the items from the aggregates, sort with `slices.SortFunc` by
  `ORDER BY`, cut at `LIMIT`.
- **Result:** one `[]float64` per column with its kind, NaN for no value (a division by zero, `AVG`
  over no rows); `Snapshot` (the frame's `w`), `Truncated`, and the files read. The tool layer
  renders them. Without `GROUP BY` there is always one row, even over no rows. *Why float64:* one
  slice type keeps the engine and the tool layer free of per-kind branches; it is exact up to 2^53,
  90 trillion currency units in cents, which no frame reaches.

### berryapps wiring

| File | Change |
|---|---|
| `backend/db/frame_sql.go` | Done: aliases for `PrepareFrameSQL`, `FramePeriodStarts`, the frame and statement types and the column kinds (`SQLDay`, `SQLCents`, `SQLRef`…). `FrameSource` joins them with the read side |
| `backend/core/agent/agenttools/frame_sql.go` | Done: `NewFrameSQLTool(card, frames, recordsByCollection)`, input D10. Resolves `PERIOD()` with `run.ResolvePeriod`, quoted names with `ResolveRecords` on the column's collection, and names `Ref` columns with `FrameSQLRecords.NamesByID`. Turns `ErrFrameNotBuilt` into a message: the dataset is being rebuilt, retry in about 20 minutes |
| `backend/core/agent/agenttools/table.go`, `tool.go` | Done: `Table`, `ValidateTables`, `Answer.Tables`; the input schema takes nested structs (strict objects) |
| `backend/core/agent/protocol.go`, `agent_tools.go` | Done: `ReplyFrame.Tables`, validated with the charts |
| `frontend/core/agent/agent-table.ts`, `domain-components/AgentTable.svelte` | Done: cell formats, inline bars, scroll with a sticky header; the chat stores `tables` on the reply row and shows them after the charts. A chart's title is optional |
| `backend/example/tools/tools.go` | Registers `example_frame_sql` with its frames, their column kinds and labels, and `RequiredAPIs` |
| `backend/example/types/sales.go` | D5, done |

## Reads are live (`Fresh`)

FrameSQL always reads `Fresh()`: the agent must see the record entered a minute ago. The cost, from
`Fresh`'s own documentation, is one delta read of about 10–30 minutes of writes of the whole entity,
once per statement, plus the files (change 3 overlaps the two). A statement that tolerates old data
could skip it; that is a later option, not v1.

## Limits

- One frame per statement; no joins, and no second statement (the result ends the turn).
- A range of `Keys[0]` is at most 400 values (the `QueryFrame` cap), and the files read are capped.
- Aggregates are `SUM`, `COUNT(*)` and `AVG` over frame rows (D3). `COUNT(*)` counts frame rows, not
  source records. An average over every value of `Keys[0]` in the range, values without a row
  included, needs the range's length, which v1 doesn't expose.
- No filters on Sums columns or on aggregates (no `HAVING`).

## Tests

- **Parser** (`framesql/parse_test.go`): a table of valid statements to their AST, and of invalid
  ones to their exact error text. Fuzz: arbitrary input never panics.
- **Planner:** the pushdown split (`Eq` to the read, file-key filters, residual Rows conditions)
  for each shape of `WHERE`; `PERIOD()` and quoted names through fake resolvers; the typing of
  items; every validation rule has a failing case.
- **Engine, against brute force:** random frames (1–3 Keys, any kinds) and random statements; the
  engine's rows must equal a naive aggregation over the same files, with the files fed in shuffled
  orders (`Scan` promises none).
- **Live read:** the `ormcheck` frame on the in-memory store: write, materialize, write more (so the
  delta is non-empty), and compare a statement's result with the records. A file the delta doesn't
  touch comes through `Scan` unchanged (change 2), and `Exec` returns the same rows as before.
- **Benchmark:** 90 files × 5,000 rows on the in-memory store, time and allocations per statement,
  `Exec` before and after change 1. Then the same statement on S3 Express and on the fallback
  bucket, to set the file cap and to decide changes 3 and 4.

## Implementation order

1. D5 is done; deploy it early so the test frame rebuilds before FrameSQL needs it.
2. `dataframe/` + `dynamo/`: change 1 (streaming `Scan`, file-key filter), change 2, `FrameSource`,
   `Exec` rewritten on `Scan`, the benchmark. Existing tests must pass unchanged.
3. Done: `framesql/parse.go` and `framesql/plan.go`, with tests.
4. Done: `framesql/engine.go`, with the brute-force test.
5. Measure on S3 Express (the Lambda role's `s3express:CreateSession` and `core/frames` on the
   directory bucket come first, D9); then changes 3 and 4 only if the numbers ask for them.
6. Once the design settles: README section "FrameSQL", `RATIONALE.md` entries (from D10–D13 and the
   *Why* notes here), the `dynamo/skill/SKILL.md` section.
7. Commit and push inside the submodule.
8. berryapps: `agenttools/frame_sql.go` and the chat table are done. Left: the example's
   registration, golden discovery eval lines, deploying the tools layer (`create-agent-tool`), and
   checking a real answer's table and chart in the browser. The eval must also show that the
   existing report tools still win their own questions: a generic SQL tool matches almost any data
   question.

## Alternatives considered

- **JSON query plan.** Rejected for the agent (D1): more nesting to get wrong, and the agent is
  better at SQL. A parsed statement compiles to the same plan struct, so nothing is lost.
- **A SQL parser library** (vitess, pingcap). Wider syntax, a large dependency for a subset this
  small (D2).
- **The engine reads files itself.** Rejected (D4): it would duplicate the log, delta and
  snapshot logic that `Fresh` already gets right, and drift from it.
- **Parquet + DuckDB.** Gives full SQL, but drops the frame codec, adds a heavy dependency to the
  Lambda and loses the live merge.
- **Expand every file to `FrameRow`s.** What `Exec` does; it is the allocation cost this plan
  removes (change 1).
- **`TODAY()` and date literals.** Rejected (D6): date arithmetic is where models slip, and the agent
  already writes the period notation for every report tool.
- **A `period` argument next to `sql`.** Rejected for `PERIOD()` inside the statement: `Keys[0]` is
  not always a day, and the statement stays the one place that says what is read.
- **`RETURN TABLE(...)` / `RETURN CHART(...)` inside the statement.** Rejected (D10): it forces SQL
  that isn't SQL; the table and the chart are arguments.
- **A chart drawn automatically from the result's shape.** The first version drew one when the
  first column was a day or a record and the rest were values. Replaced by the `chart` argument:
  the shape guessed wrong on two dimensions (day × product), which `series_by` now draws.
- **A markdown table in the message.** Rejected (D13): the chat's sanitizer drops table tags, and
  the renderer could not align numbers or draw bars.
- **Statement errors after the name lookups.** Rejected (D12): a resumed run would fail where the
  model can't see it.
- **`DESCRIBE` / `SHOW FRAMES` statements.** Rejected (D8): a tool's result is the user's reply, so
  the model would never read them.
- **Per-worker accumulators merged at the end.** Rejected: `Scan` serializes the fold, which costs
  far less than the reads it overlaps; it would add a merge and its tests for nothing.
- **Dense-slice accumulators and a top-k heap.** Rejected for v1: one map and one sort cover every
  shape; a faster layout waits for a profile that asks for it.
