# Plan — FrameSQL: a small SQL for agents over DataFrames (`dynamo`)

Status: **plan, nothing implemented.** It builds on `DATA_FRAMES_PLAN.md` (the frames, their files,
the log and `QueryFrame(...).Fresh()`). Where the two disagree about how frames are stored or kept
live, that plan wins.

## Goal

An agent answers "top 10 products by amount in the last 90 days" by writing one SQL statement over
a DataFrame, and gets rows back. The engine aggregates the frame's files **as they are read**, in
columns, without building a record per row, and **never reads files or logs itself**: it consumes
the ORM's standard live read, `QueryFrame(...).Fresh()`.

```sql
SELECT product_id, SUM(amount) AS revenue
FROM day_product
WHERE fecha BETWEEN TODAY() - 90 AND TODAY()
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10
```

## Decisions

**D1 — The language is SQL, not a JSON plan. Decided.** The optimization target is the agent: models
write SQL fluently, and one statement is easier to get right than a nested object. Cost: a parser.

**D2 — The parser is hand-written. Decided.** A closed subset (below) in about 300 lines, with error
messages written for an agent. No new dependency; same approach as the hand-written frame codec.

**D3 — "Sales" means amount. Decided.** The reports the agent builds rank and total by `amount`.
`quantity` stays available. No record-count column is added: `COUNT(*)` counts rows of the frame
(see "Aggregates"), not sales.

**D4 — The engine reads through `QueryFrame`. Decided.** Reading files, the `_idx`, the log and the
delta stays in one place. FrameSQL adds a columnar exit to that read (below) and consumes it.

**D5 — `Amount` goes in `day-product`. Decided, applied in `backend/example/types/sales.go`.**
`day-product` had only Quantity, so "top products by amount over 90 days" was impossible there, and
on `day-client-product` it reads every client's file of every day (days × clients files).

- It is a shape change, so the next runs rebuild the frame (`RebuildDataFramesAll`, derived data):
  the first run after the deploy resets it, the one after builds it. The file grows by about 12
  bits per row.
- Frames declared in code only change on deploy; the nightly `example-rebuild-sales-frames` and
  `./app.sh fn-db rebuild-frames` can force the rebuild.

## Architecture

```text
SQL text
  └ parse (hand-written)                          sqlparse.go     pure
  └ plan against the frame's columns              plan.go         pure: WHERE → Eq/Between pushdown + residual filters
  └ QueryFrame(frame).Eq/Between.Fresh().Scan(fn) data_frame_query.go   the standard live read, columnar exit
  └ fold each file into per-worker accumulators   engine.go       pure
  └ merge, HAVING, ORDER BY, LIMIT                engine.go       top-k heap
  └ rows + columns + snapshot                     result
```

The parse, plan and engine are pure and unit-testable without DynamoDB or S3. Only the `Scan` call
touches storage, and it is the existing read.

### Changes to the standard read (`dynamo/` and `dynamo/dataframe/`)

Four changes, each useful to every caller of `QueryFrame`:

1. **A columnar exit: `FrameQuery.Scan(fn func(keys [3]int64, file dataframe.File) error) error`.**
   It hands over each selected file as the columns the decoder already produced (`RowIDs`, `Sums`),
   in key order, with no `FrameRow` per row. `Exec` is rewritten on top of it.
   *Why:* `Exec` allocates a struct and a `sums` slice for every row: 450 thousand allocations for
   90 days of `day-product`. That is the cost that matters, not the decode.
2. **The delta touches only the files it changes.** In `ReadFreshFiles`, `rowSumsOf` / `addRowSums` /
   `fileOf` (a map per file) run only for files named by `deltasBetween`. Other files pass through
   as decoded. *Why:* today every file is rebuilt as a map even when no change lands in it, and a
   90-day read has one or two touched days.
3. **Files are handed over as they arrive (optional).** `ReadFiles` holds all files at once. A
   variant calling `fn` as each is decoded keeps memory at workers × one file. 90 files are about
   10 MB, so this waits for a measurement.
4. **A fused column scan (optional).** `ScanFile` unpacking block by block straight into the fold,
   skipping Sums columns the query doesn't use by walking block heads. Only worth it if profiling
   shows the decode, not allocation, dominates.

What stays expanded, honestly: a file a delta changed is merged as a map. That is the live part of
the read and it is one or two files of many.

### The language

A closed subset. Anything outside it fails with a message that says how to rewrite.

```text
SELECT  item [, item]...
FROM    frame
WHERE   condition [AND condition]...
[GROUP BY column [, column]... ]
[HAVING condition [AND condition]...]
[ORDER BY name [ASC|DESC] [, ...]]
[LIMIT n]
```

- **`FROM`** is the frame name (`day-product`; `day_product` is accepted too). One frame per
  statement. No joins, subqueries, `DISTINCT`, `OFFSET` or `UNION`.
- **Columns** are the snake_case names of the frame's Keys, Rows and Sums fields (`fecha`,
  `client_id`, `product_id`, `quantity`, `amount`). `DESCRIBE` (below) lists them.
- **`item`** is a grouping column, an aggregate, or an expression over aggregates, with `AS alias`.
  Arithmetic: `+ - * /`, parentheses, integer and decimal literals. `/` is decimal division.
- **Aggregates** are over the rows of the frame's files inside the filter:
  - `SUM(col)`, `MIN(col)`, `MAX(col)`, `AVG(col)`: over the Sums column per (file row).
    `MAX(amount)` with `GROUP BY product_id` is the best **day** of that product (each file holds
    one day's sum), not the largest sale.
  - `COUNT(*)`: the number of (file, row) pairs in each group: with `GROUP BY product_id` over
    `day-product` that is **the days the product sold**. It is not the number of sales (D3).
- **Date functions** on `fecha` (a UnixDay): `TODAY()`, `WEEK(fecha)`, `MONTH(fecha)`; literals as
  `'2026-09-30'`. Dates are resolved with the same calendar rules as `agenttools/period.go`.
- **Money** is integer cents, as stored. The result says which columns are cents; the tool layer
  formats them.

**Validation, before any read:**

| Rule | Why |
|---|---|
| `WHERE` bounds `Keys[0]` with `=`, or `BETWEEN`, or `>=`/`<=`/`>`/`<` pairs: at most 400 days | `QueryFrame`'s limit; also the cost bound |
| `=` on later Keys, in order, is pushed to the read; skipping one means the rest are not pushed | `QueryFrame` takes only a leading run of Eq on the later Keys |
| Any other filter (`product_id IN (...)`, `BETWEEN`, `<`, `amount > 0`) is applied by the engine after the read | the read takes no other predicate |
| `GROUP BY` columns are Keys or Rows of the frame (or `WEEK(fecha)` / `MONTH(fecha)`) | a Sums column is a value, not a dimension |
| Every non-aggregate `SELECT` item is in `GROUP BY` | standard SQL |
| `LIMIT` is required unless the result is at most `maxRows` (default 1,000) | bounds what the agent reads back |

**Errors an agent can act on** (all name the frame and the fix): *"`day_product` has no column
`client_id`; use `day_client_product`"*, *"`WHERE` must bound `fecha` (a range of at most 400
days)"*, *"`fecha` range is 612 days; the maximum is 400"*, *"`product_id IN (…)` is applied after
reading every file of the range; narrow `fecha` first"* (a warning in the result, not an error).

### Engine

- **Per-worker accumulators.** `Scan` runs the files through N workers; each folds into its own
  accumulator and the partials are merged at the end. Every aggregate is decomposable: sums add,
  min/max combine, `COUNT(*)` adds, `AVG` is `SUM`/`COUNT` computed after the merge.
- **Accumulator layout.** One group column with a small id range (`max − min` under a cap): a dense
  slice indexed by `id − min`, one array per aggregate. Otherwise an open-addressing hash from the
  packed group key to a slot, with the aggregates in parallel arrays. Group keys are `int64`s
  (the frame is integers only), so no strings are hashed.
- **Filters** on Rows or Sums columns run per row inside the fold, on the decoded columns.
- **Order and limit.** `ORDER BY ... LIMIT k` uses a heap of size k: O(groups × log k). Without
  `LIMIT`, a sort of the merged groups, capped by `maxRows`.
- **Result:** `Columns` (name, type: integer, cents, day, decimal), `Rows [][]any`-free: typed
  columns as `[]int64` / `[]float64` slices, plus `Snapshot` (the frame's `w`), `Fresh bool` and the
  files read. The tool layer renders them.

### Introspection for the agent

- `DESCRIBE day_product` and `SHOW FRAMES` are parsed as statements of the same tool. They return
  each frame's name, Keys, Rows, Sums, whether it takes `Fresh`, and one example query. The
  descriptions come from the schema's column names; bilingual labels live in the tool layer.
- The tool's own description carries the grammar above and two examples, so the agent seldom needs
  `DESCRIBE`.

### berryapps wiring

| File | Change |
|---|---|
| `backend/db/db.go` | Alias for the SQL entry point |
| `backend/core/agent/agenttools/` | A `frame_sql` tool: input `{sql}`, output a table/chart card. Uses the period notation for `TODAY()` arithmetic. See the `create-agent-tool` skill |
| `backend/example/types/sales.go` | D5, done: `Amount` in `day-product`'s Sums |

## Reads are live (`Fresh`)

FrameSQL always reads `Fresh()`: the agent must see the sale entered a minute ago. The cost, from
`Fresh`'s own documentation, is one delta read of about 10–30 minutes of writes of the whole entity,
once per statement, plus the files. A statement that tolerates old data could skip it; that is a
later option (`/*+ stale */` or a tool flag), not v1.

## Limits

- One frame per statement; no joins. Combining two frames is two statements, joined by ID in the
  tool layer (names via `fetch-record-by-id`).
- A range is at most 400 days (the `QueryFrame` cap).
- `day-client-product` without a `client_id =` reads every client's file of every day. The planner
  estimates the files from the `_idx` of each day and refuses above a cap, naming the cheaper frame.
- Aggregates are limited to what a Sums column can give: no `COUNT(DISTINCT)`, medians or
  percentiles.
- `COUNT(*)` is rows of files, not sales (D3).

## Tests

- **Parser** (`sqlparse_test.go`): a table of valid statements to their AST, and of invalid ones to
  their exact error text. Fuzz: arbitrary input never panics.
- **Planner:** the pushdown split (`Eq`/`Between` to the read, the rest residual) for each shape of
  `WHERE`; every validation rule has a failing case.
- **Engine, against brute force:** random frames and statements; the engine's rows must equal a
  naive aggregation over the same files (dense and hash accumulators, 1 and many workers, both
  merge orders).
- **Live read:** the `ormcheck` frame on the in-memory store: write, materialize, write more
  (so the delta is non-empty), and compare a statement's result with the records. A file the delta
  doesn't touch must come through `Scan` unchanged (change 2).
- **Allocation:** a benchmark of 90 files × 5,000 rows, before and after change 1, reporting
  allocations per statement.

## Implementation order

1. D5 is done (`Amount` on `day-product`); deploy it early so the frame rebuilds before FrameSQL needs it.
2. `dataframe/` + `dynamo/`: change 1 (`Scan`) and change 2 (delta only where it lands), with the
   allocation benchmark; `Exec` rewritten on `Scan`. Existing tests must pass unchanged.
3. `sqlparse.go` and `plan.go`, with tests.
4. `engine.go`, with the brute-force test.
5. Measure; then changes 3 and 4 only if the numbers ask for them.
6. README section "FrameSQL", `RATIONALE.md` entries, the `dynamo/skill/SKILL.md` section.
7. Commit and push inside the submodule.
8. berryapps: the `frame_sql` tool, the golden discovery eval, deploy
   the tools layer (`create-agent-tool`).

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
