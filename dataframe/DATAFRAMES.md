# DataFrames (`github.com/ivanjoz/genix-orm/dataframe`)

A DataFrame is a **precomputed group-by report of a table, stored as small columnar files** in a
file store (S3). It answers questions like "minutes and points per employee, for a year and a
program" with one GET, without reading the records.

This module holds all the logic and no database code. A backend (genix-orm's `dynamo` today,
`scylla` later) plugs in through the `Backend` interface, a `utils.FileStore`, and a few duties on
its write path (section 9). It depends only on the standard library and genix-orm's `utils`.

```text
dataframe/
  table.go       Column, Spec → Compile → Table / Frame; file keys, the write check
  codec.go       the frame file format (section 3)
  changelog.go   the per-record changelog and valuesAt (section 4)
  run.go         Materialize: build, rebuild and incremental runs (sections 5–6)
  read.go        Read: plain and fresh (section 7)
  backend.go     Backend, State, ErrFrameNotBuilt (section 9)
```

## 1. What a frame is

A frame is declared by three things:

| Part | What it is | Example (voluntariado enrollments) |
|---|---|---|
| **Keys** (1–3 integer columns) | which **file** a record goes to | `Year`, `ProgramID` |
| **Rows** (1 integer column) | which **row** of that file | `EmployeeID` |
| **Sums** (integer or float columns), and/or **Count** | what each row adds up | `RecordedMinutes`, `Points` |

So every distinct `(Year, ProgramID)` gets one file, and inside it one row per employee holding the
sum of that employee's minutes and points over their enrollments in that year and program:

```text
file dtf/3gz10/enrollment-report/P49QA9-/2026/3      (Year 2026, ProgramID 3)

  EmployeeID   RecordedMinutes   Points
  1001         120               10
  1002          60                5
  1005         300               20
```

A soft-deleted record (`Status = 0`) is in no file. A record that changes a Key moves to another
file; one that changes its Rows value moves to another row; one that changes a Sum changes its row.

A GroupBy counter (dynamo) keeps one item per group. A frame keeps a whole table of rows per file,
so a report over thousands of employees is one read.

## 2. The lifecycle at a glance

```text
  write path (backend)                 schedule (every ~10 min)                 read
  ─────────────────────                ─────────────────────────                ────
  every record write also              Materialize, per frame:                  Read(selection):
  stores the record's                    not built / shape changed /              the selected files,
  created and changelog                  too far behind → BUILD a new             as the last run left
  (what it held in the                   generation from a full scan              them (plain), or plus
  last ~36 h)                            otherwise → RUN: rewrite only the        the records written
          │                              files the recently written records       since (fresh)
          │                              touched, then move the snapshot
          └────────── records ──────────►        │                                       ▲
                                                 └──────── files in S3 ──────────────────┘
```

Four ideas carry everything:

1. **Files are the report**, one per Keys value, each stamped with the moment (**snapshot**) it
   is exact at.
2. **The frame's state** (kept by the backend) names the current **generation** folder, the
   **shape** of its files, and the frame's snapshot **w**: every file is exact at w or later.
3. **Every record knows its recent past** (its changelog), so the module can ask any record "what
   did you hold at moment X?". That is what lets a run update a file by subtracting what a record
   held at w and adding what it holds now, without an append log.
4. **Every step is conditional and idempotent**: a crashed run leaves nothing wrong, the next one
   finishes it.

## 3. The frame format

### 3.1 Folders and generations

```text
dtf/<table id>/<frame>/<generation>/<k1>            file of a 1-key frame
dtf/<table id>/<frame>/<generation>/<k1>/<k2>_<k3>  file of a 2–3-key frame
```

- `<table id>` is the backend's numeric table id and `<generation>` the build time in Unix
  milliseconds, both in `utils.EncodeUintBase64` (genix-orm's URL-safe base64 in ASCII order):
  `78901377` → `3gz10`, a 2026 build → 7 characters such as `P49QA9-`. A build writes a whole new
  generation beside the old one, switches the state to it, then deletes the old one, so readers
  never see a half-built frame.
- Keys are non-negative integers written in decimal. Grouping by `<k1>/` lets a read select a
  range of the first Key and list the files under each value (section 7).
- A missing file means "no rows". A file whose rows all sum to 0 is not created.

### 3.2 The frame state

One record per frame, kept by the backend (dynamo: an item `pk = <table base pk>‖000`,
`sk = "f" + <frame name>`):

| Field | Meaning |
|---|---|
| `Generation` | the folder of the current files; empty when not built |
| `Shape` | a hash of the file layout (Keys/Rows/Sums column ids, Count, format version). If the declaration changes, the shape changes and the frame is rebuilt |
| `Snapshot` (w) | a SUnixTime: every file holds the records as of w or later |
| lock holder + expiry | so only one run or build works on the frame at a time |

### 3.3 The file: columnar, compressed

A file holds its rows sorted by Rows value, stored **by column**: first the Rows column, then one
column per Sum (in declaration order), then the record count when the frame has `Count`. Floats are
stored as integers, `round(v × 10⁶)`, so adding and subtracting the same value is exact.

```text
u8       format version
uvarint  snapshot: the SUnixTime this file is exact at
uvarint  row count n                       (nothing follows when n = 0)
column   Rows    first value, smallest step, then the n − 1 steps less that smallest step
column   Sum 1   minimum, then each value less the minimum
column   …       one per Sum, then Count
```

Each column becomes a list of small **non-negative residuals**, and those are what get packed:

1. **Transform, fixed by the column's role** (so no transform byte is stored):
   - Rows ascend, so they are **delta-coded**: consecutive IDs become steps of 1, and subtracting
     the smallest step turns them into zeros.
   - Sums get their **minimum subtracted** (frame of reference), which also makes negative sums
     non-negative.
2. **Common divisor:** the residuals are divided by the largest of 1000, 100, 10, 8, 5, 2 that
   divides them all (amounts in multiples of 1000 save 10 bits each).
3. **Bit packing in blocks of 128:** each block stores a head byte with its bit width, then every
   residual packed at that width with no gaps. The width is the cheapest exactly: either the widest
   residual's bit length, or a narrower one where the few larger residuals keep their extra high
   bits in **patches** (position + high bits). Patches handle the typical heavy tail of sums, such
   as a few top sellers per block.

Worked example: the file above (3 rows, two Sums).

| Column | Values | After transform | Divisor | Residuals | Width |
|---|---|---|---|---|---|
| EmployeeID | 1001, 1002, 1005 | first 1001, min step 1, steps 0, 2 | 2 | 0, 1 | 1 bit |
| RecordedMinutes | 120, 60, 300 | min 60: 60, 0, 240 | 10 | 6, 0, 24 | 5 bits |
| Points | 10, 5, 20 | min 5: 5, 0, 15 | 5 | 1, 0, 3 | 2 bits |

The whole file is 22 bytes, 7 of them header. A file of thousands of rows stays a few bytes per row.
`codec_test.go` compares the sizes with and without each step.

## 4. How records feed the frames: created and changelog

To update a file incrementally, a run must know, for every record written since w, **what it held
at w** (to take it out) and **what it holds now** (to put it in). Records don't keep old versions,
so each one carries two small extra values beside its data:

- **created**: the SUnixTime it was first written (dynamo: the item attribute `crt`).
- **changelog**: a compact binary list (dynamo: `cgl`) of the values its **tracked columns** held
  before each write that changed them, newest first, covering the last ~36 hours.

The tracked columns are every frame's Keys, Rows and Sums, plus `Status`. With the current values,
`Updated`, created and the changelog, `valuesAt(X)` rebuilds what the record held at any moment X
within that window:

- if `X < created`, the record did not exist yet;
- otherwise, start from the current values and undo every change made after X, newest first.

The changelog stays small because an entry lists only the columns that changed, with variable-width
values (2–5 bytes for small non-negative values), and entries older than `MaxDelta` (65 535 ticks of
2 s ≈ 36.4 h) are dropped on the next write. An insert has no changelog. The byte layout is
documented in `changelog.go`. The backend never builds it by hand: it calls `Table.NextChangelog` on
every write (section 9).

Because every operation only asks "what did this record hold at X?", runs, builds and fresh reads
are exact whatever order the writes landed in.

## 5. Building and rebuilding a frame

A frame is **built** when it has no generation yet, when its shape changed, or when its snapshot is
so old that the changelogs no longer reach it (within an hour of the 36-hour window). `Materialize`
does this on its own; `Rebuild(table, backend, store, frameName, now)` forces it, for example after a
data fix that bypassed the write path.

Step by step (`rebuildFrame`):

1. Take the frame's lock (5 minutes; its writes stop 30 s before it ends).
2. Pick the target `T = now − 2 minutes` (`TargetLag`) and a new generation name.
3. `ScanHistories`: read every record of the table, page by page, and add what each one held **at T**
   (`valuesAt(T)`) to its file and row in memory. Records created after T are left out.
4. Write every file of the new generation at snapshot T. The files are new (`If-None-Match`).
5. `CommitGeneration`: switch the state to the new generation, with the frame's shape and w = T.
   From this moment reads use it.
6. Delete every other generation in the frame's folder.

Writes that land during the scan don't break it: each record's changelog still says what it held at
T. The next run picks up everything written after T.

## 6. Keeping it current: the incremental run

`Materialize(table, backend, store, now)` runs on a schedule (berryapps: every 10 minutes, from
`core/frames`). For each frame that doesn't need a build (section 5), it runs:

1. **Lock** the frame. A frame held by another run is skipped.
2. **Target** `T = now − 2 minutes`. Every write stamped before T has landed and is visible to the
   backend's index on `Updated`.
3. **Find what moved:** `HistoriesWrittenAfter(w)`, the only records whose values can differ from
   what the files hold.
4. **Find the files to touch:** every file those records were in at any moment of `[w, T]`: at w,
   after each of their changes, and at T. A record that moved from Program 3 to Program 4 touches
   both files.
5. **Rewrite each file:** read it (rows, its own snapshot `s`, its ETag). Then, for every moved
   record, subtract what it held at `max(s, w)` and add what it holds at T. Write the file at
   snapshot T, conditioned on the ETag read. A file already at T or later is left as is. A missing
   file that would stay empty is not created.
6. **Commit:** `CommitSnapshot(w → T)`, only if the lock, the generation and w are still the ones
   read.

**Crashes.** A run that dies halfway leaves some files at T and the rest at w, with the state still
at w. The next run reads each file's own snapshot and subtracts from there, so nothing is counted
twice or lost. A run whose lock expired fails its conditional writes and its commit, and stops.

## 7. Reading a frame

`Read(table, frame, backend, store, selection, isFresh)` (dynamo: `Repo.QueryFrame(name)`).

1. **Select the files.** A `Selection` takes a range of the first Key (at most 400 values) and, for
   the later Keys, the values of a leading run (`PinnedKeys`):
   - every Key pinned: the file keys are built directly, one GET each;
   - otherwise: the folder of each first-Key value is listed, then the matching files are read.
2. **Read the state.** If the frame is not built in its current shape, the read fails with
   `ErrFrameNotBuilt`; the next `Materialize` builds it.
3. **Plain read:** decode the files and return their rows, as the last run left them (2 to 12
   minutes behind with a 10-minute schedule).
4. **Fresh read:** also read `HistoriesWrittenAfter(w)` and bring the selected files up to the
   records' current values, the same subtract/add as a run, including files a change created that
   are not listed yet. If a run moved w while it read, it starts over (3 attempts).
5. Rows come back sorted by Keys, then Rows value, as `Row{Keys, RowID, Sums}`. Sums are in
   declaration order, followed by the count.

```go
// berryapps, through the dynamo adapter
rows, err := types.Enrollments.QueryFrame("enrollment-report").
    Eq(types.Enrollments.T.Year, 2026).
    Eq(types.Enrollments.T.ProgramID, 3).
    Exec()
for _, row := range rows {
    minutes := row.Sum(types.Enrollments.T.RecordedMinutes)
}
```

## 8. Declaring frames

In the dynamo schema (berryapps `voluntariado/types/activities.go`):

```go
DataFrames: []db.DataFrame{{
    Name: "enrollment-report",
    Keys: db.Cols(table.Year, table.ProgramID),
    Rows: table.EmployeeID,
    Sums: db.Cols(table.RecordedMinutes, table.Points),
}},
```

The table also needs a GSI on `Updated` for `HistoriesWrittenAfter`. The adapter turns this into
`dataframe.Compile`, which any backend calls once at schema compile time:

```go
table, err := dataframe.Compile("Enrollment", 78901377, []dataframe.Spec{{
    Name: "enrollment-report",
    Keys: []dataframe.Column{{FieldName: "Year", ColumnID: 24}, {FieldName: "ProgramID", ColumnID: 25}},
    Rows: dataframe.Column{FieldName: "EmployeeID", ColumnID: 2},
    Sums: []dataframe.Column{{FieldName: "RecordedMinutes", ColumnID: 10}, {FieldName: "Points", ColumnID: 18}},
}}, &dataframe.Column{FieldName: "Status", ColumnID: 14})
```

- `ColumnID` is the field's stable id (genix-orm's `cb` tag). Files and changelogs name columns by
  it, so renaming a Go field keeps them.
- Names: `a-z`, `0-9`, `-`. 1–3 Keys and the Rows are integers (non-negative values); Sums are
  integers or floats; a frame needs Sums or `Count`.
- `Table.Tracked` lists the tracked columns in column id order. **Every tracked-values slice the
  backend passes in follows that order**, with floats already scaled (`ScaledFloat`).
- Limits: at most 63 tracked columns, column ids 1..255, values within ±2⁶⁰.

## 9. What a backend provides

### The file store (`utils.FileStore`)

`GetFile` (with ETag), `PutFileIfMatch` (conditional on the ETag, `""` = must not exist),
`ListFiles`, `DeleteFiles`. Errors wrap `utils.ErrFileNotFound` / `utils.ErrFilePreconditionFailed`.
It is the same store as every other genix-orm feature that keeps files; `utils.NewMemoryFileStore()`
is the in-memory one for tests.

### `Backend`

| Method | What it does |
|---|---|
| `ReadState(frame)` | the frame's `State{Generation, Shape, Snapshot}`, read consistently |
| `TakeLock(frame, holderID, now, until)` | a conditional write: take the lock if absent or expired; returns the state |
| `ReleaseLock(frame, holderID)` | free it if still held by holderID (best effort) |
| `CommitSnapshot(ctx, frame, holderID, state, w)` | move the snapshot, only if the lock, generation and old snapshot still hold |
| `CommitGeneration(ctx, frame, holderID, generation, w)` | make a built generation current (with `frame.Shape`) |
| `HistoriesWrittenAfter(moment)` | every record with `Updated > moment`, as `History` (dynamo: the GSI on Updated) |
| `ScanHistories(onPage)` | every record of the table, page by page |

A `History` is built with `NewHistory(recordKey, trackedValues, created, updated, changelog)`.

### Duties on the write path

1. Before writing over a stored record, compute `Table.NextChangelog(stored changelog, stored
   Updated, written Updated, stored values, written values)` and store the result with the record
   (none when nil). This needs the stored record: read it, or prove it with a conditional write.
2. Store `created` on insert and keep it on every later write.
3. Call `Table.CheckValues(tracked)` before the write (negative Keys/Rows, out-of-range values).
4. Never delete a record physically: soft-delete it (`Status = 0`), which takes it out of the frames.
5. Stamp `Updated` with the write time (SUnixTime).

## 10. Exactness and limits

- Exact for writes that read the record they replace (dynamo: `Modify`, `PutManyIfVersion`,
  `PutMany`). Two unconditional writes racing on one record compute their changelog from the same
  read: the loser's change is lost until a rebuild.
- A silent rewrite that changes tracked columns without moving `Updated` (dynamo:
  `ReprocessRecords`) is invisible to runs: rebuild after it.
- A frame whose runs stop for more than ~35 hours is rebuilt on the next run (one table scan).
- A write stamped by a clock more than 2 minutes behind could land below a T a run already passed.
- A row whose sums net to 0 disappears from its file. Declare `Count` if a row must stay visible
  while its records exist.

## 11. Tests

- `dataframe_test.go` drives the real `Materialize` and `Read` with an in-memory `Backend` and file
  store over random histories, and checks them against brute force. The histories include inserts,
  updates of every column, floats, negative sums, soft deletes and restores, and runs that crash
  halfway.
- It also covers the changelog's sizes, its shift/drop rules, and `valuesAt` against the recorded
  history.
- `codec_test.go` covers the file format and its compression steps.

## 12. Adding a backend (scylla)

1. Map the schema's frame declarations to `Spec`/`Column` and call `Compile`.
2. Store `created` and the changelog as two columns of the row, outside the record's blob, and
   follow the write-path duties (section 9).
3. Keep one state row per frame (generation, shape, snapshot, lock expiry and holder), with
   conditional updates (LWT) for `TakeLock` / `CommitSnapshot` / `CommitGeneration`.
4. Serve `HistoriesWrittenAfter` from an index or a materialized view on `Updated`.
5. Pass the same `utils.FileStore` as the other features (S3).
