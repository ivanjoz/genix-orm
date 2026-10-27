# DataFrames (`github.com/ivanjoz/genix-orm/dataframe`)

A DataFrame is a **precomputed group-by report of a table, stored as small columnar files** in a
file store (S3). It answers questions like "minutes and points per employee, for a year and a
program" with one GET, without reading the records.

This module holds all the logic and no database code. A backend (genix-orm's `dynamo` and `scylla`)
plugs in through the `Backend` interface, a `utils.FileStore`, and a few duties on its write path
(section 9). It depends only on the standard library and genix-orm's `utils`.

A frame follows the table's **partition** (a company): every partition has its own files and its own
versions file, and a read names one partition. One state, one lock and one run cover every partition
of a frame. dynamo has no partitions and puts every record in partition 0.

```text
dataframe/
  table.go       Column, Line, Spec → Compile → Table / Frame; LinesOf, file keys
  projection.go  ProjectionOf: a frame declared by a ToFrames method of the record (section 1.1)
  codec.go       the frame file format (section 3)
  versions.go    the versions file: 255 slots, the moment each was last raised (section 3.4)
  changelog.go   the per-record changelog of lines, History and linesAt (section 4)
  run.go         Materialize: build, rebuild and incremental runs; RebuildPartition (sections 5–6)
  read.go        Read: plain and fresh (section 7)
  changed.go     ReadChanged: the files a client holding raw copies must replace (section 7.1)
  backend.go     Backend, State, ErrFrameNotBuilt (section 9)
  state_file.go  FileState: the state in a file of the store, for scylla (section 3.2)
```

## 1. What a frame is

A frame is declared by three things:

| Part | What it is | Example (voluntariado enrollments) |
|---|---|---|
| **Keys** (1–3 integer columns) | which **file** a record goes to | `Year`, `ProgramID` |
| **Rows** (1–3 integer columns) | which **row** of that file | `EmployeeID` |
| **Sums** (integer or float columns), and/or **Count** | what each row adds up | `RecordedMinutes`, `Points` |

So every distinct `(Year, ProgramID)` gets one file, and inside it one row per employee holding the
sum of that employee's minutes and points over their enrollments in that year and program:

```text
file dtf/3gz10/enrollment-report/P49QA9-/0/2026/3    (partition 0, Year 2026, ProgramID 3)

  EmployeeID   RecordedMinutes   Points
  1001         120               10
  1002          60                5
  1005         300               20
```

With several Rows columns a row is one combination of their values: `Rows: DepartmentID, ProgramID,
EmployeeID` gives one row per department × program × employee. Declare them **from the widest group
to the narrowest**: the file stores them nested like folders, each department once with its programs
under it and each program once with its employees under it (section 3.3).

A soft-deleted record (`Status = 0`) is in no file. A record that changes a Key moves to another
file; one that changes a Rows value moves to another row; one that changes a Sum changes its row.

A GroupBy counter (dynamo) keeps one item per group. A frame keeps a whole table of rows per file,
so a report over thousands of employees is one read.

### 1.1 Lines: what a record puts in a frame

The engine never reads a record's columns. A record gives each frame its **lines**: a `Line` is one
`[]int64` of the frame's Keys, Rows and Sums values, in that order, and a record gives 0..n of
them. Everything below (the changelog, the runs, the reads) compares and sums lines.

A frame is declared one of two ways:

- **By columns** (`Keys`/`Rows`/`Sums`/`Count`, above): the record gives one line from those
  columns, none when `Status = 0`.
- **By a `ToFrames` method** of the record, returning a slice of a frame struct whose fields are
  tagged `frame:"key"`, `frame:"row"` or `frame:"sum"` (`ProjectionOf`). The method decides
  everything: a sale gives one line per product it sold, an annulled one none. A list column, a
  status rule or a currency conversion stay in the method; the engine only sees the lines. The
  struct's fields are the frame's columns, ids by field position.

```go
type SaleSummaryFrame struct {
    Date       int16 `frame:"key"`
    ProductID  int32 `frame:"row"`
    SubDivisor int16 `frame:"row"`
    AmountPEN  int32 `frame:"sum"`
    SoldUnits  int32 `frame:"sum"`
}
func (e *SaleOrder) ToSaleSummaryFrames() []SaleSummaryFrame
```

Two lines of one record with the same Keys and Rows add up in their row. `Count` adds 1 per line.
`Spec.Version` is part of the frame's shape: raise it when the method's logic changes and the frame
is rebuilt.

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
dtf/<table id>/<frame>/<generation>/<partition>/<k1>            file of a 1-key frame
dtf/<table id>/<frame>/<generation>/<partition>/<k1>/<k2>_<k3>  file of a 2–3-key frame
dtf/<table id>/<partition>.versions                             the partition's versions file (3.4)
dtf/<table id>/<frame>.state                                    the frame's state (scylla, 3.2)
```

- `<table id>` is the backend's numeric table id and `<generation>` the build time in Unix
  milliseconds, both in `utils.EncodeUintBase64` (genix-orm's URL-safe base64 in ASCII order):
  `78901377` → `3gz10`, a 2026 build → 7 characters such as `P49QA9-`. A build writes a whole new
  generation beside the old one, for every partition, switches the state to it, then deletes the
  old one, so readers never see a half-built frame.
- The partition comes after the generation: a generation spans every partition, so the build's
  "delete every other generation" stays one listing of the frame's folder.
- The partition and the Keys are non-negative integers written in decimal. Grouping by `<k1>/`
  lets a read select a range of the first Key and list the files under each value (section 7).
- The versions and state files sit beside the frames' folders with a suffix (`.versions`,
  `.state`), never inside one, so no listing of a frame's folder sees them.
- A missing file means "no rows". A file whose rows all sum to 0 is not created.

### 3.2 The frame state

One record per frame, for every partition, kept by the backend. dynamo: an item `pk = <table base
pk>‖000`, `sk = "f" + <frame name>`, with conditional updates. scylla: `FileState`, the file
`dtf/<table id>/<frame>.state` (`{"g", "s", "w"}`), written conditioned on the ETag it had when the
lock was taken, with the lock itself external (`AcquireLock`: fareward in genix). The lock only
spares duplicate work: a run whose lock lapsed and that another overtook fails its file writes and
its commit on their ETags.

| Field | Meaning |
|---|---|
| `Generation` | the folder of the current files; empty when not built |
| `Shape` | the file layout and what fills it (Keys/Rows/Sums column ids, Count, the zero sums mask, `Version`, format version). If the declaration changes, the shape changes and the frame is rebuilt |
| `Snapshot` (w) | a SUnixTime: every file holds the records as of w or later |
| lock holder + expiry | so only one run or build works on the frame at a time |

### 3.3 The file: columnar, compressed

A file holds its rows sorted by their Rows values, stored **by column**: first the Rows, then one
column per Sum (in declaration order), then the record count when the frame has `Count`. Floats are
stored as integers, `round(v × 10⁶)`, so adding and subtracting the same value is exact.

```text
u8       format version (4), bit 7 set when the file carries the zero sums mask
u8       slot: the file's versions slot, 1..255 (section 3.4)
uvarint  snapshot: the SUnixTime this file is exact at
uvarint  row count n                       (nothing follows when n = 0)
rows     the Rows columns, nested (below)
[u8      zero sums mask: bit s set when summed column s holds a non-zero value]
column   Sum 1   minimum, then each value less the minimum
column   …       one per Sum, then Count (with the mask: only those whose bit is set)
```

**The zero sums mask** is opt-in per frame (`UseZeroSumsMask`, at most 8 summed columns Count
included): a summed column that is zero in the whole file then costs nothing. Declare the Sums
from the most to the least often non-zero. It pays where most files leave some Sums empty (no
sub-units sold, no debt left); a frame whose Sums are always set skips the byte.

**A constant column** (all its residuals zero, below) is written as its minimum and a divisor of 0,
with no blocks: 2 bytes for an all-zero column, and a nested level whose nodes all hold one value
(a `SubDivisor` that never varies) costs a byte or two per column.

**The Rows, nested.** With one Rows column it is a single ascending column: first value, smallest
step, then the n − 1 steps less that smallest step. With 2 or 3, the sorted rows form a tree (a
department, its programs, each program's employees) and the file stores it level by level, so an
outer value is written once instead of on every row:

```text
uvarint  g1: the distinct values of Rows[0]
level 1  those g1 values, an ascending column
level k  for each node of level k − 1 (a distinct prefix of the Rows before):
           column  how many values of Rows[k−1] it holds      (minimum, then each less it)
           column  the first of them                          (minimum, then each less it)
           steps   the steps between its next ones, all nodes in a row (smallest step, then each less it)
```

The last level holds the n rows. The reader rebuilds each outer column by repeating a node's value
over the rows under it. Packing the IDs into one integer instead (`department << 46 | program << 31
| employee`) makes the step jump by 2³¹ or more at every department and program boundary, a 4–6 byte
patch each, and needs a bit budget and pack/unpack code in the module. `TestFrameFileNestedRowsSize`
measures a volunteer-month file of 6 small sums: 4–11% smaller nested (the Rows alone, 8–18%).

Each column becomes a list of small **non-negative residuals**, and those are what get packed:

1. **Transform, fixed by the column's role** (so no transform byte is stored):
   - Rows ascend, so they are **delta-coded**: consecutive IDs become steps of 1, and subtracting
     the smallest step turns them into zeros. The steps inside the nodes of a nested level are
     coded the same way.
   - Sums, and a nested level's counts and first values, get their **minimum subtracted** (frame
     of reference), which also makes negative sums non-negative.
2. **Common divisor:** the residuals are divided by the largest of 1000, 100, 10, 8, 5, 2 that
   divides them all (amounts in multiples of 1000 save 10 bits each). Divisor 0 marks a constant
   column: every residual is zero and no blocks follow.
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

The whole file is 23 bytes, 8 of them header. A file of thousands of rows stays a few bytes per row.
`codec_test.go` compares the sizes with and without each step.

### 3.4 The versions file: which copies are stale

A client (the browser) keeps raw files and must learn which of its copies changed without asking
for each one. `dtf/<table id>/<partition>.versions` (1,025 bytes) answers that for every frame of
the table in that partition, so one company's writes never mark another's copies stale:

```text
u8        version (1)
u32       counter: how many slots were ever handed out
u32 × 255 per slot 1..255, the last moment (SUnixTime) a file on it was rewritten
```

- A file gets a slot when it is first written: `counter % 255 + 1`, round-robin, stored in the
  file's header. Files share slots; there is no eviction.
- Before a run or a build writes files, it **raises** their slots to its target T (never lowers
  them), with a conditional write on the ETag, retried. Raising comes before the file writes and
  before the commit, so a crash leaves slots too new (a needless refetch), never too old.
- A copy taken at snapshot S on slot k is stale when `SlotTime(k) > S`. Another file on the same
  slot rewritten later makes it look stale too: a refetch of an unchanged file, which is cheap. Slot
  0 (a file no run created yet) always reads as stale.

The client never reads the versions file itself: `ReadChanged` (7.1) checks it server-side.

## 4. How records feed the frames: created and changelog

To update a file incrementally, a run must know, for every record written since w, **what it held
at w** (to take it out) and **what it holds now** (to put it in). Records don't keep old versions,
so each one carries two small extra values beside its data:

- **created**: the SUnixTime it was first written (dynamo: the item attribute `crt`).
- **changelog**: a compact binary list (dynamo: `cgl`) of the **lines** (1.1) each frame held
  before each write that changed them, newest first, covering the last ~36 hours.

The changelog is a function of the lines, not of the record's columns: a write that changes a
column no frame's lines depend on logs nothing. With the current lines, `Updated`, created and the
changelog, `linesAt(frame, X)` rebuilds the lines the record gave a frame at any moment X within
that window:

- if `X < created`, the record did not exist yet;
- otherwise, start from the current lines and take, for each frame, the lines logged by the oldest
  change made after X (what it held just before it), if any.

An entry lists only the frames whose lines changed, each under a 16-bit tag of its shape and its
payload length, so the entry of a frame whose declaration changed is skipped (that frame is rebuilt
anyway). Values are zigzag varints. Entries older than `MaxDelta` (65 535 ticks of 2 s ≈ 36.4 h)
are dropped on the next write. An insert has no changelog. The byte layout is documented in
`changelog.go`. The backend never builds it by hand: it calls `Table.NextChangelog` on every write
(section 9).

Because every operation only asks "what did this record hold at X?", runs, builds and fresh reads
are exact whatever order the writes landed in.

**scylla keeps no changelog on the record.** Each write that inserts a record or changes a frame's
lines appends one row to the table's change log, `<table>__dtf_log`, with the same body as a
changelog entry (`Table.EncodeChangedLines`): the lines the changed frames held before it, or an
insert flag. A History is then built from the record's lines now, taken from the stored row, and
its logged changes (`Table.NewHistoryOfChanges`); linesAt reads them the same way. See section 12.

## 5. Building and rebuilding a frame

A frame is **built** when it has no generation yet, when its shape changed, or when its snapshot is
so old that the changelogs no longer reach it (within an hour of the 36-hour window). `Materialize`
does this on its own; `Rebuild(table, backend, store, frameName, now)` forces it, for example after a
data fix that bypassed the write path.

Step by step (`rebuildFrame`):

1. Take the frame's lock (5 minutes; its writes stop 30 s before it ends).
2. Pick the target `T = now − 2 minutes` (`TargetLag`) and a new generation name.
3. `ScanHistories`: read every record of the table, page by page, and add what each one held **at T**
   (its lines, `linesAt(frame, T)`) to their files and rows in memory. Records created after T are left out.
4. Give every file a new slot raised to T (3.4), then write the files of the new generation at
   snapshot T. The files are new (`If-None-Match`).
5. `CommitGeneration`: switch the state to the new generation, with the frame's shape and w = T.
   From this moment reads use it.
6. Delete every other generation in the frame's folder.

Writes that land during the scan don't break it: each record's changelog still says what it held at
T. The next run picks up everything written after T.

**One partition** (`RebuildPartition(table, backend, store, partition, now)`): after a partition's
rows changed outside the write path (a backup restore deletes them), its files are computed again
in the current generation, frame by frame under the lock: scan the partition, write every file of
it at T conditioned on its ETag, w left as is. A stored file no record is in any more is rewritten
empty, not deleted, so a later run that moves a record out of it still finds what it held. Each
file's own snapshot (T, past w) is what the next run subtracts from. A frame not built yet is left
to Materialize.

## 6. Keeping it current: the incremental run

`Materialize(table, backend, store, now)` runs on a schedule (berryapps: every 10 minutes, from
`core/frames`). For each frame that doesn't need a build (section 5), it runs:

1. **Lock** the frame. A frame held by another run is skipped.
2. **Target** `T = now − 2 minutes`. Every write stamped before T has landed and is visible to the
   backend's index on `Updated`.
3. **Find what moved:** `HistoriesWrittenAfter(w)`, the only records whose values can differ from
   what the files hold.
4. **Find the files to touch:** every file those records were in at any moment of `[w, T]`: at w,
   at their creation, after each of their changes, and at T (`filesOfHistory`). A record that moved
   from Program 3 to Program 4 touches both files. The creation moment counts because an insert
   has no changelog entry: a record inserted and moved away within `[w, T]` passed through a file
   that a client may hold with it inside.
5. **Rewrite each file:** read it (rows, its own snapshot `s`, its slot, its ETag). Then, for every
   moved record, subtract what it held at `max(s, w)` and add what it holds at T. Raise the slots of
   the files to rewrite to T and give the new ones a slot (3.4), then write each at snapshot T,
   conditioned on the ETag read. A file already at T or later is left as is. A missing file that
   would stay empty is not created.
6. **Commit:** `CommitSnapshot(w → T)`, only if the lock, the generation and w are still the ones
   read.

**Crashes.** A run that dies halfway leaves some files at T and the rest at w, with the state still
at w. The next run reads each file's own snapshot and subtracts from there, so nothing is counted
twice or lost. A run whose lock expired fails its conditional writes and its commit, and stops.

## 7. Reading a frame

`Read(table, frame, backend, store, partition, selection, isFresh)` (dynamo: `Repo.QueryFrame(name)`;
scylla: `db.QueryFrame[Record](partition, name)`). Every step below is within the partition.

1. **Select the files.** A `Selection` takes a range of the first Key (at most 400 values) and, for
   the later Keys, the values of a leading run (`PinnedKeys`):
   - every Key pinned: the file keys are built directly, one GET each;
   - otherwise: the folder of each first-Key value is listed, then the matching files are read.
2. **Read the state.** If the frame is not built in its current shape, the read fails with
   `ErrFrameNotBuilt`; the next `Materialize` builds it.
3. **Plain read:** decode the files and return their rows, as the last run left them (2 to 12
   minutes behind with a 10-minute schedule).
4. **Fresh read:** also read `PartitionHistoriesWrittenAfter(partition, w)` and bring the selected files up to the
   records' current values, the same subtract/add as a run, including files a change created that
   are not listed yet. If a run moved w while it read, it starts over (3 attempts).
5. Rows come back sorted by Keys, then Rows values, as `Row{Keys, RowValues, Sums}`. RowValues
   follow `Frame.Rows`; Sums are in declaration order, followed by the count.

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

### 7.1 Raw files for a client: ReadChanged

`ReadChanged(table, frame, backend, store, partition, selection, heldGeneration, held, now)` (dynamo:
`QueryFrame(name)…ExecChanged(heldGeneration, heldFiles)`) serves a client that keeps the raw files
and decodes them itself. The client sends the generation of its copies and, per file, its keys,
snapshot and slot, in the query string:

```text
g=P49QA9-&h=2026_3.396172200.7,2026_4.396172200.12      <keys '_'-joined>.<snapshot>.<slot>
```

The answer holds only the selected files the client must replace:

| Case | What goes out |
|---|---|
| another generation (a rebuild), or a file it doesn't hold | every selected file |
| a held copy on slot 0, or whose slot time is past its snapshot | the stored bytes |
| a file a record written since w touches, last at or after the copy's snapshot | the file moved to `now`, as a fresh read |
| a held file that no longer exists | no rows, snapshot `now`, slot 0 |

A copy at snapshot S holds every write up to S: a later write was either taken by a run (which
raised the file's slot past S before moving w) or is among the records written after w. So the
client ends as current as a fresh read. Held copies in the selection are always checked, even
unlisted ones, so a file whose records all left is answered empty.

The wire body (`AppendChangedFiles`, `DecodeChangedFiles`; the browser has its own decoder):

```text
u8       wire version (1)
uvarint  generation length, generation bytes
u8       Rows columns, u8 summed columns (Sums, then the count): what the files decode with
uvarint  w: the frame's snapshot
uvarint  file count
file*    uvarint key count, uvarint × keys, uvarint snapshot, u8 slot,
         uvarint content length (0 = no rows), the file's bytes (3.3)
```

## 8. Declaring frames

In the dynamo schema (berryapps `voluntariado/types/activities.go`):

```go
DataFrames: []db.DataFrame{{
    Name: "enrollment-report",
    Keys: db.Cols(table.Year, table.ProgramID),
    Rows: db.Cols(table.EmployeeID),
    Sums: db.Cols(table.RecordedMinutes, table.Points),
}},
```

Or by a method (1.1), in either backend (genix `sales/types/sales.go`):

```go
DataFrames: []db.DataFrame{{
    Name: "sale-summary", Version: 1, UseZeroSumsMask: true,
    ToFrames: db.ToFrames((*SaleOrder).ToSaleSummaryFrames),
}},
```

The dynamo table also needs a GSI on `Updated` for `HistoriesWrittenAfter`. The adapter turns either
into `dataframe.Compile`, which any backend calls once at schema compile time:

```go
table, err := dataframe.Compile("Enrollment", 78901377, []dataframe.Spec{{
    Name: "enrollment-report",
    Keys: []dataframe.Column{{FieldName: "Year", ColumnID: 24}, {FieldName: "ProgramID", ColumnID: 25}},
    Rows: []dataframe.Column{{FieldName: "EmployeeID", ColumnID: 2}},
    Sums: []dataframe.Column{{FieldName: "RecordedMinutes", ColumnID: 10}, {FieldName: "Points", ColumnID: 18}},
    LinesOf: linesOfEnrollment, // the record's one line, none when Status = 0
}})
```

A ToFrames frame passes `Projection: dataframe.ProjectionOf(method)` instead of the columns and
`LinesOf`.

- `ColumnID` is the column's stable id (dynamo: genix-orm's `cb` tag; scylla: the field position;
  ToFrames: the frame struct's field position). Only the shape names columns by it.
- Names: `a-z`, `0-9`, `-`. 1–3 Keys and 1–3 Rows are integers (non-negative values), no column
  twice; Sums are integers or floats; a frame needs Sums or `Count`.
- `Table.LinesOf(record)` gives every frame's lines, checked: the right length, non-negative Keys
  and Rows, values within ±2⁶⁰, floats already scaled (`ScaledFloat`).
- A ToFrames frame's rows read back with an empty `FrameRow.Key` (its columns are not the
  record's); its Keys are filtered by the record column of the same field name.

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
| `HistoriesWrittenAfter(moment)` | every record of every partition written after moment, as `History` (dynamo: the GSI on Updated; scylla: the change log) |
| `PartitionHistoriesWrittenAfter(partition, moment)` | the same for one partition (fresh reads) |
| `ScanHistories(onPage)` | every record of the table, page by page |
| `ScanPartitionHistories(partition, onPage)` | every record of one partition, page by page (`RebuildPartition`) |

A `History` names its partition and is built with `Table.NewHistory(partition, recordKey, lines,
created, updated, changelog)` (dynamo), or `Table.NewHistoryOfChanges(partition, recordKey, lines,
changes)` from logged change bodies, newest first (scylla); `lines` are `Table.LinesOf` of the record
now. `FileState` implements the five state methods over a file of the store.

### Duties on the write path

1. Before writing over a stored record, take `Table.LinesOf` of the **whole** stored record and of
   the record as written (a ToFrames method may read any field), then compute
   `Table.NextChangelog(stored changelog, stored Updated, written Updated, stored lines, written
   lines)` and store the result with the record (none when nil). This needs the stored record: read
   it, or prove it with a conditional write. `LinesOf` refuses lines out of range before the write.
2. Store `created` on insert and keep it on every later write.
3. Write nothing for the frames when no frame's lines changed.
4. Never delete a record physically: soft-delete it (`Status = 0`), which takes it out of the frames.
5. Stamp `Updated` with the write time (SUnixTime).

## 10. Exactness and limits

- Exact for writes that read the record they replace (dynamo: `Modify`, `PutManyIfVersion`,
  `PutMany`). Two unconditional writes racing on one record compute their changelog from the same
  read: the loser's change is lost until a rebuild.
- A silent rewrite that changes a frame's lines without moving `Updated` (dynamo:
  `ReprocessRecords`) is invisible to runs: rebuild after it.
- A frame whose runs stop for more than ~35 hours is rebuilt on the next run (one table scan).
- A write stamped by a clock more than 2 minutes behind could land below a T a run already passed.
- A row whose sums net to 0 disappears from its file. Declare `Count` if a row must stay visible
  while its records exist.

## 11. Tests

- `dataframe_test.go` drives the real `Materialize` and `Read` with an in-memory `Backend` and file
  store over random histories, and checks them against brute force. The histories include inserts,
  updates of every column, floats, negative sums, soft deletes and restores, and runs that crash
  halfway, forced rebuilds, rebuilds of one partition and a tiny slot cycle (2 slots), over records
  spread on 3 partitions, every check per partition. Its frames cover columns, a nested masked
  frame, and a ToFrames frame giving several lines per record (repeated rows included). A simulated
  client per partition syncs through `ReadChanged` and the wire at random moments and must always
  match the brute force.
- `TestHistoryOfChangesMatchesTheHistory` builds Histories the scylla way (lines now plus logged
  changes after a window's start) and checks `linesAt` against the recorded history.
- `state_file_test.go` covers `FileState`: a held lock refuses a second taker, and a holder another
  run overtook fails its commit.
- `changed_test.go` covers the versions file (round-robin, raise-only, no-op writes) and the wire.
- It also covers the changelog's sizes, its shift/drop rules, and `linesAt` against the recorded
  history.
- `codec_test.go` covers the file format and its compression steps, 1 to 3 nested Rows columns,
  truncated files, a decode fuzzer, the zero sums mask and constant columns
  (`TestZeroSumsMaskAndConstantColumns`). `vectors_test.go` pins the bytes the browser decoder is
  tested on (`testdata/frame_wire_vectors.json`).

## 12. The scylla backend

genix-orm's `scylla` (`data_frame.go`, `data_frame_log.go`, `data_frame_backend.go`):

- **Declaration:** `TableSchema.DataFrames` (`db.DataFrame`). The table needs an int32 partition
  column and one integer key; frame columns are persisted integers or floats, the status column is
  the one named `status`. A column's id is its field position, so moving a field changes the shapes
  and rebuilds the frames. A filter on a ToFrames frame names the record column of the key's field
  name (`Between(table.Date, …)` selects the frame struct's `Date` key).
- **Change log**, one table per table with frames, created by `DeployScylla`:

  ```sql
  CREATE TABLE <table>__dtf_log (
    half_hour int, partition_id int, ts int, record_id int, changes blob,
    PRIMARY KEY ((half_hour), partition_id, ts, record_id)
  ) WITH default_time_to_live = 172800 AND compaction = {TimeWindowCompactionStrategy, 6 h}
  ```

  `half_hour` is unix seconds / 1800 and `ts` the microseconds since it started: a run reads every
  partition's writes of a half hour with one query, a fresh read slices its partition. `changes` is
  `u8 format (2), u8 flags (insert = 1), then the changelog body` (section 4: the lines the changed
  frames held before the write). A row of another format is skipped: a deploy that changes it
  changes every frame's shape, so they are rebuilt.
- **Write path:** one select per partition reads the **whole stored row** of every record written
  (shared with the delta log's read). The stored row is decoded into a record, the written columns
  are laid over a copy of it, and both give their lines (`Table.LinesOf`). The write logs every
  record it inserts or whose lines it changes, in an unlogged batch at LocalQuorum, **before** the
  record rows. A change the table holds always has its log row; a log row whose write then failed
  is harmless, because a History starts from what the table holds. A write that changes no frame's
  lines logs nothing.
- **Histories:** the log after w, then the stored rows of the records it names, then the log again: a
  write landing between the value read and the second log read is undone back to what the record
  held before it. A record only the second read names changed after the run's target and is left
  to the next run. A rebuild scans the table a page at a time and reads the log after each page.
- **State and lock:** `dataframe.FileState`, with `scylla.AcquireFrameLock` as the lock (genix
  installs a fareward try-lock). LWT is not used: it does not run on tablet keyspaces.
- **Concurrent writes:** two writers of one record that both read the stored row before either
  writes may log stale before lines for an intermediate moment. The frame still ends on what the
  table holds, because every History starts from the stored row.
