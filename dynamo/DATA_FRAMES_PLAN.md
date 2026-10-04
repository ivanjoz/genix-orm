# Plan — DataFrames: group-by files in S3, kept up to date by the cron (`dynamo`)

Status: **implemented in genix-orm (2026-10-04), not committed; the live check has not run yet.**
D1–D4 are decided. Where this document and the code disagree, the code wins; the choices made
during implementation are in `RATIONALE.md` ("DataFrames: implementation choices"), and the
reference is the README section "DataFrames".

## Goal

A table can declare DataFrames. A DataFrame is an aggregate of the table's records, stored as
compact files in S3 (an S3 Express One Zone directory bucket, or a general purpose bucket). A
reader gets it with one GET per file and never scans DynamoDB. The 10-minute cron adds the changes
since its last run to the files. For `SaleLine`:

```text
day-product          key(Fecha)           → [ProductID: Quantity: Amount, ...]   one file per day
day-client-product   key(Fecha, ClientID) → [ProductID: Quantity: Amount, ...]   one folder per day, one file per client
```

- **Inserts come from DynamoDB.** The cron reads the records written since its last run, using
  their `UpdatedVersion`.
- **Updates and deletes also need the record's previous values.** The write path already has them,
  because it reads the stored version for GroupBy. It appends the old values to the `.log` of each
  frame the write changes. If an update touches no column of a frame, that frame's log gets nothing.
- **Reprocessing works one day at a time, by the first key.** A day can be rebuilt from DynamoDB.
  The day's hash file lets the rebuild write only the files that changed.
- **A write shows up in the frames 10–20 minutes later.** See "Which version a run reaches".

## Decisions

**D1 — `CreatedVersion` instead of `UpdatedCount`. Decided:** a frame table carries the managed
`UpdatedVersion` and `CreatedVersion`, and no `UpdatedCount`.

The request first asked for an `updated_count` column so a run can tell inserts from updates.
What a run actually needs to know is whether the frames
**already hold** the record, meaning it was inserted at or before W (the version the last run
reached). A count can't tell that apart.

`CreatedVersion` is a field of each record: the `UpdatedVersion` of the write that inserted it.
`UpdatedVersion` is one number per write call, so every record inserted in the same call shares
the same `CreatedVersion`. Updates copy it unchanged from the stored version. Nothing indexes it:
the run reads it from the records the delta read already returned, and from the log entries.

**The delta read stays on `UpdatedVersion`.** The TypeDelta index sorts its rows by
`UpdatedVersion`, and `Query().Delta(W)` returns every record written after W. `UpdatedCount`
would index nothing and would decide nothing.

Example, with the last run at W = 100:

| Version | Write | Stored afterwards | Log entry |
|---|---|---|---|
| 101 | one call inserts lines A and B | A: upv 101, crv 101 · B: upv 101, crv 101 | — |
| 102 | A changes only `CategoryIDs` (no frame column) | A: upv 102, crv 101 | none |
| 103 | C (inserted long ago, at version 40) goes from Quantity 5 to 3 | C: upv 103, crv 40 | C, old Quantity 5 |

`Delta(100)` returns A, B and C:

| Line | `CreatedVersion` rule | `UpdatedCount` rule |
|---|---|---|
| A | crv 101 > 100: new, add it | count 1 and no log entry: "updated, nothing changed", skipped. **Wrong** |
| B | crv 101 > 100: new, add it | count 0: insert, add it |
| C | crv 40 ≤ 100: already held, subtract 5, add 3 | count 1 with a log entry: subtract 5, add 3 |

A deleted record isn't returned by the delta read at all, so only its log entry exists. That is
why the entry carries the record's `CreatedVersion` too.

A count could also work, with one extra rule: every record's first update is logged, even when no
frame column changed, so the entry tells when the record was inserted. That rule was rejected. It
costs the same storage (one int32 per record) plus an S3 append per first update, and the run rule
gets more complex.

**D2 — the bucket. Decided:** the recommended option below. The ORM only defines the
`dataframe.Store` interface; the S3 implementation lives in berryapps (`core/frames`).

- Every update of a frame table appends to S3 from the API Lambda.
- S3 Express needs `s3express:CreateSession` on the Lambda role. The role doesn't have it today, and
  only an account admin can grant it.
- On a general purpose bucket, an append is emulated: a GET of the small log, then a conditional PUT.
  That is the same mechanism genix-search uses today on `[search].bucket`
  (`berryapps-lambda-artifacts`).

**Recommended:**

- Add a new `[frames]` config section (`bucket`, `root = "frames/"`).
- Start on the same general purpose bucket.
- Switch to a directory bucket once the role is granted. That is a config change only.

**D3 — keys in the day hash file. Decided:** `_idx`, keys and hashes. As you asked, `_hashes` holds only the uint32 hashes, in the
same order as the files. Two problems follow:

- To pair each hash with its file, a run or a rebuild must LIST the day folder. A directory bucket
  lists keys unsorted, so the list must be sorted client-side.
- Suppose a run dies after creating a new client's file but before rewriting `_hashes`. The hashes
  no longer line up with the files, and nothing says which file is the extra one. Every file of
  that day must then be read again.

**Recommended:** an `_idx` file that stores, per file, its key followed by its uint32 hash. The
key is a uvarint delta of the ClientID.

- It costs 5–6 bytes per client.
- No LIST is needed.
- After a crash, the next run touches that file again, which repairs the index.
- A reader gets "which clients bought on day D" in one GET.

**D4 — negative sums. Decided:** Sums are unsigned by default.

- **Default.** On a frame without the flag, a write whose Sums value is below 0 fails, and the
  handler gets the error. Since every value is ≥ 0, a group's sum can never go negative, so the
  cron run never has to stop over one.
- **The flag.** `AllowNegativeSums: true` lets a frame accept negative values.
- **The flag is a validation rule only, not an encoding choice.** The columnar format (Formats)
  subtracts each column's minimum, so every packed residual is ≥ 0 even when values are negative.
  Only the column's base carries a sign: one bit on one number per column. That is the saving the
  flag was meant to buy, and it is now free on every frame. So the flag is not part of the shape.
- `SaleLine` doesn't set the flag: `sale_rules.go` already rejects `Quantity <= 0`.


## Declaration

```go
// DataFrame is an aggregate of the records kept as files in the frame store (data_frame.go): one
// file per distinct value of Keys, holding one row per distinct value of Rows, sorted, with the
// sum of each Sums column. Keys[0] is the reprocessing unit (a day): a rebuild recomputes a range
// of it from the records.
type DataFrame struct {
	// Name is the frame's folder: kebab-case, unique in the entity. Renaming it orphans the files.
	Name string
	// Keys name the file, 1 to 3 integer Cols. With 1, Keys[0] is the file; with 2–3, Keys[0] is a
	// folder and the rest name the file inside it.
	Keys []Coln
	// Rows is the integer Col each file lists, ascending.
	Rows Coln
	// Sums are the integer Cols summed per row, one column each in the file. A write with a
	// negative value fails, unless AllowNegativeSums.
	Sums []Coln
	// AllowNegativeSums accepts negative Sums values. It only changes that validation: the file
	// format stores negative and non-negative columns the same way.
	AllowNegativeSums bool
}

type Schema struct {
	// ...
	DataFrames []DataFrame
}
```

`SaleLine` (`backend/example/types/sales.go`):

```go
const (
	FrameDayProduct       = "day-product"
	FrameDayClientProduct = "day-client-product"
)

type SaleLine struct {
	// ... existing fields, then:
	UpdatedVersion int32 `json:"upv,omitempty" cb:"12"`
	CreatedVersion int32 `json:"crv,omitempty" cb:"13"`
}

Indexes: []db.Index{
	// ... existing, plus the delta index the frames read inserts through:
	{Type: db.TypeDelta, Keys: db.Cols(table.Status)},
},
DataFrames: []db.DataFrame{
	{Name: FrameDayProduct, Keys: db.Cols(table.Fecha), Rows: table.ProductID, Sums: db.Cols(table.Quantity, table.Amount)},
	{Name: FrameDayClientProduct, Keys: db.Cols(table.Fecha, table.ClientID), Rows: table.ProductID,
		Sums: db.Cols(table.Quantity, table.Amount)},
},
```

**Compile rules (`compile.go`, a violation panics at boot):**

- `Name` matches `^[a-z0-9-]+$` and is unique in the entity.
- Keys, Rows and Sums columns:
  - They are scalar integer `Col`s with a `cb` tag. The shape hash is built from the cb ids, so
    renaming a Go field does not reset the frame.
  - Keys has 1–3 columns, Sums has at least 1, and no column appears twice.
- `Keys[0]` leads the entity's `Keys`, a GSI or a local index. A rebuild must be able to query a
  range of it.
- The record has the managed `UpdatedVersion` and `CreatedVersion` (`int32`). Frames make the table
  versioned, as `GroupDelta` does.
- The entity declares a TypeDelta index with no pinned Keys. The run reads inserts through it.
- The entity has no `Partition` (v1).
- `readsStoredVersion()` is true when the table has frames.

**Write-time check:** a value below 0 fails the write in two cases:

- **A frame Key or Rows value**, always. These values become file names and deltas.
- **A Sums value**, unless the frame sets `AllowNegativeSums` (D4).

## Managed columns

- **`UpdatedVersion`**: as today, the write sequence of the base pk.
- **`CreatedVersion`**: set by the ORM, never by handlers.
  - A write with no stored version (InsertMany, PutIfAbsent, an expected version of 0, or PutMany
    finding nothing stored) sets it to the write's `UpdatedVersion`.
  - Any other write copies it from the stored version.
  - Records stored before the column existed read 0, which correctly means "older than every
    snapshot". No migration is needed.
- **`Status`**: a version with `Status == 0` counts in no frame, as with GroupBy.

**The version is reserved after the stored read.**

- **Why it matters.** The rule below reads "what the record held at version X" from the log entry
  with the lowest `newVersion` above X. That is only correct if every write's version is higher
  than the version of the record it replaced.
- **What goes wrong today.** `putMany` reserves the version (`prepareWrite`) before
  `storedVersions` reads the record. A slow writer can take version 11, then read the record
  another writer just stored at version 12, and replace it. Its entry, with `newVersion` 11, then
  claims the record held the version-12 values at version 10.
- **The change.** For tables that read their stored version, `putMany` will assign IDs, then read,
  then stamp the version. This adds one sequential round trip on those writes.
- **Already correct.** `PutManyIfVersion` and `Modify` read before they reserve, and their write
  condition proves the read.

## Storage

```text
<root>example_sale_line/day-product/_log.<shape>               pending changes (appends)
<root>example_sale_line/day-product/20730                      Fecha 20730
<root>example_sale_line/day-client-product/_log.<shape>
<root>example_sale_line/day-client-product/20730/412           Fecha 20730, ClientID 412
<root>example_sale_line/day-client-product/20730/_idx        the day's keys and hashes (D3)
```

- **Folders.** Each frame has one folder under its entity's folder.
  - In a 1-key frame, `Keys[0]` is the file name.
  - In a 2–3-key frame, `Keys[0]` is a folder, and the remaining Keys name the file, joined by `_`.
  - Every value is written in decimal.
- **`<shape>`** is 8 hex digits of the frame's shape hash: the format version and the cb ids of
  Keys, Rows and Sums. After a shape change, Lambdas still running the old code append to a log
  nobody reads. Frames are derived data, so a better codec later is just a format version bump:
  the shape changes and the next runs rebuild every file.
- **The state item lives in DynamoDB.** It can be written conditionally and read consistently,
  which S3 does not offer as simply:

  ```text
  pk  = base pk ‖ 000      the bookkeeping pk, beside the GroupBy counters (sk g…) and slot versions (sk v)
  sk  = "f" + frame name
  w        the snapshot (UpdatedVersion) the frame's files hold; absent = never built
  nx, nxt  the upv sequence value the last run read and when (unix seconds): the next run's target
  sh       the shape hash the files were built with
  lo, le   lease owner and expiry
  ```

## Formats

Frame files are raw bytes, written and read by hand in `dataframe/codec.go`: no colbin, no
general-purpose compressor. They are **columnar**: all the Rows values of a file, then all the
values of each Sums column.

Each column goes through the same four steps:

1. **A transform** turns the values into small non-negative residuals.
2. **A divisor.** The residuals are divided by the largest of 1000, 100, 10, 8, 5 and 2 that
   divides them all. It is stored once as a `uvarint` before the blocks (1 when none divides).
3. **The residuals are split into blocks of 128.**
4. **Each block is bit-packed at its own width.** The few values that don't fit that width are
   stored as patches.

colbin's `column` package is the reference for steps 3–4. The divisor and the patches are new
here.

### Transforms

The transform is fixed by the column's role, so the file stores no transform byte.

| Column | Base, stored once | Residuals |
|---|---|---|
| Rows (ascending) | `uvarint` first value, `uvarint` minStep | `(v[i] − v[i−1]) − minStep` for i ≥ 1: delta, then the smallest step subtracted |
| Each Sums column | `varint` (zigzag) min | `v[i] − min`: frame of reference |

- **Rows: delta, then the smallest step subtracted.** Product IDs are unique and ascending, so
  every step is ≥ 1. Subtracting the smallest step (usually 1) turns a run of consecutive IDs into
  zeros.
- **Sums: the column minimum subtracted from every value.** Every residual is then ≥ 0, even with
  negative values. Only the base can carry a sign, which is why it is a zigzag varint (D4).
- **The base is stored outside the blocks.** Folding the first value into the residuals would set
  the first block's width from one element. colbin measured +97% from that on timestamps.
- **The divisor applies to the residuals, after the minimum is subtracted.**
  - Quantities stored as multiples of 1000 (three implied decimals) lose 10 bits each. Prices in
    whole currency units (cents that are multiples of 100) lose 6.6 bits each.
  - In the size test this step alone halves the file.
  - **Only those six divisors are tried, for speed** (`commonDivisor`). It is not a GCD.
    - Every value drops the candidates it isn't a multiple of. An odd value drops 2, 8, 10, 100 and
      1000 at once.
    - The next value only tests the candidates still left, and the scan stops once none is left.
    - The divisors are constants, so each `%` compiles to a multiply and a shift.
    - Measured on 5,000 multiples of 1000: 9.5 µs, against 28.6 µs for the Euclid GCD. On a column
      with no divisor it stops at the first value that rules them all out.
  - **The largest surviving candidate wins.** On multiples of 40, both 8 and 10 divide; 10 saves
    more bits.
  - **What's lost.** Divisors outside the set (200, 25, 3, 12) are not found. A column of multiples
    of 200 is divided by 100 and keeps 1 more bit per value.
  - **The set lives only in the encoder.** The file stores the divisor as a `uvarint`, and the
    decoder multiplies by whatever it reads. Changing the set needs no format change.
  - It is per column, so a single value that isn't a multiple takes the divisor down to 1 for the
    whole column. A divisor per block would contain that, at a byte per block. It's left out until
    real data shows the case.

### Blocks

```text
block := u8 head   bits 0–6  width w (0..64): every residual of the block is packed at w bits
                   bit 7     patched: a u8 patch count follows the head
         packed    the block's residuals, low w bits each, least significant bit first, no gaps:
                   ceil(count × w / 8) bytes (a full block is exactly 16 × w bytes)
         patches   per patch: u8 position in the block, uvarint(residual >> w)
```

- **Choosing the width.** The encoder picks w for each block by exact cost. It counts the
  residuals' bit lengths once (65 buckets) and scores every w from 0 to the widest:

  ```text
  1 + ceil(count × w / 8) + [patched: 1 + Σ over residuals longer than w of (1 + uvarintLen(residual >> w))]
  ```

  The cost depends only on the bit lengths, so scoring needs no second pass over the values.
- **Mapping to your strategies:**
  - Your **max number length** is the unpatched width: the bit length of the block's largest
    residual.
  - Your **min bits** is the patched width: values longer than it become patches instead of
    widening the whole block.
  - The encoder computes both per block and keeps the cheaper one.
- **Why not continuation bits.** A varint with a 15+1 or 7+1 unit pays a continuation bit in every
  unit of every value, and rounds each value up to whole bytes. A packed width pays neither.
  - Example: 128 quantities where, after the minimum, 127 are ≤ 7 and one is 39.
  - Packed at 6 bits: 97 B. Packed at 3 bits with one patch: 52 B.
  - varint16: 256 B. A 7+1 varint: 128 B.
  - colbin replaced exactly this kind of varint with bit-packed blocks. It measured the blocks
    smaller on all five of its shapes (−59% to +1%) and 2–3× faster to decode. colbin's blocks have
    no patches. Patches are for heavy tails, which sales sums have: a few top sellers in every block
    of 128 products.
- **Width 0** is a block where every residual is 0. It costs 1 byte, so a constant column is
  nearly free.
- **Decoding** reads each value with one 64-bit load, one shift and one mask, with no dependency
  on the previous value. The 8 bytes after the value are read too, so the reader pads its GET
  buffer with 8 zero bytes and never needs a slow tail path. Patches are then ORed in, and the
  inverse transform (a prefix sum, or + min) runs last.

### Frame file

```text
u8        format version (1)
uvarint   snapshot: the UpdatedVersion whose state the file holds
uvarint   row count n (0: an emptied file, kept so its snapshot survives)
column    Rows: uvarint first, uvarint minStep, uvarint divisor, blocks of the n − 1 residuals
column    each Sums column in declaration order: varint min, uvarint divisor, blocks of the n residuals
          (no columns at all when n = 0)
```

- A row whose sums are all 0 is dropped before encoding.
- The file hash is a CRC-32C of everything after the snapshot. A file rewritten at a newer
  snapshot with the same rows keeps its hash.
- **Measured** (`TestFrameFileSizeExample`, implemented in `dataframe/codec.go`). The test file is
  one day of 5,000 products drawn from a 20,000-product catalog. Quantities are multiples of 1000
  (log-normal, median 5 units). Unit prices are cents, multiples of 100, log-uniform from 1.00 to
  500.00.

  | Column | Bytes | Bits/row | With no divisor or patches |
  |---|---:|---:|---:|
  | ProductID (5,000 of 1..20,000) | 2,657 | 4.25 | 3,054 |
  | ProductID (1..5,000, consecutive) | 43 | 0.07 | 43 |
  | Quantity | 3,674 | 5.88 | 10,874 |
  | UnitPrice | 5,667 | 9.07 | 10,043 |
  | Amount (Quantity / 1000 × UnitPrice) | 7,780 | 12.45 | 13,295 |

  | File: ProductID + Quantity + UnitPrice | Size |
  |---|---:|
  | this codec | **11.72 KB** (2.40 B/row) |
  | without the divisor | 22.01 KB |
  | without patches | 13.06 KB |
  | without either | 23.42 KB |
  | varint16 rows (the first draft) | 24.99 KB |

  UnitPrice is close to its entropy (log2 500 ≈ 9 bits): a price spread evenly over a range has
  nothing left to remove. If many products share the same prices, a dictionary would be the next
  step, but it's out of scope.
- **A client's day file with 5 rows** comes to about 25–30 B: the header and the bases dominate.

### Log entry (`_log.<shape>`, appended)

The log is row-wise and short-lived, since every run truncates it, so it uses Go's standard
varints (`binary.AppendUvarint` / `AppendVarint`). The column codec only pays off on whole files.

```text
uvarint   entry length (the bytes after this field)
uvarint   newVersion       UpdatedVersion of the write that replaced the record (a Delete reserves one)
uvarint   createdVersion   of the record (D1)
uvarint   sk length, then the record's sk
u8        kind: 1 the old version counted in the frame (Status ≠ 0), 0 it did not, 2 a cancel marker
when 1:   uvarint each Keys value, uvarint the Rows value, varint each Sums value — of the old version
```

An entry is about 25 B for `SaleLine`. A **cancel marker** voids the entries of the same sk and
`newVersion`: `PutManyIfVersion` appends one for each record that lost its condition (see "Write
path").

### Day index (`_idx`, 2–3-key frames, D3)

```text
uvarint   file count n
column    the first remaining key: the Rows transform (ascending; minStep may be 0 on a 3-key frame)
column    the next remaining key (3-key frames only): the Sums transform
u32 LE × n   CRC-32C of each file, in the same order: hashes don't compress, so they stay raw
```

If D3 is answered "keyless", the file is `_hashes` instead: only the u32s, in the order of the
folder's sorted LIST.

## Write path

| Writer | Stored version | Log entries |
|---|---|---|
| `InsertMany`, `PutIfAbsent` | none (new) | none; `CreatedVersion = UpdatedVersion` |
| `PutMany` / `Put` / `Controller.PutRecords` | read, as today for GroupBy | one per frame whose values changed, for records that existed |
| `PutManyIfVersion` / `Modify` | the caller's consistent read (or the write cache) | the same; a record that loses its condition then gets a cancel marker |
| `Delete` | read | one per frame the old version counted in; the Delete reserves a version for `newVersion` |

"Changed" compares the frame's values of the stored and the written version: Keys, Rows and Sums,
or nil when `Status == 0`.

**Order of a write:**

1. Assign IDs.
2. Read the stored versions.
3. Reserve the version.
4. Append the entries: one append per frame per call, frames in parallel.
5. Write the new hidden rows, then the base items, then delete the stale hidden rows.
6. ADD the GroupBy counters.
7. Bump the slot versions.

**The log goes before the base write**, because the two kinds of error are not equally bad:

- **A missing entry drifts the frame.** The run would believe the frame columns didn't change.
- **An extra entry is harmless when the record is unchanged.** That is what's left when the base
  write then failed: the run reads the record itself, and the entry's old values are what the
  record still holds.
- **A losing conditional write is not.** Its entry's old values are what it read, which another
  write has since replaced; at a snapshot between the two versions, `valuesAt` would return the
  stale values. So `PutManyIfVersion` appends a cancel marker for each record that lost, and the
  run drops the entries a marker voids. A run reads the log only once its target has settled, so
  the marker is there by then (see Limits for the window that remains).

**Appending** (berryapps' `core/frames` implements only the general purpose path today):

- **General purpose bucket (implemented):** a GET, then a PUT with `If-Match` (`If-None-Match` when
  the log is missing). A 412, or a 409 for a concurrent conditional write, retries the pair after a
  random pause of up to `attempt × 50 ms`, 30 attempts. Each round has one winner per log: measured
  on the real bucket, 20 concurrent appenders to one log all landed in about 8 s.
- **S3 Express (not built):** a `PutObject` with `WriteOffsetBytes` set to the object's size.
  - The size is cached per process. On a mismatch, the writer does a HEAD and retries, up to 16
    times.
  - Offset 0 creates the log.
  - S3 Express caps the appends per object (verify before relying on it). Because the run rewrites
    the log every 10 minutes, the count resets each time. Past the cap, an append falls back to the
    GET + PUT path.

## The snapshot rule

The core of the design is one pure function, unit-tested without S3 or DynamoDB:

```go
// frameValuesAt returns what one incarnation of a record contributed to a frame at snapshot
// version X: its Keys, Rows and Sums values, or nil when it counted nowhere. An incarnation is an
// sk plus its CreatedVersion: a record deleted and inserted again with the same Keys is two.
func frameValuesAt(incarnation recordIncarnation, X int64) *frameValues {
	if incarnation.createdVersion > X {
		return nil // created after X
	}
	if entry := incarnation.firstEntryAfter(X); entry != nil {
		return entry.oldValues // what it held right before its first frame change after X (nil: Status 0)
	}
	if incarnation.current != nil {
		return incarnation.current.values // no frame change since X (nil: Status 0)
	}
	return nil // deleted
}
```

A run from W to X' does two things per incarnation:

- It takes `valuesAt(W)` out of its file.
- It adds `valuesAt(X')` to its file. Usually that is the same file, so the result is one signed
  delta per row.

**Your example:** 200 records with `UpdatedVersion > W`.

- 195 are inserts. `CreatedVersion > W`, so before is nil and after is the record.
- 5 are updates, with `CreatedVersion ≤ W`:
  - 2 have entries: before is the entry's old values, after is the record.
  - 3 have no entry: before and after are both the record, so they are skipped without touching a
    file.

The table also covers the cases the count-based rule missed:

| Case (W = last run, X' = this run) | before = `valuesAt(W)` | after = `valuesAt(X')` |
|---|---|---|
| Inserted after W | nil | the record |
| Inserted after W, then updated (any column) | nil | the record |
| Updated after W, no frame column changed | the record | the record → skipped |
| Updated after W, frame columns changed | old values of the first entry above W | the record |
| `Status` 1 → 0 after W | the entry's old values | nil |
| Deleted after W (no record left) | the entry's old values | nil |
| Deleted and inserted again after W | old incarnation: the entry's old values; new one: nil | old: nil; new: the record |
| Changed again after X' (not settled yet) | as above | old values of the first entry above X' |
| Entry written, then its base write failed (phantom) | the entry's old values = the record | the record → skipped |
| Two plain `PutMany` racing on one record | both entries hold what both read | the record that landed last |

Unlike the GroupBy counters, racing writers don't drift a frame. Each entry holds exactly what its
writer read, and with the version reserved after the read, that is the record as of the version
just below the entry.

## Materialization run

**Which version a run reaches.**

- A version is reserved before its write lands. A run may only stop at version X' once every write
  that took a version ≤ X' has landed.
- Writes run inside the API Lambda, so they land within its timeout. So **settle = the Lambda
  timeout (480 s) + 60 s**, the same margin `core/cron` uses for stale dispatches.
- Each run reads the sequence value for the next run (`nx`). The next run, at least `settle` later,
  stops at that value.
- So a write reaches the frames 10–20 minutes later.

**Steps, per frame.** The frames of one entity share steps 4a–4c.

1. **Take the lease.** An `UpdateItem` on the state item, conditioned on there being no lease or an
   expired one. The lease expires after 10 minutes. If it's held, skip the frame; that's not an
   error.
2. **No state, or `sh` differs from the code's shape:** reset.
   - Set `w` absent, `nx` = the sequence value now, `sh` = the new shape.
   - Delete the old shape's log.
   - Release the lease. The next run builds the frame.
3. **`nxt` is newer than `now − settle`:** release the lease and stop. This happens when a retried
   run arrives early.
4. **If `w` is absent, run RebuildAll at X' = `nx`. Otherwise, run a compaction from W = `w` to
   X' = `nx`:**
   1. **Records.** A consistent Query of the whole-entity delta index from W, every Status. Unlike
      `Query().Delta()`, it keeps a record whose row moved while the read ran (a later write
      landed): it is still written after W.
   2. **Log.** GET `_log.<shape>`, **after** step 1. An entry is appended before its write lands, so
      every change step 1 saw already has its entry in the log step 2 reads.
   3. **Missing records.** For records that have entries but weren't returned by step 1 (deleted,
      or a phantom entry): a consistent BatchGetItem, then the log is read again, for the same
      reason as step 2.
   4. **Compare.** Per incarnation, before = `valuesAt(W)` and after = `valuesAt(X')`. Skip it when
      they're equal.
   5. **Update the files.** For each touched file:
      - GET it. A missing file counts as empty.
      - If the file's snapshot is already X', a crashed run with this same target wrote it: leave it.
      - Apply −before and +after.
      - If the rows changed, PUT the file with snapshot X'. A file left with no rows is kept, so its
        snapshot survives.
   6. **Update the day indexes.** Rewrite the `_idx` of every touched day folder.
5. **Commit.** Set `w = X'`, `nx` = the sequence value now, `nxt` = now. The write is conditioned on
   holding the lease.
6. **Truncate the log.** Rewrite it keeping only the entries with `newVersion > X'`, conditioned
   with `If-Match` on the ETag read. On a 412, re-read and retry. Nothing new can land at or below
   X': those writes have all settled.
7. **Release the lease.**

**If a run crashes:**

| Crash after | What the next run does |
|---|---|
| Part of step 4e | Targets the same X', because `nx` only moves at commit. Files already at X' are left as they are; the rest are applied. |
| Step 4f, before the commit | The same, and it rewrites the indexes of the folders it touches, which are the same folders. |
| The commit, before truncation | Ignores entries ≤ the new W (the rule reads only entries above W) and drops them when it truncates. |
| Anywhere, with the lease held | Waits: the lease expires after 10 minutes. |

## Rebuild

`RebuildDataFrames(frame, fromKey, toKey)` recomputes the days [from, to] of `Keys[0]` at the
committed snapshot W. It runs under the lease and writes only what differs:

1. Take the lease, and read W = `w`. A frame not built yet fails with `dataframe.ErrNotBuilt`.
2. Gather the incarnations to recompute:
   - The records with `Keys[0]` in the range, from one Query, every Status (consistent when
     `Keys[0]` leads the entity Keys; a GSI can't be).
   - Then the log. An incarnation found only there (deleted, or moved out of the range since W)
     has an entry above W, and `valuesAt(W)` is that entry's old values: no record read needed.
3. Compute the files: the sum of `valuesAt(W)` over those incarnations, at snapshot W.
4. Write:
   - For a 1-key frame, PUT each day file, or DELETE it when nothing produces it.
   - For a 2–3-key frame, compare against the day's `_idx`: PUT the files whose hash differs,
     DELETE the listed files no longer produced, and rewrite `_idx` if it changed.
5. Release the lease. `w` does not move, so the next compaction continues from W.

**`RebuildDataFramesAll`** does the same for every day: a Query of the entity's whole pk, a PUT of
every file it produces, and a DELETE of every other object under the frame folder but the log. It
runs on a frame's first build, after a shape change, and from `fn-db rebuild-frames`.

## Read API

```go
lines := types.SaleLines.T
rows, err := types.SaleLines.QueryFrame(types.FrameDayClientProduct).
	Eq(lines.ClientID, clientID).Between(lines.Fecha, fromDay, toDay).Exec()
for _, row := range rows {
	_ = row.Key.Fecha + row.Key.ClientID + row.Key.ProductID // Key: the frame Keys and Rows set
	_ = row.Sum(lines.Quantity) + row.Sum(lines.Amount)
}

// FrameRow is one row of a frame file, as Group is one GroupBy counter.
type FrameRow[E any] struct {
	Key E
	// unexported: the sums, the frame
}
func (row FrameRow[E]) Sum(column Coln) int64
```

- **`Keys[0]` takes `Eq` or `Between`.** A Between is an integer range of at most 400 values, read
  as one GET per day, in parallel (10 at a time).
- **Before the first build, or after a shape change until it is rebuilt**, `Exec` fails with
  `dataframe.ErrNotBuilt` (one GetItem of the state item per call).
- **Each later key takes `Eq`, or can be left out** together with every key after it. Leaving it
  out reads every file of the day, using the keys in `_idx`.
- **A missing file means no rows.** Rows come sorted by key, then by the Rows value. v1 has no
  filter on Rows; the caller filters.
- **Files are at the last run's snapshot.** While a run is writing, some files can be one run ahead
  of others.

## berryapps wiring

| File | Change |
|---|---|
| `backend/example/types/sales.go` | `SaleLine`: `UpdatedVersion` (cb 12), `CreatedVersion` (cb 13), the TypeDelta index, `DataFrames`, the frame name constants |
| `backend/db/db.go` | Aliases `DataFrame`, `FrameRow[E]`, plus `SetDataFrames(store, settle)` |
| `backend/core/config.go`, `config.example.toml` | `[frames]` with `bucket` and `root` (D2) |
| `backend/core/frames/` (new, like `core/textsearch`) | Builds the store from `[frames]` at boot. `cron.go` registers `frames-materialize` with `Schedule: "*:*"`; it calls `MaterializeDataFrames()` on every `db.RegisteredTables()` entry that has frames and joins the per-table errors |
| `backend/core/admin/database_cli.go` | Op `rebuild-frames <entity> [frame] [from to]`, next to `rebuild-groups` |
| `backend/core/health/` | Frames bucket reachability: a GET of a missing key that answers NotFound |
| `backend/example/cron.go` | `example-rebuild-sales-frames` at `09:10` UTC, rebuilding yesterday and today. It's a safety net for the drift sources under "Limits" |

The ORM side adds `Controller.MaterializeDataFrames() error`,
`Controller.RebuildDataFrames(frame string, fromKey, toKey int64) error` and
`Controller.RebuildDataFramesAll(frame string) error`, so berryapps can call them on any table
without its type.

## Cost (`SaleLine`)

- **Insert:** one extra hidden delta row (1 WCU). No S3 request.
- **Update:**
  - The version reservation moves after the read: one extra sequential round trip.
  - One append per frame whose columns changed (up to 2), in parallel. On S3 Express that's about
    1 PUT each; on a general purpose bucket, a GET plus a PUT of the log.
- **Delete:** one version reservation plus the appends.
- **Run, per frame:**
  - About 4 state-item requests.
  - One delta Query plus a BatchGetItem of the records changed in the window.
  - One log GET and one log PUT.
  - One GET plus at most one PUT per touched file (usually today's file and the clients who bought).
  - One GET plus one PUT per touched day `_idx`.
- **S3 request prices** are per 1,000 requests, so all of this is negligible at this volume.

## Limits

- **Negative values on a frame without `AllowNegativeSums` (D4).** New writes with one are
  rejected. A record that already held one before the frame was declared is aggregated as it is:
  the format stores it like any other value.
- **InsertMany of a record that was already stored** counts it twice until its day is rebuilt.
  GroupBy has the same limit.
- **Writes from a Lambda still on the previous code** right after a deploy that adds a frame aren't
  logged. The nightly rebuild repairs them.
- **`DeleteRecordsAll` wipes the state item.** The frame then restarts: the next two runs rebuild
  it and delete the old files.
- **Files edited or deleted by hand.** A range rebuild of a 2–3-key frame trusts the `_idx`
  hashes: a file whose hash matches is not read. Repair hand edits with `RebuildDataFramesAll`
  (`fn-db rebuild-frames` without a range).
- **A losing conditional write still in flight long after the winner.** The cancel marker is
  appended when the loser learns it lost. A run reads the log at least `settle` (Lambda timeout +
  60 s) after its target was read, so it can miss a marker only when the winning write took more
  than about 60 s between reserving its version and landing, while the loser read the record in
  that window. The run then sees no change for the winner's write, and the frame keeps the record's
  old values until its day is rebuilt.
- **A stale insert.** A `PutManyIfVersion` insert (expected version 0) that read "absent", then
  lands after another writer inserted and deleted the same key, carries a version older than that
  delete. At a snapshot between the other insert and its delete, both incarnations count. It lasts
  one run: the next one ends on the right sums. The insert condition (`attribute_not_exists`) can't
  see a delete that already happened.
- **A crash between the log append and the base write** leaves a phantom entry, harmless on its
  own. If another write to the same record lands later with a version below the phantom's (a slow
  writer), the phantom's old values are stale for the snapshots between the two. The run after the
  phantom's version is exact again.
- **Large first rebuild.** `RebuildDataFramesAll` reads the whole entity. That's fine for
  `SaleLine` today; a large entity needs a paginated rebuild per day range first.

## Tests and verification

**genix-orm, offline (`go test ./...`):**

- **Codecs:**
  - **Packing.** Every width 0..64 round-trips at every partial-block length, with and without the
    8-byte padding.
  - **Width choice.** No other width (patched or not) gives a smaller block than the one the
    encoder chose. This is checked exhaustively on random blocks, as colbin's
    `TestArrayTransformChoiceIsOptimal` does.
  - **Patches.** 0, 1 and 128 patches per block.
  - **Columns.** Rows with minStep 0 (3-key indexes), 1 and larger; Sums with a negative min, the
    int64 extremes and a constant column.
  - **Round trips** of whole frame files, log entries and `_idx`.
  - **The file hash** ignores the snapshot.
  - **Bad input.** `FuzzFrameDecode`: arbitrary and truncated input must never panic.
- **Size.** `TestFrameFileSizeExample` prints the measured table under "Frame file" (synthetic
  data: one day of 5,000 products). A report on the real `SaleLine` corpus is left for after go-live.
- **Validation** (`TestCheckFrameValues`): a negative Keys or Rows value always fails, a negative
  Sums value fails unless the frame allows it.
- **`valuesAt`:** one case per row of the snapshot table (`TestFrameValuesAt`), and the grouping
  of records and entries into incarnations, cancel markers included.
- **Write path** (`TestFrameLogEntriesOfWrites`): a write logs exactly the frames whose values
  changed, with the stored version's values.
- **The randomized run test, which is the spec** (`TestDataFrameRunsMatchBruteForce`, 250 seeds):
  - Writers go through the real write-path code in steps (read, reserve, log, land), as
    `PutManyIfVersion` does: a writer whose read was replaced loses and appends its cancel
    markers; some crash after logging.
  - Runs, range rebuilds and full rebuilds are interleaved with them. Runs crash at random file
    writes, and writers move on between a run's record read and its log reads.
  - After each commit and rebuild, every file must equal a brute-force aggregate of the history at
    the frame's snapshot (or, for a file a crashed run left ahead, at its target), and every
    `_idx` must list exactly its folder's hashes. A range rebuild of a 2–3-key frame must not
    rewrite a file.
  - Writer lifetimes are bounded and the settle is twice that, so the run is exact; the windows
    under "Limits" are outside it.
- **Rules:** the compile panics (`TestDataFrameDeclarationRules`).

**Live check (`ormcheck`):** a frame on `ormcheck_frame_line`, on the in-memory store: write,
materialize twice (the first run only records the target), read it back with `QueryFrame`, update
and delete, materialize again, and compare with the records. Settle is 0 there.

**berryapps:**

- `./app.sh test` and `./app.sh fn-check`.
- Two `fn-cron-tick` runs at least 9 minutes apart. The first records `nx`; the second builds the
  frames.
- Cross-check: the `day-product` frame must match `QueryGroups(Fecha, ProductID)` Quantity sums
  over the last 30 days, exactly. Any difference means drift in one of them.
- Cancel a sale. Two runs later, its lines must be gone from both frames.

## Out of scope (v1)

- Fan-out frames: a ColSlice in Keys or Rows, such as per category.
- Float Sums, and a record-count column.
- Entities with a `Partition`.
- Filters on Rows.
- Consumers: endpoints, agent tools, the frontend.
- Replacing the `SaleLine` GroupBy `(Fecha, ProductID)` with the frame. That comes later, once the
  cross-check has held for a while.

## Implementation order

1. **genix-orm:** `Schema.DataFrames`, the compile rules, `CreatedVersion`, and the reservation
   moved after the stored read, with tests.
2. **Codecs** (`dataframe/codec.go`): the column codec, the frame file, the log entry and
   `_idx`, with tests and the size report on the `SaleLine` corpus.
3. **`dataframe.Store`** (`dataframe/store.go`):
   - The interface: Get, Put (If-Match / If-None-Match), Append, List, Delete.
   - An S3 implementation: native append on Express, emulated on general purpose buckets.
   - An in-memory implementation for tests.
4. **The write path:** log entries and the Delete version.
5. **The run:** `frameValuesAt`, the run itself, the state, the lease and truncation, plus the
   randomized crash test.
6. **Rebuild** for a range and for everything, plus the shape reset.
7. **`QueryFrame`.**
8. **Docs and release of the submodule:**
   - The README section "DataFrames", the skill's `SKILL.md`, the `RATIONALE.md` entries, and
     `ormcheck`.
   - Commit and push inside the submodule.
9. **berryapps wiring** (the table above).
10. **Go live:**
    - Declare the frames on `SaleLine`.
    - Deploy the backend (`deploy-to-aws`).
    - Watch the first two runs, then run the cross-check.
11. **The nightly `example-rebuild-sales-frames`.**

## Alternatives considered

- **Recompute every touched file from DynamoDB, with no log.** It's simpler, but every run would
  re-read whole days (every line of today, every 10 minutes) instead of only the changes.
- **`UpdatedCount`:** see D1.
- **Settle on each record's `Updated` time instead of the sequence value.** The latency is the same
  here, because settle (540 s) is close to the cron period. It would also need a timestamp in every
  delete entry, and it can't promise that each run's target version only moves forward.
- **Rows of varint16 values** (the first draft of this plan), or **a per-column first-unit varint**
  (k bytes, then 7+1 units). Both pay a continuation bit per unit of every value and round every
  value up to whole bytes. They are kept only as baselines in the size report.
- **Importing `colbin/column`.** Ruled out by request: the frame codec is written by hand. colbin
  stays the reference for the bit-packed blocks, and its measurements are cited above.
- **A transform scored per column** (raw, delta, frame of reference, constant), as colbin does.
  Here each column's role already fixes the best one: Rows are ascending, so delta; Sums are
  unordered, so frame of reference, which is never larger than raw. A constant column is width 0.
  The transform byte and the scoring pass would buy nothing.
