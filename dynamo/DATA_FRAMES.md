# DataFrames — how they are built, kept up to date and read (`dynamo`)

This is the reference of the DataFrames implementation as it is: what a frame is, how the ORM feeds
it on every write, how the scheduled run and the reads turn that into files, and which invariants
hold it together. The decisions and the alternatives that led here are in `DATA_FRAMES_PLAN.md`.
Where this document and the code disagree, the code wins.

## 1. What a DataFrame is

A DataFrame is a group-by aggregate of one table, declared in its schema and kept as small columnar
files in an object store (S3). Each file holds the rows of one value of the frame's **Keys**; each
row is one value of its **Rows** column with the **Sums** of the records that fall in it, and with
`Count`, how many records those are.

```go
// backend/example/types/sales.go, SaleLine
DataFrames: []db.DataFrame{
	{Name: "day-product", Keys: db.Cols(table.Fecha), Rows: table.ProductID, Sums: db.Cols(table.Quantity, table.Amount)},
	{Name: "day-client-product", Keys: db.Cols(table.Fecha, table.ClientID), Rows: table.ProductID,
		Sums: db.Cols(table.Quantity, table.Amount)},
},
// Sale
DataFrames: []db.DataFrame{
	{Name: "day-client", Keys: db.Cols(table.Fecha), Rows: table.ClientID, Sums: db.Cols(table.Total, table.Quantity), Count: true},
	{Name: "day-payment", Keys: db.Cols(table.Fecha), Rows: table.PaymentMethod, Sums: db.Cols(table.Total, table.Quantity), Count: true},
},
```

- `day-product` has one file per day: every product sold that day with its quantity and amount.
- `day-client-product` has one folder per day and one file per client in it.
- `day-client` has one file per day: every client who bought that day with their total, units and
  number of sales.
- `Keys[0]` is the unit of reprocessing (a day): reads and rebuilds take a range of it.

Reading:

```go
lines := types.SaleLines.T
rows, err := types.SaleLines.QueryFrame(types.FrameDayClientProduct).
	Eq(lines.ClientID, clientID).Between(lines.Fecha, fromDay, toDay).Exec()
for _, row := range rows {
	_ = row.Key.Fecha + row.Key.ClientID + row.Key.ProductID // Key: a record with the Keys and Rows set
	_ = row.Sum(lines.Quantity) + row.Sum(lines.Amount)
}
```

The files trail the records: the scheduled run brings them up to date every 10 minutes, so a plain
read sees a write 10 to 20 minutes later. `.Fresh()` returns the rows as the records hold them now.

## 2. Map of the code

| Where | What |
|---|---|
| `dynamo/data_frame.go` | `SetDataFrames`, the compile rules (`compileDataFrames`, `frameFolder`), the write deadline (`frameWriteWindow`), and the write-path hooks: `checkFrameValues`, `stampCreatedVersions`, `frameWritesOf`, `appendFrameLogEntries`, `cancelFrameWrites`, `appendFrameDeleteEntries` |
| `dynamo/data_frame_run.go` | The state item and checkpoints, `MaterializeDataFrames` (the run), `expressCompactFrame`, `RebuildDataFrames` / `RebuildDataFramesAll`, the record readers of a compaction |
| `dynamo/data_frame_query.go` | `QueryFrame`, `FrameRow`, `.Fresh()` |
| `dynamo/repo.go`, `modify.go`, `controller.go` | The writes that call the hooks: `PutMany`, `InsertMany`, `PutIfAbsent`, `Delete`, `PutManyIfVersion`, `Modify`, `DecodeRecords`. `Controller` exposes the run and the rebuilds without the record type |
| `dynamo/dataframe/` | The storage side; it knows nothing of DynamoDB. `dataframe.go` (Frame, keys, shape), `codec.go` (formats), `run.go` (incarnations, compactions, rebuilds, the log), `index.go` (`_idx` / `_ixt`), `query.go` (selection, plain and fresh file reads, express compaction), `lock.go` (the frame lock), `store.go` (`Store`, `MemoryStore`) |
| `backend/core/frames/` (berryapps) | The S3 store, installed at boot (`installStore`), and the `frames-materialize` cron job |
| `backend/db/db.go` (berryapps) | Aliases: `db.DataFrame`, `db.FrameRow`, `db.FrameStore`, `db.SetDataFrames`, `db.ErrFrameNotBuilt` |

## 3. Declaring a frame

```go
type DataFrame struct {
	Name              string // the frame's name: ^[a-z0-9-]+$, unique in the entity
	Keys              []Coln // 1 to 3 integer Cols: Keys[0] a file (1 key) or a folder (2–3 keys)
	Rows              Coln   // the integer Col each file lists, ascending
	Sums              []Coln // the integer Cols summed per row
	AllowNegativeSums bool   // accepts negative Sums values (they are stored like any other)
	Count             bool   // one more summed column after the Sums: 1 per record
}
```

**`Count`** is no field of the record: `frameValuesOf` puts a 1 after the Sums values of every record
it counts (Status ≠ 0), so the file's last Sums column is the number of records per row and every
other part of the code (log, compactions, rebuilds, codec) sums it like any column. `SumsCount` is
`len(Sums) + 1`, the shape takes the Sums ids plus `+count`, and `FrameRow.Count()` reads it.

`compileDataFrames` (at `NewRepo`) panics on a violation:

- Keys, Rows and Sums are scalar integer `Col`s with a `cb` tag, none listed twice; 1–3 Keys, at
  least one Sums column or `Count`.
- `Keys[0]` leads the entity's `Keys`, a GSI or a local index: a rebuild queries a range of it.
  When it leads the base `Keys`, that read is consistent; through a GSI it can't be.
- The entity has a `TypeDelta` index with no pinned Keys (`{Type: TypeDelta, Keys: Cols(t.Status)}`):
  the run reads the records written since its snapshot through it.
- The entity has no `Partition`.
- The record has `UpdatedVersion` and `CreatedVersion`, both `int32`. A table with frames is
  versioned and reads its stored records before every write (`readsStoredVersion`).
- Two frames of the entity whose folders collide (section 5) fail the boot.

**Managed columns.** Handlers never set them:

- `UpdatedVersion`: the write sequence of the base pk, stamped on every write.
- `CreatedVersion`: the `UpdatedVersion` of the write that inserted the record, copied from the
  stored version on every later write (`stampCreatedVersions`). A record stored before the column
  existed reads 0: older than every snapshot.
- `Status`: a record with `Status == 0` counts in no frame (soft delete).

**Write-time check** (`checkFrameValues`): a negative Keys or Rows value always fails the write (they
name files and are delta-coded); a negative Sums value fails unless `AllowNegativeSums`.

## 4. Versions and snapshots

Everything hangs on one idea: **a file holds its frame at a snapshot**, the sum of what every
record held at one `UpdatedVersion` X.

- Inserts are found in DynamoDB: a record whose `CreatedVersion > X` did not exist at X.
- Updates and deletes destroy the old values, so **the write logs them**: before landing, it appends
  to the frame's log an entry "record `sk` (incarnation `CreatedVersion`) held these values right
  below version `newVersion`".
- **`valuesAt(X)`** of one incarnation (`dataframe/run.go`): nil when created after X; otherwise the
  old values of its first logged change above X; otherwise its current values. An incarnation is an
  sk plus its `CreatedVersion`: a record deleted and inserted again is two.
- A compaction from W to a target takes `valuesAt(W)` out of the files and adds `valuesAt(target)`.
  `incarnations()` groups the records and log entries above W; a **cancel marker** (an entry with
  `IsCancel`) voids the entries of the same sk and `newVersion`: their write never landed.

**Why the version is reserved after the stored read.** An entry claims "the record held these
values right below my version". That is only true if the write's version is above the version of
the record it replaced, so on a table with frames `putMany` assigns IDs, reads the stored records,
and only then stamps the version (`prepareWriteReadingStored`). `PutManyIfVersion` and `Modify` read
before they reserve by construction.

**Settling.** A version is reserved before its write lands, so a compaction may only target a
version once every write that took a version up to it has landed (or given up and logged its cancel
markers). The write deadline bounds that:

- **Write deadline** (`frameWriteDeadline`, 10 s, `SetDataFrames`): a write lands within 10 s of
  the stored read it diffs against (or of its version reservation when it reads nothing). Its log
  append, new hidden rows and base items carry a context deadline; past it the write lands nothing
  more and returns `ErrWriteDeadline`.
- **Cancel grace** (3 s): a write's cancel markers carry a deadline 3 s after its own.
- **Settle** = 2 × 10 s + 3 s + 2 s margin = **25 s** (`frameSettle`). A losing conditional write
  read the record before the winner landed, so it finishes one deadline plus the grace after that.

**Checkpoints** (state item `nx`, `nxt`, `px`). A checkpoint is a value of the upv sequence and the
time it was read, sequence first: every version up to it was reserved by then, so it is settled 25 s
later.

- `pushFrameCheckpoint`: when the newest checkpoint is more than 25 s old (or there is none), read
  the sequence, then the clock, and set `px = nx, nx = value, nxt = now`, conditioned on `nxt` being
  the one read. Of two pushers one wins; the other keeps its state, as valid.
- `settledTarget(now)`: `nx` once `now − nxt > 25 s`, else `px` (settled by construction: a push
  only happens once the newest has settled).
- Runs and fresh reads push. The target of a compaction is the newest settled checkpoint.

## 5. Storage

### Objects in the store

```text
<root>2JY-k/-Ba0pU/_log.<shape>       day-product: the pending changes (appended by the writes)
<root>2JY-k/-Ba0pU/_lock              the compaction holding the frame, and until when
<root>2JY-k/-Ba0pU/20730              Fecha 20730 (a 1-key frame: Keys[0] is the file)
<root>2JY-k/21TPlh/_log.<shape>       day-client-product
<root>2JY-k/21TPlh/20730/412          Fecha 20730, ClientID 412 (Keys[1..] name the file, joined by "_")
<root>2JY-k/21TPlh/20730/_idx         the day's files: their keys and hashes
<root>2JY-k/21TPlh/20730/_ixt         index entries appended since the last merge
```

- **The frame folder** (`frameFolder`) is `<table>/<frame>/` in the ORM's order-preserving base64
  (`-0-9A-Z_a-z`, all safe in S3 keys): the entity's TableID in 5 characters (`example_sale_line`
  = 55717936 = `2JY-k`), then the FNV-32a hash of the frame's name in 6. The files follow the
  TableID as the items do. Each frame's folder is listed by the schema introspection
  (`TableSchema.DataFrames`), to find its files by hand.
- Keys values are written in decimal.
- `<root>` is the store's own prefix (berryapps: `[frames].root`, default `frames/`).
- **`<shape>`** is 8 hex digits of `ShapeOf`: a CRC-32C of the format version, the folder and the
  cb ids of Keys, Rows and Sums (plus `+count` with `Count`). A new shape (a column changed, a new codec, a new folder scheme)
  resets the frame: the next runs rebuild every file (section 7). Lambdas still on the old code
  append to the old shape's log, which nobody reads.

### The state item (DynamoDB)

```text
pk = base pk ‖ 000      beside the GroupBy counters (sk g…) and the slot versions (sk v)
sk = "f" + frame name
w        the snapshot every file holds; absent: not built (a new frame, or a new shape)
nx, nxt  the newest checkpoint and when it was read (unix seconds)
px       the previous checkpoint, settled
ix       the days whose _ixt has entries to merge into their _idx (a number set)
sh       the shape the files were built with
```

Read consistently (`readFrameState`). Only the holder of the frame's lock moves `w`, `ix` and `sh`;
anyone may push a checkpoint.

### Formats (`dataframe/codec.go`)

- **Frame file**, columnar raw bytes: `u8` format version (1), `uvarint` snapshot, `uvarint` row
  count n, then the Rows column (ascending: first value, smallest step, residual steps) and each
  Sums column (minimum, residuals). Residuals are divided by the largest of 1000, 100, 10, 8, 5, 2
  that divides them all and bit-packed in blocks of 128 at the width with the smallest exact cost,
  the few longer residuals carried as patches. A row whose sums are all 0 is dropped; a file with
  0 rows is valid (a compaction keeps an emptied file, so its snapshot moves). Measured: a day of
  5,000 products with Quantity and a price is about 11.7 KB (2.4 B/row); a client's day file with
  5 rows about 25–30 B.
- **`FileHash`**: CRC-32C of the file after its snapshot. A file rewritten at a later snapshot with
  the same rows keeps its hash.
- **Log entry**, row-wise varints: length, `newVersion`, `createdVersion`, sk, then a kind byte
  (0: the old version counted nowhere; 1: it counted, followed by its Keys, Rows and Sums; 2: a
  cancel marker).
- **`_idx`**: file count, the second key as an ascending column, the third key (3-key frames) as a
  Sums-style column, then one `u32` hash per file.
- **`_ixt`**: blocks of `uvarint length` + an `_idx` body, one per append.

### The day index (`_idx` + `_ixt`, 2–3-key frames, `dataframe/index.go`)

- `_idx` lets a read list a day's files in one GET, and lets a rebuild skip the files whose hash
  already matches.
- An express compaction never rewrites `_idx`: it appends one block per touched day to `_ixt` with
  the keys and hashes of the files it wrote, and adds the day to `ix`.
- `readIndex` reads `_ixt`, then `_idx`, and applies `_idx` first and the `_ixt` blocks after it in
  order: **the last entry wins**. That order is the order the files were written in, because one
  compaction at a time holds the frame (section 8). Reading `_ixt` first never misses an entry: a
  merge rewrites `_idx` before it deletes `_ixt`.
- The run merges: for each day it touched or listed in `ix`, it rewrites `_idx` with the merged
  entries and deletes `_ixt`; the commit then removes those days from `ix`.
- The keys are exact. A hash can be stale only when a compaction crashed between a file and its
  entry; the next compaction of that file indexes it again.

## 6. The write path

Every write to a table with frames goes through the same steps (`repo.go`, `modify.go`):

1. **`checkFrameValues`** on the records to write.
2. **The window opens** (`frameWriteWindowFrom`): at the stored read the write diffs against, or at
   the version reservation when it reads nothing.
3. **Stored read, then version.** The stored records are read consistently, then the version is
   reserved and stamped. `CreatedVersion` is stamped from the stored record, or set to the new
   `UpdatedVersion` for an insert.
4. **Log append, before the base write** (`appendFrameLogEntries`). For every record with a stored
   version and, per frame, whose frame values change (`frameValuesOf` differs, Status 0 = nil), one
   entry with the stored values. One `Append` per frame, frames in parallel. An insert logs nothing:
   the run finds it in DynamoDB through the delta index.
5. **Land**: the new hidden rows and the base items, all with the window's deadline.
6. **Cancel markers** (`cancelFrameWrites`) for the records logged that did not land: never sent,
   or lost their condition. A record whose request failed without an answer may have landed: it
   keeps its entry. The markers have the 3 s grace.
7. Stale hidden-row deletes, GroupBy counters and slot versions come after, outside the window:
   frames don't read them.

Per write call:

| Call | Stored read | Log entries |
|---|---|---|
| `PutMany` | consistent BatchGetItem of the records whose ID it didn't just assign | one per changed frame value; base items go in batches of 25, in order, so on an error the records after the batch that failed get cancel markers (that batch may have landed) |
| `InsertMany` | none: the caller says they are new | none. A record that was in fact stored counts twice until rebuilt |
| `PutIfAbsent` | none: it only writes when absent | none (an insert); the hidden rows go after the base item, within the window |
| `PutManyIfVersion`, `Modify` | the caller's read, or the write cache of `GetManyForUpdate` (the window starts at the oldest cached read) | as `PutMany`; a record that loses its condition gets a cancel marker |
| `Delete` | consistent read of the record | when the record counts in a frame, it reserves a version for the delete and logs its values (`appendFrameDeleteEntries`); a delete not sent gets cancel markers |
| `DecodeRecords` (`Controller`, imports) | — | validates frame values only |

A table with frames needs the store set (`SetDataFrames`) before its first update or delete.

## 7. The scheduled run (`MaterializeDataFrames`)

berryapps' `frames-materialize` job (every 10-minute slot) calls it on every registered table. Per
frame (`materializeDataFrame` → `runDataFrame`):

1. **Take the lock** (`dataframe.TakeLock`). Held by another compaction: skip the frame, no error.
   Then read the state item.
2. **Reset** when there is no state or `sh` differs from the frame's shape: delete the old shape's
   log, then set `sh`, `nx` = the sequence now, `nxt` = now, and remove `w`, `px`, `ix`. Stop. The
   frame is not built: `QueryFrame` returns `ErrNotBuilt`.
3. **Push a checkpoint** when the newest has settled, and take the target T = the newest settled
   checkpoint.
4. **Build or compact:**
   - No `w` and no target: nothing to do (the reset's checkpoint hasn't settled).
   - No `w`: **build** (`RebuildAllFiles`) at T from every record (a consistent Query of the whole
     entity) and the log: write every file and `_idx`, delete every other object of the folder but
     the log and the lock. This happens on the second run after the reset.
   - Otherwise **compact** (`CompactFrame`) from W = `w` to max(W, T), merging the days in `ix`:
     1. *Records*: a consistent Query of the whole-entity delta index for `UpdatedVersion > W`
        (`frameRecordsWrittenAfter`, which keeps a record whose row moved during the read).
     2. *Log*: GET `_log.<shape>` after the records: an entry is appended before its write lands,
        so every change the record read saw is in the log.
     3. *Missing records*: the sks with entries above W that the read didn't return (deleted, or a
        write that failed after logging) are read by key (consistent BatchGetItem), and the log is
        read again.
     4. *Files*: every file that any change in (W, T] names, its values at W, at T or in any entry
        in between (`filesChangedBetween`). Each one (`compactFile`, 10 in parallel): GET it; at T
        or past it, leave it; otherwise take its rows, add the deltas from max(its snapshot, W) to
        T, and PUT it at snapshot T, even when its rows didn't change and when it is left empty.
     5. *Indexes* (2–3-key frames): `updateIndexes` rewrites the `_idx` of every touched day and
        every day in `ix`, merging and deleting their `_ixt`.
5. **Commit**: one `UpdateItem` sets `w = T` and removes the merged days from `ix`, with the lock's
   write deadline (section 8).
6. **Truncate the log** (`TruncateLog`): rewrite it keeping only the entries above the new `w`,
   conditioned on the ETag read (writers append meanwhile; re-read and retry, 8 attempts).
7. **Release the lock** (deferred, also after an error, so the next slot retries at once).

When an express compaction already moved `w` past T, the run only merges the days in `ix`.

## 8. The lock (`dataframe/lock.go`)

One compaction at a time writes a frame's files, indexes and `w`: the scheduled run, a rebuild, or
the express compaction of a fresh read. The lock is the object `_lock` of the frame folder: a varint
expiry (unix ms) followed by the holder's random id, taken and kept with conditional writes.

- **Take**: GET `_lock`. Live (expiry ahead): return nil, skip. Missing: PUT with
  `If-None-Match: *`. Expired or unreadable: PUT over it with `If-Match` on the ETag read. Of
  concurrent takers, S3 answers 412 (or 409) to all but one. It lasts `LockDuration` = 20 s.
- **Write through it**: every write of the holder goes through `lock.put`, `lock.append`,
  `lock.Delete` or, for DynamoDB, `lock.WriteContext()` (`commitFrameState`). Each gets a context
  that ends 2 s before the lock expires. With under 10 s left, `WriteContext` first renews the lock
  (`If-Match` on the holder's last ETag): if another compaction took it over, it fails with
  `ErrLockLost` and the holder writes nothing more. A lock that expired but nobody took over renews.
- **Why the deadline makes it safe**: the lock is taken over only once expired, and the holder
  sends nothing later than 2 s before expiry, so the old holder's last write lands before the new
  holder's first. The 2 s cover a request in flight and clock skew between Lambdas.
- **Release**: write the lock with expiry 0, `If-Match` on the holder's ETag (a 412 means it was
  taken over: ignored). A crashed holder never releases; its lock expires 20 s after its last
  renewal, and until then the runs skip the frame.

**What a crashed holder, or one that lost its lock, leaves**: files written at its target, above
`w`, maybe not indexed. Two rules handle them:

1. **Each file takes the changes after max(its own snapshot, W)**, and a file at the target or past
   it is left as it is. A file older than W was touched by no change since, so it holds the frame
   at W too; a missing file is empty at W. Readers, runs and compactions all compute the same rows.
2. **A compaction rewrites every file a change in its window names**, even unchanged, so the
   file's snapshot moves to the target. Example: a line moves into a client's file at 101 and out at
   103, and a compaction to 101 writes the file and crashes. The next compaction, to 103, sees no
   net change there; without this rule the file would keep the line at snapshot 101, and once `w`
   passed 103 every reader would take it as it is.

## 9. Reading (`QueryFrame`)

```go
rows, err := repo.QueryFrame(name).Eq(...).Between(...).Exec()      // the files as the runs left them
rows, err := repo.QueryFrame(name).Eq(...).Fresh().Exec()            // the records as they are now
```

**The selection** (`Exec`):

- `Keys[0]` takes an `Eq` or a `Between` over at most 400 values (`MaxFirstKeys`).
- Each later Key takes an `Eq`, on a leading run: pinning `Keys[2]` needs `Keys[1]` pinned. A Key
  left out reads every file of the day through its index. No other column is accepted.
- The state item is read first; a frame not built in its current shape returns `ErrNotBuilt`.

**A plain read**:

1. `SelectFileKeys`: with every later Key pinned, the files are named directly; otherwise each
   day's `_ixt` + `_idx` lists them (one GET each, days in parallel).
2. `ReadFiles`: GET and decode the files, 10 in parallel. A missing file has no rows.
3. Rows come back sorted by the Keys, then by Rows, as `FrameRow[E]` (`Key` holds the Keys and
   Rows values, `Sum(column)` a Sums value, `Count()` the record count of a `Count` frame).

**A fresh read** (`readFreshFiles` → `dataframe.ReadFreshFiles` at W = `w`):

1. Read the incarnations changed after W, as a compaction does (delta index, log, by-sk reads).
2. Select the files as a plain read, plus every file inside the selection that a change names (it
   may not be indexed yet), and read them.
3. Each file plus the deltas from max(its snapshot, W) to now: the rows as the records hold them now.
   A write counts once it has landed.
4. Read the state again. If `w` moved (a compaction committed and may have truncated entries the
   read needed), start over from the new `w`, up to 3 times.
5. **Express compaction** (`expressCompactFrame`), in the same request:
   - Push a checkpoint if the newest has settled.
   - M = the newest checkpoint settled when the read began. Go on only when M > W and more than 100
     incarnations changed in (W, M] (`frameExpressMinChanges`): every write up to M had landed
     before the read, so the read already holds the values at M, with no extra DynamoDB read.
   - Take the lock (held: skip). Re-read the state: go on only if `w` is still W.
   - `FreshRead.CompactTo`: the files changes in (W, M] name, brought to M as in the run (rules 1–2),
     then one `_ixt` block per touched day.
   - Commit `w = M` and `ADD ix` the touched days, in one `UpdateItem` under the lock's deadline.
     `ErrLockLost` is not an error: the files it wrote are ahead of `w`, as a crashed one's.
   - Release the lock, return the rows. The log is left to the scheduled run.
   - An error of the express compaction (other than a lost lock) fails the read.

**Cost of a fresh read**: two GetItems of the state, the delta Query plus a BatchGetItem of the
records changed since W (consistent), one log GET, the files, and at most every 25 s a checkpoint
push. The express compaction adds the lock (GET, PUT, release PUT), a GetItem, a GET and PUT per
touched file, one `_ixt` append per touched day and the commit.

## 10. Rebuilds

The fix for drift: a write from a Lambda still on code without the frame, an `InsertMany` of a
stored record, files edited by hand. Both take the frame's lock, waiting up to a minute (12 tries,
5 s apart), read the state (a frame not built fails with `ErrNotBuilt`), rebuild at `w` and release.
`w` does not move: the next compaction continues from it.

- **`RebuildDataFrames(frame, fromKey, toKey)`** (`""` = every frame; at most 400 values of
  `Keys[0]`): queries the records with `Keys[0]` in the range (consistent when `Keys[0]` leads the
  base Keys), adds the incarnations only the log knows (deleted, or moved out of the range since
  `w`), and computes the files at `w` (`RebuildFilesInRange`):
  - 1-key frame: PUT each day's file, or DELETE it when nothing produces it.
  - 2–3-key frame: compare with the day's index (`_idx` with `_ixt` over it; a corrupt index
    lists nothing). PUT the files whose hash differs, DELETE the listed files nothing produces,
    then rewrite `_idx` and delete `_ixt` when anything changed or `_ixt` existed. A file whose
    hash matches is not read: hand edits need the full rebuild.
- **`RebuildDataFramesAll(frame)`**: every record (a consistent Query of the entity), every file
  and `_idx` written without reading the stored ones, then every other object of the folder deleted
  but the log and the lock (files nothing produces, every `_ixt`, other shapes' files). `ix` is
  left: the next run merges a day without `_ixt` at no cost. The run's first build is this same
  function.

## 11. berryapps wiring

| Where | What |
|---|---|
| `config.toml` `[frames]` | `bucket` (required when any table declares frames: the boot fails without it) and `root` (default `frames/`) |
| `core/frames/frames.go` | `installStore` (a boot check): builds the S3 store and calls `db.SetDataFrames(store, 10 s)`. Local runs use the same bucket: they write the production table and must append to the same logs |
| `core/frames/store.go` | `s3FrameStore` on a general purpose bucket. `Append` is a GET plus a PUT conditioned on the ETag, retried up to 30 times with jitter within the write's context. `PutIfMatch` maps 412/409 to `ErrFramePreconditionFailed` and returns the new ETag. `Delete` batches `DeleteObjects` by 1,000. The S3 Express One Zone store (native append) waits for `s3express:CreateSession` on the Lambda role |
| `core/frames/cron.go` | `frames-materialize`, `Schedule: "*:*"` (every 10-minute slot): `MaterializeDataFrames()` on every registered table, errors joined |
| `example/cron.go` | `example-rebuild-sales-frames` at 09:10 UTC: `RebuildDataFrames("", yesterday, today)` on `Sale` and `SaleLine`, the safety net for drift |
| `core/admin/database_cli.go` | `fn-db` op `rebuild-frames` (`table`, `frame`, `from`, `to`; `from = to = 0` rebuilds everything; dry run unless `apply`) |
| `core/health/health.go` | Probes the store: PUT, GET, DELETE of `_health`, then a GET that must answer NotFound (needs `s3:ListBucket`) |

## 12. Tests

`go test ./...` in `genix-orm/dynamo`:

- **Codec** (`dataframe/codec_test.go`): round trips of files at every width and block boundary,
  the width choice against every alternative, the hash ignoring the snapshot, `_idx` / `_ixt` round
  trips, truncated and fuzzed input refused without a panic (`FuzzFrameDecode`), the size example.
- **`TestValuesAt`, `TestIncarnationsGroupAndCancel`** (`dataframe/run_test.go`): one case per
  snapshot rule, and the grouping into incarnations with cancel markers.
- **`TestFrameLock`**: taken, refused while live, renewed by a write, taken over once expired (the
  old holder's write fails, its release leaves the new lock), released and taken at once.
- **`TestDataFrameDeclarationRules`**, **`TestCheckFrameValues`**, **`TestFrameLogEntriesOfWrites`**
  (a write logs exactly the frames whose values change), **`TestFrameWriteWindow`** (the deadline
  and the grace), **`TestFrameCountColumn`** (the count's place, a soft delete counting nothing, a
  count-only frame, the shape).
- **`TestDataFrameRunsMatchBruteForce`, the spec** (250 seeds; its `day-client-product` counts): writers go through the real write
  path in steps (read, reserve, log, land / lose / crash), interleaved with runs, range and full
  rebuilds, fresh reads and express compactions (half of them slow, holding the lock across steps,
  some losing it), on a lock clock of 2 s per step. Compactions crash at random writes and keep
  their lock. After every commit and rebuild, every file must equal a brute-force aggregate of the
  history at its own snapshot (the frame's when older), and with no crash pending the indexes must
  list exactly the files and their hashes. Every fresh read must equal the records as they are. The
  test also fails when one of its events (a takeover, a lost lock, a skip, a crash...) never
  happens.

`ormcheck.Run` (the "ORM check" action of berryapps' `./app.sh deploy`) runs the frames live on
DynamoDB, on the check table `ormcheck_frame_line` with an in-memory store,
moving the ORM's clock (`dynamo.Now`) past each settle (its `day-product` counts): the first build, updates and deletes, a
fresh read before the run, a range rebuild that rewrites no file, `RebuildDataFramesAll`, and 101
inserts written into the files by an express compaction and merged by the next run.

## 13. Limits

- **The files trail the records** by 10 to 20 minutes for plain reads; `.Fresh()` is exact for
  every write that has landed.
- **`InsertMany` of a stored record** counts it twice until its day is rebuilt.
- **Writes from Lambdas on older code** after a deploy that adds a frame aren't logged; the nightly
  rebuild repairs them.
- **A write slower than 10 s fails** with `ErrWriteDeadline`; the client retries.
- **A crash between the log append and the base write** leaves a phantom entry, harmless on its
  own. A stale insert (a `PutManyIfVersion` insert that lands after another writer inserted and
  deleted the key) can count twice for one run.
- **A crashed compaction holds the frame 20 s**: runs skip it, express compactions skip their
  write, rebuilds wait.
- **The lock relies on the clocks**: a skew past 2 s, or a request landing more than 2 s after its
  deadline, could let two holders' writes overlap.
- **Hand-edited files** are only caught by `RebuildDataFramesAll`.
- **`DeleteRecordsAll` wipes the state item**: the frame restarts and its next build deletes the old
  files.
- **`RebuildDataFramesAll` reads the whole entity**, as the first build does.

## 14. Invariants to keep when changing this code

- A write appends its log entry **before** its base write lands, and reserves its version **after**
  the stored read.
- A compaction targets only a **settled** checkpoint, and reads the records **before** the log.
- Only the lock holder writes files, indexes, `w`, `ix` and `sh`, and **every** such write goes
  through the lock (`lock.put` / `append` / `Delete` / `WriteContext`).
- A compaction rewrites **every file a change in its window names**, and every reader takes each
  file's changes after max(its snapshot, `w`).
- `_ixt` is only appended, after its files are written; only runs and rebuilds rewrite `_idx` and
  delete `_ixt`.
- Anything that changes where or how files are stored must change the shape (`ShapeOf`), so
  existing frames reset and rebuild instead of being read in the wrong place or format.
- The randomized test is the spec: a change must keep it green, and a new failure mode deserves a
  new event in it.
