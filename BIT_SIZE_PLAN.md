# Plan — packed key slots in bits instead of decimal digits

Status: **implemented (2026-09-29), not committed.** Decisions taken:
- Q1: the setter is `Size(n)`.
- Q2: `Autoincrement(n)` counts random bits too; the old `Autoincrement(3)` sites are now `(8)`.
- Q3: a slot is a range cap. Any value that does not fit panics, on writes and on query bounds.
  W3's fixed-shift proposal was dropped because it would zero range-sized columns; no truncation
  remains anywhere.
- Q4: the widths in section C, as proposed.
- Q5: added as `db.DecodePackedKey[RecordT](id)`.
- Q6: `dynamo` was left out (comments only).
- Q7: 64/32 bits with the sign flip for virtual columns; 63 bits for `KeyIntPacking`.
- W6: 27-bit version slot for int32 Delta views.

Where this document and the code disagree, the code wins. Decisions made during implementation are
in `scylla/RATIONALE.md` and `db/RATIONALE.md`.

## Goal

Replace `Col.DecimalSize(digits)` with a bit-width setter (`BitSize(bits)`; the name is an open
question below). Slot capacity changes from `10^n` to `2^n`, and every packing step changes from
`* 10^n` to `<< n`. Example: `DecimalSize(3)` (0..999) becomes `BitSize(10)` (0..1023).

Data is wiped, so there is no migration and no compatibility layer.

## Does it make sense? Yes, with three costs

**What we gain**
- **Exact budgets.** A non-negative int64 holds 63 bits and an int32 holds 31. The decimal budgets
  of 19 and 9 are only approximate. `KeyIntPacking` and the int64 range views start at 19 digits,
  but int64 tops out at 9.22e18. That means a 19-digit layout overflows once the leading value
  passes ~922…: `PackProductStockID` wraps negative for `WarehouseID >= 92234`. With bits the
  budget is exact and the compile-time check can prove the layout fits.
- **Denser slots.** A slot is sized to the real range instead of the next power of ten. A value
  up to 5000 needs 4 digits (10 000 slots) but only 13 bits (8192). The Go type also caps the
  width: an int32 `Updated` fits in 31 bits, while `DecimalSize(10)` gives it ~33.2.
- **Cheaper, simpler arithmetic.** `Pow10Int64`, `countBase10DigitsNonNegative` and
  `trimRightToDigitsNonNegative` become `1<<n`, `bits.Len64` and `>>`. Truncating a slot buckets
  the value into power-of-two groups.

**What it costs**
1. Packed IDs stop being readable by eye (details in W1).
2. The frontend and app-level hand-written mirrors must switch to shifts (W2).
3. The DynamoDB/D1 mirror in `backend/cloud` is decimal-string based and needs a bits→digits
   bridge (W5).

## Scope — what changes

### A. ORM API and metadata (`genix-orm/db`)
- `db/column.go`: `DecimalSize(size int8)` → `BitSize(bits int8)`; `setDecimalSize` → `setBitSize`.
- `db/colinfo.go`: `ColumnInfo.DecimalDigits` → `SlotBits`; `SetDecimalSize` → `SetSlotBits`
  (interface and impl).
- `db/tablestruct.go:360`: `Autoincrement(randDecimalSize)` and `AutoincrementRandDigits`, but
  only if decision Q2 is "bits too".
- `db/tablestruct.go:495`, `db/schema.go:40`: rename field copies and comments.

### B. Scylla packing core (`genix-orm/scylla`)
| File | Change |
|---|---|
| `index_int_packing.go` | `computePacked…`/`decompose…` use shift/mask; `trimRightToDigits` → a bit trim (see W3); `countBase10Digits` → `bits.Len64`; `totalDigits` (9/19) → `totalBits` (31/63) |
| `packed_indexes.go` | budget checks `<= 8` / `<= 18` → bits; first slot = `totalBits - sum(trailing)`; int32 whole-value trim → 31 bits |
| `index_view_compile.go` | `totalDigitsForPackedView` 19/9 → 63/31; the `radixes[0] > 17` guard → bits; `packedValueCeiling` / `clampPackedUpperBound` → bit math (see W4); the unconstrained slot top `Pow10(slot)-1` → `(1<<slot)-1` |
| `index_delta_view.go` | `deltaVersionDigits{Int32,Int64,Elastic}` and `maxPackedInt64Digits` → bit constants (see W6); a FixedValues max sizes its slot with `bits.Len64(max)`; `maxDeltaVersionValue` → `(1<<bits)-1` |
| `insert-update.go:360-400` | the random suffix `seq*10^r + rand(10^r)` (Q2); `KeyIntPacking` `remainingDigits := 19` → `remainingBits := 63`, `shift := 1<<remaining` |
| `select_helpers.go:600-635` | the same `KeyIntPacking` range math, in bits |
| `select_compute.go:75-90, 214` | `sumSlotDigits` → `sumSlotBits`; GroupBy bounds use `1<<` |
| `helpers.go:290, 366` | `Pow10Int64` stays only if something decimal still uses it; `GetRandomInt64(digits)` → `rand.Int64N(1<<bits)` (Q2) |
| `reflect.go:330-405` | rename the field transfers |
| `deploy.go:474` | comment only |
| Panic messages | every `"DecimalSize()"` / `"digit budget"` string → bits wording |

### C. Schema call sites (backend) — proposed widths, to confirm
The proposal keeps at least the same capacity, and uses the Go type's width where that is tighter.

| Site | Now | Proposed | Capacity |
|---|---|---|---|
| `Updated` in views (`security/perfiles.go`, `business/generales.go`, `core/types/users.go`, `exec/demo2.go`) | `DecimalSize(10)` | `BitSize(31)` | full int32 |
| `Date` / `IssueDate` / `Day` UnixDay (`finance/types/cash_banks.go`, `logistics/types/product-stock-movement.go`, `invoicing/types/invoice_summary.go`, `core/types/credit_history.go`) | `(5)` | `BitSize(15)` | 32 767 days = int16 max |
| `WarehouseID`, `CashBankID` | `(5)` | `BitSize(17)` | 131 071 |
| `ProductID` in `product-stock.go` / `product-stock-movement.go` | `(9)` / `(8)` | `BitSize(30)` / `BitSize(27)` | 1.07e9 / 1.34e8 |
| `PresentationID`, `SubDivisor` | `(4)` | `BitSize(14)` | 16 383 |
| `Status`, movement `Type` | `(2)` | `BitSize(7)`, or smaller once the real ranges are known | 127 |
| `Autoincrement(3)` random suffix | 3 digits | `Autoincrement(10)` if Q2 = bits | 1024 |

`ProductStock` key after the change: 17 + 30 + 14 = 61 of 63 bits, leaving 2 spare bits at the
bottom. Today it leaves 1 spare digit.

### D. Hand-written mirrors of the packed layout (these break silently if they are missed)
- `backend/logistics/types/stock_movement_apply.go:42` `PackProductStockID`: `*1e14 + *1e5 + *10`
  → `<<46 | <<16 | <<2` (derived from the table above).
- `frontend/routes/logistics/products-stock/stock-movement.ts:6`: the same formula in JS (see W2).
- `backend/finance/cash_banks.go:91-92`: comment only.

Not affected: `imageID = autoincrement*10 + configDigit`, `accesoID*10 + nivel` and the
signup-request `seq*10^6` IDs. These are app-level decimal encodings that don't use the ORM.

### E. Cloud mirror (`backend/cloud/orm-meta.go`, `query_conditions.go`)
- `IndexKeyMeta.Digits` comes from `DecimalDigits` today. It becomes `digitsForBits(SlotBits)`,
  which is the decimal length of `(1<<bits)-1`. The mirror keeps its zero-padded decimal strings
  (W5). `inferKeyDigits` stays as it is, because it is type-based.
- Update the error strings that say `add .DecimalSize(n)`.

### F. Tests and docs
- Tests: `scylla/index_delta_view_test.go`, `group_by_test.go`, `shared_schema_test.go`, and any
  test asserting literal packed values or `slotDigits`. The expected numbers will all change.
- Living docs: `scylla/ORM_INTERNALS.md`, `backend/docs/ORM_DATABASE_QUERY.md`,
  `.agents/skills/create-database-tables/SKILL.md`, `.agents/skills/delta-cache-api/SKILL.md`,
  and the `DecimalSize` analogy in `dynamo/README.md`, `dynamo/schema.go:98` and
  `dynamo/encoding.go:26`.
- Historical plans and findings (`ACCOUNTING_MODEL_PLAN.md`, `invoicing/PLAN.md`,
  `invoicing/FINDINGS.md`, `EXPENSES.md`, the RATIONALE entries) stay as they are, because they
  record past decisions.

### G. Out of scope unless you say otherwise
- The `genix-orm/dynamo` backend. It already has its own `Base(width)` in base-64 characters and a
  decimal `AutoincrementRandomPadding`. Its only link to `DecimalSize` is the comments.

## Execution order
1. Core math in `index_int_packing.go` plus the `db` API rename. The build breaks everywhere,
   which is expected.
2. `packed_indexes.go`, `index_view_compile.go`, `index_delta_view.go`, `insert-update.go`,
   `select_helpers.go`, `select_compute.go`.
3. Run the ORM unit tests and rewrite the expected values.
4. Backend schema call sites (C), app/frontend mirrors (D), cloud mirror (E).
5. `go build ./...` and `go vet` in the backend. Run the `static-project-validation` skill.
6. Compare the `Packed index registered` / `Delta view registered` startup log lines before and
   after. Any view that flips from int to bigint is a regression to review (W6).
7. Update the docs and skills. Commit and push inside `genix-orm` first, then the root.

---

## Design warnings

**W1 — Packed IDs stop being human-readable.** Today `CashBankMovement.ID = 1_20725_000000042`
reads directly as cashbank 1, day 20725, seq 42 in a DB dump, a log or a URL. In bits it is an
opaque 19-digit number. Debugging needs a decoder: a `db.DecodePackedKey(table, value)` helper, or
the decomposed components printed next to the value in debug logs. Decide whether that helper is
part of this change.

**W2 — JavaScript precision is now a design constraint you can't ignore.** JS numbers are exact
only up to 2^53. `stock-movement.ts` already builds the ProductStock ID as a JS float. It is exact
today only because `WarehouseID * 1e14` stays under 9e15 while `WarehouseID < 90`. In bits the
warehouse shift is 46, so the ID is exact while `WarehouseID < 128`. That is slightly better, but
the limit still exists. Two more things to know:
- JS `<<` works on **32-bit** ints. The frontend mirror must use `* 2 ** n` (or `BigInt`), never `<<`.
- Any packed key wider than 53 bits that reaches the browser as a number is lossy. Keep such keys
  ≤ 53 bits, or send them as strings/BigInt.

**W3 — Truncation is value-dependent and becomes non-monotonic much sooner in bits. Must fix.**
`trimRightToDigitsNonNegative` drops the low digits by `len(value) - slot`, and that shift depends
on the value's own length. Crossing a magnitude boundary changes the shift, so the packed value
**goes down** and the view's sort order and range scans break.
- Decimal today: `Updated.DecimalSize(8)` (group_by tests; the same pattern as `.Int32()` views)
  shifts by 1 digit until SUnixTime reaches 1e9, around the year 2065.
- Ported naively to bits: `BitSize(24)` shifts by `bits.Len(Updated) - 24`. SUnixTime crosses 2^29
  around **2035**, and at that point the packed values drop.

The proposal is a **fixed shift** per slot, `typeBits(column) - slotBits`, for example
`31 - 24 = 7` for an int32. Every value then lands in the same 128-unit bucket forever, and it
stays monotonic. The same issue applies to the whole-value 31-bit trim on int32 packed
indexes. This is the one change where the port is not mechanical.

**W4 — Overflow in the 63-bit upper bound.** The range builders compute an exclusive bound
`prefix + (1 << remainingBits)`. With a full 63-bit layout that is `1<<63`, which overflows
int64 to a negative number. Today the decimal code avoids this by capping at 18 digits
(`maxPackedInt64Digits`, and `min(…, 18)` in `packedValueCeiling`). The bit version must either
cap the layouts at 62 bits or compute the bounds in `uint64` and clamp them to `math.MaxInt64`.
The recommendation is to clamp and keep the full 63 bits. `clampPackedUpperBound` already exists
for this.

**W5 — The cloud mirror stays decimal.** DynamoDB and D1 compare zero-padded decimal strings, so
bits have no meaning there. `digitsForBits` converts `BitSize(10)` to 4 padded digits (0..1023 →
`"0000"`–`"1023"`). The mirror then packs a little looser than Scylla, but it still orders
correctly. The alternative is to switch the mirror to fixed-width hex. That is more work and less
readable, and it isn't needed.

**W6 — The version-slot constants trade version headroom for leading-key room.** An int32 delta
key must satisfy `leadingMax × versionSlot + versionMax ≤ 2^31 - 1`. The leading key needs no
power-of-two width: `planDeltaSlots` checks the real packed maximum against `MaxInt32`, so the
leading key just gets whatever is left. That stays true in bits, so bits never lose total room.
The one real change is that the version slot can no longer be exactly 10^8. The nearest choices
are 2^26 (67M) and 2^27 (134M):

| Version slot | Versions | Max leading key in an int32 |
|---|---|---|
| 8 digits (today) | 100M | 20 |
| 26 bits | 67M | 31 |
| 27 bits | 134M | 15 |

Chosen: **27 bits**, which keeps at least today's version headroom. The key slots are not a
setting; each one is sized from its `FixedValues`. So the room the keys get in an int32 is what
bounds the declarations: all key ranges together must fit in 16 combinations. Every current
int32 Delta view already fits. The widest single key is `State` 0..6 in invoicing, and
`crm/types/client_provider.go` `Status`(0..1) + `Type`(1..2) packs to ≈ 9.4e8. The views
`product-stock.go` (`WarehouseID` 17 bits) and `generales.go` (elastic `ListID`) are bigint in
either system.
Constants:
- int32: 27 bits
- int64: 34 bits
- elastic: 30 bits

Step 6 of the execution order checks for any view that flips from int to bigint.

**W7 — The first-slot rule stays the same, but its capacity is now exact.** The first
`KeyIntPacking` / view column still takes whatever budget is left over. In decimal that slot could
silently overflow int64 (see the gain above). In bits, the compiler can panic at schema time
whenever the declared layout exceeds 63/31 bits.

**W8 — Grepability of the name.** `.Size(` is already used by gocql's `Batch.Size()` /
`connPicker.Size()`, and it doesn't say what unit it counts (bytes? chars? bits?).
`.BitSize(` is unique in the repo and says what it means.

**W9 — Gaining the sign bit (64/32 instead of 63/31) needs a sign-flip, because Scylla is
signed.** Packed values are never negative, so the math can run in `uint64`. But the physical
columns are CQL `bigint` / `int`, and Scylla has no unsigned integer type (`varint` is variable
length). They compare as **signed**, so writing a raw `uint64` above `MaxInt64` stores a negative
number, which sorts *before* 0 and breaks every range scan. The fix is to store
`int64(packed ^ (1<<63))` (`^ (1<<31)` for int32). That maps unsigned order onto signed order,
and every query bound goes through the same flip. The cost depends on where the packed value lives:
- **Virtual packed columns** (`zz_…` range and Delta view keys, `zz_ixp_` / `zz_gixp_` packed
  indexes) are never seen by the app. The flip stays inside `computePacked…`, `decompose…` and the
  bound builders. This is cheap, and it doubles the leading slot. For example, an int32 Delta key
  gets 5 bits (32 combinations) next to a 27-bit version slot.
- **`KeyIntPacking`** writes the record's real `ID` (Go `int64`), and that ID flows to handlers,
  `PackProductStockID`, JSON and the frontend. Using the 64th bit there means either negative IDs
  everywhere, or changing the ID fields to `uint64` and still flipping on write. Either way it
  collides with W2: JS is already lossy past 2^53, so a 64-bit ID gains nothing in the browser.
- The existing `NonNegative` guards (`panic` on packed < 0) change meaning: a stored negative
  becomes normal and gets decoded. W4 carries over as well, because the exclusive bound
  `1<<64` overflows `uint64` the same way.

---

## Open decisions (need your answer before starting)

- **Q1 — Name.** `BitSize(n)` (recommended, see W8), `Bits(n)`, or `Size(n)` as you wrote it?
- **Q2 — `Autoincrement(randDigits)`.** Should its random suffix switch to bits as well
  (`Autoincrement(10)` = 1024 random values)? That is recommended, so the column API has one unit.
  The alternative is to leave it decimal.
- **Q3 — Truncation (W3).** Adopt the fixed per-slot shift `typeBits - slotBits`? Or forbid
  truncation entirely and panic when a value exceeds its slot, the way Delta views already guard
  `maxDeltaVersionValue`?
- **Q4 — Widths in section C.** Accept the proposed widths, or give me the real ranges for
  `Status`, the movement `Type` and `SubDivisor` so they can be tighter?
- **Q5 — Decoder helper (W1).** Include `db.DecodePackedKey` in this change, or not?
- **Q6 — `dynamo` package (G).** Leave it out, as proposed?
- **Q7 — Sign bit (W9).** Recommended: the full 64/32 bits with the sign-flip for virtual packed
  columns only, and 63 bits for `KeyIntPacking` IDs so they stay positive `int64`. The
  alternatives are 64 bits everywhere (uint64 or negative IDs), or 63/31 everywhere (the simplest
  option).
