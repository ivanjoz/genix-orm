# RATIONALE — dynamo

## Omit-empty colbin encoding is set on import
**Context** — This driver puts the *whole* record in one colbin blob (the `"d"` attribute), not one
column per field, so every field a record leaves untouched is paid for inside that blob. colbin's
omit-empty flag is process-global and encoder-side, and has to be on before the first `Marshal`.
**Decision** — `func init() { colbin.SetOmitEmpty(true) }` in `client.go`, next to the marshaling
section it applies to. The scylla driver does the same in its own `init()`; both are needed because
this is a separate Go module with its own copy of the dependency, and either driver can be the only
one an entry point imports.
**Rationale** — Setting it from a caller means every entry point has to remember, and one that
forgets writes dense blobs with nothing to report it. The flag changes encoding only — both forms
are self-describing per column, so a reader never has to match its writer — and the one semantic
price, a `*T` pointing at `T`'s zero value decoding back as `nil`, is not a shape this project
stores. See `scylla/RATIONALE.md` for the version break the v0.1.0 upgrade carries.
