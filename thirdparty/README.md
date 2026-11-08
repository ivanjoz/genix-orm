# thirdparty — forked dependencies

| Fork | Upstream | Change |
| --- | --- | --- |
| `gocql/` | `github.com/scylladb/gocql@v1.13.0` | `recreate.go` deleted |

`recreate.go` holds package-level `template.Must(...)` vars and provides only
`KeyspaceMetadata.ToCQL()`, a scylla-manager helper nothing in this repo or its consumers
references. Keeping it links `text/template` into every binary that uses the ORM for nothing:
**262,144 bytes**.

It is *not* a correctness fix and it is not what re-enables the linker's dead-method elimination —
`ToCQL` is unreachable, so `text/template`'s dynamic `MethodByName` never links either way. It is
purely dead weight. This is the weakest of the size changes in this project by a wide margin
(20,437 vendored lines for 0.8% of the consuming binary), and it is deliberately the easiest to
revert: restore the `replace` in `../go.mod` to `github.com/scylladb/gocql v1.13.0`, do the same in
the consuming app's `go.mod`, and `rm -rf gocql/`.

## Why the fork lives here and not in the app

gocql is this module's dependency — only `scylla/` imports it — so the fork belongs with the module
that owns it.

The catch is that **a `replace` directive in a non-main module is ignored by Go.** The one in
`../go.mod` applies when genix-orm is the main module (its own `go build` / `go test`), and does
nothing when the app builds. The consuming app must therefore declare the same replace against this
same directory:

```go
// in the app's go.mod
replace github.com/gocql/gocql v1.6.0 => ./genix-orm/thirdparty/gocql
```

Both point at one tree, so there is still only one copy and one place to patch. If the two ever
disagree, the app's wins and this module's tests would be exercising a different driver than
production — worth checking first if a size regression shows up with no source change.

## Upgrading

Bump the version in `regenerate.sh` **and** in both `go.mod` files, then:

```sh
./regenerate.sh
cd .. && go build ./... && go test ./...
```

The script fails if `text/template` is still reachable after the deletion, which is what would
happen if upstream moved those templates into another file.

## Licensing

The fork keeps its upstream `LICENSE` and `AUTHORS` and remains under those terms. Nothing is
modified, only `recreate.go` removed.
