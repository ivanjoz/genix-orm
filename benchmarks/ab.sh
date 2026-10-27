#!/usr/bin/env bash
# ab.sh <old ref> [new checkout] [bench regexp] [rounds]: builds the benchmarks against a git worktree
# of <old ref> (A) and against the new checkout (B, default ./orm), then runs both binaries in
# alternating rounds at GOMAXPROCS=1 and compares them with benchstat. The laptop drifts ±5–9%
# between runs: never compare against a stale baseline.
#   ./ab.sh main .. 'Build|Rebuild' 6
set -euo pipefail
cd "$(dirname "$0")"
old="${1:?old ref}"; new="${2:-$(readlink -f orm 2>/dev/null || echo ..)}"; pattern="${3:-.}"; rounds="${4:-6}"
benchstat=$(command -v benchstat || echo "$HOME/go/bin/benchstat")
previous=$(readlink orm 2>/dev/null || echo ..)
work=$(mktemp -d); mkdir -p bin
trap 'git -C "$new" worktree remove --force "$work/a" >/dev/null 2>&1 || true; ln -sfn "$previous" orm; rm -rf "$work"' EXIT
git -C "$new" worktree add --detach "$work/a" "$old" >/dev/null
./link.sh "$work/a" >/dev/null && go test -c -o bin/a.test .
./link.sh "$new" >/dev/null && go test -c -o bin/b.test .
./link.sh "$work/a" >/dev/null && (cd . && ./bin/a.test -test.run Golden -test.count 1 >/dev/null) || echo "ab.sh: A fails the golden test" >&2
./link.sh "$new" >/dev/null && ./bin/b.test -test.run Golden -test.count 1 >/dev/null || { echo "ab.sh: B fails the golden test" >&2; exit 1; }
: > bin/a.txt; : > bin/b.txt
for round in $(seq "$rounds"); do
  echo "round $round/$rounds" >&2
  GOMAXPROCS=1 ./bin/a.test -test.run '^$' -test.bench "$pattern" -test.benchtime 3x -test.benchmem >> bin/a.txt
  GOMAXPROCS=1 ./bin/b.test -test.run '^$' -test.bench "$pattern" -test.benchtime 3x -test.benchmem >> bin/b.txt
done
"$benchstat" A=bin/a.txt B=bin/b.txt
