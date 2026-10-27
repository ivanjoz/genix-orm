#!/usr/bin/env bash
# bench.sh [go test -bench regexp] [extra go test flags...]: checks the golden hashes, then runs the
# benchmarks against ./orm at GOMAXPROCS=1 (the Lambda has a fraction of one vCPU).
#   ./bench.sh                         every phase, every case, -count 6
#   ./bench.sh 'Build/sales' -count 10
set -euo pipefail
cd "$(dirname "$0")"
[ -e orm ] || ./link.sh
pattern="${1:-.}"; shift || true
count=(-count 6)
for arg in "$@"; do [[ "$arg" == -count* ]] && count=(); done
go test -run Golden -count 1 .
GOMAXPROCS=1 go test -run '^$' -bench "$pattern" -benchtime 3x "${count[@]}" "$@" .
