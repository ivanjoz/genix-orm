#!/usr/bin/env bash
# link.sh [genix-orm checkout]: points ./orm, which go.mod's replaces go through, at the checkout
# under test. Default: this branch's own code (..). While iterating, point it at the main checkout.
set -euo pipefail
cd "$(dirname "$0")"
target="${1:-..}"
[ -d "$target/dataframe" ] || { echo "link.sh: $target has no dataframe/" >&2; exit 1; }
ln -sfn "$(cd "$target" && pwd)" orm
echo "orm -> $(readlink orm) ($(git -C orm rev-parse --short HEAD 2>/dev/null || echo '?')$(git -C orm diff --quiet 2>/dev/null || echo ', dirty'))"
