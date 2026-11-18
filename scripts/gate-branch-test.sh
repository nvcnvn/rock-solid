#!/bin/bash
# Checks what scripts/gate-branch.sh --plan picks for known commits of main's history.
set -euo pipefail
cd "$(dirname "$0")/.."
XDG_CACHE_HOME=$(mktemp -d)
export XDG_CACHE_HOME
trap 'rm -rf "$XDG_CACHE_HOME"' EXIT

# expect COMMIT WHAT STEPS: the plan's run lines name exactly STEPS, and each grep -E pattern after them matches the plan.
expect() {
	local commit=$1 what=$2 steps=$3 got
	shift 3
	plan=$(scripts/gate-branch.sh --plan "$commit")
	got=$(sed -n 's/^run  \([^:]*\):.*/\1/p' <<<"$plan" | paste -s -d ' ' -)
	[ "$got" = "$steps" ] || { echo "gate-branch-test: $what: runs \"$got\", want \"$steps\"" >&2; exit 1; }
	for p in "$@"; do
		grep -qE -- "$p" <<<"$plan" || { echo "gate-branch-test: $what: no \"$p\" in: $plan" >&2; exit 1; }
	done
}

expect 60bb08f "a docs-only commit" "lint" "MODEL_GATE_BASE=$(git rev-parse 60bb08f^) "
expect 1c029ed "a voters-only model commit" "models-voters lint"
expect d5792d8 "a test of internal/meta" "go-test seeds lint" "go-test: .*/internal/meta( |$)" "seeds: .*TestTxnsMatchTheOracle"
expect 579776c "the voter model with internal/meta" "models-sessions models-voters go-test seeds lint" "go-test: .*/internal/meta( |$)"
echo "gate-branch-test: ok"
