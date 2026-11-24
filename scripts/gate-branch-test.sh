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
expect d5792d8 "a test of internal/meta" "go-test seeds linux-test lint" "go-test: .*/internal/meta( |$)" "seeds: .*TestTxnsMatchTheOracle" \
	"linux-test: .* go test -p 1 -count=1 -timeout 30m [^ ]*/cmd/rock-solid [^ ]*/internal/driver/tcp$"
expect 579776c "the voter model with internal/meta" "models-sessions models-voters go-test seeds linux-test lint" "go-test: .*/internal/meta( |$)"
expect bc531d7 "a Linux-only test of internal/driver/uring" "go-test seeds linux-test lint" \
	"linux-test: .*--security-opt seccomp=unconfined .*golang:$(sed -n 's/^toolchain go//p' go.mod) go test -p 1 -count=1 -timeout 30m [^ ]*/internal/driver/uring$"
RS_GATE_NO_LINUX=1 expect bc531d7 "the same with RS_GATE_NO_LINUX=1" "go-test seeds lint" "^skip linux-test: by operator"
echo "gate-branch-test: ok"
