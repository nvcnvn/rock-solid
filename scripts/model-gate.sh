#!/bin/bash
# Ties the code to the TLA+ models with no one reading either.
#   scripts/model-gate.sh            checks the rule map and every commit since MODEL_GATE_BASE
#   scripts/model-gate.sh for FILE   lists the model rules whose plants FILE holds, what to read before changing it
# The rule map, scripts/model-rules.tsv, must name each negative model configuration, and each row a
# plant its package holds and a test whose file names that plant, or an open checklist item. That the
# test fails under its plant is the test's own assertion; this only keeps the link from going missing.
# A commit that changes a protocol file must also change models/ or say why not in a line
# "Model: none, <reason>". MODEL_GATE_BASE defaults to the merge base with origin/main.
set -euo pipefail
cd "$(dirname "$0")/.."

map=scripts/model-rules.tsv
protocol='^(internal/chunkstore/(keep|placement|replica|replica_lease|replica_peer|replica_recover|store|table|wire)\.go|internal/lease/lease\.go|internal/meta/(placement|shard|state)\.go|internal/raftshard/(host|round|shard|storage)\.go|third_party/raft/[^/]*\.go)$'
failed=0
fail() {
	echo "model gate: $*" >&2
	failed=1
}

rows() { grep -v '^#' "$map"; }

if [ "${1:-}" = for ]; then
	file=${2:?usage: scripts/model-gate.sh for FILE}
	rows | while IFS=$'\t' read -r rule plant _; do
		[ "$plant" = open ] && continue
		for p in $(echo "${plant#*.}" | tr , ' '); do
			if lines=$(grep -n -w "$p" "$file" | cut -d: -f1 | paste -s -d, -); then
				printf '%s\t%s\t%s:%s\n' "$rule" "$(sed -n '1s/^\\\* expect: //p' "models/${rule%%/*}/${rule#*/}.cfg")" "$p" "$lines"
			fi
		done
	done
	exit 0
fi

while IFS= read -r f; do
	rule=${f#models/}
	rule=${rule%.cfg}
	[ "$(rows | cut -f1 | grep -c -x "$rule")" = 1 ] || fail "$rule needs exactly one row in $map"
done < <(ls models/*/neg-*.cfg)

while IFS=$'\t' read -r rule plant test; do
	[ -f "models/$rule.cfg" ] || fail "$rule: no such configuration"
	if [ "$plant" = open ]; then
		case $test in
		"phase "*) ! grep -q "^## Phase ${test#phase }:" docs/build-checklist.md || fail "$rule: $test is in the checklist, name its item or its plant" ;;
		*) grep -q -F -- "- [ ] **$test**" docs/build-checklist.md || fail "$rule: \"$test\" is no unticked checklist item, name its plant" ;;
		esac
		continue
	fi
	pkg=${plant%%.*}
	file=$(grep -r -l -E "^func $test\(" internal --include='*_test.go' | head -1 || true)
	[ -n "$file" ] || fail "$rule: no test $test"
	for p in $(echo "${plant#*.}" | tr , ' '); do
		grep -r -q -w "$p" "internal/$pkg" --include='*.go' || fail "$rule: internal/$pkg holds no plant $p"
		[ -z "$file" ] || grep -q -w "$p" "$file" || fail "$rule: $file does not name $p"
	done
done < <(rows)

base=${MODEL_GATE_BASE:-}
if [ -z "$base" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
	base=$(git merge-base origin/main HEAD 2>/dev/null || true)
fi
if [ -n "$base" ]; then
	while IFS= read -r c; do
		files=$(git diff-tree --no-commit-id --name-only -r "$c")
		echo "$files" | grep -v '_test\.go$' | grep -q -E "$protocol" || continue
		echo "$files" | grep -q '^models/' && continue
		git log -1 --format=%B "$c" | grep -q -E '^Model: none, .+' ||
			fail "$(git log -1 --format='%h %s' "$c"): changes a protocol file, so change models/ or add a line \"Model: none, <reason>\""
	done < <(git rev-list --no-merges "$base..HEAD")
fi
exit $failed
