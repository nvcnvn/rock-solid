#!/bin/bash
# Ties the code to the TLA+ models with no one reading either.
#   scripts/model-gate.sh            checks the rule map and every commit since MODEL_GATE_BASE
#   scripts/model-gate.sh for FILE   lists the model rules whose plants FILE holds, what to read before changing it
# The rule map, scripts/model-rules.tsv, must name each negative model configuration, and each row a
# plant its package holds and a test whose file names that plant, or an open checklist item. That the
# test fails under its plant is the test's own assertion; this only keeps the link from going missing.
# A commit that changes a protocol file must also change a spec or configuration under models/, or
# say why not in a line "Model: none, <reason>" of its body. Every .go file of a protocol package is
# a protocol file unless the exclusions name it, so a new file starts covered. MODEL_GATE_BASE
# defaults to the merge base with origin/main; with no base the commits go unchecked, which fails in CI.
set -euo pipefail
cd "$(dirname "$0")/.."

map=scripts/model-rules.tsv
protocol='^(internal/(chunkstore|lease|meta|raftshard)|third_party/raft)/.*\.go$'
exclude='_test\.go$|^internal/chunkstore/(admission|capacity|cleaner|detector|disks|index|pool)\.go$|^internal/meta/txn\.go$'
tab=$'\t'
shape="^[^$tab]+$tab(open$tab.+|[a-z]+\.[A-Za-z]+(,[A-Za-z]+)*${tab}Test[A-Za-z0-9_]+)\$"
failed=0
fail() {
	echo "model gate: $*" >&2
	failed=1
}

rows() { grep -v '^#' "$map"; }

if [ "${1:-}" = for ]; then
	file=${2:?usage: scripts/model-gate.sh for FILE}
	[ -f "$file" ] || { echo "model gate: no file $file" >&2; exit 2; }
	found=0
	while IFS=$'\t' read -r rule plant _; do
		[ "$plant" = open ] && continue
		cfg=models/${rule%%/*}/${rule#*/}.cfg
		for p in $(echo "${plant#*.}" | tr , ' '); do
			if lines=$(grep -n -w "$p" "$file" | cut -d: -f1 | paste -s -d, -); then
				found=1
				printf '%s\t%s\t%s:%s\n\t%s\n' "$rule" "$(sed -n '1s/^\\\* expect: //p' "$cfg")" "$p" "$lines" "$(sed -n '2s/^\\\* //p' "$cfg")"
			fi
		done
	done < <(rows)
	if [ "$found" = 0 ]; then
		echo "No plant is in $file, which does not mean no rule applies: $(rows | grep -c "${tab}open$tab") rules have no plant yet."
		echo "Find the protocol in docs/test-scenarios.md, and its open rows in $map."
	fi
	exit 0
fi

while IFS= read -r f; do
	rule=${f#models/}
	rule=${rule%.cfg}
	[ "$(rows | cut -f1 | grep -c -x "$rule")" = 1 ] || fail "$rule needs exactly one row in $map"
done < <(ls models/*/neg-*.cfg)

while IFS= read -r row; do
	fail "malformed row: $row"
done < <(rows | grep -v -E "$shape" || true)

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
done < <(rows | grep -E "$shape" || true)

base=${MODEL_GATE_BASE:-}
if [ -z "$base" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
	base=$(git merge-base origin/main HEAD 2>/dev/null || true)
fi
if [ -z "$base" ]; then
	echo "model gate: no base commit, so commits were not checked" >&2
	[ -z "${CI:-}" ] || failed=1
else
	while IFS= read -r c; do
		# A merge's own changes are what no parent's merge would give: its conflict resolutions.
		if [ "$(git rev-list --parents -n 1 "$c" | wc -w)" -gt 2 ]; then
			files=$(git show --remerge-diff --format= --name-only "$c")
		else
			files=$(git diff-tree --no-commit-id --name-only -r "$c")
		fi
		touched=$(echo "$files" | grep -E "$protocol" | grep -v -E "$exclude" || true)
		[ -n "$touched" ] || continue
		modelled=$(echo "$files" | grep -E '^models/.*\.(tla|cfg)$' || true)
		[ -z "$modelled" ] || continue
		git log -1 --format=%b "$c" | grep -q -E '^Model: none, .+' ||
			fail "$(git log -1 --format='%h %s' "$c"): changes $(echo "$touched" | head -1), so change models/ or add a body line \"Model: none, <reason>\""
	done < <(git rev-list "$base..HEAD")
fi
exit $failed
