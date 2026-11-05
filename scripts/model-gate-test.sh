#!/bin/bash
# Plants each violation scripts/model-gate.sh must catch in a throwaway clone, since a gate that cannot fail passes too.
set -euo pipefail
repo=$(cd "$(dirname "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git clone -q --shared "$repo" "$tmp/r"
cp "$repo"/scripts/model-gate.sh "$repo"/scripts/model-rules.tsv "$tmp/r/scripts/"
cd "$tmp/r"
git config user.name test
git config user.email test@example.com
git add -A
git commit -q --allow-empty -m "the gate under test"
base=$(git rev-parse HEAD)

# expect WANT NAME [MESSAGE]: the gate's exit status is WANT and its output holds MESSAGE, and the clone goes back to base.
expect() {
	local got=0 out
	out=$(MODEL_GATE_BASE=$base scripts/model-gate.sh 2>&1) || got=$?
	if [ "$got" != "$1" ] || ! echo "$out" | grep -q -F -- "${3:-}"; then
		echo "model-gate-test: $2: exit $got, want $1 and \"${3:-}\": $out" >&2
		exit 1
	fi
	git checkout -q -f --detach "$base"
	git clean -q -fd
}

edit() {
	echo "// $RANDOM" >>"$1"
	git add -A
}

row() { printf '%s\n' "$1" >>scripts/model-rules.tsv; }
keep=$'lease.noStepCheck\tTestPlantedLeaseBugsBreakThePair'

expect 0 "a clean tree"
edit internal/lease/lease.go && git commit -q -m "no model"
expect 1 "a protocol change with no model change and no reason" "changes internal/lease/lease.go"
edit internal/lease/lease.go && git commit -q -m "fix" -m "Model: none, a timer"
expect 0 "a protocol change with a reason"
edit internal/lease/lease.go && git commit -q -m "fix" -m "Model: none, "
expect 1 "an empty reason" "changes internal/lease/lease.go"
edit internal/lease/lease.go && git commit -q -m "Model: none, in the subject"
expect 1 "a reason in the subject" "changes internal/lease/lease.go"
edit internal/lease/lease.go && edit models/epoch/epoch.tla && git commit -q -m "both"
expect 0 "a protocol change with its model"
edit internal/lease/lease.go && touch models/README && git add -A && git commit -q -m "stray file"
expect 1 "a protocol change with a stray file under models/" "changes internal/lease/lease.go"
edit internal/lease/lease_test.go && git commit -q -m "test"
expect 0 "a test change"
edit docs/design.md && git commit -q -m "doc"
expect 0 "a doc change"
edit internal/chunkstore/recover.go && git commit -q -m "no model"
expect 1 "a change to a file the list leaves in by default" "changes internal/chunkstore/recover.go"
edit third_party/raft/quorum/joint.go && git commit -q -m "no model"
expect 1 "a change below the fork's top directory" "changes third_party/raft/quorum/joint.go"
touch internal/chunkstore/replica_new.go && git add -A && git commit -q -m "new file"
expect 1 "a new file of a protocol package" "changes internal/chunkstore/replica_new.go"
edit internal/chunkstore/cleaner.go && git commit -q -m "excluded"
expect 0 "a change to an excluded file"

git checkout -q -b side "$base"
edit docs/glossary.md && git commit -q -m "side"
git checkout -q --detach "$base"
edit docs/design.md && git commit -q -m "ours"
git merge -q --no-edit --no-ff side
expect 0 "a clean merge"
git checkout -q -B side "$base"
edit docs/glossary.md && git commit -q -m "side"
git checkout -q --detach "$base"
git merge -q --no-commit --no-ff side >/dev/null 2>&1
edit internal/lease/lease.go && git commit -q -m "merge with a change of its own"
expect 1 "a merge carrying a protocol change" "changes internal/lease/lease.go"

cp models/epoch/neg-no-step-check.cfg models/epoch/neg-new.cfg
expect 1 "a negative with no row" "epoch/neg-new needs exactly one row"
row "epoch/neg-gone"$'\t'"$keep"
expect 1 "a row with no configuration" "epoch/neg-gone: no such configuration"
sed -i.bak 's/lease\.noStepCheck/lease.noSuchPlant/' scripts/model-rules.tsv
expect 1 "a plant the package does not hold" "holds no plant noSuchPlant"
sed -i.bak "s/$keep/lease.noStepCheck	TestNoSuchTest/" scripts/model-rules.tsv
expect 1 "a test that does not exist" "no test TestNoSuchTest"
sed -i.bak "s/$keep/lease.noStepCheck	TestPlantedBugsBreakTheirTraces/" scripts/model-rules.tsv
expect 1 "a test whose file does not name the plant" "does not name noStepCheck"
sed -i.bak 's/^- \[ \] \*\*MetaStore invariants\*\*/- [x] **MetaStore invariants**/' docs/build-checklist.md
expect 1 "an open row whose item is ticked" "is no unticked checklist item"
sed -i.bak '/^epoch\/neg-no-step-check/d' scripts/model-rules.tsv
expect 1 "a removed row" "epoch/neg-no-step-check needs exactly one row"
sed -i.bak 's/^\(epoch\/neg-no-step-check\).*/\1/' scripts/model-rules.tsv
expect 1 "a row of a rule alone" "malformed row"
sed -i.bak "s/$keep/lease.	TestPlantedLeaseBugsBreakThePair/" scripts/model-rules.tsv
expect 1 "a row with an empty plant" "malformed row"
sed -i.bak "s/$keep/lease	TestPlantedLeaseBugsBreakThePair/" scripts/model-rules.tsv
expect 1 "a row with a bare package" "malformed row"
sed -i.bak "s/$keep/lease.noStepCheck	/" scripts/model-rules.tsv
expect 1 "a row with no test" "malformed row"

git remote remove origin
out=0; CI=true scripts/model-gate.sh >/dev/null 2>&1 || out=$?
[ "$out" = 1 ] || { echo "model-gate-test: no base in CI: exit $out, want 1" >&2; exit 1; }
