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

# expect WANT NAME: the gate's exit status is WANT, and the clone goes back to base.
expect() {
	local got=0
	MODEL_GATE_BASE=$base scripts/model-gate.sh >/dev/null 2>&1 || got=$?
	if [ "$got" != "$1" ]; then
		echo "model-gate-test: $2: exit $got, want $1" >&2
		exit 1
	fi
	git reset -q --hard "$base"
	git clean -q -fd
}

edit() {
	echo "// $RANDOM" >>"$1"
	git add -A
}

expect 0 "a clean tree"
edit internal/lease/lease.go && git commit -q -m "no model"
expect 1 "a protocol change with no model change and no reason"
edit internal/lease/lease.go && git commit -q -m "fix" -m "Model: none, a timer"
expect 0 "a protocol change with a reason"
edit internal/lease/lease.go && git commit -q -m "fix" -m "Model: none, "
expect 1 "an empty reason"
edit internal/lease/lease.go && edit models/epoch/epoch.tla && git commit -q -m "both"
expect 0 "a protocol change with its model"
edit internal/lease/lease_test.go && git commit -q -m "test"
expect 0 "a test change"
edit docs/design.md && git commit -q -m "doc"
expect 0 "a doc change"
cp models/epoch/neg-no-step-check.cfg models/epoch/neg-new.cfg
expect 1 "a negative with no row"
printf 'epoch/neg-gone\tlease.noStepCheck\tTestPlantedLeaseBugsBreakThePair\n' >>scripts/model-rules.tsv
expect 1 "a row with no configuration"
sed -i.bak 's/lease\.noStepCheck/lease.noSuchPlant/' scripts/model-rules.tsv
expect 1 "a plant the package does not hold"
sed -i.bak 's/lease\.noStepCheck\tTestPlantedLeaseBugsBreakThePair/lease.noStepCheck\tTestNoSuchTest/' scripts/model-rules.tsv
expect 1 "a test that does not exist"
sed -i.bak 's/lease\.noStepCheck\tTestPlantedLeaseBugsBreakThePair/lease.noStepCheck\tTestPlantedBugsBreakTheirTraces/' scripts/model-rules.tsv
expect 1 "a test whose file does not name the plant"
sed -i.bak 's/^- \[ \] \*\*MetaStore invariants\*\*/- [x] **MetaStore invariants**/' docs/build-checklist.md
expect 1 "an open row whose item is ticked"
sed -i.bak '/^epoch\/neg-no-step-check/d' scripts/model-rules.tsv
expect 1 "a removed row"
