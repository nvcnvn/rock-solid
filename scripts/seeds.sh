#!/bin/bash
# Runs every seeded test over its per-commit seeds, each split across JOBS processes (default: the
# CPUs, at most 4) of GOMEMLIMIT (default 1GiB) each. SEEDS replaces every test's count and
# SEEDS_FIRST its first seed; arguments keep only the tests named. A failing seed prints its replay
# command and, for an ops test, its reduced case.
set -euo pipefail
cd "$(dirname "$0")/.."

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
GOMEMLIMIT=${GOMEMLIMIT:-1GiB}
export GOTOOLCHAIN GOMEMLIMIT

# package, test, seeds per commit
tests="
internal/sim/toy TestTheFixedProtocolKeepsEveryAcknowledgedWrite 10000
internal/disklog TestEveryAcceptedItemDecodes 2000
internal/disklog TestAnythingSubmitAcceptsDecodes 2000
internal/disklog TestRandomCrashesKeepAcknowledgedRecords 500
internal/disklog TestOracleCatchesReplayPastATornRound 1000
internal/disklog TestQuarantinedSegmentsAreNeverReused 500
internal/chunkstore TestReplayPastPendingWrites 2000
internal/lsm TestMetamorphic 1000
internal/chunkstore TestMetamorphic 1000
"

cpus=$(getconf _NPROCESSORS_ONLN)
jobs=${JOBS:-$((cpus < 4 ? cpus : 4))}
only=" ${*:-} "
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
total=0
start=$SECONDS

while read -r pkg test count; do
	[ -n "$pkg" ] || continue
	if [ "$only" != "  " ] && [[ "$only" != *" $test "* ]]; then
		continue
	fi
	bin="$tmp/${pkg//\//_}.test"
	[ -x "$bin" ] || go test -c -o "$bin" "./$pkg"
	n=${SEEDS:-$count}
	began=$SECONDS
	rm -f "$tmp"/out.*
	for ((i = 0; i < jobs; i++)); do
		(cd "$pkg" && "$bin" -test.run "^$test\$" -test.timeout 30m -seeds "$n" ${SEEDS_FIRST:+-seeds.first "$SEEDS_FIRST"} \
			-seeds.shard "$i/$jobs" >"$tmp/out.$i" 2>&1 || touch "$tmp/failed.$i") &
	done
	wait
	if compgen -G "$tmp/failed.*" >/dev/null; then
		for f in "$tmp"/failed.*; do
			cat "$tmp/out.${f##*.}"
		done
		echo "FAIL $pkg $test over $n seeds" >&2
		exit 1
	fi
	echo "ok $pkg $test: $n seeds in $((SECONDS - began)) s"
	total=$((total + n))
done <<<"$tests"
echo "$total seeds in $((SECONDS - start)) s"
