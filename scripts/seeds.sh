#!/bin/bash
# Runs every seeded test in its list over its seeds, each split across JOBS processes (default: the
# CPUs, at most 4) of GOMEMLIMIT (default 1GiB) and TIMEOUT (default 30m, 0 for none) each. SEEDS
# replaces every test's count and SEEDS_FIRST its first seed; arguments keep only the tests named.
# A failing seed prints its replay command and, for an ops test, its reduced case.
set -euo pipefail
cd "$(dirname "$0")/.."

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
GOMEMLIMIT=${GOMEMLIMIT:-1GiB}
export GOTOOLCHAIN GOMEMLIMIT

# package, test, seeds
tests="
internal/sim/toy TestTheFixedProtocolKeepsEveryAcknowledgedWrite 10000
internal/disklog TestEveryAcceptedItemDecodes 2000
internal/disklog TestAnythingSubmitAcceptsDecodes 2000
internal/disklog TestRandomCrashesKeepAcknowledgedRecords 500
internal/disklog TestOracleCatchesReplayPastATornRound 1000
internal/disklog TestQuarantinedSegmentsAreNeverReused 500
internal/disklog TestSlotRoundsKeepTheTraceRules 300
internal/chunkstore TestReplayPastPendingWrites 2000
internal/lsm TestMetamorphic 1000
internal/chunkstore TestMetamorphic 1000
"

cpus=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)
jobs=${JOBS:-$((cpus < 4 ? cpus : 4))}
only=" ${*:-} "
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
trap 'kill $(jobs -p) 2>/dev/null; exit 130' INT TERM
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
	pids=()
	for ((i = 0; i < jobs; i++)); do
		(cd "$pkg" && exec "$bin" -test.run "^$test\$" -test.timeout "${TIMEOUT:-30m}" -seeds "$n" \
			${SEEDS_FIRST:+-seeds.first "$SEEDS_FIRST"} -seeds.shard "$i/$jobs" >"$tmp/out.$i" 2>&1) &
		pids+=($!)
	done
	failed=0
	for ((i = 0; i < jobs; i++)); do
		if ! wait "${pids[$i]}"; then
			cat "$tmp/out.$i"
			failed=1
		elif grep -q "no tests to run" "$tmp/out.$i"; then
			failed=2
		fi
	done
	if [ $failed -eq 2 ]; then
		echo "$pkg has no test $test" >&2
	fi
	if [ $failed -ne 0 ]; then
		echo "FAIL $pkg $test over $n seeds" >&2
		exit 1
	fi
	echo "ok $pkg $test: $n seeds in $((SECONDS - began)) s"
	total=$((total + n))
done <<<"$tests"
if [ "$only" != "  " ] && [ $total -eq 0 ]; then
	echo "no seeded test named$only" >&2
	exit 1
fi
echo "$total seeds in $((SECONDS - start)) s"
