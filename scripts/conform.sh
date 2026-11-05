#!/bin/bash
# Spec conformance: runs the simulator's conformance traces and has TLC 1.7.4 check each is a behavior
# of models/epoch/epoch.tla ("Spec conformance" in docs/test-scenarios.md). Not part of lint or CI.
#   scripts/conform.sh                  every trace of TestConform*
#   scripts/conform.sh file.trace ...   those traces alone
#   scripts/conform.sh plants           traces can fail: on each of COUNT seeds (default 10) from FIRST
#                                       (default 0) whose unplanted trace passes, each plant of PLANTS
#                                       (default all of conformPlants), one row each; a run stops at its
#                                       first broken invariant, or goes on with CONFORM_RUNON=1
# Each trace is one light TLC run, in turn: one worker, HEAP (default 2g), TLC_SECONDS (default 400).
# KEEP=dir keeps the traces and data, and skips a check whose spec and data have not changed.
set -euo pipefail
cd "$(dirname "$0")/.."

version=1.7.4
jar=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/tla2tools-$version.jar
[ -f "$jar" ] || scripts/models.sh ci epoch/ci-asks.cfg >/dev/null

tmp=${KEEP:-$(mktemp -d)}
mkdir -p "$tmp/traces"
[ -n "${KEEP:-}" ] || trap 'rm -rf "$tmp"' EXIT
tmp=$(cd "$tmp" && pwd)

# steps prints a data module's trace lines.
steps() { sed -n '/^Trace == <</,$p' "$1" | sed '1d;$d' | sed '$d'; }

# data writes trace $1's data module into its check directory.
data() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	if ! "$tmp/tracedata" "$1" >"$dir/data.new"; then
		rm -f "$dir/data.new"
		return 1
	fi
}

# check has TLC take the data module of trace $1, saving its verdict as $dir/verdict: "ok ...", or the line no state could take.
check() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	key=$(cat models/epoch/epoch.tla models/epoch/trace_epoch.tla models/epoch/trace_epoch.cfg "$dir/data.new" | cksum)
	if [ -f "$dir/verdict" ] && [ "$(cat "$dir/key" 2>/dev/null)" = "$key" ]; then
		grep -q '^ok' "$dir/verdict"
		return
	fi
	rm -f "$dir/verdict"
	mv "$dir/data.new" "$dir/trace_data.tla"
	cp models/epoch/epoch.tla models/epoch/trace_epoch.tla models/epoch/trace_epoch.cfg "$dir"
	total=$(steps "$dir/trace_data.tla" | wc -l | tr -d ' ')
	start=$SECONDS
	(cd "$dir" && perl -e 'alarm shift; exec @ARGV' "${TLC_SECONDS:-400}" java -Xmx"${HEAP:-2g}" -XX:+UseSerialGC -cp "$jar" tlc2.TLC -workers 1 -config trace_epoch.cfg trace_epoch.tla) >"$dir/out" 2>&1 || true
	secs=$((SECONDS - start))
	if grep -q 'Invariant TraceIncomplete is violated' "$dir/out"; then
		echo "ok $total lines, ${secs}s" >"$dir/verdict"
	else
		depth=$(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' "$dir/out")
		if [ -n "$depth" ]; then
			{
				echo "no step for line $depth of $total, ${secs}s"
				steps "$dir/trace_data.tla" | sed -n "${depth}p"
			} >"$dir/verdict"
		else
			{
				echo "no verdict after ${secs}s"
				tail -20 "$dir/out"
			} >"$dir/verdict"
		fi
	fi
	echo "$key" >"$dir/key"
	grep -q '^ok' "$dir/verdict"
}

if [ "${1:-}" = plants ]; then
	CONFORM_DIR=$tmp/traces CONFORM_PLANTS=${PLANTS:-} GOMEMLIMIT=4GiB go test -p 4 ./internal/chunkstore -run '^TestPlantsKeepTraces$' \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-10}"
	go build -o "$tmp/tracedata" ./tools/tracedata
	for ((seed = ${FIRST:-0}; seed < ${FIRST:-0} + ${COUNT:-10}; seed++)); do
		base=$tmp/traces/primaries-$seed.trace
		data "$base"
		if ! check "$base"; then
			echo "seed $seed: baseline fails, no evidence: $(head -1 "$tmp/check/primaries-$seed/verdict")"
			continue
		fi
		echo "seed $seed: baseline passes, $(cat "$tmp/check/primaries-$seed/verdict")"
		steps "$tmp/check/primaries-$seed/trace_data.tla" >"$tmp/base.lines"
		for f in "$tmp"/traces/*-"$seed".trace; do
			name=$(basename "$f" .trace)
			[ "$name" != "primaries-$seed" ] || continue
			stop=$(cut -c1-80 "${f%.trace}.stop" | head -1)
			if ! data "$f"; then
				echo "$name: NO DATA, stop: $stop"
				continue
			fi
			steps "$tmp/check/$name/data.new" >"$tmp/plant.lines"
			same=$(cmp "$tmp/base.lines" "$tmp/plant.lines" 2>&1 || true)
			case $same in
			*"EOF on "*base.lines*) first=$(($(wc -l <"$tmp/base.lines") + 1)) ;;
			*" line "*) first=$(echo "$same" | sed 's/.* line \([0-9]*\).*/\1/') ;;
			*)
				echo "$name: not fired, $(wc -l <"$tmp/plant.lines" | tr -d ' ') lines are a prefix of the baseline's, stop: $stop"
				continue
				;;
			esac
			if check "$f"; then
				echo "$name: PASSED, fired at line $first, $(cat "$tmp/check/$name/verdict"), stop: $stop"
			else
				echo "$name: rejected, fired at line $first, stop: $stop: $(cat "$tmp/check/$name/verdict" | cut -c1-200 | tr '\n' ' ')"
			fi
		done
	done
	exit 0
fi

if [ $# -eq 0 ]; then
	CONFORM_DIR=$tmp/traces GOMEMLIMIT=4GiB go test -p 4 -run "${RUN:-TestConform}" ./internal/chunkstore
	set -- "$tmp"/traces/*.trace
fi
go build -o "$tmp/tracedata" ./tools/tracedata

failed=0
for f in "$@"; do
	name=$(basename "$f" .trace)
	if ! data "$f"; then
		echo "FAIL $name: no data"
		failed=1
	elif check "$f"; then
		echo "ok   $name: $(sed 's/^ok //' "$tmp/check/$name/verdict")"
	else
		echo "FAIL $name: $(cat "$tmp/check/$name/verdict")"
		failed=1
	fi
done
exit $failed
