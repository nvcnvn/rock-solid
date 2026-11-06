#!/bin/bash
# Spec conformance: runs the simulator's conformance traces and has TLC 1.7.4 check each is a behavior
# of models/epoch/epoch.tla ("Spec conformance" in docs/test-scenarios.md). Not part of lint or CI.
#   scripts/conform.sh                  every trace of TestConform*, or of the test RUN names, seeded
#                                       ones over COUNT seeds (default 10) from FIRST (default 0)
#   scripts/conform.sh file.trace ...   those traces alone
#   scripts/conform.sh plants           traces can fail: on each of COUNT seeds (default 10) from FIRST
#                                       (default 0) whose unplanted trace passes, each plant of PLANTS
#                                       (default all of conformPlants), one row each; a run stops at its
#                                       first broken invariant, or goes on with CONFORM_RUNON=1
# Each trace is one light TLC process with its own directory: one worker, HEAP (default 2g), TLC_SECONDS
# (default 400). JOBS (default 1) of them run at once, and the lines print in order when all are done.
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
		rm -f "$dir/data.new" "$dir/verdict"
		return 1
	fi
}

# check has TLC take the data module of trace $1, saving its verdict as $dir/verdict: "ok ...", the safety invariant a line broke, or the line no state could take. A check with no verdict is not kept, since a longer limit may give one.
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
	# TLC unpacks its standard modules into java.io.tmpdir and deletes them on exit, so a shared one fails a parse beside it.
	mkdir -p "$dir/tmp"
	(cd "$dir" && perl -e 'alarm shift; exec @ARGV' "${TLC_SECONDS:-400}" java -Xmx"${HEAP:-2g}" -Djava.io.tmpdir="$dir/tmp" -XX:+UseSerialGC -cp "$jar" tlc2.TLC -workers 1 -config trace_epoch.cfg trace_epoch.tla) >"$dir/out" 2>&1 || true
	rm -rf "$dir/states" "$dir/tmp"
	secs=$((SECONDS - start))
	broken=$(sed -n 's/^Error: Invariant \([A-Za-z]*\) is violated.*/\1/p' "$dir/out" | head -1)
	depth=$(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' "$dir/out")
	if [ "$broken" = TraceIncomplete ]; then
		echo "ok $total lines, ${secs}s" >"$dir/verdict"
	elif [ -n "$broken" ]; then
		at=$(($(sed -n 's/^\/\\ line = \([0-9]*\)$/\1/p' "$dir/out" | tail -1) - 1))
		{
			echo "invariant $broken broken by line $at of $total, ${secs}s"
			steps "$dir/trace_data.tla" | sed -n "${at}p"
		} >"$dir/verdict"
	elif [ -n "$depth" ]; then
		{
			echo "no step for line $depth of $total, ${secs}s"
			steps "$dir/trace_data.tla" | sed -n "${depth}p"
		} >"$dir/verdict"
	else
		{
			echo "no verdict after ${secs}s"
			tail -20 "$dir/out"
		} >"$dir/verdict"
		rm -f "$dir/key"
		return 1
	fi
	echo "$key" >"$dir/key"
	grep -q '^ok' "$dir/verdict"
}

# plain leaves trace $1's result line in its directory.
plain() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	if ! data "$1"; then
		echo "FAIL $name: no data"
	elif check "$1"; then
		echo "ok   $name: $(sed 's/^ok //' "$dir/verdict")"
	else
		echo "FAIL $name: $(cat "$dir/verdict")"
	fi >"$dir/line.new"
	mv "$dir/line.new" "$dir/line"
}

# planted leaves plant trace $1's table row in its directory, against its seed's baseline.
planted() {
	name=$(basename "$1" .trace)
	dir=$tmp/check/$name
	mkdir -p "$dir"
	base=$tmp/check/primaries-${name##*-}/base.lines
	stop=$(cut -c1-80 "${1%.trace}.stop" | head -1)
	(
		if ! data "$1"; then
			echo "$name: NO DATA, stop: $stop"
			exit 0
		fi
		steps "$dir/data.new" >"$dir/plant.lines"
		same=$(cmp "$base" "$dir/plant.lines" 2>&1 || true)
		case $same in
		*"EOF on "*base.lines*) first=$(($(wc -l <"$base") + 1)) ;;
		*" line "*) first=$(echo "$same" | sed 's/.* line \([0-9]*\).*/\1/') ;;
		*)
			echo "$name: not fired, $(wc -l <"$dir/plant.lines" | tr -d ' ') lines are a prefix of the baseline's, stop: $stop"
			exit 0
			;;
		esac
		if check "$1"; then
			echo "$name: PASSED, fired at line $first, $(cat "$dir/verdict"), stop: $stop"
		elif grep -q '^no verdict' "$dir/verdict"; then
			echo "$name: NO VERDICT, fired at line $first, $(head -1 "$dir/verdict"), stop: $stop"
		else
			echo "$name: rejected, fired at line $first, stop: $stop: $(cut -c1-200 "$dir/verdict" | tr '\n' ' ')"
		fi
	) >"$dir/line.new"
	mv "$dir/line.new" "$dir/line"
}

# each runs function $1 on each later argument, JOBS at a time, each in its own process.
each() {
	fn=$1
	shift
	[ $# -gt 0 ] || return 0
	export -f steps data check plain planted
	export tmp jar
	printf '%s\0' "$@" | xargs -0 -n 1 -P "${JOBS:-1}" "$BASH" -c 'set -euo pipefail; "$@"' each "$fn"
}

if [ "${1:-}" = plants ]; then
	rm -f "$tmp"/traces/*
	CONFORM_DIR=$tmp/traces CONFORM_PLANTS=${PLANTS:-} GOMEMLIMIT=4GiB go test -p 4 ./internal/chunkstore -run '^TestPlantsKeepTraces$' \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-10}"
	go build -o "$tmp/tracedata" ./tools/tracedata
	seeds=$(seq "${FIRST:-0}" $((${FIRST:-0} + ${COUNT:-10} - 1)))
	bases=()
	for seed in $seeds; do
		bases+=("$tmp/traces/primaries-$seed.trace")
	done
	each plain "${bases[@]}"
	rows=()
	for seed in $seeds; do
		dir=$tmp/check/primaries-$seed
		grep -q '^ok' "$dir/verdict" || continue
		steps "$dir/trace_data.tla" >"$dir/base.lines"
		for f in "$tmp"/traces/*-"$seed".trace; do
			[ "$(basename "$f")" = "primaries-$seed.trace" ] || rows+=("$f")
		done
	done
	each planted ${rows[@]+"${rows[@]}"}
	for seed in $seeds; do
		dir=$tmp/check/primaries-$seed
		if ! grep -q '^ok' "$dir/verdict"; then
			echo "seed $seed: baseline fails, no evidence: $(head -1 "$dir/verdict")"
			continue
		fi
		echo "seed $seed: baseline passes, $(cat "$dir/verdict")"
		for f in "$tmp"/traces/*-"$seed".trace; do
			name=$(basename "$f" .trace)
			[ "$name" = "primaries-$seed" ] || cat "$tmp/check/$name/line"
		done
	done
	exit 0
fi

if [ $# -eq 0 ]; then
	rm -f "$tmp"/traces/*
	CONFORM_DIR=$tmp/traces GOMEMLIMIT=4GiB go test -p 4 -run "${RUN:-TestConform}" ./internal/chunkstore \
		-seeds.first "${FIRST:-0}" -seeds "${COUNT:-10}"
	set -- "$tmp"/traces/*.trace
fi
go build -o "$tmp/tracedata" ./tools/tracedata

each plain "$@"
failed=0
for f in "$@"; do
	line=$tmp/check/$(basename "$f" .trace)/line
	cat "$line"
	grep -q '^ok' "$line" || failed=1
done
exit $failed
