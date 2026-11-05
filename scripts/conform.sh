#!/bin/bash
# Spec conformance: runs the simulator's conformance traces and has TLC 1.7.4 check each is a behavior
# of models/epoch/epoch.tla ("Spec conformance" in docs/test-scenarios.md). Not part of lint or CI.
#   scripts/conform.sh                  every trace of TestConform*
#   scripts/conform.sh file.trace ...   those traces alone
# Each trace is one light TLC run: one worker, HEAP (default 2g). KEEP=dir keeps the traces and data.
set -euo pipefail
cd "$(dirname "$0")/.."

version=1.7.4
jar=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/tla2tools-$version.jar
[ -f "$jar" ] || scripts/models.sh ci epoch/ci-asks.cfg >/dev/null

tmp=${KEEP:-$(mktemp -d)}
mkdir -p "$tmp/traces"
[ -n "${KEEP:-}" ] || trap 'rm -rf "$tmp"' EXIT

if [ $# -eq 0 ]; then
	CONFORM_DIR=$PWD/$tmp/traces
	case $tmp in /*) CONFORM_DIR=$tmp/traces ;; esac
	CONFORM_DIR=$CONFORM_DIR GOMEMLIMIT=4GiB go test -p 4 -run "${RUN:-TestConform}" ./internal/chunkstore
	set -- "$tmp"/traces/*.trace
fi
go build -o "$tmp/tracedata" ./tools/tracedata

failed=0
for f in "$@"; do
	name=$(basename "$f" .trace)
	dir=$tmp/check/$name
	rm -rf "$dir"
	mkdir -p "$dir"
	cp models/epoch/epoch.tla models/epoch/trace_epoch.tla models/epoch/trace_epoch.cfg "$dir"
	if ! "$tmp/tracedata" "$f" >"$dir/trace_data.tla"; then
		echo "FAIL $name: no data"
		failed=1
		continue
	fi
	total=$(grep -c '^  \[k |->' "$dir/trace_data.tla" || true)
	start=$SECONDS
	(cd "$dir" && java -Xmx"${HEAP:-2g}" -XX:+UseSerialGC -cp "$jar" tlc2.TLC -workers 1 -config trace_epoch.cfg trace_epoch.tla) >"$dir/out" 2>&1 || true
	secs=$((SECONDS - start))
	if grep -q 'Invariant TraceIncomplete is violated' "$dir/out"; then
		echo "ok   $name: $total lines, ${secs}s"
	else
		depth=$(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' "$dir/out")
		echo "FAIL $name: no step for line ${depth:-?} of $total, ${secs}s"
		if [ -n "$depth" ]; then
			grep '^  \[k |->' "$dir/trace_data.tla" | sed -n "${depth}p"
		else
			tail -20 "$dir/out"
		fi
		failed=1
	fi
done
exit $failed
