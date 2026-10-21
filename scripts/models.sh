#!/bin/bash
# Checks the TLA+ models under models/ with TLC 1.7.4. Each models/<name>/ holds the spec <name>.tla
# and its configurations: ci*.cfg and deep*.cfg must pass, neg-*.cfg must fail, and a negative
# configuration's first line, "\* expect: <name>", names the invariant or property it must violate.
#   scripts/models.sh [ci|deep|sim] [model...]
# ci (the default) runs the ci and negative configurations, each failing past 5 minutes; deep runs
# the deep ones, each failing past 4 hours; sim runs TLC's random simulation of each ci
# configuration for SIM_SECONDS (default 3600) at depth 100 with a fresh seed, printed for replay.
# TLC uses WORKERS (default 4) workers and a heap of HEAP (default 4g).
set -euo pipefail
cd "$(dirname "$0")/.."

version=1.7.4
sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
jar=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/tla2tools-$version.jar
if ! echo "$sha256  $jar" | shasum -a 256 -c --status 2>/dev/null; then
	mkdir -p "$(dirname "$jar")"
	curl -fsSL -o "$jar.tmp" "https://github.com/tlaplus/tlaplus/releases/download/v$version/tla2tools.jar"
	echo "$sha256  $jar.tmp" | shasum -a 256 -c --status || { echo "tla2tools.jar checksum mismatch" >&2; exit 1; }
	mv "$jar.tmp" "$jar"
fi

mode=${1:-ci}
shift || true
case $mode in
ci) limit=300 ;;
deep) limit=14400 ;;
sim) limit=${SIM_SECONDS:-3600} ;;
*) echo "usage: $0 [ci|deep|sim] [model...]" >&2; exit 2 ;;
esac
models=${*:-$(ls models)}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failed=0

# tlc model cfg [flags...]: runs TLC under the time limit, leaving its output in $tmp/out and the
# seconds it took in $secs.
tlc() {
	local model=$1 cfg=$2
	shift 2
	local start=$SECONDS
	(cd "models/$model" && perl -e 'alarm shift; exec @ARGV' "$limit" \
		java -Xmx"${HEAP:-4g}" -XX:+UseParallelGC -cp "$jar" tlc2.TLC \
		-workers "${WORKERS:-4}" -metadir "$tmp/meta" -config "$cfg" "$@" "$model.tla") \
		>"$tmp/out" 2>&1 && status=0 || status=$?
	secs=$((SECONDS - start))
	rm -rf "$tmp/meta"
	return $status
}

summary() {
	local depth states
	depth=$(sed -n 's/.*depth of the complete state graph search is \([0-9]*\).*/\1/p' "$tmp/out")
	states=$(sed -n 's/.* \([0-9]*\) distinct states found.*/\1/p' "$tmp/out" | tail -1)
	echo "depth ${depth:-?}, ${states:-?} distinct states, ${secs}s"
}

for model in $models; do
	[ -f "models/$model/$model.tla" ] || { echo "FAIL $model: no models/$model/$model.tla"; failed=1; continue; }
	for kind in ci deep; do
		if ! compgen -G "models/$model/$kind*.cfg" >/dev/null; then
			echo "FAIL $model: no $kind configuration"
			failed=1
		fi
	done
	case $mode in
	ci | deep) cfgs=$(cd "models/$model" && ls "$mode"*.cfg 2>/dev/null || true) ;;
	sim) cfgs=$(cd "models/$model" && ls ci*.cfg 2>/dev/null || true) ;;
	esac
	for cfg in $cfgs; do
		if [ "$mode" = sim ]; then
			seed=$RANDOM$RANDOM
			if tlc "$model" "$cfg" -simulate -depth 100 -seed "$seed" || [ $status = 142 ]; then
				if ! grep -q "Error:" "$tmp/out"; then
					echo "ok   $model/$cfg: seed $seed, ${secs}s"
					continue
				fi
			fi
			echo "FAIL $model/$cfg: seed $seed, replay with -simulate -depth 100 -seed $seed"
		elif tlc "$model" "$cfg"; then
			echo "ok   $model/$cfg: $(summary)"
			continue
		elif [ $status = 142 ]; then
			echo "FAIL $model/$cfg: over its ${limit}s budget; move its largest constant to deep"
		else
			echo "FAIL $model/$cfg: $(summary)"
		fi
		cat "$tmp/out"
		failed=1
	done
	[ "$mode" = ci ] || continue
	for cfg in $(cd "models/$model" && ls neg-*.cfg 2>/dev/null || true); do
		want=$(sed -n '1s/^\\\* expect: *//p' "models/$model/$cfg")
		if [ -z "$want" ]; then
			echo "FAIL $model/$cfg: first line must be \"\\* expect: <name>\""
			failed=1
		elif tlc "$model" "$cfg"; then
			echo "FAIL $model/$cfg: passed, but must violate $want"
			failed=1
		elif grep -Eq "(Invariant|Temporal property|Action property) $want (is|was) violated" "$tmp/out"; then
			echo "ok   $model/$cfg: violates $want, ${secs}s"
		else
			echo "FAIL $model/$cfg: did not violate $want"
			cat "$tmp/out"
			failed=1
		fi
	done
done
exit $failed
