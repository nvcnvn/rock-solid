#!/bin/bash
# Runs the Raft shard as Maelstrom nodes (tools/maelstrom) under the lin-kv workload with network
# partitions, on 3 and then 5 nodes, each of which must check linearizable, and then on 5 with
# reads served from the local map, which must not, up to three times, since only some partitions
# expose them. Maelstrom is downloaded once into a cache outside the repository, its jar checked
# against the pin on every run; the last invocation's runs are left in its store/. Needs Java 17 or
# later. Without gnuplot Maelstrom's latency graphs fail and its overall verdict is unknown, so a
# run is judged by its results.edn: the workload valid and no checker invalid.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=v0.2.4
SHA256=301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799
JAR_SHA256=7d35db06546a737134a4dd4eb3b7dfb0955537df992d922d18cc716080853f67
TIME_LIMIT=${TIME_LIMIT:-30}
dir=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/maelstrom/$VERSION
jar=$dir/maelstrom/lib/maelstrom.jar
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

if ! echo "$JAR_SHA256  $jar" | shasum -a 256 -c --status 2>/dev/null; then
	mkdir -p "$dir"
	curl -fsSL -o "$tmp/maelstrom.tar.bz2" "https://github.com/jepsen-io/maelstrom/releases/download/$VERSION/maelstrom.tar.bz2"
	echo "$SHA256  $tmp/maelstrom.tar.bz2" | shasum -a 256 -c --status || { echo "maelstrom.tar.bz2 checksum mismatch" >&2; exit 1; }
	tar xjf "$tmp/maelstrom.tar.bz2" -C "$tmp"
	rm -rf "$dir/maelstrom"
	mv "$tmp/maelstrom" "$dir/maelstrom"
	echo "$JAR_SHA256  $jar" | shasum -a 256 -c --status || { echo "maelstrom.jar checksum mismatch" >&2; exit 1; }
fi

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
export GOTOOLCHAIN
go build -o "$tmp/node" ./tools/maelstrom
printf '#!/bin/sh\nexec "%s/node" -stale-reads\n' "$tmp" >"$tmp/stale-node"
chmod +x "$tmp/stale-node"
rm -rf "$dir/store"

# run runs lin-kv on bin over n nodes and prints the workload's verdict line.
run() {
	local bin=$1 n=$2
	results=$dir/store/latest/results.edn
	rm -f "$dir/store/latest"
	(cd "$dir" && java -Xmx2g -Djava.awt.headless=true -jar "$jar" test -w lin-kv \
		--bin "$tmp/$bin" --node-count "$n" --time-limit "$TIME_LIMIT" --rate 100 --concurrency 2n \
		--nemesis partition --nemesis-interval 5 >"$dir/last.log" 2>&1) || true
	if [ ! -f "$results" ]; then
		echo "$bin, $n nodes: no results; see $dir/last.log"
		return
	fi
	echo "$bin, $n nodes: $(grep '^ :workload {:valid?' "$results"), $(tail -1 "$dir/last.log")"
}

failed=0
for n in 3 5; do
	run node "$n"
	if [ ! -f "$results" ] || ! grep -q '^ :workload {:valid? true' "$results" || grep -q ':valid? false' "$results"; then
		echo "lin-kv failed on $n nodes; see $dir/store" >&2
		failed=1
	fi
done
caught=0
for _ in 1 2 3; do
	run stale-node 5
	if [ -f "$results" ] && grep -q '^ :workload {:valid? false' "$results"; then
		caught=1
		break
	fi
done
if [ $caught = 0 ]; then
	echo "the checker missed reads served from the local map in three runs; see $dir/store" >&2
	failed=1
fi
exit $failed
