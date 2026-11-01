#!/bin/bash
# Runs the Raft shard as Maelstrom nodes (tools/maelstrom) under the lin-kv workload with network
# partitions, on 3 and then 5 nodes, each of which must check linearizable, and once more with reads
# served from the local map, which must not. Maelstrom is downloaded once, checked against its
# pinned hash, into a cache outside the repository, where its runs leave their store/ too. Needs
# Java 17 or later. Without gnuplot Maelstrom's latency graphs fail and its overall verdict is
# unknown, so a run is judged by its results.edn: the workload valid and no checker invalid.
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=v0.2.4
SHA256=301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799
TIME_LIMIT=${TIME_LIMIT:-30}
dir=${XDG_CACHE_HOME:-$HOME/.cache}/rock-solid/maelstrom/$VERSION

if [ ! -f "$dir/maelstrom/lib/maelstrom.jar" ]; then
	mkdir -p "$dir"
	curl -fsSL -o "$dir/maelstrom.tar.bz2" "https://github.com/jepsen-io/maelstrom/releases/download/$VERSION/maelstrom.tar.bz2"
	echo "$SHA256  $dir/maelstrom.tar.bz2" | shasum -a 256 -c -
	tar xjf "$dir/maelstrom.tar.bz2" -C "$dir"
	rm "$dir/maelstrom.tar.bz2"
fi

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
export GOTOOLCHAIN
go build -o "$dir/node" ./tools/maelstrom
printf '#!/bin/sh\nexec "%s/node" -stale-reads\n' "$dir" >"$dir/stale-node"
chmod +x "$dir/stale-node"

# run runs lin-kv on bin over n nodes and prints the workload's verdict line.
run() {
	local bin=$1 n=$2
	(cd "$dir" && java -Xmx2g -Djava.awt.headless=true -jar maelstrom/lib/maelstrom.jar test -w lin-kv \
		--bin "$dir/$bin" --node-count "$n" --time-limit "$TIME_LIMIT" --rate 100 --concurrency 2n \
		--nemesis partition --nemesis-interval 5 >"$dir/last.log" 2>&1) || true
	results=$dir/store/latest/results.edn
	echo "$bin, $n nodes: $(grep '^ :workload {:valid?' "$results"), $(tail -1 "$dir/last.log")"
}

failed=0
for n in 3 5; do
	run node "$n"
	if ! grep -q '^ :workload {:valid? true' "$results" || grep -q ':valid? false' "$results"; then
		echo "lin-kv failed on $n nodes; see $dir/store/latest" >&2
		failed=1
	fi
done
run stale-node 3
if ! grep -q '^ :workload {:valid? false' "$results"; then
	echo "the checker missed reads served from the local map; see $dir/store/latest" >&2
	failed=1
fi
exit $failed
