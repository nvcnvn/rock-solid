#!/usr/bin/env bash
# K6: dependency footprint, unsafe usage, compile time and binary size of redb and fjall.
set -euo pipefail
cd "$(dirname "$0")"
REG=$(ls -d ~/.cargo/registry/src/index.crates.io-*/ | head -1)
echo "# K6 $(uname -r) $(rustc --version) $(date -u +%Y-%m-%dT%H:%MZ) loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"
for p in redb-only fjall-only; do
  (cd sizes/$p && cargo fetch -q)
  n=$(cd sizes/$p && cargo tree -e normal --prefix none | sort -u | wc -l)
  echo "K6 $p: unique crates in normal dependency tree (incl. itself) = $n"
  (cd sizes/$p && cargo tree -e normal --prefix none | sort -u | sed 's/^/    /')
done
for c in redb-4.4.0 fjall-3.1.12 lsm-tree-3.1.10; do
  d="$REG/$c/src"
  lines=$(find "$d" -name '*.rs' -exec cat {} + | wc -l)
  blocks=$(grep -rE '\bunsafe\b *(\{|fn|impl)' "$d" --include='*.rs' | grep -v '^\s*//' | wc -l)
  echo "K6 $c: src lines=$lines unsafe blocks/fns/impls=$blocks"
done
grep -n 'deny(unsafe_code)\|forbid(unsafe_code)' "$REG"/fjall-3.1.12/src/lib.rs "$REG"/lsm-tree-3.1.10/src/lib.rs "$REG"/redb-4.4.0/src/lib.rs || true
for p in none redb-only fjall-only; do
  t="$PWD/target/sizes-$p"
  rm -rf "$t"
  s=$(date +%s.%N)
  (cd sizes/$p && CARGO_TARGET_DIR="$t" cargo build -q --release)
  e=$(date +%s.%N)
  bin="$t/release/$p"
  strip -o "$t/stripped" "$bin"
  printf 'K6 %s: clean release build %.1fs (loadavg %s) binary=%d bytes stripped=%d bytes\n' \
    "$p" "$(echo "$e - $s" | bc)" "$(cut -d' ' -f1 /proc/loadavg)" "$(stat -c %s "$bin")" "$(stat -c %s "$t/stripped")"
done
