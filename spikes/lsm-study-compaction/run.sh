#!/bin/sh
# Reproduces results-model.txt, results-real.txt and results-pebble.txt; pass the ones to run
# (model real pebble), default all. Pebble takes about 15 minutes on 4 shared vCPUs.
set -e
cd "$(dirname "$0")"
go build -o bin/lsmc .
hdr="# $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) vCPUs (shared); kernel $(uname -r); $(go version | cut -d' ' -f3); pebble v2.1.7; rock-solid internal/lsm at $(git -C ../.. rev-parse --short HEAD); $(date -u +%F)"
run() { echo "\$ ./bin/lsmc $*"; ./bin/lsmc "$@"; echo; }
for what in ${*:-model real pebble}; do
  case $what in
  model) {
    echo "$hdr"
    echo "## S4 scale: 44 MB chunk index of 40 B entries, flushes of 1.3 MB and 33 MB"
    run -mode model
    echo "## validation scale, the same bytes in 404 B entries (compare with results-real.txt)"
    run -mode model -keys 110000 -entry 404 -small 3300 -large 82500 -flushes 200
    echo "## a 1.6 GB index (MetaStore-like shard), the same flushes"
    run -mode model -keys 40000000 -flushes 4000
    echo "## whole-chunk index: a few dozen entries per flush, cleaner passes 25x that"
    run -mode model -keys 760000 -small 40 -large 1000 -flushes 4000
  } > results-model.txt ;;
  real) { echo "$hdr"; run -mode real -keys 110000 -small 3300 -large 82500 -flushes 200; } > results-real.txt ;;
  pebble) { echo "$hdr"; run -mode pebble -keys 110000 -small 3300 -large 82500 -flushes 200; } > results-pebble.txt ;;
  esac
done
