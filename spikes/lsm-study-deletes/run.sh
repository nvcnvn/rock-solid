#!/bin/sh
# Builds both harnesses and writes results-*.txt; the first line names the kernel, Go, Pebble and commit.
set -e
cd "$(dirname "$0")"
header() { echo "# $(uname -r) $(go version | cut -d' ' -f3) pebble v2.1.7, $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(nproc) vCPUs shared with other benchmarks, rock-solid $(git rev-parse --short HEAD), $(date -u +%Y-%m-%d)"; }
go build -o bin/pebblex ./pebblex
go build -o bin/ours ./ours
{ header; go test -v -run 'TestRangeModel|TestDropModel' ./lsmd 2>&1 | grep -v '^    --- PASS\|^=== RUN'; go test -run TestPlantedDrop ./lsmd 2>&1 | grep -c '^    --- FAIL' | sed 's/^/planted bug (drop a block whose first key is covered): failing runs of 300: /'; } > results-tests.txt
{ header; ./bin/pebblex -exp=p1,p2,p3 2>&1 | grep -v 'Found 0 WALs'; } > results-pebble.txt
{ header; ./bin/ours -exp=o1,o2; } > results-ours.txt
