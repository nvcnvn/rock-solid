#!/bin/sh
# Builds the harness and writes results-f.txt (filters alone), results-ci.txt (chunk index) and
# results-ms.txt (MetaStore) and results-s.txt (scans); the first line of each names the kernel, Go, Pebble and commit.
set -e
cd "$(dirname "$0")"
go build -o bin/lsm-study-reads .
header() { echo "# $(uname -r) $(go version | cut -d' ' -f3) $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(nproc) vCPUs shared with other benchmarks, pebble $(git -C work/pebble rev-parse --short=12 HEAD), rock-solid $(git rev-parse --short HEAD), $(date -u +%Y-%m-%d)"; }
L="-exp=l -total=2000000 -lookups=100000 -runs=6,12,24 -filters=bloom10,fuse8"
{ header; for i in 1 2 3; do ./bin/lsm-study-reads -exp=f; done; } > results-f.txt 2>&1
{ header; ./bin/lsm-study-reads $L -index=ci-whole,ci-overwrite; } > results-ci.txt 2>&1
{ header; ./bin/lsm-study-reads $L -index=metastore; } > results-ms.txt 2>&1
{ header; ./bin/lsm-study-reads -exp=s -total=2000000 -runs=6,12,24; } > results-s.txt 2>&1
