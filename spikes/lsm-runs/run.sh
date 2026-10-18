#!/bin/sh
# Builds the harness and writes results-<name>.txt; the first line names the kernel, Go and commit.
set -e
cd "$(dirname "$0")"
go test ./lsmx ./cix
go build -o bin/lsm-runs .
header() { echo "# $(uname -r) $(go version | cut -d' ' -f3) $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(nproc) vCPUs shared with other benchmarks, rock-solid $(git rev-parse --short HEAD), $(date -u +%Y-%m-%d)"; }
run() { name=$1; shift; { header; ./bin/lsm-runs "$@"; } > "results-$name.txt" 2>&1; }
run a "-exp=a3,a1,a2" -scale=0.5
run b "-exp=b" -maxflushes=3000
run c "-exp=c"
