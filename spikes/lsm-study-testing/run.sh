#!/bin/sh
# Builds the harness, runs it on internal/lsm and on the planted-bug copies, and
# measures CRC32C against a compaction. Writes results.txt.
set -eu
cd "$(dirname "$0")"
./plant.sh > work/plant.log
go vet ./... && go vet -tags planted ./...
go build -o bin/meta . && go build -tags planted -o bin/meta-planted .
{
	echo "# $(uname -sr), $(go version | cut -d' ' -f3), rock-solid $(git rev-parse --short HEAD), $(date -u +%F)"
	cat work/plant.log
	./bin/meta -seeds 200 -ops 3000
	./bin/meta-planted -planted-only -seeds 20 -ops 3000
	echo "# internal/lsm's own unit tests run against each planted copy"
	for p in p1 p2 p3 p4 p5; do
		cp ../../internal/lsm/*_test.go work/plant/$p/lsm/
		printf '%s: ' $p
		(cd work/plant/$p/lsm && go test -count=1 . 2>&1 | grep -E '^--- FAIL' | sed 's/ (.*//' | tr '\n' ' ')
		echo
	done
	(cd dd && go test -count=1 -v . 2>&1 | grep -E '^(--- |ok|FAIL)')
	./bin/meta -crc
} > results.txt 2>&1
