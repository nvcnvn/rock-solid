#!/bin/sh
# Runs every test of the spike and writes results.txt; about a minute.
set -e
cd "$(dirname "$0")"
{
	echo "# $(uname -r), $(go version | cut -d' ' -f3-4), rock-solid $(git -C ../.. rev-parse --short HEAD), $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	go test -count=1 -v -timeout 30m ./... 2>&1
} | tee results.txt
