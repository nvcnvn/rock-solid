#!/usr/bin/env bash
# Builds the bubble tests for Linux and runs them in a container (or natively on Linux).
# Usage: ./run.sh > results.txt   Needs Go 1.27 and, off Linux, docker.
set -euo pipefail
cd "$(dirname "$0")"
arch=$(go env GOARCH)
GOOS=linux GOARCH=$arch go test -c -o bubble.test .
if [ "$(uname -s)" = Linux ]; then
	./bubble.test -test.v -test.count=1
else
	run() { docker run --rm --platform "linux/$arch" -v "$PWD:/t" "$@"; }
	run alpine:3 sh -c 'uname -sr; /t/bubble.test -test.v -test.count=1'
	for opts in "--cpus=2.5" "--cpus=2.5 --security-opt seccomp=unconfined"; do
		echo "== docker run $opts"
		# shellcheck disable=SC2086
		run $opts alpine:3 /t/bubble.test -test.v -test.count=1 -test.run TestContainer
	done
fi
rm -f bubble.test
