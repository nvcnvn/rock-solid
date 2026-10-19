#!/bin/sh
# Runs the lane model and the stale-header tests, and writes results.txt; about 30 s.
# The tests use internal/disklog's test harness, so they run in a copy of the committed tree with
# testdata/stale_header_test.go added to internal/disklog, first as committed, then with
# next-seq.diff applied, then with the variant the study rejects. The checkout itself is not changed.
set -eu
cd "$(dirname "$0")"
root=$(git rev-parse --show-toplevel)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git -C "$root" archive HEAD | tar -x -C "$tmp"
cp testdata/stale_header_test.go "$tmp/internal/disklog/"
{
	echo "# $(go version | cut -d' ' -f3-4), rock-solid $(git -C "$root" rev-parse --short HEAD), $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo
	python3 -I lanes.py
	echo
	echo "## Stale headers, committed code"
	(cd "$tmp" && go test -count=1 -run 'TestStudy' -v ./internal/disklog 2>&1) || true
	echo
	echo "## Stale headers and the whole disklog suite, with next-seq.diff"
	(cd "$tmp" && patch -s -p1 < "$OLDPWD/next-seq.diff" && go test -count=1 -run 'TestStudy' -v ./internal/disklog 2>&1 && go test -count=1 ./internal/... 2>&1) || true
	echo
	echo "## next-seq.diff taking the next sequence at checkpoint begin, which counts rounds in flight"
	(cd "$tmp" && { grep -q 'NextSeq: l.durableSeq,' internal/disklog/groupcommit.go || { echo "next-seq.diff not applied"; exit 1; }; } &&
		sed -i.bak 's/NextSeq: l.durableSeq,/NextSeq: l.nextSeq,/' internal/disklog/groupcommit.go &&
		go test -count=1 -run 'TestRandomCrashesKeepAcknowledgedRecords' ./internal/disklog 2>&1) || true
} | tee results.txt
