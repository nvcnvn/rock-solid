#!/bin/sh
# Fetches the sources this study cites into work/src, pinned to the commits it read.
set -eu
cd "$(dirname "$0")"
mkdir -p work/src
cd work/src

clone_sha() { # dir url sha
	[ -d "$1" ] && return
	git init -q "$1"
	git -C "$1" fetch -q --depth 1 "$2" "$3"
	git -C "$1" checkout -q FETCH_HEAD
}
clone_tag() { # dir url tag
	[ -d "$1" ] || git clone -q --depth 1 --branch "$3" "$2" "$1"
}

clone_sha pebble-master https://github.com/cockroachdb/pebble a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
clone_tag pebble-v2.1.7 https://github.com/cockroachdb/pebble v2.1.7
clone_tag rocksdb-v11.8.1 https://github.com/facebook/rocksdb v11.8.1

raw() { # dir repo sha path...
	d=$1 repo=$2 sha=$3
	shift 3
	mkdir -p "$d"
	for f in "$@"; do
		mkdir -p "$d/$(dirname "$f")"
		curl -sSfL -o "$d/$f" "https://raw.githubusercontent.com/$repo/$sha/$f"
	done
}
raw cockroach cockroachdb/cockroach ea997fe3b5fb48b4d8892e5b69c6d867ed2ec256 \
	pkg/storage/pebble.go pkg/storage/ballast.go \
	pkg/util/admission/io_load_listener.go \
	pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go
raw scylladb scylladb/scylladb 780d9fef0ba7bd98a2f6a4c90ea723c29054e11e \
	compaction/compaction.cc compaction/incremental_compaction_strategy.hh \
	docs/architecture/compaction/compaction-strategies.rst
raw linux torvalds/linux v6.12 block/ioctl.c
