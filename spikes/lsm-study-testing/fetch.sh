#!/bin/sh
# Shallow clones of the sources this study cites, at the refs it read.
set -eu
cd "$(dirname "$0")"
mkdir -p vendor
clone() { # dir url ref
	[ -d "vendor/$1" ] && return
	git init -q "vendor/$1"
	git -C "vendor/$1" fetch -q --depth 1 "$2" "$3"
	git -C "vendor/$1" checkout -q FETCH_HEAD
}
clone pebble-master https://github.com/cockroachdb/pebble a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
clone pebble-v2.1.7 https://github.com/cockroachdb/pebble v2.1.7
clone rocksdb-v11.8.1 https://github.com/facebook/rocksdb v11.8.1
