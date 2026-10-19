#!/bin/sh
# Shallow clones of the sources this study cites, at the refs it read, into work/src.
# The study reads them; nothing here builds them.
set -e
cd "$(dirname "$0")"
mkdir -p work/src
cd work/src
# Pebble master at a7b8321871e3 (2026-10-14): a commit, so fetch it by hash.
if [ ! -d pebble-master ]; then
  git init -q pebble-master
  git -C pebble-master fetch -q --depth 1 https://github.com/cockroachdb/pebble a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
  git -C pebble-master checkout -q FETCH_HEAD
fi
[ -d pebble-v2.1.7 ] || git clone -q --depth 1 --branch v2.1.7 https://github.com/cockroachdb/pebble pebble-v2.1.7
[ -d rocksdb-v11.8.1 ] || git clone -q --depth 1 --branch v11.8.1 https://github.com/facebook/rocksdb rocksdb-v11.8.1
