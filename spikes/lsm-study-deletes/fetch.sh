#!/bin/sh
# Sources the study cites, shallow at the versions read. The harness itself needs only `go`:
# go.mod pins github.com/cockroachdb/pebble/v2 v2.1.7 and replaces rock-solid with ../.. for lsmd.
set -e
cd "$(dirname "$0")"
mkdir -p vendor && cd vendor
[ -d pebble-master ] || { git clone --filter=blob:none https://github.com/cockroachdb/pebble pebble-master && git -C pebble-master checkout a7b8321871e3; }
[ -d pebble-v2.1.7 ] || git clone --depth 1 --branch v2.1.7 https://github.com/cockroachdb/pebble pebble-v2.1.7
[ -d rocksdb-v11.8.1 ] || git clone --depth 1 --branch v11.8.1 https://github.com/facebook/rocksdb rocksdb-v11.8.1
