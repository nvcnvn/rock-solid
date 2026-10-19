#!/bin/sh
# Fetches the sources this study cites into vendor/, read-only references; the harness itself
# needs only `go mod download` (Pebble v2.1.7 through go.mod, internal/lsm through the replace).
set -e
cd "$(dirname "$0")"
mkdir -p vendor
clone_ref() { # dir url ref
  [ -d "vendor/$1" ] && return
  git init -q "vendor/$1"
  git -C "vendor/$1" fetch -q --depth 1 "$2" "$3"
  git -C "vendor/$1" checkout -q FETCH_HEAD
}
clone_ref pebble-master https://github.com/cockroachdb/pebble a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
clone_ref pebble-v2.1.7 https://github.com/cockroachdb/pebble v2.1.7
clone_ref rocksdb-v11.8.1 https://github.com/facebook/rocksdb v11.8.1
go mod download
