#!/bin/sh
# Fetches the sources this spike reads and builds against, shallow, at the commits it cites.
set -e
cd "$(dirname "$0")"
mkdir -p work
clone() { # dir url ref
  [ -d "work/$1" ] && return
  git init -q "work/$1"
  git -C "work/$1" fetch -q --depth 1 "$2" "$3"
  git -C "work/$1" checkout -q FETCH_HEAD
}
clone pebble https://github.com/cockroachdb/pebble a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
clone pebble-v2.1.7 https://github.com/cockroachdb/pebble v2.1.7
clone rocksdb https://github.com/facebook/rocksdb v11.8.1
