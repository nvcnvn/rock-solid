#!/bin/sh
# Fetches the sources this study cites into work/src, read-only references at pinned refs.
set -e
cd "$(dirname "$0")"
mkdir -p work/src
cd work/src
pin() { # dir url ref
  [ -d "$1" ] && return
  git init -q "$1"
  git -C "$1" remote add origin "$2"
  git -C "$1" fetch -q --depth 1 origin "$3"
  git -C "$1" checkout -q FETCH_HEAD
}
pin pebble-master https://github.com/cockroachdb/pebble.git a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
pin rocksdb-v11.8.1 https://github.com/facebook/rocksdb.git refs/tags/v11.8.1
pin titan https://github.com/tikv/titan.git b21afb5d7a50f960fbf47a19c103ebc6c43636a3
