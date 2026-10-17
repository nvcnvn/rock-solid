#!/usr/bin/env bash
# Fetches pinned sources into vendor/ (gitignored):
# - etcd-raft at the release tag being ported, for its testdata and for line counts;
# - protoc 21.12 (reports itself as 3.21.12), which raft-rs 0.7.0's protobuf-codec
#   build needs: protobuf-build bundles no macOS arm64 protoc and accepts only 3.x.
set -euo pipefail
cd "$(dirname "$0")"
TAG=v3.7.0
COMMIT=b867cf13f6bc0dae21204302df97bc2355c3af55
PROTOC_VER=21.12
mkdir -p vendor
if [ ! -d vendor/raft ]; then
  git -c advice.detachedHead=false clone -q --depth 1 --branch "$TAG" https://github.com/etcd-io/raft.git vendor/raft
fi
got=$(git -C vendor/raft rev-parse HEAD)
[ "$got" = "$COMMIT" ] || { echo "vendor/raft is at $got, want $COMMIT ($TAG)" >&2; exit 1; }
if [ ! -x vendor/protoc/bin/protoc ]; then
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64) plat=osx-aarch_64 ;;
    Darwin-x86_64) plat=osx-x86_64 ;;
    Linux-x86_64) plat=linux-x86_64 ;;
    Linux-aarch64) plat=linux-aarch_64 ;;
  esac
  curl -sSfL -o vendor/protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VER/protoc-$PROTOC_VER-$plat.zip"
  mkdir -p vendor/protoc && (cd vendor/protoc && unzip -q -o ../protoc.zip) && rm vendor/protoc.zip
fi
echo "etcd-raft $TAG $COMMIT; $(vendor/protoc/bin/protoc --version)"
