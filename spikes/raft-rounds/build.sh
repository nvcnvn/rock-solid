#!/bin/sh
# Builds bin/rounds against etcd-raft v3.7.0 (go.mod) and bin/rounds36 against v3.6.0,
# from a copy of the sources in work/v36 whose go.mod pins v3.6.0.
set -e
cd "$(dirname "$0")"
go build -o bin/rounds .
rm -rf work/v36 && mkdir -p work/v36
cp main.go pb36.go work/v36/
cat > work/v36/go.mod <<MOD
module github.com/nvcnvn/rock-solid/spikes/raft-rounds

go 1.27
MOD
(cd work/v36 && go get go.etcd.io/raft/v3@v3.6.0 golang.org/x/sys@v0.49.0 >/dev/null 2>&1 && go mod tidy >/dev/null 2>&1 && go build -tags raft36 -o ../../bin/rounds36 .)
