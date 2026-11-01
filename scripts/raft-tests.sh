#!/bin/bash
# Runs etcd-raft's own unit and datadriven tests on the pinned fork in third_party/raft, with the
# main module's toolchain, at most 4 packages at a time of GOMEMLIMIT (default 1GiB) each.
set -euo pipefail
cd "$(dirname "$0")/.."

GOTOOLCHAIN=$(sed -n 's/^toolchain //p' go.mod)
GOMEMLIMIT=${GOMEMLIMIT:-1GiB}
export GOTOOLCHAIN GOMEMLIMIT

cd third_party/raft
go test -p 4 ./...
