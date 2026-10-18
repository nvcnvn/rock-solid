#!/usr/bin/env bash
# Reproduces the panic on upstream etcd-raft: ./repro.sh [tag]
# Needs git and Go. Clones etcd-raft into ./work and runs the scenario through its own
# datadriven interaction test, with every upstream testdata file removed so only this one runs.
set -euo pipefail
cd "$(dirname "$0")"
tag=${1:-v3.7.0}
rm -rf work
git -c advice.detachedHead=false clone -q --depth 1 --branch "$tag" https://github.com/etcd-io/raft.git work/raft
echo "# etcd-raft $tag ($(git -C work/raft rev-parse HEAD)), $(go version)"
rm work/raft/testdata/*.txt
cp async_snapshot_overtakes_apply.txt work/raft/testdata/
cd work/raft
# -rewrite regenerates expected output, so the only possible failure is the panic itself.
go test -count=1 -run TestInteraction . -rewrite 2>&1 | grep -E 'panic during|^panic:|^(ok|FAIL)' || true
