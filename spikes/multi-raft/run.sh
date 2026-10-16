#!/usr/bin/env bash
# Reproduces results.txt: ./run.sh > results.txt 2>&1
set -u
cd "$(dirname "$0")"
export CARGO_TARGET_DIR="$PWD/target"
cargo build --release -q 2>&1 | grep -v '^$'
B=./target/release/multi-raft-spike
G=${GROUPS_N:-1000}
DEFAULT="--hb 50 --elect 150-300"
TUNED="--hb 100 --elect 1000-2000"

echo "# kernel $(uname -r), openraft 0.9.25, raft (raft-rs) 0.7.0, tokio 1.47.1, $(rustc --version | cut -d' ' -f1-2), $(nproc) cores, $(date -I)"
echo "# loadavg at start: $(cut -d' ' -f1-3 /proc/loadavg) (other agents were compiling and benchmarking on this host)"

run() { echo; timeout 600 "$B" "$@"; }

echo; echo "##### M1-M3: memory, idle CPU, elections ($G groups x 3 replicas in one process)"
run raftrs --groups "$G" $DEFAULT --idle 10
run raftrs --groups "$G" $DEFAULT --tick 10 --idle 10
run raftrs --groups "$G" $TUNED --idle 10
run raftrs --groups "$G" $TUNED --tick 10 --idle 10
run openraft --groups "$G" $DEFAULT --idle 10 --threads 1
run openraft --groups "$G" $TUNED --idle 10 --threads 1
run openraft --groups "$G" $DEFAULT --idle 10 --threads 4

echo; echo "##### M5: quiescence (driver stops ticking idle groups)"
run raftrs --groups "$G" $DEFAULT --idle 10 --quiesce
run openraft --groups "$G" $DEFAULT --idle 10 --threads 1 --quiesce

echo; echo "##### M1: openraft RSS growth over a long idle window (100 groups, 120 s)"
run openraft --groups 100 $DEFAULT --idle 120 --threads 1 --no-kill
run raftrs --groups 100 $DEFAULT --idle 120 --no-kill

echo; echo "##### M4/M5: writes, 100 B proposals over all groups, closed loop"
for inf in 4 32; do
  run raftrs --groups "$G" $TUNED --write 5 --inflight "$inf"
  run openraft --groups "$G" $TUNED --write 5 --inflight "$inf" --threads 1
done
run openraft --groups "$G" $TUNED --write 5 --inflight 32 --threads 4
echo; echo "# simulated 1 ms fsync: raft-rs one fsync per loop round for all Readys; openraft one shared WAL task"
run raftrs --groups "$G" $TUNED --write 5 --inflight 32 --fsync-us 1000
run openraft --groups "$G" $TUNED --write 5 --inflight 32 --threads 1 --fsync-us 1000
run openraft --groups "$G" $TUNED --write 5 --inflight 32 --threads 4 --fsync-us 1000

echo; echo "##### M7: determinism, 10 runs per variant in separate processes (30 groups, seed 11)"
sim() {
  local mode=$1 variant=$2
  local hashes
  hashes=$(for _ in $(seq 10); do timeout 120 "$B" "$mode" --groups 30 --variant "$variant" --seed 11 | grep -o 'trace=[0-9a-f]*'; done | sort | uniq -c)
  local distinct
  distinct=$(echo "$hashes" | wc -l)
  local want=$3
  local tag="[FAIL]"
  { [ "$want" = same ] && [ "$distinct" = 1 ]; } || { [ "$want" = differ ] && [ "$distinct" -gt 1 ]; } && tag="[OK ]"
  echo "$tag M7 $mode variant=$variant: got=$distinct distinct traces in 10 runs want=$want"
  echo "$hashes" | sed 's/^/      /'
}
sim raftrs-sim plain differ
sim raftrs-sim seeded same
sim openraft-sim plain differ
sim openraft-sim seeded same
echo; timeout 120 "$B" raftrs-sim --groups 30 --variant seeded --seed 12 | cut -c1-120
timeout 120 "$B" openraft-sim --groups 30 --variant seeded --seed 12 | cut -c1-120

echo; echo "##### M6: dependency footprint and advisories"
for p in openraft raft; do
  echo "$p: $(cargo tree -p $p -e normal --prefix none | sed 's/ (\*)//' | sort -u | wc -l) crates in its normal dependency tree (including itself)"
  echo "  notable: $(cargo tree -p $p -e normal --prefix none | sed 's/ (\*)//' | grep -E '^(tokio|protobuf|getrandom|libc|clap|chrono|fxhash) ' | sort -u | tr '\n' ' ')"
done
cargo deny check advisories 2>&1 | grep -E 'advisories (ok|FAILED)|^error\[|ID: RUSTSEC|Crate:' | sed 's/^/  /'
