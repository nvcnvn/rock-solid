#!/bin/sh
# Runs every test of the README; ./run.sh > results.txt 2>&1 (about 15 minutes).
# Profiles land in work/.
set -e
cd "$(dirname "$0")"
./build.sh
mkdir -p work
echo "# $(uname -sr), $(go version | cut -d' ' -f3), etcd-raft v3.7.0 and v3.6.0, $(nproc) vCPUs, $(date -u +%Y-%m-%dT%H:%MZ), loadavg $(cut -d' ' -f1-3 /proc/loadavg) (machine shared with other benchmarks)"
B=./bin/rounds
M="-warmup 0.5 -measure 1.5"

echo "### R: closed loop, 32 proposals in flight per group (32,000), flush 0.95 ms"
$B $M -label "R1 unbounded"
$B $M -max-bytes 1048576 -label "R2 max-bytes 1 MiB"
$B $M -max-bytes 262144 -label "R3 max-bytes 256 KiB"
$B $M -max-cpu-us 1000 -label "R4 collection CPU bound 1 ms"
$B $M -loops 2 -label "R5 2 loops per node"
$B $M -loops 4 -label "R6 4 loops per node"
$B $M -loops 4 -max-bytes 1048576 -label "R7 4 loops, 1 MiB"
$B $M -disk nvme -label "R8 unbounded, flush by size (NVMe spike N3 curve)"
$B $M -disk nvme -max-bytes 1048576 -label "R9 1 MiB, flush by size"
$B $M -batch=false -label "R10 one entry per proposal (multi-raft spike's shape)"
$B $M -batch=false -max-bytes 1048576 -label "R11 one entry per proposal, 1 MiB"

echo "### R: closed loop, flush 0.1 ms"
$B $M -flush-us 100 -label "R12 unbounded"
$B $M -flush-us 100 -max-bytes 1048576 -label "R13 1 MiB"
$B $M -flush-us 100 -loops 4 -label "R14 4 loops"

echo "### O: open loop (Poisson), flush 0.95 ms and 0.1 ms"
for f in 950 100; do
  for r in 30000 60000 100000 130000; do
    $B $M -mode open -rate $r -flush-us $f -label "O flush ${f}us rate $r, 1 loop"
    $B $M -mode open -rate $r -flush-us $f -max-bytes 262144 -label "O flush ${f}us rate $r, 1 loop, 256 KiB"
    $B $M -mode open -rate $r -flush-us $f -loops 2 -label "O flush ${f}us rate $r, 2 loops"
    $B $M -mode open -rate $r -flush-us $f -loops 4 -label "O flush ${f}us rate $r, 4 loops"
  done
done

echo "### A: allocations and heap, etcd-raft v3.7.0 against v3.6.0"
for v in rounds rounds36; do
  ./bin/$v $M -mode open -rate 60000 -label "A $v open 60,000/s wire" -memprof work/$v-open.pprof
  ./bin/$v $M -mode open -rate 60000 -wire=false -label "A $v open 60,000/s in-process, no encoding"
  ./bin/$v $M -label "A $v closed 32,000 in flight wire" -memprof work/$v-closed.pprof
done
for v in rounds rounds36; do
  for p in open closed; do
    echo "### pprof $v $p: alloc_objects during the measurement"
    go tool pprof -sample_index=alloc_objects -base work/$v-$p.pprof.base -top -nodecount=25 bin/$v work/$v-$p.pprof.end 2>/dev/null | sed -n '1,32p'
  done
done
