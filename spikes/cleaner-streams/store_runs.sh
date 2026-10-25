#!/bin/sh
# Q15b: C1's uniform runs with runs in their own stream, under the store's conditions one at a time,
# for the targets of the store's write amplification test (docs/test-scenarios.md, "Cleaner write
# amplification in simulation"): the store's segment count, 15 or 16 segments free, the share of
# data the store holds at the same disk fill (meta_reserve=0.0125) and records of the writes its
# rounds carried under write admission.
cd "$(dirname "$0")"
BIN=target/release/cleaner-streams
A="wl=uniform meta=1 run_stream=2 streams=3 warm=4 windows=2"
run() { $BIN "$@" 2>&1 | grep -E '^RUN|panicked|cannot'; }
# The store's targets.
run $A tag=Q15b:target:80 segs=1024 seg_mb=4 u=0.8 meta_reserve=0.0125 free_low=16 rec_kb=48
run $A tag=Q15b:target:85 segs=1024 seg_mb=4 u=0.85 meta_reserve=0.0125 free_low=16 rec_kb=32
run $A tag=Q15b:target:90 segs=2048 seg_mb=8 u=0.9 meta_reserve=0.0125 free_low=16 rec_kb=28
# The gap at 80% with 80% of the disk in data, a condition at a time.
run $A tag=Q15b:gap80:base segs=4096 seg_mb=4 u=0.8 meta_reserve=0.02
run $A tag=Q15b:gap80:segs segs=1024 seg_mb=4 u=0.8 meta_reserve=0.02
run $A tag=Q15b:gap80:free segs=1024 seg_mb=4 u=0.8 meta_reserve=0.02 free_low=15
run $A tag=Q15b:gap80:records segs=1024 seg_mb=4 u=0.8 meta_reserve=0.02 free_low=15 rec_kb=44
run $A tag=Q15b:gap80:data segs=1024 seg_mb=4 u=0.82 meta_reserve=0.02 free_low=15 rec_kb=44
# 64 MiB segments, 4,096 of them, a condition at a time.
for u in 0.8 0.9; do
  run $A tag=Q15b:64m:$u:base segs=4096 seg_mb=64 u=$u meta_reserve=0.02
  run $A tag=Q15b:64m:$u:free segs=4096 seg_mb=64 u=$u meta_reserve=0.02 free_low=16
  run $A tag=Q15b:64m:$u:data segs=4096 seg_mb=64 u=$u meta_reserve=0.0125 free_low=16
done
run $A tag=Q15b:64m:0.8:records segs=4096 seg_mb=64 u=0.8 meta_reserve=0.0125 free_low=16 rec_kb=48
run $A tag=Q15b:64m:0.9:records segs=4096 seg_mb=64 u=0.9 meta_reserve=0.0125 free_low=16 rec_kb=28
run $A tag=Q15b:4096:0.95 segs=4096 seg_mb=4 u=0.95 meta_reserve=0.02 free_low=15 rec_kb=44
