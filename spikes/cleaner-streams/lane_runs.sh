#!/bin/sh
# A3 with the slot headers pinned (PIN_HDR=1): in the hot stream (whole_stream=0) and in a lane of their own (whole_stream=2).
cd "$(dirname "$0")"
BIN=target/release/cleaner-streams
for ww in 0.5 0.2; do
  for u in 0.5 0.8 0.9; do
    for ws in 2 0; do
      PIN_HDR=1 $BIN tag=A3:$ww:$u:ws$ws wl=mixed whole_f=0.5 whole_w=$ww u=$u slots=1 slot_hdr_in_log=1 whole_stream=$ws seg_mb=64 segs=1024 free_low=8 warm=4 windows=2 2>&1 | grep -E '^(RUN|LANE)|panicked|cannot' 
    done
  done
done
