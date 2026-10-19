#!/bin/sh
# Usage table cost per checkpoint: full table vs incremental edits plus snapshots.
# About 25 minutes on one shared core. Output: results-manifest.txt
set -e
cd "$(dirname "$0")"
mkdir -p bin
go build -o bin/manifestsim ./manifestsim
out=results-manifest.txt
{
  echo "# $(uname -sr) $(go version | cut -d' ' -f3) $(date -u +%Y-%m-%dT%H:%MZ) manifestsim (segment-level model, greedy cleaner interleaved with user writes, reserve 8, checkpoint every 64 MiB of log writes)"
  for args in \
    "-disk 256 -seg 64 -warm 0.6 -meas 0.2" \
    "-disk 1024 -seg 64 -warm 0.6 -meas 0.15" \
    "-disk 1024 -seg 64 -warm 0.6 -meas 0.15 -hotcold" \
    "-disk 1024 -seg 64 -warm 0.6 -meas 0.15 -ext 16" \
    "-disk 1024 -seg 64 -warm 0.6 -meas 0.15 -fill 0.8" \
    "-disk 1024 -seg 4 -warm 0.6 -meas 0.15" \
    "-disk 4096 -seg 64 -warm 0.5 -meas 0.05 -ext 16" ; do
    echo "## manifestsim $args"
    ./bin/manifestsim $args
  done
} > $out 2>&1
cat $out
