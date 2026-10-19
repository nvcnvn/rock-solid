#!/bin/sh
# Runs the index-writes-per-move matrix and writes results.txt. About 15 minutes on 4 shared vCPUs.
set -e
cd "$(dirname "$0")"
mkdir -p bin work
go build -o bin/blobsim ./sim
out=results.txt
echo "# $(uname -sr), $(go version | cut -d' ' -f3), $(date -u +%Y-%m-%d), 2048 segments of 1024 blocks, warm 4 / measure 2 capacities" > "$out"
for wl in uniform hc90; do
  for u in 0.80 0.90 0.95; do
    for order in chunk origin; do
      echo "bin/blobsim -wl $wl -u $u -order $order"
    done
  done
done | xargs -P 2 -I{} sh -c '{} > work/$(echo "{}" | tr " ./" "___").txt'
cat work/*.txt >> "$out"
