#!/bin/sh
# Usage: DEV=vda ./run.sh vm -> results-vm.txt. Writes a 1 GiB file ($FILE, default work/log.dat) and deletes it at the end.
set -eu
cd "$(dirname "$0")"
tag=${1:-vm}
dev=${DEV:-vda}
reps=${REPS:-3}
file=${FILE:-work/log.dat}
out=results-$tag.txt
mkdir -p bin work
go build -o bin/logpercore .
trap 'rm -f "$file"' EXIT
{
	echo "kernel $(uname -r), $(go version | cut -d' ' -f3), dev $dev ($(cat /sys/block/$dev/queue/write_cache), $(ls /sys/block/$dev/mq | wc -l) hw queues), $(nproc) CPUs, $(date -u +%Y-%m-%dT%H:%MZ), loadavg $(cut -d' ' -f1-3 /proc/loadavg)"
	for r in $(seq "$reps"); do
		for b in 1 8 64; do GOMAXPROCS=8 ./bin/logpercore -mode hop -batch $b -dur 2s; done
		for k in 1 2 4 8; do
			for m in perloop owner; do GOMAXPROCS=12 ./bin/logpercore -mode $m -k $k -depth 8 -dur 3s -dev "$dev" -file "$file"; done
		done
	done
	echo "loadavg at end $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee "$out.raw"
python3 -I summarize.py < "$out.raw" | tee "$out"
cat "$out.raw" >> "$out"
rm -f "$out.raw"
