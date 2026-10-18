#!/bin/bash
# Runs every test and writes results-<tag>.txt. Writes a 1 GiB file under work/ (WORK=... to put it on
# another disk), takes about 2 minutes, and deletes the file at the end (KEEP=1 keeps it).
# usage: DEV=nvme0n1 ./run.sh [tag]
cd "$(dirname "$0")"
DEV=${DEV:-nvme0n1}
export DEV
OUT=results${1:+-$1}.txt
WORK=${WORK:-work}
F=$WORK/data
mkdir -p "$WORK"
[ -x bin/stream ] || ./build.sh
d=/sys/block/$DEV
model=$( (tr -s ' ' <"$d/device/model" | sed 's/ $//') 2>/dev/null)
{
	echo "### $(uname -r), ${model:-$DEV}, write_cache=$(cat $d/queue/write_cache), fua=$(cat $d/queue/fua)," \
		"hw queues=$(ls $d/mq | wc -l), $(findmnt -no FSTYPE,OPTIONS -T "$WORK"), $(nproc) CPUs," \
		"$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(date -Iseconds)"
	[ -f "$F" ] || bin/stream prep "$F" size_mb=1024
	bin/stream suite "$F" secs=${SECS:-5}
} 2>&1 | tee "$OUT"
[ "${KEEP:-0}" = 1 ] || rm -f "$F"
