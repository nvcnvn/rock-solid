#!/bin/bash
# Reproduces every measurement (no root) and writes results-<tag>.txt. Writes about 1.3 GB, takes about 5 minutes, to the
# disk under work/ and deletes the test file at the end (KEEP=1 keeps it).
# usage: ./run.sh [tag]
cd "$(dirname "$0")"
. ./env.sh
DEV=${DEV:-nvme0n1}
export DEV
OUT=results${1:+-$1}.txt
F=work/data
mkdir -p work
[ -x bin/flush ] || ./build.sh
[ -x vendor/fio/fio ] || ./fetch.sh

q=/sys/block/$DEV/queue
d=/sys/block/$DEV/device
stat_flush() { awk '{print $16, $17, $5, $7}' /sys/block/$DEV/stat; }
section() { echo; echo "### $*"; echo "loadavg: $(cat /proc/loadavg)"; }
want() { # id what got want
	if [ "$3" = "$4" ]; then echo "[OK ] $1 $2 got=$3 want=$4"; else echo "[FAIL] $1 $2 got=$3 want=$4"; fi
}

fio_job() { # id, extra fio args
	local id=$1
	shift
	section "$id fio $*"
	read -r f0 ft0 w0 _ < <(stat_flush)
	fio --name="$id" --filename=$F --size=256M --fallocate=none --rw=randwrite --bs=4k --direct=1 --iodepth=1 \
		--ioengine=psync --runtime=10 --time_based --percentile_list=50:90:99:99.9 "$@" 2>&1 |
		grep -vE '^\s*$|Starting|Jobs:'
	read -r f1 ft1 w1 _ < <(stat_flush)
	echo "    device: $((f1 - f0)) flushes, $((w1 - w0)) write requests, device time per flush" \
		"$(awk -v a=$((ft1 - ft0)) -v b=$((f1 - f0)) 'BEGIN{printf "%.3f", b ? a / b : 0}') ms (includes background)"
}

{
	echo "### $(uname -r), $(tr -s ' ' <$d/model | sed 's/ $//') fw $(cat $d/firmware_rev), $(fio --version)," \
		"bpftrace $(bpftrace --version | cut -d' ' -f2), ext4 $(findmnt -no OPTIONS -T .), $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(date -Iseconds)"
	read -r F_START _ _ _ < <(stat_flush)
	W_START=$(awk '{print $7}' /sys/block/$DEV/stat)

	section "N1 drive facts from sysfs (no root)"
	for f in model firmware_rev sqsize queue_count hmb transport; do echo "    device/$f = $(tr -s ' ' <$d/$f)"; done
	for f in write_cache fua nr_requests max_hw_sectors_kb max_sectors_kb scheduler logical_block_size physical_block_size; do
		echo "    queue/$f = $(cat $q/$f)"
	done
	echo "    pcie link = $(cat $d/device/current_link_speed) x$(cat $d/device/current_link_width), vendor:device = $(cat $d/device/vendor):$(cat $d/device/device)"
	echo "    pci runtime pm: power/control = $(cat $d/device/power/control), runtime_status = $(cat $d/device/power/runtime_status)"
	for p in default_ps_max_latency_us apst_primary_timeout_ms apst_primary_latency_tol_us apst_secondary_timeout_ms apst_secondary_latency_tol_us force_apst; do
		echo "    nvme_core.$p = $(cat /sys/module/nvme_core/parameters/$p)"
	done
	read -r bf bft _ _ < <(stat_flush)
	echo "    since boot: $bf flushes, $(awk -v a=$bft -v b=$bf 'BEGIN{printf "%.3f", a / b}') ms device time per flush (all processes)"
	want N1 "volatile write cache" "$(cat $q/write_cache)" "write back"
	want N1 "FUA supported" "$(cat $q/fua)" 1
	want N1 "host memory buffer in use (DRAM-less)" "$(cat $d/hmb)" 1

	section "N2 prepare: fallocate + write the whole file once"
	bin/flush prep $F size_mb=256

	section "B0 background disk activity from other processes"
	bin/flush bg $F secs=5

	section "N2 4K O_DIRECT pwrite + fdatasync, QD1, random offsets"
	bin/flush qd1 $F id=N2 bs=4096 n=5000 mode=fdatasync
	section "N2b same, sequential offsets (log-structured append)"
	bin/flush qd1 $F id=N2b bs=4096 n=5000 mode=fdatasync seq=1
	section "N2c 4K O_DIRECT pwrite only, no flush (write latency without a preceding flush)"
	bin/flush qd1 $F id=N2c bs=4096 n=5000 mode=none want_fpo_x100=0

	section "N2d 4K pwrite + fdatasync into fallocated but never written extents (sequential)"
	rm -f work/unwritten
	bin/flush prep work/unwritten size_mb=64 fill=0
	bin/flush qd1 work/unwritten id=N2d bs=4096 n=2000 mode=fdatasync seq=1
	rm -f work/unwritten

	section "N3 bytes per flush: one O_DIRECT pwrite of 4K..4M, then one fdatasync"
	bin/flush sweep $F id=N3 budget_mb=64 min_n=32 max_n=2000 seq=1
	section "N3b one 64K record made durable with one flush or several"
	bin/flush split $F id=N3b n=300

	section "N4 empty fdatasync (nothing dirty)"
	bin/flush qd1 $F id=N4 n=5000 mode=empty

	section "N5 O_DIRECT|O_DSYNC 4K (FUA write instead of a flush)"
	bin/flush qd1 $F id=N5 bs=4096 n=2000 mode=dsync want_fpo_x100=0
	section "N5b O_DIRECT|O_DSYNC 64K"
	bin/flush qd1 $F id=N5b bs=65536 n=500 mode=dsync want_fpo_x100=0
	section "N5c O_DIRECT 4K + fsync (not fdatasync)"
	bin/flush qd1 $F id=N5c bs=4096 n=2000 mode=fsync

	section "N6 idle gap before each 4K write + fdatasync"
	bin/flush gaps $F id=N6 n_1s=20
	section "N6b 3 s idle (past nvme_core.apst_secondary_timeout_ms) before each 4K write + fdatasync, sleeping"
	bin/flush qd1 $F id=N6b bs=4096 n=10 gap_us=3000000
	section "N6c same, spinning"
	bin/flush qd1 $F id=N6c bs=4096 n=10 gap_us=3000000 spin=1

	section "N7 concurrent 4K write + fdatasync threads, unpinned"
	bin/flush threads $F id=N7 secs=5 max=8
	section "N7b same, all threads pinned to CPU 2 (one blk-mq hardware queue)"
	bin/flush threads $F id=N7b secs=5 max=8 cpu=2

	fio_job N8a --fdatasync=1
	fio_job N8b --fsync=1
	fio_job N8c --sync=dsync
	fio_job N8d --fdatasync=1 --rw=write

	read -r F_END _ _ _ < <(stat_flush)
	W_END=$(awk '{print $7}' /sys/block/$DEV/stat)
	section "end"
	echo "    device total during run (all processes): $((F_END - F_START)) flushes, $(((W_END - W_START) / 2048)) MiB written"
} 2>&1 | tee "$OUT"

[ "${KEEP:-0}" = 1 ] || rm -f $F
