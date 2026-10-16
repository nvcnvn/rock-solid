#!/bin/bash
# Root-only half of the spike: the drive's own identify data and features (nvme-cli), then
# per-command NVMe latency from the nvme:nvme_setup_cmd and nvme:nvme_complete_rq tracepoints
# (nvme-lat.bt) while the harness and fio run as the invoking user. Writes results-ebpf.txt.
# Changes no system state; there is nothing to tear down. Writes about 300 MB under work/.
# usage: sudo ./trace-root.sh
set -e
[ "$(id -u)" = 0 ] || { echo "run as: sudo $0"; exit 1; }
cd "$(dirname "$0")"
U=${SUDO_USER:?run through sudo from the account that owns this directory}
. ./env.sh
CTRL=${CTRL:-/dev/nvme0}
NS=${NS:-/dev/nvme0n1}
F=work/data-ebpf
OUT=results-ebpf.txt
as_user() { runuser -u "$U" -- "$@"; }
[ -x bin/flush ] || as_user ./build.sh
[ -x vendor/sysroot/usr/bin/bpftrace ] || as_user ./fetch.sh
as_user mkdir -p work

trace() { # id, description, command run as the user
	local id=$1 what=$2
	shift 2
	echo
	echo "### $id $what"
	echo "loadavg: $(cat /proc/loadavg)"
	echo "    kind: 1 flush, 2 write, 3 write+FUA, 4 read, 5 other, 6 write submitted <100 us after a flush completed;" \
		"first key 1 = submitted by the harness or fio, 0 = other processes"
	bpftrace -q nvme-lat.bt -c "runuser -u $U -- $*"
}

{
	echo "### $(uname -r), $(tr -s ' ' </sys/class/nvme/nvme0/model | sed 's/ $//') fw $(cat /sys/class/nvme/nvme0/firmware_rev)," \
		"bpftrace $(bpftrace --version | cut -d' ' -f2), $(nvme version | head -1), $(date -Iseconds)"

	echo
	echo "### E0 nvme id-ctrl $CTRL (vwc = volatile write cache, oncs = optional commands, hmpre/hmmin = HMB size in 4 KiB units, ps = power states)"
	nvme id-ctrl "$CTRL" | grep -vE '^(sn|subnqn) '
	echo
	echo "### E0 decoded fields"
	nvme id-ctrl "$CTRL" -H | grep -iE -A3 '^(vwc|oncs|fna|apsta|hmpre|hmmin|npss|sanicap)\b' || true
	echo
	echo "### E0 features: 0x06 volatile write cache, 0x02 power management, 0x0c APST, 0x0d host memory buffer"
	for f in 0x06 0x02 0x0c 0x0d; do nvme get-feature "$CTRL" -f $f -H 2>&1 || true; done
	echo
	echo "### E0 nvme smart-log (wear, data written, temperature)"
	nvme smart-log "$CTRL"

	as_user bin/flush prep $F size_mb=256 >/dev/null
	trace E1 "4K O_DIRECT write + fdatasync at QD1 (N2)" bin/flush qd1 $F id=E1 n=3000 mode=fdatasync seq=1 bg_secs=1
	trace E2 "4K O_DIRECT write, no flush (N2c)" bin/flush qd1 $F id=E2 n=3000 mode=none want_fpo_x100=0 bg_secs=1
	trace E3 "empty fdatasync (N4)" bin/flush qd1 $F id=E3 n=3000 mode=empty bg_secs=1
	trace E4 "O_DIRECT|O_DSYNC 4K, FUA (N5)" bin/flush qd1 $F id=E4 n=1000 mode=dsync want_fpo_x100=0 bg_secs=1
	trace E5 "4K write + fsync (N5c)" bin/flush qd1 $F id=E5 n=1000 mode=fsync bg_secs=1
	trace E6 "bytes per flush 4K..4M (N3)" bin/flush sweep $F id=E6 budget_mb=16 min_n=16 max_n=500 seq=1 bg_secs=1
	trace E7 "8 threads pinned to one CPU (N7b)" bin/flush threads $F id=E7 secs=3 max=8 cpu=2 bg_secs=1
	trace E8 "8 threads unpinned (N7)" bin/flush threads $F id=E8 secs=3 max=8 bg_secs=1
	trace E9 "fio randwrite 4k --fdatasync=1 (N8a)" fio --name=E9 --filename=$F --size=256M --fallocate=none \
		--rw=randwrite --bs=4k --direct=1 --iodepth=1 --ioengine=psync --runtime=5 --time_based --fdatasync=1
} 2>&1 | tee "$OUT"
chown "$U": "$OUT"
rm -f $F
