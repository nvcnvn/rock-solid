#!/bin/bash
# I2: SPDK's NVMe initiator against an SPDK nvmf target exporting a malloc bdev over vfio-user,
# all as a normal user, no hugepages. Writes results-vfiouser.txt.
set -u
D=$(cd "$(dirname "$0")" && pwd)
. "$D/env.sh"
OUT=$D/results-vfiouser.txt
V=$D/work/vfu.$$
SOCK=$D/work/tgt.$$.sock
NQN=nqn.2019-07.io.spdk:cnode0
TRID="trtype:VFIOUSER traddr:$V subnqn:$NQN"
mkdir -p "$V"
TGT=
stop() { [ -n "$TGT" ] && kill "$TGT" 2>/dev/null && wait "$TGT" 2>/dev/null; }
trap stop EXIT

{
	echo "### $(uname -r), $(lscpu | grep 'Model name' | sed 's/  */ /g'), SPDK $(cat "$SPDK/VERSION" 2>/dev/null || git -C "$SPDK" describe --tags) / DPDK $(cat "$SPDK/dpdk/VERSION"), uid $(id -u), no hugepages, $(date -I)"
	echo "### target: nvmf_tgt --no-huge -m 0x2 (core 1, poll mode), VFIOUSER transport, Malloc0 64 MB 4K blocks"
} > "$OUT"
"$SPDK/build/bin/nvmf_tgt" --no-huge -s 512 -m 0x2 -r "$SOCK" --base-virtaddr 0x10000000000 > "$D/work/tgt.log" 2>&1 &
TGT=$!
for _ in $(seq 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
R() { python3 "$SPDK/scripts/rpc.py" -s "$SOCK" "$@" > /dev/null; }
R nvmf_create_transport -t VFIOUSER
R bdev_malloc_create -b Malloc0 64 4096
R nvmf_create_subsystem $NQN -a -s SPDK0
R nvmf_subsystem_add_ns $NQN Malloc0
R nvmf_subsystem_add_listener $NQN -t VFIOUSER -a "$V" -s 0

run() {
	echo "### bin/nvme_intr $*" >> "$OUT"
	"$D/bin/nvme_intr" --no-huge --base 0x20000000000 "$@" "$TRID" 2>&1 | grep -v "SO_RCVBUF\|ns_data_alloc_mode" >> "$OUT"
	echo >> "$OUT"
}
run
run --interrupt
echo "### bin/nvme_intr --interrupt --trust-request   (killed after 5 s, first 20 lines)" >> "$OUT"
timeout 5 "$D/bin/nvme_intr" --no-huge --base 0x20000000000 --interrupt --trust-request "$TRID" 2>&1 \
	| grep -v "SO_RCVBUF\|ns_data_alloc_mode" | head -20 >> "$OUT"
echo "exit status ${PIPESTATUS[0]} (124 = killed by timeout)" >> "$OUT"
echo >> "$OUT"
run --bdev
run --bdev --interrupt
cat "$OUT"
