#!/bin/bash
# I4: the guest's test list (guest-init.sh, IOMMU mode) against a real spare NVMe drive that
# setup-root.sh bound to vfio-pci.  sudo ./run-real.sh <BDF>  -> results-real.txt
set -u
D=$(cd "$(dirname "$0")" && pwd)
. "$D/env.sh"
BDF=${1:?usage: sudo $0 <pci BDF>}
[ "$(basename "$(readlink /sys/bus/pci/devices/$BDF/driver)")" = vfio-pci ] || { echo "$BDF is not on vfio-pci: run setup-root.sh first"; exit 1; }
OUT=$D/results-real.txt
TR="trtype:PCIe traddr:$BDF"
BASE=--base\ 0x10000000000
{
	echo "### $(uname -r), $(lscpu | grep 'Model name' | sed 's/  */ /g'), SPDK $(git -C "$SPDK" describe --tags) / DPDK $(cat "$SPDK/dpdk/VERSION"), $(date -I)"
	echo "### $BDF: $(lspci -s "$BDF" 2>/dev/null), $(lspci -s "$BDF" -vv 2>/dev/null | grep -o 'MSI-X: Enable[+-] Count=[0-9]*')"
	echo "### cpuidle: $(cat /sys/devices/system/cpu/cpuidle/current_governor_ro 2>/dev/null), states $(cat /sys/devices/system/cpu/cpu0/cpuidle/state*/name 2>/dev/null | tr '\n' ' ')"
} > "$OUT"
run() { echo "### nvme_intr $*"; "$D/bin/nvme_intr" "$@" 2>&1 | grep -v "SO_RCVBUF\|ns_data_alloc_mode"; echo; }
perf() {
	echo "### spdk_nvme_perf $*"
	/usr/bin/time -f "real %e s user %U s sys %S s" "$SPDK/build/bin/spdk_nvme_perf" -r "$TR" -c 0x1 -o 4096 -t 10 -s 256 "$@" 2>&1 | grep -E "Total|real|ERROR|Couldn"
	echo
}
{
	run $BASE "$TR"
	run $BASE --interrupt "$TR"
	run $BASE --bdev "$TR"
	run $BASE --bdev --interrupt "$TR"
	run $BASE --interrupt --no-huge --mem 128 "$TR"
	for qd in 1 32 128; do perf -q $qd -w randread; perf -q $qd -w randread -E; done
} | tee -a "$OUT"
