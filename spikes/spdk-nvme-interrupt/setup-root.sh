#!/bin/bash
# I4: hands a SPARE NVMe controller to vfio-pci so SPDK can drive it, and reserves 1 GB of
# hugepages. Undo with teardown-root.sh. Run as root:  sudo ./setup-root.sh <BDF> --wipe-ok
# The tests overwrite the first 1 MB of namespace 1 and read the first 256 MB: use a drive
# whose data you do not need. Refuses any controller that backs a mounted filesystem, swap,
# LVM/dm/md member, or the root disk, and any IOMMU group shared with another bound device.
set -euo pipefail
BDF=${1:?usage: sudo $0 <pci BDF, e.g. 0000:06:00.0> --wipe-ok}
[ "${2:-}" = --wipe-ok ] || { echo "refusing: pass --wipe-ok to confirm $BDF may be overwritten"; exit 1; }
DEV=/sys/bus/pci/devices/$BDF
[ -e "$DEV" ] || { echo "no PCI device $BDF"; exit 1; }
case $(cat "$DEV/class") in 0x0108*) ;; *) echo "refusing: $BDF is not an NVMe controller (class $(cat "$DEV/class"))"; exit 1 ;; esac

# PCI BDF of the NVMe controller behind a block device (partition, namespace or dm/md member).
bdf_of_block() {
	local b; b=$(basename "$(readlink -f "/sys/class/block/$1")")
	[ -e "/sys/class/block/$b/partition" ] && b=$(basename "$(dirname "$(readlink -f "/sys/class/block/$b")")")
	for s in /sys/class/block/"$b"/slaves/*; do [ -e "$s" ] && bdf_of_block "$(basename "$s")"; done
	readlink -f "/sys/class/block/$b/device" 2>/dev/null | grep -oE '[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9a-f]' | tail -1 || true
}
IN_USE=$(
	{ findmnt -rn -o SOURCE | grep '^/dev/'; awk 'NR>1{print $1}' /proc/swaps; } | sort -u |
		while read -r src; do bdf_of_block "$(basename "$(readlink -f "$src")")"; done | sort -u
)
ROOT_BDF=$(bdf_of_block "$(basename "$(readlink -f "$(findmnt -n -o SOURCE /)")")")
echo "root filesystem is on controller $ROOT_BDF; controllers in use: $(echo $IN_USE)"
if [ "$BDF" = "$ROOT_BDF" ] || grep -qx "$BDF" <<< "$IN_USE"; then
	echo "refusing: $BDF backs a mounted filesystem or swap (root disk: $ROOT_BDF)"
	exit 1
fi
for ns in "$DEV"/nvme/nvme*/nvme*n*; do
	[ -e "$ns" ] || continue
	n=$(basename "$ns")
	if ls /sys/class/block/"$n"/holders/* /sys/class/block/"$n"p*/holders/* > /dev/null 2>&1; then
		echo "refusing: $n has holders (LVM, dm, md): $(ls /sys/class/block/"$n"*/holders/)"
		exit 1
	fi
done

GROUP=$(basename "$(readlink "$DEV/iommu_group" 2>/dev/null)" 2>/dev/null || true)
[ -n "$GROUP" ] || { echo "no IOMMU group: enable VT-d / AMD-Vi in firmware and boot with intel_iommu=on"; exit 1; }
for d in /sys/kernel/iommu_groups/"$GROUP"/devices/*; do
	o=$(basename "$d")
	[ "$o" = "$BDF" ] && continue
	drv=$(basename "$(readlink "$d/driver" 2>/dev/null)" 2>/dev/null || true)
	[ -z "$drv" ] || [ "$drv" = vfio-pci ] || { echo "refusing: IOMMU group $GROUP also holds $o (driver $drv)"; exit 1; }
done
for i in /sys/class/iommu/*/intel-iommu/ecap; do
	[ -e "$i" ] && [ $(( (0x$(cat "$i") >> 3) & 1 )) = 1 ] || echo "warning: no interrupt remapping on $(dirname "$(dirname "$i")"); vfio_iommu_type1 refuses the group unless allow_unsafe_interrupts=1"
done

[ "$(id -u)" = 0 ] || { echo "checks passed; run as root to bind"; exit 1; }
modprobe vfio-pci
ORIG=$(basename "$(readlink "$DEV/driver" 2>/dev/null)" 2>/dev/null || echo none)
echo "$ORIG" > "/run/rs-spike-$BDF.driver"
[ -e "$DEV/driver" ] && echo "$BDF" > "$DEV/driver/unbind"
echo vfio-pci > "$DEV/driver_override"
echo "$BDF" > /sys/bus/pci/drivers_probe
echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
echo "bound $BDF ($ORIG -> $(basename "$(readlink "$DEV/driver")")), /dev/vfio/$GROUP, hugepages $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2 MB"
echo "MSI-X vectors: $(lspci -s "$BDF" -vv 2>/dev/null | grep -o 'MSI-X: Enable[+-] Count=[0-9]*' || echo unknown)"
echo "next: sudo ./run-real.sh $BDF   then: sudo ./teardown-root.sh $BDF"
