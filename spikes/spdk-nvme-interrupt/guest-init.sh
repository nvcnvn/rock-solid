#!/bin/busybox sh
# /init of the QEMU guest (see mkinitrd.sh). Runs as root inside the guest only.
# Kernel command line: rs.mode=iommu (vIOMMU present) or rs.mode=noiommu (vfio no-IOMMU mode).
/bin/busybox --install -s /bin
export PATH=/bin
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
mkdir -p /dev/hugepages /tmp /var/tmp /var/run
mount -t hugetlbfs nodev /dev/hugepages
mount -t tmpfs tmp /tmp
MODE=$(sed -n 's/.*rs\.mode=\([a-z]*\).*/\1/p' /proc/cmdline)
echo "### guest: $(uname -r), rs.mode=$MODE, $(nproc) vCPUs, $(sed -n 's/MemTotal: *//p' /proc/meminfo)"
dmesg | grep -E "DMAR: (IOMMU enabled|Intel\(R\) Virtualization|Host address width)|Enabled IRQ remapping|DMAR-IR: Enabled" | sed 's/^\[[ 0-9.]*\] /  dmesg: /'

for m in irqbypass iommufd vfio vfio_iommu_type1 vfio-pci-core vfio-pci uio uio_pci_generic; do
	if [ "$m" = vfio ] && [ "$MODE" = noiommu ]; then
		insmod /lib/modules/$m.ko enable_unsafe_noiommu_mode=1
	else
		insmod /lib/modules/$m.ko
	fi
done

BDF=
for d in /sys/bus/pci/devices/*; do
	[ "$(cat $d/class)" = 0x010802 ] && BDF=$(basename $d)
done
echo "  NVMe controller: $BDF, driver: $(basename "$(readlink /sys/bus/pci/devices/$BDF/driver)" 2>/dev/null)"

bind() {
	DEV=/sys/bus/pci/devices/$BDF
	[ -e $DEV/driver ] && echo $BDF > $DEV/driver/unbind
	echo $1 > $DEV/driver_override
	echo $BDF > /sys/bus/pci/drivers_probe
	echo "  bound $BDF to $(basename "$(readlink $DEV/driver)"), iommu_group $(basename "$(readlink $DEV/iommu_group)"), /dev/vfio: $(ls /dev/vfio | tr '\n' ' ')"
}

TR="trtype:PCIe traddr:$BDF"
run() {
	echo "### nvme_intr $*"
	/bin/nvme_intr "$@" 2>&1 | grep -v "SO_RCVBUF\|ns_data_alloc_mode"
	echo
}
perf() {
	echo "### spdk_nvme_perf $*"
	time /bin/spdk_nvme_perf -r "$TR" -c 0x1 -o 4096 -t 5 -s 256 "$@" 2>&1 \
		| grep -v -E "^\s*$|SO_RCVBUF|ns_data_alloc_mode|^Initializ|^Attach|^Associat|^Starting|^=|^Latency\(us\)|^Device Information|^\s+:"
	echo
}

echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
echo "  hugepages: $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages) x 2 MB"
bind vfio-pci
echo

if [ "$MODE" = iommu ]; then
	run --n 1 "$TR"
	run --base 0x1000000000 "$TR"
	run --base 0x1000000000 --interrupt "$TR"
	run --base 0x1000000000 --bdev "$TR"
	run --base 0x1000000000 --bdev --interrupt "$TR"
	run --base 0x1000000000 --interrupt --no-huge "$TR"
	run --base 0x1000000000 --interrupt --no-huge --mem 128 "$TR"
	perf -q 1 -w randread
	perf -q 1 -w randread -E
	perf -q 32 -w randread
	perf -q 32 -w randread -E
	bind uio_pci_generic
	run --base 0x1000000000 --interrupt --n 1 "$TR"
else
	run "$TR"
	run --interrupt "$TR"
	run --interrupt --no-huge --mem 128 "$TR"
	bind uio_pci_generic
	run --interrupt "$TR"
fi
echo "### guest done"
poweroff -f
