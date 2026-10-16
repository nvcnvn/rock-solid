#!/bin/bash
# I3: boots the host kernel in a KVM guest with an emulated NVMe controller and runs guest-init.sh
# as root inside it. No host root: needs only read/write access to /dev/kvm.
# usage: ./run-qemu.sh iommu [tag]    q35 + intel-iommu (intremap, kernel-irqchip=split) -> results-qemu-iommu[-tag].txt
#        ./run-qemu.sh noiommu [tag]  no vIOMMU, vfio enable_unsafe_noiommu_mode -> results-qemu-noiommu[-tag].txt
set -e
cd "$(dirname "$0")"
. ./env.sh
MODE=${1:?iommu or noiommu}
KVER=$(uname -r)
OUT=results-qemu-$MODE${2:+-$2}.txt
[ -f work/initrd.cpio.gz ] || ./mkinitrd.sh
[ -f work/nvme.img ] || truncate -s 1G work/nvme.img
if [ "$MODE" = iommu ]; then
	MACHINE="q35,accel=kvm,kernel-irqchip=split"
	IOMMU="-device intel-iommu,intremap=on"
else
	MACHINE="q35,accel=kvm"
	IOMMU=
fi
{
	echo "### host $KVER, $(lscpu | grep 'Model name' | sed 's/  */ /g'), $(qemu-system-x86_64 --version | head -1), SPDK $(git -C "$SPDK" describe --tags) / DPDK $(cat "$SPDK/dpdk/VERSION"), $(date -I)"
	echo "### guest: same kernel, -machine $MACHINE $IOMMU, -smp 4 -m 2G, -device nvme,ioeventfd=on on a 1 GB raw file (cache=none, aio=native)"
} > "$OUT"
timeout 600 qemu-system-x86_64 -nodefaults -no-reboot -display none -monitor none -serial stdio \
	-L vendor/sysroot/usr/share/seabios -L vendor/sysroot/usr/share/qemu \
	-machine $MACHINE -cpu host -smp 4 -m 2G $IOMMU \
	-drive file=work/nvme.img,format=raw,if=none,id=nvm,cache=none,aio=native \
	-device nvme,serial=rs-spike,drive=nvm,ioeventfd=on \
	-kernel vendor/sysroot/boot/vmlinuz-$KVER -initrd work/initrd.cpio.gz \
	-append "console=ttyS0 quiet loglevel=3 rs.mode=$MODE" 2>&1 | tr -d '\r' | tee -a "$OUT"
