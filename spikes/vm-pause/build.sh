#!/bin/bash
# Builds bin/vmpause-guest (static, the guest's init; also the host bench),
# bin/initrd.cpio: the init plus ptp_vmclock.ko.zst and ptp_kvm.ko.zst, which
# the init loads itself, so the guest needs no shell or modprobe; and
# bin/vmlinux, the kernel decompressed for Firecracker.
set -eu
cd "$(dirname "$0")"
. ./env.sh
mkdir -p bin work/initrd/lib/modules
gcc -O2 -Wall -static -o bin/vmpause-guest vmpause_guest.c
cp bin/vmpause-guest work/initrd/init
for m in ptp_vmclock ptp_kvm; do
  cp "$SYSROOT/usr/lib/modules/$KVER/kernel/drivers/ptp/$m.ko.zst" work/initrd/lib/modules/
done
(cd work/initrd && find . | LC_ALL=C sort | "$SYSROOT/usr/bin/busybox" cpio -o -H newc) > bin/initrd.cpio 2>/dev/null
cp "$SYSROOT/boot/vmlinuz-$KVER" bin/vmlinuz
[ -s bin/vmlinux ] || sh vendor/src/extract-vmlinux bin/vmlinuz > bin/vmlinux
ls -l bin
