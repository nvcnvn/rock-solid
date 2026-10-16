#!/bin/bash
# Packs the QEMU guest's initramfs: static busybox, the host kernel's vfio/uio modules, and the
# spike binaries with their shared libraries. Output: work/initrd.cpio.gz. No root needed.
set -e
cd "$(dirname "$0")"
. ./env.sh
KVER=$(uname -r)
R=work/initrd-root
mkdir -p $R/bin $R/lib/modules $R/lib64 $R/lib/x86_64-linux-gnu $R/proc $R/sys $R/dev
cp /usr/bin/busybox $R/bin/busybox
cp bin/nvme_intr $R/bin/
cp "$SPDK/build/bin/spdk_nvme_perf" $R/bin/
for l in $(ldd bin/nvme_intr "$SPDK/build/bin/spdk_nvme_perf" | awk '/=>/{print $3}' | sort -u); do
	cp -L "$l" $R/lib/x86_64-linux-gnu/
done
cp -L /lib64/ld-linux-x86-64.so.2 $R/lib64/
# glibc dlopens libgcc_s for pthread_cancel, which spdk_nvme_perf calls on exit.
cp -L /lib/x86_64-linux-gnu/libgcc_s.so.1 $R/lib/x86_64-linux-gnu/
# SPDK initialises OpenSSL at startup and fails without its config file.
mkdir -p $R/usr/lib/ssl
cp -L /usr/lib/ssl/openssl.cnf $R/usr/lib/ssl/
for m in irqbypass iommufd vfio vfio_iommu_type1 vfio-pci-core vfio-pci uio uio_pci_generic; do
	zstd -q -d -f "$(modinfo -k "$KVER" -n $m)" -o $R/lib/modules/$m.ko
done
cp guest-init.sh $R/init
chmod +x $R/init
(cd $R && find . | cpio -o -H newc --quiet | gzip -1) > work/initrd.cpio.gz
echo "work/initrd.cpio.gz: $(du -h work/initrd.cpio.gz | cut -f1), staged in $R"
