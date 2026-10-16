#!/bin/bash
# Shallow-clones SPDK v26.09 with libvfio-user and builds it with --with-vfio-user and the apps
# (nvmf_tgt, spdk_nvme_perf), unpacks QEMU from Ubuntu .debs into vendor/sysroot, and extracts the
# host kernel's image (from its .deb; /boot/vmlinuz is root-only) for the QEMU guest. No root, no apt install.
# Reuses the dev-header sysroot of ../bypass-drivers (run its fetch.sh first); never writes to it.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/debs vendor/sysroot
cd vendor
[ -d ../../bypass-drivers/vendor/sysroot ] || { echo "run ../bypass-drivers/fetch.sh first"; exit 1; }
[ -d spdk ] || git clone -q -c advice.detachedHead=false --depth 1 --branch v26.09 --recurse-submodules \
	--shallow-submodules https://github.com/spdk/spdk

KVER=$(uname -r)
# QEMU's other runtime libraries (libaio, liburing, librdmacm) are in the bypass-drivers sysroot.
(cd debs && apt-get download qemu-system-x86 qemu-system-common qemu-system-data seabios ipxe-qemu \
	libfdt1 libpmem1 libslirp0 libndctl6 libdaxctl1 libcmocka-dev libcmocka0 "linux-image-unsigned-$KVER")
for d in debs/*.deb; do dpkg -x "$d" sysroot; done
# libvfio-user's meson wants cmocka from pkg-config; the shared PKG_CONFIG_SYSROOT_DIR points at the
# other sysroot, so give it a .pc that relies on C_INCLUDE_PATH and LIBRARY_PATH from env.sh.
mkdir -p pkgconfig
printf 'Name: cmocka\nDescription: cmocka\nVersion: 1.1.7\nCflags:\nLibs: -lcmocka\n' > pkgconfig/cmocka.pc

. ../env.sh
cd spdk
./configure --disable-tests --disable-unit-tests --without-crypto --without-isal-crypto --without-vhost \
	--without-virtio --without-nvme-cuse --without-ocf --without-xnvme --without-fsdev --without-usdt --without-idxd \
	--without-rdma --without-uring --with-vfio-user
make -j"$(nproc)"
