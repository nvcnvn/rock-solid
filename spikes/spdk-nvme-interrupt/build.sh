#!/bin/bash
# Builds bin/nvme_intr against vendor/spdk (see fetch.sh), statically linked like ../bypass-drivers.
set -e
cd "$(dirname "$0")"
. ./env.sh
mkdir -p bin work
pc() { PKG_CONFIG_SYSROOT_DIR= PKG_CONFIG_PATH=$SPDK/build/lib/pkgconfig pkg-config "$@"; }
MODS="spdk_event_bdev spdk_bdev_nvme spdk_bdev_malloc spdk_init spdk_nvme spdk_vfio_user"
CFLAGS="-O2 -g -Wall $(pc --cflags $MODS)"
SPDK_LIBS="$(pc --libs $MODS spdk_env_dpdk)"
SYS_LIBS="-L$SPDK/build/libvfio-user/usr/local/lib $(pc --libs --static spdk_syslibs)"
gcc $CFLAGS -o bin/nvme_intr nvme_intr.c -Wl,-Bstatic,--whole-archive $SPDK_LIBS \
	-Wl,--no-whole-archive,-Bdynamic $SYS_LIBS
