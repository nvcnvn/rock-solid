#!/bin/bash
# Builds both spike programs against the shallow clones in vendor/ (see README.md for fetching them).
# usage: ./build.sh            -> bin/spdk_spike, bin/spdk_spike_nowa, bin/rdma_spike
set -e
cd "$(dirname "$0")"
. ./env.sh
SPDK=$PWD/vendor/spdk
mkdir -p bin work
# SPDK's .pc files hold absolute paths, so query them without the sysroot prefix.
pc() { PKG_CONFIG_SYSROOT_DIR= PKG_CONFIG_PATH=$SPDK/build/lib/pkgconfig pkg-config "$@"; }
MODS="spdk_event_bdev spdk_bdev_malloc spdk_bdev_aio spdk_bdev_uring spdk_init"
CFLAGS="-O2 -g -Wall $(pc --cflags $MODS) -I$SPDK/module/bdev"
SPDK_LIBS="$(pc --libs $MODS spdk_env_dpdk)"
SYS_LIBS="$(pc --libs --static spdk_syslibs)"
# DPDK is linked statically too (-Bstatic) so the binary needs no librte_*.so at runtime.
# SPDK modules and subsystems register themselves from constructors, so the archives must be
# linked whole; spdk_spike_nowa leaves that out to show what breaks.
gcc $CFLAGS -o bin/spdk_spike spdk_spike.c -Wl,-Bstatic,--whole-archive $SPDK_LIBS -Wl,--no-whole-archive,-Bdynamic $SYS_LIBS
gcc $CFLAGS -o bin/spdk_spike_nowa spdk_spike.c -Wl,-Bstatic $SPDK_LIBS -Wl,-Bdynamic $SYS_LIBS
gcc -O2 -g -Wall -o bin/rdma_spike rdma_spike.c -ldl -lpthread
