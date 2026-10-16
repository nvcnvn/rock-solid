#!/bin/bash
# Fetches pinned sources (no root, no apt install): libfuse at a tag for the low-level API and its
# NOTIFY_RESEND support, and the Linux v6.17 FUSE sources that the README cites (fs/fuse/*.c and
# the v6.17 uapi header, which the host's linux-libc-dev 6.8 header predates), and the host's
# fusermount3 3.14 source for the auto_unmount check.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/linux/fs/fuse vendor/linux/include/uapi/linux
cd vendor
[ -d libfuse ] || git clone -q -c advice.detachedHead=false --depth 1 --branch fuse-3.18.3 https://github.com/libfuse/libfuse
K=https://raw.githubusercontent.com/torvalds/linux/v6.17
for f in fs/fuse/dev.c fs/fuse/inode.c fs/fuse/fuse_i.h fs/fuse/fuse_dev_i.h fs/fuse/dir.c fs/fuse/file.c fs/fuse/control.c include/uapi/linux/fuse.h; do
	[ -s linux/$f ] || curl -fsSL -o linux/$f $K/$f
done
mkdir -p fusermount-3.14
[ -s fusermount-3.14/fusermount.c ] || curl -fsSL -o fusermount-3.14/fusermount.c https://raw.githubusercontent.com/libfuse/libfuse/fuse-3.14.0/util/fusermount.c
[ -d pydeps ] || pip3 install -q --target pydeps meson ninja
