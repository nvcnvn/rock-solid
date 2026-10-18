#!/bin/bash
# Fetches pinned sources: the FUSE sources and uapi headers of Linux 6.18.44, the stable release
# the test guest runs (the host's linux-libc-dev header is FUSE 7.39, older than FUSE over
# io_uring), dev.c and dev_uring.c at the mainline commit the README cites, and go-fuse at its
# latest tag for the source review. gofuse/ fetches go-fuse again as a Go module.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/linux/fs/fuse vendor/linux/include/uapi/linux vendor/linux-master/fs/fuse
cd vendor
K=https://raw.githubusercontent.com/gregkh/linux/v6.18.44
for f in fs/fuse/dev.c fs/fuse/dev_uring.c fs/fuse/dev_uring_i.h fs/fuse/fuse_dev_i.h fs/fuse/fuse_i.h \
	fs/fuse/inode.c fs/fuse/dir.c fs/fuse/file.c include/uapi/linux/fuse.h include/uapi/linux/io_uring.h; do
	[ -s linux/$f ] || curl -fsSL -o linux/$f $K/$f
done
M=https://raw.githubusercontent.com/torvalds/linux/32c52ec87d6245ee2d12ed5dd509e40062939b50
for f in fs/fuse/dev.c fs/fuse/dev_uring.c; do
	[ -s linux-master/$f ] || curl -fsSL -o linux-master/$f $M/$f
done
[ -d go-fuse ] || git clone -q -c advice.detachedHead=false --depth 1 --branch v2.12.0 https://github.com/hanwen/go-fuse
