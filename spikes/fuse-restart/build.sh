#!/bin/bash
# Builds libfuse 3.18.3 twice (default, and with -Denable-custom-io=true, which 3.18 needs for
# fuse_session_custom_io), then bin/rawfs (raw protocol, v6.17 uapi header), bin/llfs (custom-io
# libfuse) and bin/llfs-default (default libfuse).
set -e
cd "$(dirname "$0")"
. ./env.sh
L=$PWD/vendor/libfuse
for b in default cio; do
	opt=; [ $b = cio ] && opt=-Denable-custom-io=true
	[ -d $L/build-$b ] || meson setup $L/build-$b $L -Dexamples=false -Dutils=false -Dtests=false -Ddisable-mtab=true $opt >/dev/null
	ninja -C $L/build-$b >/dev/null
done
mkdir -p bin work
gcc -O2 -g -Wall -Wextra -I vendor/linux/include/uapi -o bin/rawfs rawfs.c
for b in default cio; do
	out=bin/llfs; [ $b = default ] && out=bin/llfs-default
	gcc -O2 -g -Wall -I $L/include -I $L/build-$b -o $out llfs.c -L $L/build-$b/lib -Wl,-rpath,$L/build-$b/lib -lfuse3 -lpthread
done
