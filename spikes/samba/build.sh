#!/bin/sh
# Builds passfs against the libfuse3 headers that fetch.sh unpacked; links the system libfuse3.so.3.
set -e
cd "$(dirname "$0")"
R=vendor/root
gcc -O2 -Wall -D_FILE_OFFSET_BITS=64 -I $R/usr/include/fuse3 -o passfs passfs.c /usr/lib/x86_64-linux-gnu/libfuse3.so.3 -lpthread
