#!/bin/sh
# Needs libfuse 3.14 headers (git clone --branch fuse-3.14.0 https://github.com/libfuse/libfuse) and libfuse3.so.3.
# usage: ./build.sh /path/to/libfuse && python3 run.py
set -e
mkdir -p inc && touch inc/libfuse_config.h
gcc -O2 -Wall -I inc -I "$1/include" -o spikefs spikefs.c /usr/lib/x86_64-linux-gnu/libfuse3.so.3 -lpthread
