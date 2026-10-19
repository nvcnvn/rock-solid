#!/bin/bash
# Builds bin/hofs against the Linux v6.18 uapi headers that fetch.sh downloads.
set -e
cd "$(dirname "$0")"
mkdir -p bin work
gcc -O2 -g -Wall -Wextra -I vendor/linux/include/uapi -o bin/hofs hofs.c
