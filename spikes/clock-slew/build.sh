#!/bin/bash
# Builds the host tools and the guest test into bin/ (clockrate static, so the
# VM guest runs it too), and, with "uml", the User-Mode Linux kernel at
# bin/linux-um.
set -eu
cd "$(dirname "$0")"
. ./env.sh
mkdir -p bin
gcc -O2 -Wall -o bin/adjtimex-read adjtimex_read.c
gcc -O2 -Wall -static -o bin/clockrate clockrate.c
gcc -O2 -Wall -static -o bin/slew-guest slew_guest.c
if [ "${1:-}" = uml ]; then
  make -C vendor/linux -s ARCH=um O="$PWD/work/um" x86_64_defconfig
  make -C vendor/linux -s ARCH=um O="$PWD/work/um" -j"$(nproc)" linux
  cp work/um/linux bin/linux-um
fi
if [ -x bin/linux-um ]; then
  mkdir -p work/root/dev work/root/proc && cp bin/slew-guest work/root/init
fi
