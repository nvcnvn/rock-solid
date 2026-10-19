#!/bin/sh
# Boots the User-Mode Linux kernel with slew-guest as init. Every adjtimex call
# runs inside the UML kernel: UML's host side reads CLOCK_MONOTONIC and never
# calls adjtimex, clock_adjtime or clock_settime (arch/um/os-Linux/time.c), so
# the host's clock is untouched; the host's NTP state is printed before and
# after to show it. Needs no root, only a writable /dev/shm.
set -eu
cd "$(dirname "$0")"
mkdir -p work/root/dev work/root/proc
cp bin/slew-guest work/root/init
echo "# host $(uname -sr) UML $(strings bin/linux-um | grep -m1 -o 'Linux version [^ ]*') $(date -u +%Y-%m-%dT%H:%M:%SZ)"
bin/adjtimex-read | sed 's/^/host before: /'
timeout 600 bin/linux-um mem=128M rootfstype=hostfs rootflags="$PWD/work/root" rw init=/init \
  con0=fd:0,fd:1 con=null quiet </dev/null 2>&1 | tr -d '\r'
bin/adjtimex-read | sed 's/^/host after: /'
