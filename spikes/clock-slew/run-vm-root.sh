#!/bin/sh
# NOT RUN. For a disposable VM only: it steps the wall clock to make a real time
# daemon slew it. Needs root. Checks with real chronyd and systemd-timesyncd what
# slew_guest.c reproduces with their adjtimex calls (G8, G9).
#
#   chrony:    ./run-vm-root.sh chrony    > results-vm-chrony.txt
#   timesyncd: ./run-vm-root.sh timesyncd > results-vm-timesyncd.txt
#
# Expected: chrony slews the 1 s offset at maxslewrate (83,333 ppm) for about
# 12 s, CLOCK_MONOTONIC and CLOCK_BOOTTIME follow, CLOCK_MONOTONIC_RAW does not.
# timesyncd slews 0.3 s through the kernel PLL at up to offset/8 per second
# (constant 1 at its 32 s minimum poll).
set -eu
[ "$(id -u)" = 0 ] || { echo "needs root, in a disposable VM" >&2; exit 2; }
[ "${I_AM_A_DISPOSABLE_VM:-}" = 1 ] || { echo "set I_AM_A_DISPOSABLE_VM=1" >&2; exit 2; }
cd "$(dirname "$0")"
echo "# $(uname -sr) $1 $(date -u +%Y-%m-%dT%H:%M:%SZ)"
case "$1" in
chrony)
	chronyc -n tracking
	# Make chronyd slew, never step (Debian and Ubuntu path).
	grep -q '^makestep' /etc/chrony/chrony.conf && sed -i 's/^makestep.*/makestep 1000 1/' /etc/chrony/chrony.conf
	systemctl restart chrony && sleep 30
	date -s "1 second" >/dev/null
	chronyc -n burst 4/4 >/dev/null
	bin/clockrate 40 1000
	chronyc -n tracking
	;;
timesyncd)
	timedatectl timesync-status || true
	date -s "0.3 second" >/dev/null
	systemctl restart systemd-timesyncd
	bin/clockrate 60 1000
	timedatectl timesync-status || true
	;;
*)
	echo "usage: $0 chrony|timesyncd" >&2
	exit 2
	;;
esac
