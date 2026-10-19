#!/bin/sh
# For a disposable VM only (run-vm.sh boots one and runs this as root in it):
# steps the wall clock so that a real time daemon slews it, and samples the
# clocks with bin/clockrate. Checks with real chronyd and systemd-timesyncd what
# slew_guest.c reproduces with their adjtimex calls (G8, G9). ptp_kvm gives
# /dev/ptp0, the host's CLOCK_REALTIME, which clockrate prints the guest's
# offset from.
#
#   chrony:    V1 Ubuntu's default sources (NTS pools over the internet), +1 s;
#              V2 the host's clock (ptp_kvm) as the only source, +1 s then -1 s
#   timesyncd: V3 Ubuntu's default server, +0.3 s, after replacing chrony
#
# Expected: chrony slews the 1 s offset at maxslewrate (83,333 ppm) for about
# 12 s, CLOCK_MONOTONIC and CLOCK_BOOTTIME follow, CLOCK_MONOTONIC_RAW does not.
# timesyncd slews 0.3 s through the kernel PLL at up to offset/8 per second
# (constant 1 at its 32 s minimum poll).
set -eu
[ "$(id -u)" = 0 ] || { echo "needs root, in a disposable VM" >&2; exit 2; }
[ "${I_AM_A_DISPOSABLE_VM:-}" = 1 ] || { echo "set I_AM_A_DISPOSABLE_VM=1" >&2; exit 2; }
cd "$(dirname "$0")"
modprobe ptp_kvm
cs=/sys/devices/system/clocksource/clocksource0
echo "# guest $(uname -sr), clocksource $(cat $cs/current_clocksource) (available: $(cat $cs/available_clocksource)), $1 $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "# /dev/ptp0: $(cat /sys/class/ptp/ptp0/clock_name)"
step() {
	echo "## step $1 at $(date -u +%H:%M:%S.%N)"
	date -s "$1" >/dev/null
}
case "$1" in
chrony)
	echo "# $(chronyd --version)"
	# Slew, never step: makestep applies only to offsets over 1000 s.
	sed -i 's/^makestep.*/makestep 1000 1/' /etc/chrony/chrony.conf
	echo "### V1 default sources, +1 s"
	systemctl restart chrony
	chronyc -n waitsync 60 0.05 >/dev/null || true
	chronyc -n sources
	chronyc -n tracking
	step "1 second"
	chronyc -n burst 4/4 >/dev/null
	bin/clockrate 300 1000 /dev/ptp0
	chronyc -n sources
	chronyc -n tracking
	journalctl -u chrony -o short-precise --no-pager --since "-6min" | grep -v NTS-KE
	echo "### V2 the host's clock (ptp_kvm) as the only source, +1 s, then -1 s"
	mkdir -p /root/sources.off && mv /etc/chrony/sources.d/*.sources /root/sources.off/
	echo 'refclock PHC /dev/ptp0 poll 0 refid HOST' > /etc/chrony/conf.d/clock-slew-phc.conf
	systemctl restart chrony
	chronyc -n waitsync 60 0.0001 >/dev/null || true
	sleep 20
	chronyc -n sources
	chronyc -n tracking
	step "1 second"
	bin/clockrate 30 1000 /dev/ptp0
	step "-1 second"
	bin/clockrate 30 1000 /dev/ptp0
	chronyc -n tracking
	journalctl -u chrony -o short-precise --no-pager --since "-2min"
	;;
timesyncd)
	echo "### V3 Ubuntu's default server, +0.3 s"
	dpkg --purge --force-depends chrony >/dev/null 2>&1
	dpkg -i systemd-timesyncd_*.deb >/dev/null
	systemctl --version | head -1
	for _ in $(seq 60); do [ -e /run/systemd/timesync/synchronized ] && break; sleep 1; done
	sleep 10
	timedatectl timesync-status || true
	step "0.3 second"
	systemctl restart systemd-timesyncd
	bin/clockrate 120 1000 /dev/ptp0
	timedatectl timesync-status || true
	journalctl -u systemd-timesyncd -o short-precise --no-pager --since "-3min"
	;;
*)
	echo "usage: $0 chrony|timesyncd" >&2
	exit 2
	;;
esac
