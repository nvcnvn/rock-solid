#!/bin/sh
# Unprivileged, read-only measurements on this host: the kernel's NTP state and
# the rates of its clocks against CLOCK_MONOTONIC_RAW. Changes nothing.
set -eu
cd "$(dirname "$0")"
echo "# $(uname -sr) $(uname -v) clocksource=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "# time daemons running: $(ps -e -o comm= | grep -E 'chrony|ntpd|timesync|ptp4l|phc2sys' | tr '\n' ' ' || true)"
grep -m1 'model name' /proc/cpuinfo | sed 's/^/# /'
bin/adjtimex-read
bin/clockrate 60 1000
bin/clockrate 10 100
