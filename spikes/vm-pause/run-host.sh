#!/bin/sh
# Unprivileged and read-only: does this host, itself a Firecracker guest, expose
# a VMClock device, and can its userspace read it?
set -eu
echo "# $(uname -sr) $(uname -v) clocksource=$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource) $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "ACPI device HIDs: $(cat /sys/bus/acpi/devices/*/hid 2>/dev/null | sort | uniq | tr '\n' ' ')"
for d in /sys/bus/acpi/devices/AMZNC10C:* /sys/bus/acpi/devices/VMCLOCK:*; do
  [ -e "$d" ] || continue
  echo "$d: path=$(cat "$d/path") status=$(cat "$d/status" 2>/dev/null || echo -) driver=$(basename "$(readlink "$d/physical_node/driver" 2>/dev/null || echo none)")"
done
ls -l /dev/vmclock* /dev/ptp* 2>&1 | sed 's/^/dev: /'
if [ -r /proc/config.gz ]; then
  zcat /proc/config.gz | grep -E '^(# )?CONFIG_(PTP_1588_CLOCK_VMCLOCK|PTP_1588_CLOCK_KVM|DEVMEM|VMGENID)[ =]' | sed 's/^/config: /'
fi
