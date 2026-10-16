#!/bin/bash
# Undoes setup-root.sh: gives the controller back to its original driver (nvme) and frees the
# hugepages.  sudo ./teardown-root.sh <BDF>
set -euo pipefail
BDF=${1:?usage: sudo $0 <pci BDF>}
DEV=/sys/bus/pci/devices/$BDF
ORIG=$(cat "/run/rs-spike-$BDF.driver" 2>/dev/null || echo nvme)
[ -e "$DEV/driver" ] && echo "$BDF" > "$DEV/driver/unbind"
echo > "$DEV/driver_override"
echo "$BDF" > /sys/bus/pci/drivers_probe
echo 0 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
rm -f "/run/rs-spike-$BDF.driver"
echo "$BDF now on $(basename "$(readlink "$DEV/driver" 2>/dev/null)" 2>/dev/null || echo none) (was $ORIG), hugepages $(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)"
