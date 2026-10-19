#!/bin/sh
# Fetches the Linux source (built as User-Mode Linux, a disposable kernel whose
# clock we may change) and flex, which the kernel build needs, into vendor/.
# With "vm": QEMU and the libraries it needs beyond the host's, from the host's
# Ubuntu release (apt-get download, nothing installed), and the Ubuntu 26.04
# minimal cloud image (chrony 4.8, systemd 259.5), whose clock the real daemons
# step and slew, with the systemd-timesyncd .deb that matches its systemd.
set -eu
cd "$(dirname "$0")"
mkdir -p vendor/sysroot
if [ "${1:-}" = vm ]; then
  mkdir -p vendor/debs
  (cd vendor/debs && apt-get download -q qemu-system-x86 qemu-system-common qemu-system-data \
    seabios libfdt1 libpmem1 libslirp0 libndctl6 libdaxctl1 libaio1t64 liburing2 librdmacm1t64 \
    libibverbs1 libnl-route-3-200)
  for d in vendor/debs/*.deb; do dpkg -x "$d" vendor/sysroot; done
  img=ubuntu-26.04-minimal-cloudimg-amd64.img
  base=https://cloud-images.ubuntu.com/minimal/releases/resolute/release-20261017
  [ -s vendor/$img ] || curl -sSfL -o vendor/$img $base/$img
  curl -sSfL $base/SHA256SUMS | grep " \*$img\$" | (cd vendor && sha256sum -c -)
  deb=systemd-timesyncd_259.5-0ubuntu3.4_amd64.deb
  [ -s vendor/$deb ] || curl -sSfL -o vendor/$deb http://archive.ubuntu.com/ubuntu/pool/main/s/systemd/$deb
  exit
fi
[ -d vendor/linux ] || git clone -q --depth 1 --branch v6.18 https://github.com/torvalds/linux.git vendor/linux
if [ ! -x vendor/sysroot/usr/bin/flex ]; then
  deb=flex_2.6.4-8.2build1_amd64.deb
  curl -sSfL -o vendor/$deb http://archive.ubuntu.com/ubuntu/pool/main/f/flex/$deb
  dpkg -x vendor/$deb vendor/sysroot
fi
