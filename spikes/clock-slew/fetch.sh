#!/bin/sh
# Fetches the Linux source (built as User-Mode Linux, a disposable kernel whose
# clock we may change) and flex, which the kernel build needs, into vendor/.
set -eu
cd "$(dirname "$0")"
mkdir -p vendor/sysroot
[ -d vendor/linux ] || git clone -q --depth 1 --branch v6.18 https://github.com/torvalds/linux.git vendor/linux
if [ ! -x vendor/sysroot/usr/bin/flex ]; then
  deb=flex_2.6.4-8.2build1_amd64.deb
  curl -sSfL -o vendor/$deb http://archive.ubuntu.com/ubuntu/pool/main/f/flex/$deb
  dpkg -x vendor/$deb vendor/sysroot
fi
