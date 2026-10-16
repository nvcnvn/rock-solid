#!/bin/bash
# Shallow-clones and builds fio, and unpacks bpftrace and nvme-cli from Ubuntu .debs into a
# user-local sysroot (no root, no apt install). Tested on Ubuntu 24.04, Linux 6.17.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/debs
cd vendor
[ -d fio ] || git clone -q -c advice.detachedHead=false --depth 1 --branch fio-3.36 https://github.com/axboe/fio
(cd fio && [ -x fio ] || { ./configure --disable-native >/dev/null && make -j"$(nproc)" >/dev/null; })
(cd debs && apt-get download bpftrace libbpfcc libclang1-18 libllvm18 nvme-cli)
for d in debs/*.deb; do dpkg -x "$d" sysroot; done
