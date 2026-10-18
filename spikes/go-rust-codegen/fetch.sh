#!/bin/bash
# Unpacks perf (Ubuntu 24.04's linux-tools, built for 6.8 but usable on newer kernels with software
# events) into vendor/sysroot and gccgo 14 into vendor/gccgo. No root, no apt install.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/debs vendor/gccgo-debs
if [ ! -x vendor/sysroot/usr/lib/linux-tools-6.8.0-147/perf ]; then
	(cd vendor/debs && apt-get download linux-tools-6.8.0-147 libtraceevent1 libnl-genl-3-200 libnl-3-200 \
		libslang2 libunwind8 libpython3.12t64 libdw1t64 libnuma1 libpci3)
	for d in vendor/debs/*.deb; do dpkg -x "$d" vendor/sysroot; done
fi
if [ ! -x vendor/gccgo/usr/bin/gccgo-14 ]; then
	(cd vendor/gccgo-debs && apt-get download gccgo-14 gccgo-14-x86-64-linux-gnu libgo-14-dev libgo23 gcc-14 \
		gcc-14-x86-64-linux-gnu cpp-14 cpp-14-x86-64-linux-gnu libgcc-14-dev gcc-14-base libisl23 libmpc3 \
		libmpfr6 libasan8 libtsan2 libhwasan0 liblsan0 libubsan1 libquadmath0 libitm1 libatomic1 libgomp1 libcc1-0)
	for d in vendor/gccgo-debs/*.deb; do dpkg -x "$d" vendor/gccgo; done
fi
