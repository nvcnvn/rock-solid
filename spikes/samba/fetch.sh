#!/bin/sh
# User-local sysroot, no root: Ubuntu 24.04 Samba 4.19 debs unpacked into vendor/root,
# smbprotocol (pure-Python SMB2/3 client) into vendor/py.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/debs vendor/root
(cd vendor/debs && apt-get download samba samba-common samba-common-bin samba-vfs-modules smbclient \
    samba-testsuite tdb-tools winbind python3-samba python3-ldb python3-talloc \
    libuid-wrapper libnss-wrapper libsocket-wrapper libfuse3-dev attr)
for d in vendor/debs/*.deb; do dpkg -x "$d" vendor/root; done
pip3 install -q --target vendor/py --break-system-packages smbprotocol
