#!/bin/bash
# Shallow-clones SPDK and rdma-core, builds a user-local sysroot of dev headers from Ubuntu .debs
# (no root, no apt install), and builds SPDK. Tested on Ubuntu 24.04, Linux 6.17.
set -e
cd "$(dirname "$0")"
mkdir -p vendor/debs
cd vendor
[ -d rdma-core ] || git clone -q -c advice.detachedHead=false --depth 1 --branch v50.0 https://github.com/linux-rdma/rdma-core
[ -d spdk ] || git clone -q -c advice.detachedHead=false --depth 1 --branch v26.09 --recurse-submodules --shallow-submodules https://github.com/spdk/spdk
S=$PWD/sysroot
(cd debs && apt-get download libaio-dev libaio1t64 libssl-dev libjson-c-dev uuid-dev libnuma-dev libkeyutils-dev \
	liburing-dev liburing2 libibverbs-dev librdmacm-dev librdmacm1t64 libncurses-dev nasm libnl-3-dev libnl-route-3-dev \
	pkgconf pkgconf-bin libpkgconf3 autoconf automake libtool m4 autotools-dev)
for d in debs/*.deb; do dpkg -x "$d" sysroot; done
# Point dev .so links at the host's runtime libraries.
for l in $(find sysroot -xtype l); do b=$(basename "$(readlink "$l")"); [ -e /usr/lib/x86_64-linux-gnu/$b ] && ln -sf /usr/lib/x86_64-linux-gnu/$b "$l"; done
# Relocate autotools (ISA-L's build needs autoreconf) from /usr/share to the sysroot.
for f in sysroot/usr/bin/{autoconf,autoheader,autom4te,autoreconf,autoscan,autoupdate,ifnames,aclocal-1.16,automake-1.16,libtoolize} sysroot/usr/share/autoconf/autom4te.cfg; do
	sed -i "s#\([^/a-z]\)/usr/share/\(autoconf\|automake\|aclocal\|libtool\)#\1$S/usr/share/\2#g; s#/usr/bin/m4#$S/usr/bin/m4#g; s#'/usr/bin/\(autom4te\|autoconf\|autoheader\|aclocal\|automake\|libtoolize\)#'$S/usr/bin/\1#g" "$f"
done
sed -i "s#'/usr/share/#'$S/usr/share/#g" sysroot/usr/share/automake-1.16/Automake/Config.pm
ln -sf aclocal-1.16 sysroot/usr/bin/aclocal
ln -sf automake-1.16 sysroot/usr/bin/automake
pip3 install -q --target pydeps meson ninja pyelftools tabulate
. ../env.sh
cd spdk
./configure --disable-tests --disable-unit-tests --disable-apps --without-crypto --without-isal-crypto --without-vhost \
	--without-virtio --without-nvme-cuse --without-ocf --without-xnvme --without-fsdev --without-usdt --without-idxd \
	--without-rdma --without-vfio-user --with-uring
make -j"$(nproc)"
