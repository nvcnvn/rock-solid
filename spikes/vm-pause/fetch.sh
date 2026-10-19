#!/bin/sh
# Fetches, from the Ubuntu 26.04 (resolute) release pocket, QEMU 10.2.1 (the
# first release with the vmclock device is 10.0.0), its libraries and glibc
# 2.43, the Linux 7.0.0-14 generic kernel and modules (ptp_vmclock), and a
# static busybox, and unpacks them into vendor/sysroot. No root: nothing is
# installed on the host. Also fetches the Firecracker v1.17.0 static release
# binary into vendor/fc, Linux v7.0's extract-vmlinux (Firecracker boots only an
# uncompressed vmlinux), and the sources the README cites into vendor/src, at
# the tags it names: QEMU v10.2.1, Linux v7.0, Firecracker v1.17.0.
set -eu
cd "$(dirname "$0")"
mkdir -p vendor/debs vendor/sysroot vendor/src
mirror=http://archive.ubuntu.com/ubuntu
for f in \
  pool/main/b/busybox/busybox-static_1.37.0-7ubuntu1_amd64.deb \
  pool/main/liba/libaio/libaio1t64_0.3.113-8build1_amd64.deb \
  pool/main/g/gcc-16/libatomic1_16-20260322-1ubuntu1_amd64.deb \
  pool/main/u/util-linux/libblkid1_2.41.3-3ubuntu2_amd64.deb \
  pool/main/libb/libbpf/libbpf1_1.6.3-1ubuntu1_amd64.deb \
  pool/main/g/glibc/libc-gconv-modules-extra_2.43-2ubuntu2_amd64.deb \
  pool/main/g/glibc/libc6_2.43-2ubuntu2_amd64.deb \
  pool/main/n/ndctl/libdaxctl1_81-1ubuntu1_amd64.deb \
  pool/main/e/elfutils/libelf1t64_0.194-4_amd64.deb \
  pool/main/d/device-tree-compiler/libfdt1_1.7.2-2ubuntu1_amd64.deb \
  pool/main/libf/libffi/libffi8_3.5.2-4_amd64.deb \
  pool/main/f/fuse3/libfuse3-4_3.18.2-1_amd64.deb \
  pool/main/g/gcc-16/libgcc-s1_16-20260322-1ubuntu1_amd64.deb \
  pool/main/g/glib2.0/libglib2.0-0t64_2.88.0-1_amd64.deb \
  pool/main/g/gmp/libgmp10_6.3.0+dfsg-5ubuntu2_amd64.deb \
  pool/main/g/gnutls28/libgnutls30t64_3.8.12-2ubuntu1_amd64.deb \
  pool/main/n/nettle/libhogweed6t64_3.10.2-1_amd64.deb \
  pool/main/r/rdma-core/libibverbs1_61.0-2ubuntu3_amd64.deb \
  pool/main/libi/libidn2/libidn2-0_2.3.8-4build1_amd64.deb \
  pool/main/libj/libjpeg-turbo/libjpeg-turbo8_2.1.5-4ubuntu4_amd64.deb \
  pool/main/libj/libjpeg8-empty/libjpeg8_8c-2ubuntu12_amd64.deb \
  pool/main/k/kmod/libkmod2_34.2-2ubuntu2_amd64.deb \
  pool/main/u/util-linux/libmount1_2.41.3-3ubuntu2_amd64.deb \
  pool/main/n/ndctl/libndctl6_81-1ubuntu1_amd64.deb \
  pool/main/n/nettle/libnettle8t64_3.10.2-1_amd64.deb \
  pool/main/libn/libnl3/libnl-3-200_3.12.0-2_amd64.deb \
  pool/main/libn/libnl3/libnl-route-3-200_3.12.0-2_amd64.deb \
  pool/main/n/numactl/libnuma1_2.0.19-1build1_amd64.deb \
  pool/main/p/p11-kit/libp11-kit0_0.26.2-2_amd64.deb \
  pool/main/p/pcre2/libpcre2-8-0_10.46-1build1_amd64.deb \
  pool/main/p/pixman/libpixman-1-0_0.46.4-1_amd64.deb \
  pool/main/p/pmdk/libpmem1_1.13.1-1.1ubuntu4_amd64.deb \
  pool/main/libp/libpng1.6/libpng16-16t64_1.6.57-1_amd64.deb \
  pool/main/r/rdma-core/librdmacm1t64_61.0-2ubuntu3_amd64.deb \
  pool/main/c/cyrus-sasl2/libsasl2-2_2.1.28+dfsg1-9ubuntu3_amd64.deb \
  pool/main/libs/libseccomp/libseccomp2_2.6.0-2ubuntu5_amd64.deb \
  pool/main/libs/libselinux/libselinux1_3.9-4build1_amd64.deb \
  pool/main/libs/libslirp/libslirp0_4.9.1-1ubuntu1_amd64.deb \
  pool/main/o/openssl/libssl3t64_3.5.5-1ubuntu3_amd64.deb \
  pool/main/libt/libtasn1-6/libtasn1-6_4.21.0-2_amd64.deb \
  pool/main/s/systemd/libudev1_259.5-0ubuntu3_amd64.deb \
  pool/main/libu/libunistring/libunistring5_1.3-2build1_amd64.deb \
  pool/main/libu/liburing/liburing2_2.14-1_amd64.deb \
  pool/main/u/util-linux/libuuid1_2.41.3-3ubuntu2_amd64.deb \
  pool/main/libz/libzstd/libzstd1_1.5.7+dfsg-3_amd64.deb \
  pool/main/l/linux-signed/linux-image-7.0.0-14-generic_7.0.0-14.14_amd64.deb \
  pool/main/l/linux/linux-modules-7.0.0-14-generic_7.0.0-14.14_amd64.deb \
  pool/main/o/openssl/openssl-provider-legacy_3.5.5-1ubuntu3_amd64.deb \
  pool/main/q/qemu/qemu-system-data_10.2.1+ds-1ubuntu3_all.deb \
  pool/main/q/qemu/qemu-system-x86_10.2.1+ds-1ubuntu3_amd64.deb \
  pool/main/q/qemu/qemu-utils_10.2.1+ds-1ubuntu3_amd64.deb \
  pool/main/s/seabios/seabios_1.17.0-1ubuntu1_all.deb \
  pool/main/z/zlib/zlib1g_1.3.dfsg+really1.3.1-1ubuntu3_amd64.deb \
  ; do
  deb=vendor/debs/$(basename "$f")
  [ -s "$deb" ] || { curl -sSfL --retry 5 -o "$deb.part" "$mirror/$f" && mv "$deb.part" "$deb"; }
  dpkg -x "$deb" vendor/sysroot
done
src() { [ -s "vendor/src/$1" ] || curl -sSfL -o "vendor/src/$1" "$2"; }
src linux-ptp_vmclock.c https://raw.githubusercontent.com/torvalds/linux/v7.0/drivers/ptp/ptp_vmclock.c
src vmclock-abi.h https://raw.githubusercontent.com/torvalds/linux/v7.0/include/uapi/linux/vmclock-abi.h
src linux-kvmclock.c https://raw.githubusercontent.com/torvalds/linux/v7.0/arch/x86/kernel/kvmclock.c
src linux-ptp_kvm_x86.c https://raw.githubusercontent.com/torvalds/linux/v7.0/drivers/ptp/ptp_kvm_x86.c
src linux-kvm-x86.c https://raw.githubusercontent.com/torvalds/linux/v7.0/arch/x86/kvm/x86.c
src extract-vmlinux https://raw.githubusercontent.com/torvalds/linux/v7.0/scripts/extract-vmlinux
fc=firecracker-v1.17.0-x86_64
mkdir -p vendor/fc
if [ ! -x vendor/fc/firecracker ]; then
  rel=https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0
  curl -sSfL --retry 5 -o "vendor/fc/$fc.tgz" "$rel/$fc.tgz"
  curl -sSfL --retry 5 -o "vendor/fc/$fc.tgz.sha256.txt" "$rel/$fc.tgz.sha256.txt"
  (cd vendor/fc && sha256sum -c "$fc.tgz.sha256.txt")
  tar -xzf "vendor/fc/$fc.tgz" -C vendor/fc --strip-components=1 "release-v1.17.0-x86_64/$fc"
  mv "vendor/fc/$fc" vendor/fc/firecracker
fi
if [ ! -d vendor/src/qemu ]; then
  git -c advice.detachedHead=false clone -q --depth 1 --branch v10.2.1 --filter=blob:none \
    --sparse https://github.com/qemu/qemu.git vendor/src/qemu
  git -C vendor/src/qemu sparse-checkout set hw/acpi hw/i386 hw/net hw/rtc system net \
    migration target/i386
fi
[ -d vendor/src/firecracker ] || git -c advice.detachedHead=false clone -q --depth 1 \
  --branch v1.17.0 https://github.com/firecracker-microvm/firecracker.git vendor/src/firecracker
