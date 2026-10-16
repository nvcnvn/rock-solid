# Source me: dev headers from ../bypass-drivers/vendor/sysroot (read only); QEMU, cmocka and the
# guest kernel from this spike's vendor/sysroot; SPDK from vendor/spdk.
D=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)
. "$D/../bypass-drivers/env.sh"
Q=$D/vendor/sysroot
export PATH=$Q/usr/bin:$PATH
export C_INCLUDE_PATH=$C_INCLUDE_PATH:$Q/usr/include
export CPLUS_INCLUDE_PATH=$C_INCLUDE_PATH
export LIBRARY_PATH=$LIBRARY_PATH:$Q/usr/lib/x86_64-linux-gnu
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$Q/usr/lib/x86_64-linux-gnu
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$D/vendor/pkgconfig
export SPDK=$D/vendor/spdk
