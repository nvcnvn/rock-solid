# Source me: points compilers and pkg-config at the user-local sysroot in vendor/ (no root, no apt install).
V=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)/vendor
S=$V/sysroot
export PATH=$S/usr/bin:$V/pydeps/bin:$PATH
export PYTHONPATH=$V/pydeps
export C_INCLUDE_PATH=$S/usr/include:$S/usr/include/x86_64-linux-gnu
export CPLUS_INCLUDE_PATH=$C_INCLUDE_PATH
export LIBRARY_PATH=$S/usr/lib/x86_64-linux-gnu
export PKG_CONFIG_PATH=$S/usr/lib/x86_64-linux-gnu/pkgconfig:$S/usr/share/pkgconfig
export PKG_CONFIG_SYSROOT_DIR=$S
export LD_LIBRARY_PATH=$S/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
