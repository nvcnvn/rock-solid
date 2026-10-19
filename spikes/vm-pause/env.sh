# source me: runs the unpacked QEMU 10.2.1 through the unpacked glibc 2.43
# loader, since the host's glibc 2.39 is older than it needs, and points it at
# the unpacked firmware.
SPIKE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)
SYSROOT=$SPIKE_DIR/vendor/sysroot
KVER=7.0.0-14-generic
QEMU="$SYSROOT/usr/lib64/ld-linux-x86-64.so.2 --library-path $SYSROOT/usr/lib/x86_64-linux-gnu:$SYSROOT/lib/x86_64-linux-gnu $SYSROOT/usr/bin/qemu-system-x86_64 -L $SYSROOT/usr/share/seabios -L $SYSROOT/usr/share/qemu"
export SPIKE_DIR SYSROOT KVER QEMU
QEMU_IMG="$SYSROOT/usr/lib64/ld-linux-x86-64.so.2 --library-path $SYSROOT/usr/lib/x86_64-linux-gnu:$SYSROOT/lib/x86_64-linux-gnu $SYSROOT/usr/bin/qemu-img"
export QEMU_IMG
