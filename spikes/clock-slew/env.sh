# source me: puts the unpacked flex and QEMU on PATH, and QEMU's libraries on
# the loader's path.
SPIKE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)
export PATH="$SPIKE_DIR/vendor/sysroot/usr/bin:$PATH"
export LD_LIBRARY_PATH="$SPIKE_DIR/vendor/sysroot/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
