# source me: puts the unpacked flex on PATH for the kernel build.
SPIKE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)
export PATH="$SPIKE_DIR/vendor/sysroot/usr/bin:$PATH"
