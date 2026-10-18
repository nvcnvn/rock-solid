# source me: perf from vendor/sysroot (see fetch.sh) and the Go toolchain the spikes use.
H=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export LD_LIBRARY_PATH=$H/vendor/sysroot/usr/lib/x86_64-linux-gnu:$H/vendor/sysroot/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export PATH=$H/vendor/sysroot/usr/lib/linux-tools-6.8.0-147:$PATH
export GOTOOLCHAIN=go1.27.0
