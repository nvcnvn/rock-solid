# Source me: puts the user-local fio, bpftrace and nvme-cli from vendor/ on PATH (no root, no apt install).
V=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)/vendor
S=$V/sysroot
export PATH=$V/fio:$S/usr/bin:$S/usr/sbin:$PATH
export LD_LIBRARY_PATH=$S/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export BPFTRACE_BTF=${BPFTRACE_BTF:-/sys/kernel/btf/vmlinux}
