# Source me: Samba from the user-local sysroot (fetch.sh), run unprivileged.
S=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)
R=$S/vendor/root
export SPIKE_ROOT=$R
export PATH=$R/usr/sbin:$R/usr/bin:$PATH
export LD_LIBRARY_PATH=$R/usr/lib/x86_64-linux-gnu/samba:$R/usr/lib/x86_64-linux-gnu:/usr/lib/x86_64-linux-gnu/samba
export PYTHONPATH=$S/vendor/py
# uid_wrapper lets smbd believe it is root (become_user etc.) while the kernel still sees uid 1000.
export SPIKE_PRELOAD=$R/usr/lib/x86_64-linux-gnu/libuid_wrapper.so:$R/usr/lib/x86_64-linux-gnu/libnss_wrapper.so
