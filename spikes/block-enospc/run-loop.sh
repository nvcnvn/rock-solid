#!/bin/bash
# Root only. $ID: ext4 on a block device that fails writes needing new space with ENOSPC, on a
# kernel without device-mapper. A loop device's backing file lives on a 192 MiB tmpfs, so a write
# to a block of the 1 GiB device that was never written needs a new tmpfs page and fails once the
# tmpfs is full, while overwrites of blocks written earlier reuse their pages, as dm-thin's
# error_if_no_space does. Mounts the tmpfs and creates one loop device under $WORK, and removes
# both on exit.
# Usage: sudo ./run-loop.sh; sudo ID=T1D MOUNT_OPTS=discard ./run-loop.sh   (results-loop.txt)
set -u
cd "$(dirname "$0")"
WORK=${WORK:-$PWD/work-loop}
TMP=$WORK/tmpfs
MNT=$WORK/mnt
POOL_MB=${POOL_MB:-192}
MOUNT_OPTS=${MOUNT_OPTS:-}
ID=${ID:-T1L}
LOOP=

cleanup() {
	umount -l "$MNT" 2>/dev/null
	[ -n "$LOOP" ] && losetup -d "$LOOP" 2>/dev/null
	umount -l "$TMP" 2>/dev/null
	rmdir "$MNT" "$TMP" "$WORK" 2>/dev/null
}
trap cleanup EXIT

say() { printf '%s\n' "$*"; }
check() { # id got want
	if [ "$2" = "$3" ]; then say "[OK ] $1 got=$2"; else say "[FAIL] $1 got=$2 want=$3"; fi
}

fill_errno() { # writes a 512 MiB file and fsyncs it; prints the errno name of the first failure
	python3 - "$MNT/big" <<'EOF'
import errno, os, sys
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
buf = os.urandom(1 << 20)
where = "write"
try:
    for _ in range(512):
        os.write(fd, buf)
    where = "fsync"
    os.fsync(fd)
    print("none")
except OSError as e:
    print(errno.errorcode[e.errno], where, file=sys.stderr)
    print(errno.errorcode[e.errno])
try:
    os.close(fd)
except OSError as e:
    print("close", errno.errorcode[e.errno], file=sys.stderr)
EOF
}

overwrite_ok() {
	dd if=/dev/urandom of="$MNT/old" bs=1M count=16 conv=notrunc,fsync status=none 2>/dev/null && echo ok || echo fail
}

mount_state() { findmnt -no OPTIONS "$MNT" | cut -d, -f1; }

[ "$(id -u)" = 0 ] || { echo "needs root" >&2; exit 1; }
echo "# $(uname -sr), $(mkfs.ext4 -V 2>&1 | head -1), $(date -u +%Y-%m-%dT%H:%MZ)"
mkdir -p "$TMP" "$MNT"
mount -t tmpfs -o size=${POOL_MB}M tmpfs "$TMP"
truncate -s 1G "$TMP/thin.img"
LOOP=$(losetup -f --show "$TMP/thin.img")
mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0,nodiscard "$LOOP"
mount ${MOUNT_OPTS:+-o $MOUNT_OPTS} "$LOOP" "$MNT"
dd if=/dev/urandom of="$MNT/old" bs=1M count=16 conv=fsync status=none
say "== $ID ext4${MOUNT_OPTS:+ -o $MOUNT_OPTS} on a loop device whose ${POOL_MB} MiB backing tmpfs fills; expect: fsync ENOSPC, the fs stays rw, overwrites work"
say "   errors behaviour: $(dumpe2fs -h "$LOOP" 2>/dev/null | grep -i 'Errors behavior' | tr -s ' ')"
dmesg -C 2>/dev/null
check "$ID first error" "$(fill_errno 2>"$WORK/fill.err")" ENOSPC
say "   failed at: $(cat "$WORK/fill.err")"
say "   backing tmpfs: $(df -h --output=used,size "$TMP" | tail -1 | tr -s ' ')"
check "$ID overwrite of old blocks" "$(overwrite_ok)" ok
rm -f "$MNT/big"; sync
touch "$MNT"/new{1..200} 2>/dev/null; sync
check "$ID mount after rm and 200 creates" "$(mount_state)" rw
check "$ID new write after rm" "$(dd if=/dev/zero of="$MNT/after" bs=1M count=4 conv=fsync status=none 2>/dev/null && echo ok || echo fail)" ok
dmesg 2>/dev/null | grep -E 'loop|EXT4-fs|I/O error|critical' | head -15 | sed 's/^/   dmesg: /'
umount "$MNT"
e2fsck -fn "$LOOP" >"$WORK/fsck.out" 2>&1; rc=$?
check "$ID fsck after unmount" "$rc" 0
grep -vE '^(Pass|e2fsck|$)' "$WORK/fsck.out" | head -12 | sed 's/^/   fsck: /'

