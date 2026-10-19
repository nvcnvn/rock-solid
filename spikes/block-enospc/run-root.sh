#!/bin/bash
# Root and device-mapper only, not run yet: what ext4 and XFS do when the block device under them runs out of
# space, failing writes with BLK_STS_NOSPC (dm-thin error_if_no_space) or holding them (dm-thin's
# default queue, released by growing the pool or failed after no_space_timeout).
# Creates loop devices and device-mapper targets under $WORK and removes them on exit.
# It reads /sys/module/dm_thin_pool/parameters/no_space_timeout but never changes it.
# Usage: sudo ./run-root.sh [T1 T2 ...] | tee results-root.txt     (about 4 minutes)
set -u
cd "$(dirname "$0")"
WORK=${WORK:-$PWD/work}
POOL=rsenospc-pool
THIN=rsenospc-thin
MNT=$WORK/mnt
DATA_MB=192        # pool data space; the thin volume is 1 GiB, so the pool runs out first
META_MB=16
BLOCK=128          # 64 KiB pool blocks, in sectors

loops=()
cleanup() {
	umount -l "$MNT" 2>/dev/null
	dmsetup remove --retry "$THIN" 2>/dev/null
	dmsetup remove --retry "$POOL" 2>/dev/null
	for l in "${loops[@]}"; do losetup -d "$l" 2>/dev/null; done
	rm -f "$WORK/data.img" "$WORK/meta.img"
}
trap cleanup EXIT

say() { printf '%s\n' "$*"; }
check() { # id got want
	if [ "$2" = "$3" ]; then say "[OK ] $1 got=$2"; else say "[FAIL] $1 got=$2 want=$3"; fi
}

pool_table() { # data sectors, extra feature args
	echo "0 $1 thin-pool ${loops[0]} ${loops[1]} $BLOCK 0 $2"
}

setup() { # error|queue, fs
	cleanup
	loops=()
	mkdir -p "$WORK" "$MNT"
	truncate -s ${META_MB}M "$WORK/meta.img"
	truncate -s ${DATA_MB}M "$WORK/data.img"
	loops+=("$(losetup -f --show "$WORK/meta.img")")
	loops+=("$(losetup -f --show "$WORK/data.img")")
	dd if=/dev/zero of="${loops[0]}" bs=4096 count=1 status=none
	local sectors=$((DATA_MB * 2048)) feat="1 skip_block_zeroing"
	[ "$1" = error ] && feat="2 skip_block_zeroing error_if_no_space"
	dmsetup create "$POOL" --table "$(pool_table $sectors "$feat")"
	dmsetup message "$POOL" 0 "create_thin 0"
	dmsetup create "$THIN" --table "0 $((1024 * 2048)) thin /dev/mapper/$POOL 0"
	case $2 in
	ext4) mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0,nodiscard /dev/mapper/$THIN ;;
	xfs) mkfs.xfs -q -f -K /dev/mapper/$THIN ;;
	esac
	mount /dev/mapper/$THIN "$MNT"
	dd if=/dev/urandom of="$MNT/old" bs=1M count=16 conv=fsync status=none
}

grow_pool() {
	truncate -s $((DATA_MB * 2))M "$WORK/data.img"
	losetup -c "${loops[1]}"
	dmsetup suspend --nolockfs "$POOL"
	dmsetup reload "$POOL" --table "$(pool_table $((DATA_MB * 4096)) "1 skip_block_zeroing")"
	dmsetup resume "$POOL"
}

fill_errno() { # writes a 512 MiB file and fsyncs it; prints the errno name of the first failure
	python3 - "$MNT/big" <<'EOF'
import errno, os, sys
fd = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
buf = os.urandom(1 << 20)
try:
    for _ in range(512):
        os.write(fd, buf)
    os.fsync(fd)
    print("none")
except OSError as e:
    print(errno.errorcode[e.errno])
try:
    os.close(fd)
except OSError as e:
    print("close", errno.errorcode[e.errno], file=sys.stderr)
EOF
}

overwrite_ok() { # rewrites 16 MiB of a file written before the pool filled
	dd if=/dev/urandom of="$MNT/old" bs=1M count=16 conv=notrunc,fsync status=none 2>/dev/null && echo ok || echo fail
}

mount_state() { findmnt -no OPTIONS "$MNT" | cut -d, -f1; }

fs_check() { # unmounts, then prints clean or damaged from e2fsck -fn or xfs_repair -n
	umount "$MNT" || { echo unmount-failed; return; }
	case $1 in
	ext4) e2fsck -fn /dev/mapper/$THIN >"$WORK/fsck.out" 2>&1 ;;
	xfs) xfs_repair -n /dev/mapper/$THIN >"$WORK/fsck.out" 2>&1 ;;
	esac && echo clean || echo damaged
}

declare -A FS=([T1]=ext4 [T2]=xfs [T3]=ext4 [T4]=xfs [T5]=ext4 [T6]=xfs)

run_error() { # T1 ext4, T2 xfs: error_if_no_space
	setup error "${FS[$1]}"
	say "== $1 ${FS[$1]} on a thin volume with error_if_no_space; expect: fsync ENOSPC, the fs stays rw, overwrites work, ext4 damaged on disk (as T1L in results-loop.txt)"
	dmesg -C 2>/dev/null
	check "$1 first error" "$(fill_errno)" ENOSPC
	check "$1 overwrite of old blocks" "$(overwrite_ok)" ok
	touch "$MNT"/new{1..200} 2>/dev/null; sync
	check "$1 mount after 200 creates" "$(mount_state)" rw
	dmesg | grep -E 'critical space|EXT4-fs|XFS' | head -12 | sed 's/^/   dmesg: /'
	if [ "${FS[$1]}" = ext4 ]; then check "$1 fsck" "$(fs_check ext4)" damaged; else say "   xfs_repair: $(fs_check xfs)"; fi
}

run_queue_grow() { # T3 ext4, T4 xfs: queue, then grow the pool after 20 s
	setup queue "${FS[$1]}"
	say "== $1 ${FS[$1]}, queue_if_no_space, pool grown after 20 s; expect: the writer waits, then completes with no error"
	dmesg -C 2>/dev/null
	local start=$SECONDS
	fill_errno > "$WORK/out" &
	local pid=$!
	sleep 20
	grow_pool
	wait $pid
	check "$1 error" "$(cat "$WORK/out")" none
	say "   took $((SECONDS - start)) s"
	check "$1 mount" "$(mount_state)" rw
	dmesg | grep -E 'critical space|EXT4-fs|XFS|blocked for more' | head -12 | sed 's/^/   dmesg: /'
	check "$1 fsck" "$(fs_check "${FS[$1]}")" clean
}

run_queue_timeout() { # T5 ext4, T6 xfs: queue until no_space_timeout fails the writes
	setup queue "${FS[$1]}"
	local t
	t=$(cat /sys/module/dm_thin_pool/parameters/no_space_timeout)
	say "== $1 ${FS[$1]}, queue_if_no_space, no_space_timeout ${t} s, never grown; expect: ENOSPC after about ${t} s, overwrites still work, ext4 damaged on disk as in T1"
	dmesg -C 2>/dev/null
	local start=$SECONDS
	check "$1 first error" "$(fill_errno)" ENOSPC
	say "   took $((SECONDS - start)) s"
	check "$1 overwrite of old blocks" "$(overwrite_ok)" ok
	check "$1 mount" "$(mount_state)" rw
	dmesg | grep -E 'critical space|EXT4-fs|XFS|blocked for more' | head -12 | sed 's/^/   dmesg: /'
	if [ "${FS[$1]}" = ext4 ]; then check "$1 fsck" "$(fs_check ext4)" damaged; else say "   xfs_repair: $(fs_check xfs)"; fi
}

[ "$(id -u)" = 0 ] || { echo "needs root" >&2; exit 1; }
echo "# $(uname -sr), $(mkfs.ext4 -V 2>&1 | head -1), $(mkfs.xfs -V), $(date -u +%Y-%m-%dT%H:%MZ)"
for t in "${@:-T1 T2 T3 T4 T5 T6}"; do
	for one in $t; do
		case $one in
		T1 | T2) run_error "$one" ;;
		T3 | T4) run_queue_grow "$one" ;;
		T5 | T6) run_queue_timeout "$one" ;;
		esac
	done
done
