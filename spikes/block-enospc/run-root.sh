#!/bin/bash
# Root and device-mapper only: what ext4 and XFS do when the block device under them runs out of
# space, failing writes with BLK_STS_NOSPC (dm-thin error_if_no_space) or holding them (dm-thin's
# default queue, released by growing the pool or failed after no_space_timeout).
# Creates loop devices and device-mapper targets under $WORK and removes them on exit.
# It reads /sys/module/dm_thin_pool/parameters/no_space_timeout but never changes it; it drops
# the slab caches (vm.drop_caches=2) in T7 to T9 to make XFS write its metadata in place.
# Per test it writes $OUT/<test>.kernel.txt (the kernel log), $OUT/<test>.fsck.txt and, after a
# failed run, $OUT/<test>.fsck-replay.txt (the check after the pool grew and a mount replayed the log).
# Usage: sudo ./run-root.sh [T1 T2 ...] | tee results-root2.txt     (about 6 minutes)
set -u
cd "$(dirname "$0")" || exit 1
WORK=${WORK:-$PWD/work}
OUT=${OUT:-$PWD/results-root2}
POOL=rsenospc-pool
THIN=rsenospc-thin
MNT=$WORK/mnt
DATA_MB=192        # pool data space; the thin volume is 1 GiB, so the pool runs out first
GROW_MB=1280       # data space after a grow: more than the whole thin volume, so it never fills again
META_MB=16
BLOCK=128          # 64 KiB pool blocks, in sectors
FSCK_LINES=60      # lines of each check echoed into the results; the full output is in $OUT

loops=()
grown=0
cleanup() {
	umount -l "$MNT" 2>/dev/null
	dmsetup remove --retry "$THIN" 2>/dev/null
	dmsetup remove --retry "$POOL" 2>/dev/null
	for l in "${loops[@]}"; do losetup -d "$l" 2>/dev/null; done
	rm -f "$WORK/data.img" "$WORK/meta.img"
}
finish() {
	cleanup
	[ -n "${SUDO_UID:-}" ] && [ -d "$OUT" ] && chown -R "$SUDO_UID:${SUDO_GID:-$SUDO_UID}" "$OUT"
}
trap finish EXIT

say() { printf '%s\n' "$*"; }
check() { # id got want
	if [ "$2" = "$3" ]; then say "[OK ] $1 got=$2"; else say "[FAIL] $1 got=$2 want=$3"; fi
}

RUN=$(date +%s)
klog_mark() { echo "rsenospc $RUN: start $1" >/dev/kmsg; }
klog() { dmesg | sed -n "/rsenospc $RUN: start $1\$/,\$p"; }
klog_has() { # id pattern; prints yes or no
	if klog "$1" | grep -qE "$2"; then echo yes; else echo no; fi
}
klog_save() { # id; saves the test's kernel log and echoes its notable lines
	klog "$1" >"$OUT/$1.kernel.txt"
	grep -E 'thin:|critical space|EXT4-fs|JBD2|XFS|lost async|blocked for more' "$OUT/$1.kernel.txt" |
		grep -v 'rsenospc' | head -20 | sed 's/^/   kernel: /'
	say "   kernel: $(wc -l <"$OUT/$1.kernel.txt") lines in $(basename "$OUT")/$1.kernel.txt"
}

pool_table() { # data sectors, extra feature args
	echo "0 $1 thin-pool ${loops[0]} ${loops[1]} $BLOCK 0 $2"
}

setup() { # error|queue, fs
	cleanup
	loops=()
	grown=0
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
	truncate -s ${GROW_MB}M "$WORK/data.img"
	losetup -c "${loops[1]}"
	dmsetup suspend --nolockfs "$POOL"
	dmsetup reload "$POOL" --table "$(pool_table $((GROW_MB * 2048)) "1 skip_block_zeroing")"
	dmsetup resume "$POOL"
	grown=1
}

wait_full() { # waits up to 30 s for the pool to run out of data space
	local tries=60
	while [ $tries -gt 0 ]; do
		dmsetup status "$POOL" | grep -q out_of_data_space && return 0
		sleep 0.5
		tries=$((tries - 1))
	done
	return 1
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

overwrite_errno() { # rewrites the 16 MiB file written before the pool filled; prints ok or the errno name
	python3 - "$MNT/old" <<'EOF'
import errno, os, sys
try:
    fd = os.open(sys.argv[1], os.O_WRONLY)
    buf = os.urandom(1 << 20)
    for i in range(16):
        os.pwrite(fd, buf, i << 20)
    os.fsync(fd)
    os.close(fd)
    print("ok")
except OSError as e:
    print(errno.errorcode[e.errno])
EOF
}

writable() { # creates an empty file; ok, or the errno name (EROFS after ext4's emergency read-only, EIO after an XFS shutdown)
	python3 - "$MNT/probe.$RANDOM" <<'EOF'
import errno, os, sys
try:
    os.close(os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644))
    print("ok")
except OSError as e:
    print(errno.errorcode[e.errno])
EOF
}

create_files() { # 1000 files in a new directory: new inode chunks and directory blocks; ok or fail
	mkdir "$MNT/d" 2>/dev/null && (cd "$MNT/d" && touch f{1..1000} 2>/dev/null) && echo ok || echo fail
}

push_metadata() { # commits the creates to the log and makes XFS write its dirty metadata in place
	python3 -c 'import os, sys; fd = os.open(sys.argv[1], os.O_RDONLY); os.fsync(fd)' "$MNT/d"
	echo 2 >/proc/sys/vm/drop_caches # the XFS inode shrinker pushes the AIL (xfs_reclaim_inodes_nr)
}

fs_check() { # id fs [suffix]; unmounts, saves the check's output, echoes its head, prints clean or damaged last
	local f="$OUT/$1.fsck${3:-}.txt" r
	umount "$MNT" || { echo unmount-failed; return; }
	case $2 in
	ext4) e2fsck -fn /dev/mapper/$THIN >"$f" 2>&1 ;;
	xfs) xfs_repair -n /dev/mapper/$THIN >"$f" 2>&1 ;;
	esac
	r=$?
	grep -vE '^ *- (agno = |[0-9:]+ |traversal|moving|scan|process|setting|check|reporting|zero log|found root|traversing)' "$f" |
		head -$FSCK_LINES | sed "s/^/   fsck${3:-}: /" >&2
	[ "$(wc -l <"$f")" -gt "$FSCK_LINES" ] && say "   fsck${3:-}: ($(basename "$OUT")/$1.fsck${3:-}.txt has the rest)" >&2
	say "   fsck${3:-}: exit $r" >&2
	[ $r = 0 ] && echo clean || echo damaged
}

replay_check() { # id fs; after a failed run: grows the pool, mounts to replay the log, unmounts, checks again
	[ $grown = 1 ] || grow_pool
	if mount /dev/mapper/$THIN "$MNT"; then
		check "$1 mount after growing the pool" ok ok
		check "$1 check after the replay" "$(fs_check "$1" "$2" -replay)" clean
	else
		check "$1 mount after growing the pool" fail ok
	fi
}

declare -A FS=([T1]=ext4 [T2]=xfs [T3]=ext4 [T4]=xfs [T5]=ext4 [T6]=xfs [T7]=xfs [T8]=xfs [T9]=xfs)

run_error() { # T1 ext4, T2 xfs: error_if_no_space
	setup error "${FS[$1]}"
	klog_mark "$1"
	say "== $1 ${FS[$1]} on a thin volume with error_if_no_space; expect: fsync ENOSPC, overwrites work, creates work and the fs stays writable until unmount; ext4 loses a new metadata block, aborts its journal at unmount and is damaged; XFS shuts down at unmount (fail_at_unmount) with a dirty log; after the pool grows a mount replays the log and both check clean"
	check "$1 first error" "$(fill_errno)" ENOSPC
	check "$1 overwrite of old blocks" "$(overwrite_errno)" ok
	touch "$MNT"/new{1..200} 2>/dev/null
	sync
	check "$1 writable after 200 creates" "$(writable)" ok
	local r
	r=$(fs_check "$1" "${FS[$1]}")
	check "$1 check" "$r" damaged
	[ "${FS[$1]}" = xfs ] && check "$1 XFS shut down at unmount" "$(klog_has "$1" 'Shutting down filesystem')" yes
	replay_check "$1" "${FS[$1]}"
	klog_save "$1"
}

run_queue_grow() { # T3 ext4, T4 xfs: queue, then grow the pool past the volume's size after 20 s
	setup queue "${FS[$1]}"
	klog_mark "$1"
	say "== $1 ${FS[$1]}, queue_if_no_space, pool grown to ${GROW_MB} MiB after 20 s; expect: the writer waits about 20 s, then completes with no error, the fs stays writable and checks clean"
	local start=$SECONDS
	fill_errno >"$WORK/out" &
	local pid=$!
	sleep 20
	grow_pool
	wait $pid
	check "$1 error" "$(cat "$WORK/out")" none
	say "   took $((SECONDS - start)) s"
	check "$1 overwrite of old blocks" "$(overwrite_errno)" ok
	check "$1 writable" "$(writable)" ok
	check "$1 I/O errors in the kernel log" "$(klog_has "$1" 'error IO|I/O error|lost async|writeback error|metadata I/O error')" no
	check "$1 check" "$(fs_check "$1" "${FS[$1]}")" clean
	klog_save "$1"
}

run_queue_timeout() { # T5 ext4, T6 xfs: queue until no_space_timeout fails the writes
	setup queue "${FS[$1]}"
	klog_mark "$1"
	local t want_ow=ok want_w=ok want_fsck=clean
	t=$(cat /sys/module/dm_thin_pool/parameters/no_space_timeout)
	[ "${FS[$1]}" = ext4 ] && want_ow=EROFS want_w=EROFS want_fsck=damaged
	say "== $1 ${FS[$1]}, queue_if_no_space, no_space_timeout ${t} s, never grown; expect: ENOSPC after about ${t} s; ext4 loses a new metadata block with the held data, aborts its journal and refuses every write (EROFS), and is damaged; XFS, whose metadata here needs no new space, keeps working and checks clean; a replay after the pool grows checks clean"
	local start=$SECONDS
	check "$1 first error" "$(fill_errno)" ENOSPC
	say "   took $((SECONDS - start)) s"
	check "$1 overwrite of old blocks" "$(overwrite_errno)" $want_ow
	check "$1 writable" "$(writable)" $want_w
	check "$1 check" "$(fs_check "$1" "${FS[$1]}")" $want_fsck
	[ $want_fsck = damaged ] && replay_check "$1" "${FS[$1]}"
	klog_save "$1"
}

run_xfs_meta_error() { # T7 grown, T8 never grown: XFS metadata refused in place while mounted
	setup error xfs
	klog_mark "$1"
	local then="the pool is grown 35 s later; expect: XFS retries the refused metadata until it lands, never shuts down and checks clean"
	[ "$1" = T8 ] && then="never grown; expect: XFS retries while mounted and stays writable, shuts down at unmount (fail_at_unmount) with a dirty log, and checks clean after the pool grows and a mount replays the log"
	say "== $1 xfs, error_if_no_space; the pool filled, 1000 files created, the log forced and the AIL pushed while mounted, so the new inode chunks and directory blocks are refused in place; $then"
	check "$1 first error" "$(fill_errno)" ENOSPC
	check "$1 creates" "$(create_files)" ok
	push_metadata
	sleep 35 # past one run of xfs_log_worker (fs.xfs.xfssyncd_centisecs, 30 s), which pushes the AIL too
	check "$1 metadata write refused while mounted" "$(klog_has "$1" 'metadata I/O error.*error 28')" yes
	check "$1 shut down while mounted" "$(klog_has "$1" 'Shutting down filesystem')" no
	check "$1 writable while refused" "$(writable)" ok
	if [ "$1" = T7 ]; then
		grow_pool
		push_metadata
		sleep 35
		check "$1 writable after the grow" "$(writable)" ok
		check "$1 check" "$(fs_check "$1" xfs)" clean
		check "$1 shut down" "$(klog_has "$1" 'Shutting down filesystem')" no
	else
		check "$1 check" "$(fs_check "$1" xfs)" damaged
		check "$1 XFS shut down at unmount" "$(klog_has "$1" 'Shutting down filesystem')" yes
		replay_check "$1" xfs
	fi
	klog_save "$1"
}

run_xfs_meta_hold() { # T9: XFS metadata held in place while mounted, then the pool grown
	setup queue xfs
	klog_mark "$1"
	say "== $1 xfs, queue_if_no_space; the pool filled by a writer that waits, then 1000 files created, the log forced and the AIL pushed, so the new metadata is held as T8's was refused; the pool grown about 20 s into the hold; expect: the writer completes with no error, no metadata error, the fs stays writable and checks clean"
	local start=$SECONDS
	fill_errno >"$WORK/out" &
	local pid=$!
	if wait_full; then check "$1 pool full" yes yes; else check "$1 pool full" no yes; fi
	check "$1 creates while held" "$(create_files)" ok
	push_metadata
	sleep $((20 - (SECONDS - start) > 5 ? 20 - (SECONDS - start) : 5))
	grow_pool
	wait $pid
	check "$1 error" "$(cat "$WORK/out")" none
	say "   took $((SECONDS - start)) s"
	push_metadata
	check "$1 writable" "$(writable)" ok
	check "$1 metadata errors" "$(klog_has "$1" 'metadata I/O error')" no
	check "$1 check" "$(fs_check "$1" xfs)" clean
	klog_save "$1"
}

[ "$(id -u)" = 0 ] || { echo "needs root" >&2; exit 1; }
mkdir -p "$OUT"
modprobe dm_thin_pool
modprobe xfs
echo "# $(uname -sr), $(mkfs.ext4 -V 2>&1 | head -1), $(mkfs.xfs -V), no_space_timeout $(cat /sys/module/dm_thin_pool/parameters/no_space_timeout) s, xfssyncd_centisecs $(cat /proc/sys/fs/xfs/xfssyncd_centisecs 2>/dev/null), $(date -u +%Y-%m-%dT%H:%MZ)"
for t in "${@:-T1 T2 T3 T4 T5 T6 T7 T8 T9}"; do
	for one in $t; do
		case $one in
		T1 | T2) run_error "$one" ;;
		T3 | T4) run_queue_grow "$one" ;;
		T5 | T6) run_queue_timeout "$one" ;;
		T7 | T8) run_xfs_meta_error "$one" ;;
		T9) run_xfs_meta_hold "$one" ;;
		esac
	done
done 2>&1
