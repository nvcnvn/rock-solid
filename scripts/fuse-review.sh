#!/bin/bash
# fuse-review.sh: the FUSE mount's review on Linux, as root through sudo: the kernel tests of
# internal/frontend/fuse and cmd/rock-solid, then `rock-solid ctl` and one `rock-solid node` over a
# file-backed disk mounting volume 1, driven from the shell: create, write, read back from another
# process and another uid, ls, unlink. Everything lives in a fresh directory under
# $RS_FUSE_BASE (default $HOME/fuse); every mount under it is unmounted and it is removed on exit.
set -euo pipefail
cd "$(dirname "$0")/.."
base=${RS_FUSE_BASE:-$HOME/fuse}
mkdir -p "$base"
work=$(mktemp -d "$base/run.XXXXXX")
pids=()
cleanup() {
	for p in "${pids[@]}"; do sudo kill "$p" 2>/dev/null || true; done
	awk -v w="$work/" 'index($5, w) == 1 {print $5}' /proc/self/mountinfo | sort -r | while read -r m; do sudo umount -l "$m" || true; done
	sudo rm -rf "$work"
}
trap cleanup EXIT

go test -c -o "$work/fuse.test" ./internal/frontend/fuse
go test -c -o "$work/cmd.test" ./cmd/rock-solid
go build -o "$work/rock-solid" ./cmd/rock-solid
echo "== kernel tests"
(cd "$work" && sudo env RS_FUSE_DIR="$work" "$work/fuse.test" -test.run 'TestAMount' -test.count=1 -test.v)
(cd "$work" && sudo env RS_FUSE_DIR="$work" RS_URING_DIR="$work" "$work/cmd.test" -test.run '^TestAMountCreatesWritesReadsAndUnlinksThroughTheKernel$' -test.count=1 -test.v -test.timeout 10m)

echo "== ctl and node from the shell"
rs="$work/rock-solid"
port=$((20000 + RANDOM % 20000))
ctl="1=127.0.0.1:$port"
mnt="$work/mnt"
mkdir "$mnt"
fallocate -l 1G "$work/dev"
sudo "$rs" ctl -node 1 -voters "$ctl" -disk "$work/ctl-disk" -format -disk-bytes $((512 << 20)) -max-replay-bytes $((64 << 20)) > "$work/ctl.log" 2>&1 &
pids+=($!)
sudo "$rs" node -node 2 -ctl "$ctl" -listen "127.0.0.1:$((port + 1))" -device "$work/dev" -mount "1:$mnt" > "$work/node.log" 2>&1 &
pids+=($!)
until "$rs" pool 1 -ctl "$ctl" -replicas 1 -min-live 1 -failure-domain disk -pg-count 8 -timeout 5s; do sleep 1; done
"$rs" volume create -pool 1 -size $((1 << 30)) -ctl "$ctl" -peers "2=127.0.0.1:$((port + 1))" -timeout 60s
for _ in $(seq 60); do mountpoint -q "$mnt" && break; sleep 1; done
mountpoint "$mnt" || { tail -50 "$work/node.log"; exit 1; }
head -c 300000 /dev/urandom > "$work/data"
sudo cp "$work/data" "$mnt/f"
sudo chmod 644 "$mnt/f"
cmp "$work/data" <(sudo cat "$mnt/f")
# uid 65534 reaches the mount by an fd, as it may not search the directories above it.
cmp "$work/data" <(sudo bash -c 'exec 3<"$1"; setpriv --reuid=65534 --regid=65534 --clear-groups cat /dev/fd/3/f' _ "$mnt")
sudo ls -l "$mnt"
[ "$(sudo ls "$mnt")" = f ]
sudo rm "$mnt/f"
[ -z "$(sudo ls "$mnt")" ]
echo "== review run passed"
