#!/usr/bin/env bash
# Reproduce every test: ./run.sh [tests...]   (default: all; KV_QUICK=1 for a smoke run)
# e.g. ./run.sh k2:redb-qr k4. Writes raw output to $OUT (default results.txt) (k6.sh writes results-k6.txt). Data lives in work/ and is deleted.
set -euo pipefail
cd "$(dirname "$0")"
export CARGO_TARGET_DIR="$PWD/target" KV_WORK="$PWD/work"
cargo build -q --release
B=target/release/kv-engines-spike
OUT=${OUT:-results.txt}
tests=("$@")
[ ${#tests[@]} -eq 0 ] && tests=(idle k1 k2 k3 k4 k5 k5c k7 idle)
model=$(cat /sys/block/nvme0n1/device/model 2>/dev/null | xargs || echo unknown)
{
  $B info "$(rustc --version | cut -d' ' -f1-2)" "date=$(date -u +%Y-%m-%dT%H:%MZ)" \
    "cpu=$(nproc)x $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)" \
    "mem=$(free -g | awk '/Mem/{print $2}')G disk=$model ($(cat /sys/block/nvme0n1/queue/write_cache)) fs=$(findmnt -no FSTYPE -T "$PWD")"
  for t in "${tests[@]}"; do
    case $t in
      k2) for e in redb redb-qr fjall; do $B k2 "$e"; done ;;
      k3) for e in redb fjall; do $B k3 "$e"; done ;;
      k2:*|k3:*) $B "${t%%:*}" "${t#*:}" ;;
      *) $B "$t" ;;
    esac
  done
} 2>&1 | tee "$OUT"
rm -rf work
