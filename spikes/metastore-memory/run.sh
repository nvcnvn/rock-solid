#!/usr/bin/env bash
# Reproduce every results file: ./run.sh [step...]   steps: names classes f1 f2 f3 big (default: all)
# F2 builds a 30M-file tree (about 12 GB of heap); the big step holds a 10M-file tree plus each
# F4 alternative and the checkpoint pieces (about 8 GB at peak).
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$HOME/.cargo/bin:$PATH" CARGO_TARGET_DIR="$PWD/target"
cargo build -q --release
B=target/release/metastore-memory-spike
steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(names classes f1 f2 f3 big)
info() {
  $B info "$(sw_vers -productName) $(sw_vers -productVersion) ($(sw_vers -buildVersion)) darwin $(uname -r)" \
    "cpu=$(sysctl -n hw.ncpu)x $(sysctl -n machdep.cpu.brand_string) mem=$(( $(sysctl -n hw.memsize) >> 30 ))G" \
    "$(rustc --version | cut -d' ' -f1-2) libc=0.2.177 rock-core=$(git rev-parse --short HEAD)$([ -z "$(git status --porcelain ../../crates/core)" ] || echo +uncommitted)" \
    "date=$(date -u +%Y-%m-%dT%H:%MZ) loadavg=$(sysctl -n vm.loadavg | tr -d '{}' | xargs)"
}
for s in "${steps[@]}"; do
  case $s in
    names) { echo "# $(info)"; $B sample-names /usr /Applications; } > name-lengths.txt ;;
    classes) cc -O1 size-classes.c -o target/size-classes
             { info; target/size-classes; } | tee results-size-classes.txt ;;
    f1|f2|f3) { info; $B "$s" name-lengths.txt; } 2>&1 | tee "results-$s.txt" ;;
    big) { info; $B big 10000000 name-lengths.txt; } 2>&1 | tee results-10m.txt ;;
    *) echo "unknown step $s" >&2; exit 1 ;;
  esac
done
