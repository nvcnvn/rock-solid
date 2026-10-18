#!/bin/bash
# Reproduces results.txt: generates 10M- and 20M-file data sets under work/ (about 5 GB), runs each Go
# variant and the Rust baseline, then deletes the data (KEEP=1 keeps it). Peak memory about 9 GB.
# usage: ./run.sh [10m] [20m]   (default: both)
set -euo pipefail
cd "$(dirname "$0")"
export GOTOOLCHAIN=${GOTOOLCHAIN:-go1.27.0}
mkdir -p work bin
go build -o bin/go-memtable .
GOEXPERIMENT=nogreenteagc go build -o bin/go-memtable-nogreentea .
(cd rust && cargo build -q --release)
names=../metastore-memory/name-lengths.txt
steps=("$@")
[ ${#steps[@]} -eq 0 ] && steps=(10m 20m)
{
	echo "# $(uname -r), $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //'), $(nproc) CPUs," \
		"$(free -g | awk '/Mem:/{print $2}') GiB, $(go version | cut -d' ' -f3), $(rustc --version | cut -d' ' -f1-2), $(date -Iseconds)"
	for s in "${steps[@]}"; do
		case $s in
		10m)
			bin/go-memtable gen files=10000000 names=$names data=work/d10.bin probes=work/p10.bin
			for v in slices arena pages; do bin/go-memtable run variant=$v data=work/d10.bin probes=work/p10.bin; done
			bin/go-memtable-nogreentea run variant=slices data=work/d10.bin probes=work/p10.bin restore=0
			rust/target/release/memtable-rust-baseline work/d10.bin work/p10.bin
			;;
		20m)
			bin/go-memtable gen files=20000000 names=$names data=work/d20.bin probes=work/p20.bin
			for v in slices pages; do bin/go-memtable run variant=$v data=work/d20.bin probes=work/p20.bin restore=0; done
			bin/go-memtable-nogreentea run variant=slices data=work/d20.bin probes=work/p20.bin restore=0
			;;
		*) echo "unknown step $s" >&2; exit 1 ;;
		esac
	done
} 2>&1 | tee results.txt
[ "${KEEP:-0}" = 1 ] || rm -rf work
