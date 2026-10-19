#!/bin/sh
# Makes work/plant/<bug>/{lsm,engine}: a copy of internal/lsm with one planted
# bug, and the harness's engine over it; planted.go (build tag planted) runs them.
set -eu
cd "$(dirname "$0")"
mod=github.com/nvcnvn/rock-solid/spikes/lsm-study-testing
rm -rf work/plant
plant() { # name file perl-expression
	d=work/plant/$1
	mkdir -p "$d/lsm" "$d/engine"
	for f in ../../internal/lsm/*.go; do
		case $f in *_test.go) ;; *) cp "$f" "$d/lsm/" ;; esac
	done
	cp "$d/lsm/$2" "$d/$2.orig"
	perl -0pi -e "$3" "$d/lsm/$2"
	if cmp -s "$d/lsm/$2" "$d/$2.orig"; then echo "plant $1: pattern did not apply" >&2; exit 1; fi
	diff -u "$d/$2.orig" "$d/lsm/$2" | sed -n '3,$p' | grep '^[-+]' | sed "s/^/  $1: /" || true
	sed "s#lsm \"github.com/nvcnvn/rock-solid/internal/lsm\"#lsm \"$mod/$d/lsm\"#" engine/engine.go > "$d/engine/engine.go"
}
plant p1 tree.go 's/fl\.older, &fl\.probe\)/nil, &fl.probe)/'
plant p2 compact.go 's/\tif len\(c\.out\.blocks\) > 0 \{\n\t\truns = append\(runs, c\.out\)\n\t\}\n\tt\.runs = append\(runs, t\.runs\[i\+len\(c\.inputs\):\]\.\.\.\)/\tt.runs = append(runs, t.runs[i+len(c.inputs):]...)\n\tif len(c.out.blocks) > 0 {\n\t\tt.runs = append(t.runs, c.out)\n\t}/'
plant p3 tree.go 's/t\.runs = slices\.Insert\(t\.runs, at, r\)/_ = at\n\tt.runs = slices.Insert(t.runs, 0, r)/'
plant p4 tree.go 's/st\.frozen, st\.frozenRoot = t\.frozen, t\.frozen\.root/st.frozenRoot = t.frozen.root/'
plant p5 compact.go 's/range t\.runs\[best\[1\]:\]/range t.runs[len(t.runs):]/'
