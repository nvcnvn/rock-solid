//go:build planted

package main

import (
	p1 "github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/work/plant/p1/engine"
	p2 "github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/work/plant/p2/engine"
	p3 "github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/work/plant/p3/engine"
	p4 "github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/work/plant/p4/engine"
	p5 "github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/work/plant/p5/engine"
)

func init() {
	for _, p := range []struct {
		name string
		mk   maker
	}{
		{"P1-flush-drops-removals-hiding-runs", p1.New},
		{"P2-compaction-installed-as-oldest", p2.New},
		{"P3-restore-ignores-run-order", p3.New},
		{"P4-reads-skip-frozen-memtable", p4.New},
		{"P5-compaction-drops-removals-hiding-older-runs", p5.New},
	} {
		engines[p.name] = p.mk
		plantNames = append(plantNames, p.name)
	}
}
