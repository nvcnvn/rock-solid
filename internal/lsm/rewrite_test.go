package lsm

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// The cleaner never copies a piece of a live run: it marks the run, and the run is merged alone,
// in steps when the tree has them, into a run in its place written elsewhere. Once the manifest
// naming the rewrite is durable every piece of the old run is reported dead, so the segment
// holds no live piece, and every key reads the same throughout and after a restore. The runs as
// the cleaner weighs them for a tree on another loop match, a run a restore bounds included.
func TestRewriteMarkedRun(t *testing.T) {
	for _, step := range []int{0, 3 * 384} {
		t.Run(fmt.Sprintf("step %d", step), func(t *testing.T) {
			tr, f, m := stepTree(t, 384, step), newFakeLog(), model{}
			for run := range 3 {
				f.segment = uint32(5 + run)
				for i := 0; i < 300; i += 1 + run {
					put(t, tr, m, fmt.Sprintf("k%03d", i), fmt.Sprintf("r%d-%03d", run, i))
				}
				f.checkpoint(t, tr, nil)
			}
			old := slices.Clone(tr.runs[1].pieces)
			if pieces, held := tr.RunsIn(6, nil); !slices.Equal(pieces, old) || held {
				t.Fatalf("segment 6 holds run pieces %v, held %v; want the middle run's %v", pieces, held, old)
			}
			if pieces, _ := tr.RunsIn(9, nil); len(pieces) != 0 {
				t.Fatalf("segment 9 holds run pieces %v", pieces)
			}
			weighsAsRunsIn(t, tr)
			if n := tr.MarkRewrite(6); n != 1 || tr.MarkRewrite(6) != 0 {
				t.Fatalf("marked %d runs, want the middle one once", n)
			}
			f.segment = 9
			var dead []disklog.BlobRef
			steps, bounded := 0, false
			for c := tr.PickCompaction(); c != nil; c = tr.PickCompaction() {
				if c.rule != ruleRewrite || len(c.inputs) != 1 {
					t.Fatalf("picked %v over %d runs, want a rewrite of one", c.rule, len(c.inputs))
				}
				if _, held := tr.RunsIn(6, nil); !held {
					t.Fatal("a run being rewritten is not held")
				}
				c.Run()
				if err := tr.Install(c); err != nil {
					t.Fatal(err)
				}
				checkReads(t, tr, m, stepProbes)
				weighsAsRunsIn(t, tr)
				_, d := f.checkpoint(t, tr, nil)
				dead = append(dead, d...)
				steps++
				if len(f.bounds) > 0 {
					bounded = true
					weighsAsRunsIn(t, f.restoreManifest(t, stepConfig(384, step)))
				}
			}
			if step > 0 && (steps < 2 || !bounded) {
				t.Fatalf("the rewrite took %d steps of %d bytes, a manifest bounding the run part way %v", steps, step, bounded)
			}
			if len(tr.runs) != 3 || slices.ContainsFunc(old, func(p disklog.BlobRef) bool { return !slices.Contains(dead, p) }) {
				t.Fatalf("%d runs after the rewrite; dead %v, the old run's pieces %v", len(tr.runs), dead, old)
			}
			if pieces, held := tr.RunsIn(6, nil); len(pieces) != 0 || held {
				t.Fatalf("segment 6 still holds run pieces %v after the rewrite, held %v", pieces, held)
			}
			weighsAsRunsIn(t, tr)
			f.drop(dead)
			checkReads(t, f.restoreManifest(t, stepConfig(384, step)), m, stepProbes)
		})
	}
}

// weighsAsRunsIn checks that the run costs a tree on another loop hands its disk's owner weigh
// every segment as RunsIn and RewriteSteps do.
func weighsAsRunsIn(t *testing.T, tr *Tree) {
	t.Helper()
	costs := tr.RunCosts(nil)
	for seg := uint32(1); seg < 12; seg++ {
		pieces, held := tr.RunsIn(seg, nil)
		steps, extra := tr.RewriteSteps(seg)
		p, h, n, x := disklog.RewriteIn(costs, seg, nil, 0, 0)
		if !slices.Equal(p, pieces) || h != held || n != steps || x != extra {
			t.Fatalf("segment %d weighs pieces %v, held %v, %d steps and %d extra from run costs; %v, %v, %d and %d from the tree", seg, p, h, n, x, pieces, held, steps, extra)
		}
	}
}
