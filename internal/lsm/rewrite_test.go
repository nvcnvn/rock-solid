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
// holds no live piece, and every key reads the same throughout and after a restore.
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
			if n := tr.MarkRewrite(6); n != 1 || tr.MarkRewrite(6) != 0 {
				t.Fatalf("marked %d runs, want the middle one once", n)
			}
			f.segment = 9
			var dead []disklog.BlobRef
			steps := 0
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
				_, d := f.checkpoint(t, tr, nil)
				dead = append(dead, d...)
				steps++
			}
			if step > 0 && steps < 2 {
				t.Fatalf("the rewrite took %d steps of %d bytes", steps, step)
			}
			if len(tr.runs) != 3 || slices.ContainsFunc(old, func(p disklog.BlobRef) bool { return !slices.Contains(dead, p) }) {
				t.Fatalf("%d runs after the rewrite; dead %v, the old run's pieces %v", len(tr.runs), dead, old)
			}
			if pieces, held := tr.RunsIn(6, nil); len(pieces) != 0 || held {
				t.Fatalf("segment 6 still holds run pieces %v after the rewrite, held %v", pieces, held)
			}
			f.drop(dead)
			checkReads(t, f.restoreManifest(t, stepConfig(384, step)), m, stepProbes)
		})
	}
}
