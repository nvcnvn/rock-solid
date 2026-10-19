// Package dd holds the reduced failures of the metamorphic harness as
// datadriven regression cases (github.com/cockroachdb/datadriven, the format
// Pebble and etcd-raft use): each case is an ops file and the history that
// internal/lsm must produce under the named option set. go test -rewrite
// regenerates the expected output.
package dd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"

	"github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/engine"
	"github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/meta"
)

var sets = map[string]meta.Options{
	"A": {Piece: 1 << 20, FlushOps: true, Compact: "ops"},
	"C": {Piece: 256, FlushOps: true, FlushEvery: 10, Compact: "none"},
	"D": {Piece: 512, FlushOps: true, FlushEvery: 25, Compact: "deferred", Absorb: 4},
	"E": {Piece: 1024, FlushOps: true, FlushEvery: 30, Compact: "eager", FailFirst: 1, RestartEach: true},
	"G": {Piece: 4096, FlushOps: true, FlushEvery: 2, Compact: "none", CompactEvery: 40},
}

func TestRegressions(t *testing.T) {
	datadriven.RunTest(t, "testdata/regressions", func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "run" {
			t.Fatalf("unknown command %s", d.Cmd)
		}
		var name string
		d.ScanArgs(t, "set", &name)
		o, ok := sets[name]
		if !ok {
			t.Fatalf("unknown set %s", name)
		}
		ops, err := meta.Parse(d.Input)
		if err != nil {
			t.Fatal(err)
		}
		got := meta.Run(ops, o, engine.New(o))
		want := meta.Run(ops, meta.Options{}, meta.NewModel())
		if i := meta.Diff(want, got); i >= 0 {
			t.Errorf("op %d %q: got %q, the model %q", i, ops[i], got[i], want[i])
		}
		var b strings.Builder
		for i, op := range ops {
			if op.Kind == "get" || op.Kind == "floor" || op.Kind == "scan" || op.Kind == "vget" || op.Kind == "vscan" || got[i] != "ok" {
				fmt.Fprintf(&b, "%s: %s\n", op, got[i])
			}
		}
		return b.String()
	})
}
