package lsm

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

// pickArgs reads a pick command's runs, newest first in MiB, the oldest held ones, and the picker
// settings it changes from the defaults.
func pickArgs(t *testing.T, d *datadriven.TestData) ([]pickRun, pickOpts) {
	o := defaultOpts()
	var mibs []float64
	held := 0
	num := func(a datadriven.CmdArg, i int) float64 {
		if len(a.Vals) <= i {
			d.Fatalf(t, "%s needs a value", a.Key)
		}
		f, err := strconv.ParseFloat(a.Vals[i], 64)
		if err != nil {
			d.Fatalf(t, "%s: %v", a.Key, err)
		}
		return f
	}
	for _, a := range d.CmdArgs {
		switch a.Key {
		case "mib":
			for i := range a.Vals {
				mibs = append(mibs, num(a, i))
			}
		case "held":
			held = int(num(a, 0))
		case "trigger":
			o.trigger = int(num(a, 0))
		case "ratio":
			o.ratio = num(a, 0)
		case "size-amp":
			o.sizeAmp = num(a, 0)
		case "buffer-mib":
			o.buffer = int(num(a, 0) * mib)
		case "limit":
			o.limit = int(num(a, 0))
		case "max-mib":
			o.maxBytes = int(num(a, 0) * mib)
		default:
			d.Fatalf(t, "unknown argument %q", a.Key)
		}
	}
	runs := make([]pickRun, len(mibs))
	for i, s := range mibs {
		runs[i] = pickRun{bytes: int(math.Round(s * mib)), held: i >= len(mibs)-held}
	}
	return runs, o
}

// The picker cases of testdata/compaction_picker: each pick prints the rule that fires and the
// runs it merges, then picks again with those runs held until nothing is due, so two merges never
// share an input.
func TestPickerCases(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker", func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "pick" {
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		runs, o := pickArgs(t, d)
		var b strings.Builder
		for {
			i, j, r := pick(runs, o)
			if r == ruleNone {
				b.WriteString("none\n")
				return b.String()
			}
			if j-i < 2 {
				d.Fatalf(t, "%v picked one run [%d, %d)", r, i, j)
			}
			for k := i; k < j; k++ {
				if runs[k].held {
					d.Fatalf(t, "%v picked [%d, %d), which holds run %d", r, i, j, k)
				}
				runs[k].held = true
			}
			fmt.Fprintf(&b, "%v [%d, %d)\n", r, i, j)
		}
	})
}
