// Command lsm-study-testing runs a metamorphic test over internal/lsm: each
// seed's ops run under several option sets and must read the same as a map
// and as each other. With -tags planted it also runs planted-bug copies.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/engine"
	"github.com/nvcnvn/rock-solid/spikes/lsm-study-testing/meta"
)

type maker func(meta.Options) meta.Engine

var engines = map[string]maker{"lsm": engine.New}

var plantNames []string

var sets = []meta.Options{
	{Name: "F-memtable-only", Piece: 1 << 20},
	{Name: "A-ops", Piece: 1 << 20, FlushOps: true, Compact: "ops"},
	{Name: "B-4K-eager", Piece: 4096, FlushOps: true, FlushEvery: 40, Compact: "eager"},
	{Name: "C-256B-nocompact", Piece: 256, FlushOps: true, FlushEvery: 10, Compact: "none"},
	{Name: "D-512B-deferred-absorb", Piece: 512, FlushOps: true, FlushEvery: 25, Compact: "deferred", Absorb: 4},
	{Name: "E-1K-fail-restart", Piece: 1024, FlushOps: true, FlushEvery: 30, Compact: "eager", FailFirst: 1, RestartEach: true},
	{Name: "G-4K-40-runs-then-merge", Piece: 4096, FlushOps: true, FlushEvery: 2, Compact: "none", CompactEvery: 40},
}

func main() {
	seeds := flag.Int("seeds", 50, "seeds to run, from 0")
	nops := flag.Int("ops", 3000, "ops per seed")
	crc := flag.Bool("crc", false, "measure CRC32C over a compaction's input pieces against the compaction")
	reduce := flag.Bool("reduce", true, "reduce the first failure of each planted bug")
	plantedOnly := flag.Bool("planted-only", false, "run only the planted-bug copies")
	flag.Parse()
	if *crc {
		runCRC()
		return
	}
	fmt.Printf("# %s, internal/lsm via replace, %d seeds x %d ops, option sets:\n", runtime.Version(), *seeds, *nops)
	for _, s := range sets {
		fmt.Printf("#   %+v\n", s)
	}
	failed := false
	if !*plantedOnly {
		start := time.Now()
		failed = runEngine("lsm", engines["lsm"], *seeds, *nops, false)
		runs := *seeds * (len(sets) + 1)
		fmt.Printf("    writes run between a flush's pieces: %d; %d runs of %d ops in %v, %v per run\n",
			meta.Absorbed, runs, *nops, time.Since(start).Round(time.Millisecond), (time.Since(start) / time.Duration(runs)).Round(time.Millisecond))
	}
	for _, name := range plantNames {
		runEngine(name, engines[name], *seeds, *nops, *reduce)
	}
	if failed {
		os.Exit(1)
	}
}

func runEngine(name string, mk maker, seeds, nops int, reduce bool) bool {
	type agg struct {
		vsModel, vsMem int
		stats          []string
	}
	per := make([]agg, len(sets))
	seedsVsModel, seedsVsMemOnly := 0, 0
	reduced := false
	for seed := range seeds {
		ops := meta.Generate(uint64(seed), nops)
		want := meta.Run(ops, meta.Options{Name: "model"}, meta.NewModel())
		hists := make([][]string, len(sets))
		anyModel, anyMem := false, false
		for i, o := range sets {
			e := mk(o)
			hists[i] = meta.Run(ops, o, e)
			if seed < 3 {
				per[i].stats = append(per[i].stats, e.Stats())
			}
			if d := meta.Diff(want, hists[i]); d >= 0 {
				per[i].vsModel++
				anyModel = true
				if name == "lsm" || per[i].vsModel == 1 {
					fmt.Printf("[FAIL] %s seed=%d set=%s op#%d %q got=%q want=%q\n", name, seed, o.Name, d, ops[d], hists[i][d], want[d])
				}
				if reduce && !reduced {
					reduced = true
					printReduced(name, seed, ops, o, mk)
				}
			}
		}
		for i := 1; i < len(sets); i++ {
			if meta.Diff(hists[0], hists[i]) >= 0 {
				per[i].vsMem++
				anyMem = true
			}
		}
		if anyModel {
			seedsVsModel++
		}
		if anyMem {
			seedsVsMemOnly++
		}
	}
	status := "[OK ]"
	if seedsVsModel > 0 {
		status = "[FAIL]"
	}
	if name != "lsm" {
		status = "[OK ]"
		if seedsVsModel == 0 {
			status = "[MISS]"
		}
	}
	fmt.Printf("%s %s: %d of %d seeds differ from the model, %d differ between option sets (no model)\n", status, name, seedsVsModel, seeds, seedsVsMemOnly)
	for i, o := range sets {
		fmt.Printf("    %-24s vs model %3d  vs F %3d", o.Name, per[i].vsModel, per[i].vsMem)
		if name == "lsm" && len(per[i].stats) > 0 {
			fmt.Printf("  seed0: %s", per[i].stats[0])
		}
		fmt.Println()
	}
	return seedsVsModel > 0
}

func printReduced(name string, seed int, ops []meta.Op, o meta.Options, mk maker) {
	fails := func(c []meta.Op) bool {
		want := meta.Run(c, meta.Options{}, meta.NewModel())
		return meta.Diff(want, meta.Run(c, o, mk(o))) >= 0
	}
	want := meta.Run(ops, meta.Options{}, meta.NewModel())
	small := meta.Reduce(ops[:meta.Diff(want, meta.Run(ops, o, mk(o)))+1], fails)
	want = meta.Run(small, meta.Options{}, meta.NewModel())
	got := meta.Run(small, o, mk(o))
	d := meta.Diff(want, got)
	var b strings.Builder
	fmt.Fprintf(&b, "    # %s: seed %d under %s reduced from %d ops to %d\n", name, seed, o.Name, len(ops), len(small))
	from := 0
	if len(small) > 30 {
		from = len(small) - 6
		fmt.Fprintf(&b, "    # ... the last %d:\n", len(small)-from)
	}
	for i, op := range small[from:] {
		i += from
		fmt.Fprintf(&b, "    %-22s # %s", op, got[i])
		if i == d {
			fmt.Fprintf(&b, "   <- want %s", want[i])
		}
		b.WriteString("\n")
	}
	fmt.Print(b.String())
}

func runCRC() {
	fmt.Printf("# %s, CRC32C over a compaction's input pieces against the compaction itself\n", runtime.Version())
	for _, c := range []struct{ runs, bytes, piece int }{{4, 1 << 20, 64 << 10}, {4, 4 << 20, 64 << 10}, {4, 96 << 10, 4096}} {
		merge, crc, total := engine.CRCShare(c.runs, c.bytes, c.piece, 5)
		fmt.Printf("runs=%d x %d KiB, piece %d B: %d KiB of pieces, compaction %v, CRC32C %v, CRC share %.1f%%\n",
			c.runs, c.bytes>>10, c.piece, total>>10, merge, crc, 100*float64(crc)/float64(merge))
	}
}
