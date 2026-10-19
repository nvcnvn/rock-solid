// Command lsm-study-compaction replays checkpoint flush-size sequences through internal/lsm's
// bucket rule, a port of RocksDB's universal picker and Pebble's leveled compaction, and prints
// run counts, bytes rewritten and space amplification for each.
package main

import (
	"flag"
	"fmt"
	"runtime"
	"strings"
)

var (
	mode    = flag.String("mode", "model", "model, real or pebble")
	keys    = flag.Int("keys", 1_100_000, "live keys in the index")
	entryB  = flag.Float64("entry", 40, "bytes per entry in a run (model)")
	valueB  = flag.Int("value", 384, "value bytes (real, pebble); keys are 16 B")
	small   = flag.Int("small", 33_000, "entries in a small flush")
	large   = flag.Int("large", 825_000, "entries in a large flush")
	flushes = flag.Int("flushes", 2000, "flushes per sequence")
	only    = flag.String("seq", "", "comma list of sequences to run (default all)")
	lbase   = flag.String("lbase", "64,4", "Pebble LBaseMaxBytes in MiB, comma list")
	seed    = flag.Uint64("seed", 1, "workload seed")
)

func main() {
	flag.Parse()
	fmt.Printf("# go %s, GOMAXPROCS %d, mode %s, keys %d, small %d, large %d, flushes %d, seed %d\n",
		runtime.Version(), runtime.GOMAXPROCS(0), *mode, *keys, *small, *large, *flushes, *seed)
	ws := sequences(*keys, *entryB, *small, *large, *flushes, *seed)
	want := func(w workload) bool { return *only == "" || strings.Contains(","+*only+",", ","+w.name+",") }
	printHeader()
	for _, w := range ws {
		if !want(w) {
			continue
		}
		switch *mode {
		case "model":
			fmt.Printf("# %s: live %.1f MB, small flush %.2f MB, large %.2f MB\n", w.name,
				float64(w.keys)*w.entryB/1e6, float64(*small)*w.entryB/1e6, float64(*large)*w.entryB/1e6)
			for _, p := range policies(float64(*small) * w.entryB) {
				simulate(w, p).print()
			}
		case "real":
			realTree(w, *valueB, *seed).print()
		case "pebble":
			for _, s := range strings.Split(*lbase, ",") {
				var mb int64
				fmt.Sscan(s, &mb)
				pebbleRun(w, *valueB, mb<<20, *seed).print()
			}
		}
	}
}
