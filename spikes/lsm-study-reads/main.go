// Command lsm-study-reads measures Pebble's table filters on rock-solid's key shapes, and what
// per-run filters save a lookup over the in-memory runs of internal/lsm.
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"syscall"
	"unsafe"

	"github.com/cockroachdb/pebble/sstable/tablefilters/binaryfuse"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/zeebo/xxh3"
)

const maxPiece = 1 << 20

// filter is one of Pebble's table filter policies, built over a run's keys.
type filter struct {
	name  string
	build func(keys [][]byte) []byte
	may   func(f, k []byte) bool
}

func bloomFilter(bpk uint32) filter {
	return filter{
		name: fmt.Sprintf("bloom%d", bpk),
		build: func(keys [][]byte) []byte {
			w := bloom.FilterPolicy(bpk).NewWriter()
			for _, k := range keys {
				w.AddKey(k)
			}
			d, _, _ := w.Finish()
			return d
		},
		may: bloom.Decoder.MayContain,
	}
}

func fuseFilter(bits int) filter {
	return filter{
		name: fmt.Sprintf("fuse%d", bits),
		build: func(keys [][]byte) []byte {
			w := binaryfuse.FilterPolicy(bits).NewWriter()
			for _, k := range keys {
				w.AddKey(k)
			}
			d, _, _ := w.Finish()
			return d
		},
		may: binaryfuse.Decoder.MayContain,
	}
}

var filters = []filter{bloomFilter(10), bloomFilter(16), fuseFilter(8), fuseFilter(12), fuseFilter(16)}

func filterByName(name string) filter {
	for _, f := range filters {
		if f.name == name {
			return f
		}
	}
	panic("no filter " + name)
}

// Key shapes.

const (
	chunkPrefix = 4 + 8 + core.ChunkIDSize
	extentKey   = chunkPrefix + 8 + 2
	inodePrefix = 1 + 4 + 8
)

func chunkID(i uint32) core.ChunkID { return core.ChunkID{Volume: 1, Inode: 2, Index: i} }

func appendChunkPrefix(dst []byte, c core.ChunkID) []byte {
	dst = binary.BigEndian.AppendUint32(dst, 1)
	dst = binary.BigEndian.AppendUint64(dst, c.Hash())
	b := c.Bytes()
	return append(dst, b[:]...)
}

func appendExtentKey(dst []byte, c core.ChunkID, first uint16) []byte {
	dst = appendChunkPrefix(dst, c)
	dst = binary.BigEndian.AppendUint64(dst, 0)
	return binary.BigEndian.AppendUint16(dst, first)
}

func msKey(dst []byte, inode uint64, tag byte, name []byte) []byte {
	dst = append(dst[:0], 0x02, 0, 0, 0, 1)
	dst = binary.BigEndian.AppendUint64(dst, inode)
	return append(append(dst, tag), name...)
}

func randName(r *rand.Rand, b []byte) []byte {
	b = b[:8+r.IntN(25)]
	for i := range b {
		b[i] = 'a' + byte(r.IntN(26))
	}
	return b
}

// Experiment F: each filter alone, on 200,000 distinct keys of one shape.

func expFilters(n int) {
	r := rand.New(rand.NewPCG(1, 1))
	shapes := []struct {
		name string
		gen  func(i int) []byte
	}{
		{"chunk prefix (28 B)", func(i int) []byte { return appendChunkPrefix(nil, chunkID(uint32(i))) }},
		{"MetaStore keys (14-45 B)", func(i int) []byte {
			if i%2 == 0 {
				return msKey(nil, uint64(i), 'a', nil)
			}
			name := make([]byte, 32)
			return msKey(nil, uint64(i/40)<<40|1, 'd', randName(r, name))
		}},
	}
	fmt.Printf("\n## F: Pebble table filters alone, %d keys per filter; CPU per call on a locked thread\n", n)
	fmt.Printf("%-26s %-8s %9s %9s %10s %10s %10s %8s\n", "keys", "filter", "bits/key", "FPR %", "build ns/k", "absent ns", "present ns", "allocs")
	for _, sh := range shapes {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = sh.gen(i)
		}
		absent := make([][]byte, 1_000_000)
		for i := range absent {
			absent[i] = sh.gen(n + 1 + i)
		}
		var hs uint64
		ch := measure(len(absent), func(i int) { hs += xxh3.Hash(absent[i]) })
		fmt.Printf("%-26s %-8s %v ns per key, sum %d\n", sh.name, "xxh3", ch.ns, hs%10)
		for _, f := range filters {
			var data []byte
			b := measure(1, func(int) { data = f.build(keys) })
			fp := 0
			for _, k := range absent {
				if f.may(data, k) {
					fp++
				}
			}
			for _, k := range keys {
				if !f.may(data, k) {
					panic("false negative")
				}
			}
			sink := 0
			ca := measure(len(absent), func(i int) {
				if f.may(data, absent[i]) {
					sink++
				}
			})
			cp := measure(len(keys), func(i int) {
				if f.may(data, keys[i]) {
					sink++
				}
			})
			fmt.Printf("%-26s %-8s %9.2f %9.3f %10.0f %10.1f %10.1f %8.2f\n", sh.name, f.name,
				float64(len(data)*8)/float64(n), 100*float64(fp)/float64(len(absent)), b.ns/float64(n), ca.ns, cp.ns, ca.allocs+cp.allocs)
		}
	}
}

// Experiment L: lookups over N runs of one index, with and without a filter per run.

// index is N runs, oldest and largest first, held both as one real tree and as one single-run tree
// per run, newest first, so a filter can skip a run.
type index struct {
	real    *lsm.Tree
	runs    []*lsm.Tree
	keys    [][][]byte
	entries int
	bytes   int
}

func buildIndex(sizes []int, entry func(run, i int, k, v []byte) ([]byte, []byte)) *index {
	x := &index{}
	var err error
	x.real, err = lsm.New(lsm.Config{MaxPiece: maxPiece})
	must(err)
	buf := make([]byte, maxPiece)
	var next uint64
	k, v := make([]byte, 0, 64), make([]byte, 0, 64)
	pieces := make([][][]byte, len(sizes))
	roots := make([]disklog.BlobRef, len(sizes))
	for ri, n := range sizes {
		for i := range n {
			k, v = entry(ri, i, k[:0], v[:0])
			must(x.real.Put(k, v))
		}
		x.entries += n
		_, err := x.real.BeginFlush()
		must(err)
		for {
			m, root, err := x.real.NextPiece(buf)
			must(err)
			if m == 0 {
				break
			}
			next++
			ref := disklog.BlobRef{Segment: 1, Block: uint32(next), LSN: next}
			must(x.real.PieceDurable(ref))
			pieces[ri] = append(pieces[ri], bytes.Clone(buf[:m]))
			if root {
				roots[ri] = ref
			}
		}
		_, err = x.real.CheckpointDone()
		must(err)
	}
	if x.real.Runs() != len(sizes) {
		panic("runs merged")
	}
	x.bytes = x.real.RunBytes()
	for ri := len(sizes) - 1; ri >= 0; ri-- {
		t, err := lsm.New(lsm.Config{MaxPiece: maxPiece})
		must(err)
		must(t.LoadRun(roots[ri], pieces[ri]))
		x.runs = append(x.runs, t)
		var it lsm.Iter
		t.Iter(&it)
		var ks [][]byte
		for ok := it.SeekGE(nil); ok; ok = it.Next() {
			ks = append(ks, bytes.Clone(it.Key()))
		}
		x.keys = append(x.keys, ks)
	}
	return x
}

// filtered is an index with one filter per run over each run's keys cut by extract, each cut's
// consecutive duplicates dropped, as RocksDB's prefix extractor feeds its filters; the binary fuse
// builder wants few duplicates, and Pebble's collector drops only consecutive ones.
type filtered struct {
	x       *index
	f       filter
	data    [][]byte
	bytes   int
	keys    int
	probes  int
	visited int
	best    []byte
}

func newFiltered(x *index, f filter, extract func(k []byte, emit func([]byte))) *filtered {
	fx := &filtered{x: x, f: f}
	for _, ks := range x.keys {
		var in [][]byte
		var last [2][]byte
		for _, k := range ks {
			slot := 0
			extract(k, func(p []byte) {
				if last[slot] == nil || !bytes.Equal(p, last[slot]) {
					last[slot] = p
					in = append(in, p)
				}
				slot++
			})
		}
		d := f.build(in)
		fx.data = append(fx.data, d)
		fx.bytes += len(d)
		fx.keys += len(in)
	}
	return fx
}

func (fx *filtered) get(k []byte) bool {
	for i, t := range fx.x.runs {
		if fx.data != nil {
			fx.probes++
			if !fx.f.may(fx.data[i], k) {
				continue
			}
		}
		fx.visited++
		if _, ok := t.Get(k); ok {
			return true
		}
	}
	return false
}

// floorIn is the greatest key <= k that shares k's first n bytes, as the chunk index's lookup
// wants it; a run whose filter rules out the prefix cannot hold such a key.
func (fx *filtered) floorIn(k []byte, n int) bool {
	best := fx.best[:0]
	found := false
	for i, t := range fx.x.runs {
		if fx.data != nil {
			fx.probes++
			if !fx.f.may(fx.data[i], k[:n]) {
				continue
			}
		}
		fx.visited++
		if fk, _, ok := t.Floor(k); ok && bytes.HasPrefix(fk, k[:n]) && (!found || bytes.Compare(fk, best) > 0) {
			best, found = append(best[:0], fk...), true
		}
	}
	fx.best = best
	return found
}

// Experiment S: a scan over N runs, the merge internal/lsm's iterator does on every step.
func expScan(total int, runCounts []int, seeks, steps int) {
	fmt.Printf("\n## S: scans over N size-tiered runs, about %d MetaStore entries; CPU per seek and per step\n", total)
	fmt.Printf("%-14s %3s %10s %10s %7s\n", "index", "N", "seek ns", "next ns", "allocs")
	for _, n := range runCounts {
		var w workload
		for _, cand := range workloads(total) {
			if cand.name == "metastore" {
				w = cand
			}
		}
		classes := n / 3
		smallest := max(4, total/((1<<(2*classes))-1)/4*4)
		x := buildIndex(tierSizes(classes, smallest), w.entry)
		r := rand.New(rand.NewPCG(9, uint64(n)))
		probes := make([][]byte, seeks)
		for i := range probes {
			probes[i] = w.ops[0].gen(r, nil)
		}
		var it lsm.Iter
		x.real.Iter(&it)
		cs := measure(seeks, func(i int) { it.SeekGE(probes[i]) })
		got := 0
		cn := measure(seeks, func(i int) {
			it.SeekGE(probes[i])
			for range steps {
				if it.Next() {
					got++
				}
			}
		})
		fmt.Printf("%-14s %3d %10.0f %10.0f %7.2f\n", w.name, n, cs.ns, (cn.ns-cs.ns)/float64(steps), cn.allocs)
		runtime.GC()
	}
}

// tierSizes is size-tiered compaction's steady shape: three runs per class, classes four times
// apart, the smallest class of smallest entries, oldest and largest first.
func tierSizes(classes, smallest int) []int {
	var s []int
	for c := classes - 1; c >= 0; c-- {
		n := smallest << (2 * c)
		s = append(s, n, n, n)
	}
	return s
}

type workload struct {
	name    string
	entry   func(run, i int, k, v []byte) ([]byte, []byte)
	extract func(k []byte, emit func([]byte))
	ops     []op
}

type op struct {
	name  string
	floor bool
	gen   func(r *rand.Rand, k []byte) []byte
}

func expLookups(total int, runCounts []int, filterNames []string, lookups int, only string) {
	fmt.Printf("\n## L: lookups over N size-tiered runs, about %d entries in all; CPU per lookup on a locked thread\n", total)
	fmt.Printf("%-14s %3s %-10s %-9s %8s %8s %8s %10s %8s %7s %7s\n", "index", "N", "op", "filter", "found %", "ns", "runs", "probes", "filter%", "fkeys/e", "allocs")
	for wi := range len(workloads(0)) {
		for _, n := range runCounts {
			w := workloads(total)[wi]
			if only != "" && !strings.Contains(only, w.name) {
				continue
			}
			classes := n / 3
			smallest := max(4, total/((1<<(2*classes))-1)/4*4)
			sizes := tierSizes(classes, smallest)
			x := buildIndex(sizes, w.entry)
			for _, o := range w.ops {
				probes := make([][]byte, lookups)
				r := rand.New(rand.NewPCG(7, uint64(n)))
				for i := range probes {
					probes[i] = o.gen(r, nil)
				}
				found := 0
				c := measure(lookups, func(i int) {
					var ok bool
					if o.floor {
						var fk []byte
						fk, _, ok = x.real.Floor(probes[i])
						ok = ok && bytes.HasPrefix(fk, probes[i][:chunkPrefix])
					} else {
						_, ok = x.real.Get(probes[i])
					}
					if ok {
						found++
					}
				})
				fmt.Printf("%-14s %3d %-10s %-9s %8.1f %8.0f %8s %10s %8s %7s %7.2f\n", w.name, n, o.name, "real tree", 100*float64(found)/float64(lookups), c.ns, fmt.Sprint(n), "-", "-", "-", c.allocs)
				for _, fname := range append([]string{"none"}, filterNames...) {
					var fx *filtered
					if fname == "none" {
						fx = &filtered{x: x}
					} else {
						fx = newFiltered(x, filterByName(fname), w.extract)
					}
					found = 0
					c := measure(lookups, func(i int) {
						var ok bool
						if o.floor {
							ok = fx.floorIn(probes[i], chunkPrefix)
						} else {
							ok = fx.get(probes[i])
						}
						if ok {
							found++
						}
					})
					pct, per := "-", "-"
					if fx.data != nil {
						pct = fmt.Sprintf("%.2f", 100*float64(fx.bytes)/float64(x.bytes))
						per = fmt.Sprintf("%.2f", float64(fx.keys)/float64(x.entries))
					}
					fmt.Printf("%-14s %3d %-10s %-9s %8.1f %8.0f %8.2f %10.2f %8s %7s %7.2f\n", w.name, n, o.name, fname,
						100*float64(found)/float64(lookups), c.ns, float64(fx.visited)/float64(lookups), float64(fx.probes)/float64(lookups), pct, per, c.allocs)
				}
			}
			fmt.Printf("%-14s %3d runs %v entries, %.1f MB of runs\n", w.name, n, sizes, float64(x.bytes)/1e6)
			runtime.GC()
		}
	}
}

func workloads(total int) []workload {
	val := func(v []byte, r *rand.Rand, n int) []byte {
		for range n {
			v = append(v, byte(r.Uint32()))
		}
		return v
	}
	prefixOnly := func(k []byte, emit func([]byte)) { emit(k[:chunkPrefix]) }
	vr := rand.New(rand.NewPCG(3, 3))
	chunksBefore := 0

	// Whole chunks: every chunk written once, as four 256-block records, so each chunk's four
	// extents sit in the run of the flush that wrote it.
	var wholeBase int
	whole := workload{
		name: "ci-whole",
		entry: func(run, i int, k, v []byte) ([]byte, []byte) {
			if i == 0 {
				wholeBase = chunksBefore
			}
			c := wholeBase + i/4
			chunksBefore = max(chunksBefore, c+1)
			return appendExtentKey(k, chunkID(uint32(c)), uint16(i%4)*256), val(v, vr, 26)
		},
		extract: prefixOnly,
	}
	whole.ops = []op{
		{"read", true, func(r *rand.Rand, k []byte) []byte {
			return appendExtentKey(k, chunkID(uint32(r.IntN(chunksBefore))), uint16(r.IntN(1024)))
		}},
		{"unwritten", true, func(r *rand.Rand, k []byte) []byte {
			return appendExtentKey(k, chunkID(uint32(chunksBefore+r.IntN(1<<20))), uint16(r.IntN(1024)))
		}},
	}

	// Random 4 KiB overwrites of a volume: every entry a one-block extent of a random chunk of
	// 1/32 as many chunks as entries, so large runs hold nearly every chunk and small ones few.
	volume := max(1, total/32)
	over := workload{
		name: "ci-overwrite",
		entry: func(run, i int, k, v []byte) ([]byte, []byte) {
			return appendExtentKey(k, chunkID(uint32(vr.IntN(volume))), uint16(vr.IntN(1024))), val(v, vr, 26)
		},
		extract: prefixOnly,
		ops: []op{{"read", true, func(r *rand.Rand, k []byte) []byte {
			return appendExtentKey(k, chunkID(uint32(r.IntN(volume))), uint16(r.IntN(1024)))
		}}},
	}

	// The MetaStore memory spike's file model: per file an attribute record and a directory entry,
	// 40 files per directory; each run holds the files created since the flush before. Filters
	// hold whole keys and each key's inode prefix, as RocksDB's whole_key_filtering with a prefix
	// extractor does.
	var files, base int
	name := make([]byte, 32)
	ms := workload{
		name: "metastore",
		entry: func(run, i int, k, v []byte) ([]byte, []byte) {
			if i == 0 {
				base = files
			}
			f := base + i/2
			files = max(files, f+1)
			if i%2 == 0 {
				return msKey(k, uint64(f)+(1<<41), 'a', nil), val(v, vr, 92)
			}
			nr := rand.New(rand.NewPCG(uint64(f), 5))
			return msKey(k, uint64(f/40)<<40|1, 'd', randName(nr, name)), val(v, vr, 9)
		},
		extract: func(k []byte, emit func([]byte)) { emit(k); emit(k[:inodePrefix]) },
	}
	ms.ops = []op{
		{"get attr", false, func(r *rand.Rand, k []byte) []byte {
			return msKey(k, uint64(r.IntN(files))+(1<<41), 'a', nil)
		}},
		{"get absent", false, func(r *rand.Rand, k []byte) []byte {
			nm := make([]byte, 32)
			return msKey(k, uint64(r.IntN(files)/40)<<40|1, 'd', randName(r, nm))
		}},
	}
	return []workload{whole, over, ms}
}

func main() {
	exp := flag.String("exp", "f,l", "experiments to run")
	total := flag.Int("total", 2_000_000, "entries per index in experiment L")
	lookups := flag.Int("lookups", 100_000, "lookups per measurement")
	runs := flag.String("runs", "6,12,24", "run counts in experiment L")
	fl := flag.String("filters", "bloom10,fuse8", "filters in experiment L")
	only := flag.String("index", "", "indexes in experiment L, all when empty")
	flag.Parse()
	var runCounts []int
	for _, s := range strings.Split(*runs, ",") {
		var n int
		fmt.Sscan(s, &n)
		runCounts = append(runCounts, n)
	}
	for _, e := range strings.Split(*exp, ",") {
		switch e {
		case "f":
			expFilters(200_000)
		case "s":
			expScan(*total, runCounts, 20_000, 50)
		case "l":
			expLookups(*total, runCounts, strings.Split(*fl, ","), *lookups, *only)
		}
	}
}

// cost is the CPU time and allocations per call of fn, run n times on a locked thread.
type cost struct {
	ns     float64
	allocs float64
}

func measure(n int, fn func(i int)) cost {
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	t0 := threadCPU()
	for i := range n {
		fn(i)
	}
	t1 := threadCPU()
	runtime.ReadMemStats(&ms1)
	return cost{ns: float64(t1-t0) / float64(n), allocs: float64(ms1.Mallocs-ms0.Mallocs) / float64(n)}
}

func threadCPU() int64 {
	var ts syscall.Timespec
	if _, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, 3, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		panic(errno)
	}
	return ts.Nano()
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func init() { runtime.LockOSThread() }
