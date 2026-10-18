// Command lsm-runs measures run counts, lookup costs and the cost of removals in the LSM tree of
// internal/lsm and the chunk index of internal/chunkstore, and the fixes prototyped in lsmx and cix.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
	"github.com/nvcnvn/rock-solid/spikes/lsm-runs/lsmx"
)

var (
	exp        = flag.String("exp", "all", "experiments to run: comma list of a1,a2,a3,b,c or all")
	scale      = flag.Float64("scale", 1, "multiplies the workload sizes")
	lookupsN   = flag.Int("lookups", 200_000, "lookups per lookup measurement")
	maxFlushes = flag.Int("maxflushes", 1000, "flushes B waits at most for removals to be merged away")
)

func main() {
	flag.Parse()
	want := func(name string) bool { return *exp == "all" || strings.Contains(","+*exp+",", ","+name+",") }
	fmt.Printf("# go %s, GOMAXPROCS %d, scale %.2f\n", runtime.Version(), runtime.GOMAXPROCS(0), *scale)
	if want("a3") {
		a3()
	}
	if want("a1") {
		a1()
	}
	if want("a2") {
		a2()
	}
	if want("b") {
		b()
	}
	if want("c") {
		c()
	}
}

func scaled(n int) int { return max(1, int(float64(n)**scale)) }

// runTrace records the run count after each flush and its compactions.
type runTrace struct {
	max, sum, n, maxSmall int
	hist                  map[int]int
}

func (r *runTrace) add(runs, small int) {
	if r.hist == nil {
		r.hist = map[int]int{}
	}
	r.max = max(r.max, runs)
	r.maxSmall = max(r.maxSmall, small)
	r.sum += runs
	r.n++
	r.hist[runs]++
}

func (r *runTrace) String() string {
	var b strings.Builder
	for k := 1; k <= r.max; k++ {
		if r.hist[k] > 0 {
			fmt.Fprintf(&b, " %d:%d", k, r.hist[k])
		}
	}
	return fmt.Sprintf("runs mean %.1f max %d, runs under 1 MiB max %d, histogram (runs:flushes)%s", float64(r.sum)/float64(r.n), r.max, r.maxSmall, b.String())
}

// smallRuns counts runs under 1 MiB through the public API: it is not exported, so the harness
// estimates it from the run count and size only when every run is small.
func smallRunsReal(t *lsm.Tree) int {
	if t.RunBytes() < 1<<20 {
		return t.Runs()
	}
	return -1
}

// a3: what a get and a floor cost over k equal runs of 1M entries in all, no Bloom filters.
func a3() {
	fmt.Println("\n## A3 lookup cost against run count (internal/lsm, 1M random 16-byte keys, 20-byte values)")
	total := scaled(1_000_000)
	for _, k := range []int{1, 2, 4, 8, 16, 32} {
		tree, err := lsm.New(lsm.Config{MaxPiece: maxPiece})
		must(err)
		r := rand.New(rand.NewPCG(3, uint64(k)))
		keys := make([][]byte, 0, total)
		var w written
		v := []byte("value-of-some-length")
		for i := range total {
			key := binary.BigEndian.AppendUint64(nil, r.Uint64())
			key = binary.BigEndian.AppendUint64(key, uint64(i))
			keys = append(keys, key)
			must(tree.Put(key, v))
			if (i+1)%(total/k) == 0 {
				w.checkpoint(tree)
			}
		}
		w.checkpoint(tree)
		r.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		get := measure(*lookupsN, func(i int) {
			if _, ok := tree.Get(keys[i%len(keys)]); !ok {
				panic("missing")
			}
		})
		miss := measure(*lookupsN, func(i int) {
			key := keys[i%len(keys)]
			key[len(key)-1] ^= 0x80
			tree.Get(key)
			key[len(key)-1] ^= 0x80
		})
		floor := measure(*lookupsN, func(i int) {
			if _, _, ok := tree.Floor(keys[i%len(keys)]); !ok {
				panic("missing")
			}
		})
		fmt.Printf("A3 runs=%-2d run_MB=%.1f  get_hit %.0f ns %.2f allocs  get_miss %.0f ns  floor %.0f ns %.2f allocs\n",
			tree.Runs(), float64(tree.RunBytes())/1e6, get.ns, get.allocs, miss.ns, floor.ns, floor.allocs)
	}
}

// a1: a MetaStore-like index under the file model, flushed every flushFiles creates, with
// overwrites of attributes and removals of files between flushes, compacting after each flush.
func a1() {
	fmt.Println("\n## A1 MetaStore-like index (file model of the MetaStore memory spike; 20% attribute overwrites, 2% removals per flush)")
	for _, flushFiles := range []int{2_000, 20_000, 200_000} {
		files := scaled(2_000_000)
		tree, err := lsm.New(lsm.Config{MaxPiece: maxPiece})
		must(err)
		r := rand.New(rand.NewPCG(1, uint64(flushFiles)))
		var w written
		var tr runTrace
		type file struct{ dirent, attr []byte }
		var live []file
		attr := make([]byte, 92)
		name := make([]byte, 32)
		key := func(inode uint64, tag byte, name []byte) []byte {
			k := []byte{0x02, 0, 0, 0, 1}
			k = binary.BigEndian.AppendUint64(k, inode)
			return append(append(k, tag), name...)
		}
		merges := 0
		made := 0
		for dir := uint64(0); made < files; {
			batch := min(flushFiles, files-made)
			for n := 0; n < batch; dir++ {
				for f := range min(20+r.IntN(31), batch-n) {
					ino := dir<<40 | uint64(2+f)
					for i := range name {
						name[i] = 'a' + byte(r.IntN(26))
					}
					dk := key(dir<<40|1, 'd', name[:8+r.IntN(25)])
					must(tree.Put(dk, binary.BigEndian.AppendUint64([]byte{1}, ino)))
					for i := range attr {
						attr[i] = byte(r.Uint32())
					}
					ak := key(ino, 'a', nil)
					must(tree.Put(ak, attr))
					live = append(live, file{dk, ak})
					n++
				}
			}
			made += batch
			for range batch / 5 {
				f := live[r.IntN(len(live))]
				attr[0]++
				must(tree.Put(f.attr, attr))
			}
			for range batch / 50 {
				i := r.IntN(len(live))
				must(tree.Delete(live[i].dirent))
				must(tree.Delete(live[i].attr))
				live[i] = live[len(live)-1]
				live = live[:len(live)-1]
			}
			w.checkpoint(tree)
			merges += compactReal(tree)
			tr.add(tree.Runs(), smallRunsReal(tree))
		}
		get := measure(*lookupsN, func(i int) {
			if _, ok := tree.Get(live[r.IntN(len(live))].attr); !ok {
				panic("missing attr")
			}
		})
		floor := measure(*lookupsN, func(i int) {
			tree.Floor(live[r.IntN(len(live))].dirent)
		})
		fmt.Printf("A1 files_per_flush=%d flushes=%d files=%d run_MB=%.1f final_runs=%d merges=%d write_amp=%.2f\n   %s\n   get %.0f ns %.2f allocs, floor %.0f ns %.2f allocs\n",
			flushFiles, tr.n, len(live), float64(tree.RunBytes())/1e6, tree.Runs(), merges, float64(w.all)/float64(w.mem), tr.String(), get.ns, get.allocs, floor.ns, floor.allocs)
	}
}

// a2: the chunk index of a disk filled with whole-chunk writes, 16 chunks (64 MiB) per flush, each
// chunk four 1 MiB records, optionally with a share of random 4 KiB overwrites.
func a2() {
	fmt.Println("\n## A2 chunk index under whole-chunk writes (16 chunks of 4 records per 64 MiB flush)")
	for _, small := range []int{0, 164} {
		e := realEnv()
		r := rand.New(rand.NewPCG(2, uint64(small)))
		var d disk
		var tr runTrace
		chunks := scaled(262_144)
		merges := 0
		for written := 0; written < chunks; {
			for range 16 {
				writeWhole(e, &d, chunk(uint32(written)), uint64(written+1), 256)
				written++
			}
			for range small {
				ci := r.IntN(written)
				e.apply(d.record(disklog.WriteEntry(1, chunk(uint32(ci)), uint64(1_000_000+tr.n), uint16(r.IntN(core.BlocksPerChunk)), 1)))
			}
			e.flush()
			merges += e.compact()
			sm := -1
			if e.runBytes() < 1<<20 {
				sm = e.runs()
			}
			tr.add(e.runs(), sm)
		}
		look := measure(*lookupsN, func(int) {
			if !e.lookup(chunk(uint32(r.IntN(chunks))), uint16(r.IntN(core.BlocksPerChunk))) {
				panic("missing extent")
			}
		})
		fmt.Printf("A2 chunks=%d small_writes_per_flush=%d flushes=%d run_MB=%.1f final_runs=%d merges=%d write_amp=%.2f\n   %s\n   lookup %.0f ns %.2f allocs\n",
			chunks, small, tr.n, float64(e.runBytes())/1e6, e.runs(), merges, float64(e.w.all)/float64(e.w.mem), tr.String(), look.ns, look.allocs)
	}
}

func variants() []func() *env {
	return []func() *env{
		realEnv,
		func() *env { return xEnv("copy", lsmx.Config{}, 0) },
		func() *env { return xEnv("cursor", lsmx.Config{CursorFloor: true}, 0) },
		func() *env { return xEnv("range", lsmx.Config{CursorFloor: true}, 2) },
		func() *env {
			return xEnv("density", lsmx.Config{DensityMerge: 0.5, DensityMinRemovals: 256}, 0)
		},
		func() *env {
			return xEnv("density+cursor", lsmx.Config{CursorFloor: true, DensityMerge: 0.5, DensityMinRemovals: 256}, 0)
		},
	}
}

// lookupChunk is the CPU and steps per lookup over every block of the chunk.
func lookupChunk(e *env, c core.ChunkID, reps int) (cost, float64) {
	s0 := e.steps()
	k := measure(reps*core.BlocksPerChunk, func(i int) {
		if !e.lookup(c, uint16(i%core.BlocksPerChunk)) {
			panic("missing extent")
		}
	})
	return k, float64(e.steps()-s0) / float64(reps*core.BlocksPerChunk)
}

// b: a chunk of 1,024 one-block extents rewritten whole, among background chunks written whole.
func b() {
	fmt.Println("\n## B removals: a chunk of 1,024 one-block extents rewritten whole")
	background := scaled(20_000)
	target := chunk(1 << 30)
	for _, flushed := range []bool{true, false} {
		for _, perRecord := range []int{1024, 256} {
			fmt.Printf("\nB fragmented_extents_flushed=%v rewrite_record_blocks=%d background_chunks=%d\n", flushed, perRecord, background)
			for _, mk := range variants() {
				e := mk()
				var d disk
				for i := range background {
					writeWhole(e, &d, chunk(uint32(i)), 1, 256)
				}
				for blk := range core.BlocksPerChunk {
					e.apply(d.record(disklog.WriteEntry(1, target, 1, uint16(blk), 1)))
				}
				if flushed {
					e.flush()
					e.compact()
				}
				before, _ := lookupChunk(e, target, 2)
				t0 := threadCPU()
				writeWhole(e, &d, target, 2, perRecord)
				rewrite := float64(threadCPU()-t0) / 1e3
				mem, memSteps := lookupChunk(e, target, 1)
				t0 = threadCPU()
				e.apply(d.record(disklog.WriteEntry(1, target, 3, 700, 1)))
				small := float64(threadCPU()-t0) / 1e3
				st0, w0 := e.stats(), e.w
				e.flush()
				e.compact()
				runs := e.runs()
				fl, flSteps := lookupChunk(e, target, 1)
				next := background
				flushes := 0
				clean := -1
				for ; flushes < *maxFlushes; flushes++ {
					if isClean(e, target, before.ns) {
						clean = flushes
						break
					}
					for range 16 {
						writeWhole(e, &d, chunk(uint32(next)), 1, 256)
						next++
					}
					e.flush()
					e.compact()
				}
				after, afterSteps := lookupChunk(e, target, 2)
				st := e.stats()
				fmt.Printf("B %-15s before %6.0f ns | rewrite %7.0f us, 4K write after %6.1f us | memtable %8.0f ns %6.1f steps %.1f allocs | flushed (%d runs) %8.0f ns %6.1f steps | clean after %4d flushes: %5.0f ns %4.1f steps | merged from flush on %6.1f MB (density merges %d) run_MB %.1f\n",
					e.name, before.ns, rewrite, small, mem.ns, memSteps, mem.allocs, runs, fl.ns, flSteps, clean, after.ns, afterSteps,
					mergedMB(e, w0, st0), st.DensityPicks-st0.DensityPicks, float64(e.runBytes())/1e6)
			}
		}
	}
}

// mergedMB is what compactions wrote since w0 and st0: counted by the copy, and for the real tree
// the run pieces flushes wrote beyond the memtables.
func mergedMB(e *env, w0 written, st0 lsmx.Stats) float64 {
	if e.name == "internal" {
		return float64(e.w.all-w0.all-(e.w.mem-w0.mem)) / 1e6
	}
	return float64(e.stats().MergeOut-st0.MergeOut) / 1e6
}

// isClean reports whether a lookup of the chunk's last block is back to about one search: by
// counted steps on the copy, by time on the real tree, which does not count them.
func isClean(e *env, c core.ChunkID, before float64) bool {
	if e.name == "internal" {
		k := measure(16, func(int) { e.lookup(c, 1023) })
		return k.ns < max(3*before, 3000)
	}
	s0 := e.steps()
	e.lookup(c, 1023)
	return e.steps()-s0 <= 2
}

// c: a block volume's chunks under random 4 KiB writes and whole-chunk rewrites, 64 MiB per flush.
func c() {
	fmt.Println("\n## C mixed block-volume workload: 512 chunks, per 64 MiB flush 4,096 random 4 KiB writes and 12 whole-chunk rewrites")
	chunks, flushes := 512, scaled(100)
	for _, mk := range variants() {
		e := mk()
		r := rand.New(rand.NewPCG(5, 5))
		var d disk
		for i := range chunks {
			writeWhole(e, &d, chunk(uint32(i)), 1, 256)
		}
		e.flush()
		e.compact()
		var smallCPU, wholeCPU int64
		var smallN, wholeN int
		var lookNs, lookMax, lookSteps float64
		var looks int
		var tr runTrace
		ver := uint64(2)
		w0, st0 := e.w, e.stats()
		for f := range flushes {
			t0 := threadCPU()
			for range 4096 {
				e.apply(d.record(disklog.WriteEntry(1, chunk(uint32(r.IntN(chunks))), ver, uint16(r.IntN(core.BlocksPerChunk)), 1)))
				ver++
			}
			t1 := threadCPU()
			smallCPU += t1 - t0
			smallN += 4096
			for range 12 {
				writeWhole(e, &d, chunk(uint32(r.IntN(chunks))), ver, 256)
				ver++
			}
			wholeCPU += threadCPU() - t1
			wholeN += 48
			if f%10 == 9 {
				s0 := e.steps()
				k := measure(4096, func(int) {
					e.lookup(chunk(uint32(r.IntN(chunks))), uint16(r.IntN(core.BlocksPerChunk)))
				})
				lookNs += k.ns
				lookMax = max(lookMax, k.ns)
				lookSteps += float64(e.steps()-s0) / 4096
				looks++
			}
			e.flush()
			e.compact()
			tr.add(e.runs(), -1)
		}
		st := e.stats()
		fmt.Printf("C %-15s 4K write %5.1f us, rewrite record %6.1f us | lookup mean %6.0f ns max %6.0f ns %5.1f steps | runs mean %.1f max %d | flushed %6.1f MB merged %6.1f MB (density merges %d) run_MB %.1f\n",
			e.name, float64(smallCPU)/float64(smallN)/1e3, float64(wholeCPU)/float64(wholeN)/1e3, lookNs/float64(looks), lookMax, lookSteps/float64(looks),
			float64(tr.sum)/float64(tr.n), tr.max, float64(e.w.mem-w0.mem)/1e6, mergedMB(e, w0, st0), st.DensityPicks, float64(e.runBytes())/1e6)
	}
}
