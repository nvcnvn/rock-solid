package lsm

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var benchEntries = flag.Int("lsm.entries", 1_000_000, "entries in the tree BenchmarkCollection collects with")

// fileModel adds files in the MetaStore memory spike's model: per file a 92-byte attribute record
// and a directory entry, 20 to 50 files per directory, names 8 to 32 bytes, in create order.
func fileModel(t testing.TB, tr *Tree, files int, seed uint64) {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, 0))
	key := func(inode uint64, tag byte, name []byte) []byte {
		k := []byte{0x02, 0, 0, 0, 1}
		k = binary.BigEndian.AppendUint64(k, inode)
		return append(append(k, tag), name...)
	}
	attr := make([]byte, 92)
	name := make([]byte, 32)
	for dir, made := uint64(0), 0; made < files; dir++ {
		for f := range min(20+r.IntN(31), files-made) {
			ino := dir<<40 | uint64(2+f)
			for i := range name {
				name[i] = 'a' + byte(r.IntN(26))
			}
			dirent := binary.BigEndian.AppendUint64(nil, ino)
			if err := tr.Put(key(dir<<40|1, 'd', name[:8+r.IntN(25)]), append(dirent, 1)); err != nil {
				t.Fatal(err)
			}
			for i := range attr {
				attr[i] = byte(r.Uint32())
			}
			if err := tr.Put(key(ino, 'a', nil), attr); err != nil {
				t.Fatal(err)
			}
			made++
		}
	}
}

// A run of the spike's file model takes near the 135 B per file its prefix-compressed blocks of 16
// entries did (MetaStore memory spike, F4).
func TestRunBytesPerFile(t *testing.T) {
	const files = 100_000
	tr := newTree(t, 1<<20)
	fileModel(t, tr, files, 1)
	memtable := float64(tr.MemtableBytes()) / files
	newFakeLog().checkpoint(t, tr, nil)
	perFile := float64(tr.RunBytes()) / files
	t.Logf("memtable %.0f B per file, run %.1f B per file", memtable, perFile)
	if perFile > 145 {
		t.Fatalf("run takes %.1f B per file", perFile)
	}
}

func benchTree(b *testing.B) (*Tree, [][]byte) { return benchTreeRanges(b, 0) }

// benchTreeRanges is four layers of 50,000 random keys, three flushed, each layer also removing
// the given number of narrow ranges between the keys.
func benchTreeRanges(b *testing.B, ranges int) (*Tree, [][]byte) {
	tr := newTree(b, 1<<20)
	r := rand.New(rand.NewPCG(9, 9))
	var keys [][]byte
	f := newFakeLog()
	for layer := range 4 {
		for range 50_000 {
			k := binary.BigEndian.AppendUint64(nil, r.Uint64())
			keys = append(keys, k)
			put(b, tr, nil, string(k), "value-of-some-length")
		}
		for range ranges {
			lo := r.Uint64() >> 1
			if err := tr.DeleteRange(binary.BigEndian.AppendUint64(nil, lo), binary.BigEndian.AppendUint64(nil, lo+1<<20)); err != nil {
				b.Fatal(err)
			}
		}
		if layer < 3 {
			f.checkpoint(b, tr, nil)
		}
	}
	return tr, keys
}

func BenchmarkGetWithRanges(b *testing.B) {
	tr, keys := benchTreeRanges(b, 1000)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, ok := tr.Get(keys[i%len(keys)]); !ok {
			b.Fatal("missing key")
		}
		i += 7919
	}
}

func BenchmarkFloorWithRanges(b *testing.B) {
	tr, keys := benchTreeRanges(b, 1000)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, _, ok := tr.Floor(keys[i%len(keys)]); !ok {
			b.Fatal("missing key")
		}
		i += 7919
	}
}

func BenchmarkIterNextWithRanges(b *testing.B) {
	tr, _ := benchTreeRanges(b, 1000)
	var it Iter
	tr.Iter(&it)
	it.SeekGE(nil)
	b.ReportAllocs()
	for b.Loop() {
		if !it.Next() {
			it.SeekGE(nil)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	tr, keys := benchTree(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, ok := tr.Get(keys[i%len(keys)]); !ok {
			b.Fatal("missing key")
		}
		i += 7919
	}
}

func BenchmarkFloor(b *testing.B) {
	tr, keys := benchTree(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, _, ok := tr.Floor(keys[i%len(keys)]); !ok {
			b.Fatal("missing key")
		}
		i += 7919
	}
}

func BenchmarkPut(b *testing.B) {
	tr, keys := benchTree(b)
	v := []byte("value-of-other-length")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if err := tr.Put(keys[i%len(keys)], v); err != nil {
			b.Fatal(err)
		}
		i += 7919
	}
}

func BenchmarkIterNext(b *testing.B) {
	tr, _ := benchTree(b)
	var it Iter
	tr.Iter(&it)
	it.SeekGE(nil)
	b.ReportAllocs()
	for b.Loop() {
		if !it.Next() {
			it.SeekGE(nil)
		}
	}
}

// BenchmarkIterNextRuns steps a scan over 6 and 24 runs of 240,000 random keys in all; the bar is
// a step over 24 runs under twice the step over 6 (LSM read path spike: 95 and 360 ns linear).
func BenchmarkIterNextRuns(b *testing.B) {
	for _, n := range []int{6, 24} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			tr := newTree(b, 1<<20)
			r := rand.New(rand.NewPCG(9, uint64(n)))
			f := newFakeLog()
			for range n {
				for range 240_000 / n {
					put(b, tr, nil, string(binary.BigEndian.AppendUint64(nil, r.Uint64())), "value-of-some-length")
				}
				f.checkpoint(b, tr, nil)
			}
			if len(tr.runs) != n {
				b.Fatalf("%d runs, want %d", len(tr.runs), n)
			}
			var it Iter
			tr.Iter(&it)
			it.SeekGE(nil)
			b.ReportAllocs()
			for b.Loop() {
				if !it.Next() {
					it.SeekGE(nil)
				}
			}
		})
	}
}

// BenchmarkCollection times a full collection with a tree of -lsm.entries live entries, half in
// runs; the bar is under 1 ms at 20 million (Go memtable spike).
func BenchmarkCollection(b *testing.B) {
	tr := newTree(b, 1<<20)
	r := rand.New(rand.NewPCG(4, 4))
	k := make([]byte, 16)
	for i := range *benchEntries {
		binary.BigEndian.PutUint64(k, r.Uint64())
		binary.BigEndian.PutUint64(k[8:], uint64(i))
		put(b, tr, nil, string(k), "v")
		if i == *benchEntries/2 {
			newFakeLog().checkpoint(b, tr, nil)
		}
	}
	runtime.GC()
	var worst time.Duration
	for b.Loop() {
		start := time.Now()
		runtime.GC()
		worst = max(worst, time.Since(start))
	}
	b.ReportMetric(float64(worst.Microseconds())/1000, "worst-ms")
	runtime.KeepAlive(tr)
}

// BenchmarkCollectionUnderGarbage looks up keys in a memtable on the off-heap slab, beside a
// goroutine making 256 MB/s of short-lived request garbage, with GOMEMLIMIT 512 MiB above the live
// heap, as the daemon sets it: in a tree of one entry, then of -lsm.entries. From runtime/metrics
// over each run, gc-cpu-% is collection's share of the process's CPU, as the Go memtable spike
// counted it, and gc-cycles the collections. The bar is 0.0% for the tree: the full tree's share
// above the one-entry tree's, which is the garbage's own.
func BenchmarkCollectionUnderGarbage(b *testing.B) {
	b.Run("entries=1", func(b *testing.B) { collectUnderGarbage(b, 1) })
	b.Run(fmt.Sprintf("entries=%d", *benchEntries), func(b *testing.B) { collectUnderGarbage(b, *benchEntries) })
}

func collectUnderGarbage(b *testing.B, entries int) {
	tr := newTree(b, 1<<20)
	r := rand.New(rand.NewPCG(5, 5))
	keys := make([]byte, 8*entries)
	for i := 0; i < len(keys); i += 8 {
		binary.BigEndian.PutUint64(keys[i:], r.Uint64())
		put(b, tr, nil, string(keys[i:i+8]), "value-of-some-length")
	}
	runtime.GC()
	m := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/gc/cycles/total:gc-cycles"}, {Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(m)
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(int64(m[3].Value.Uint64()) + 512<<20))
	gc0, total0, cycles0 := m[0].Value.Float64(), m[1].Value.Float64(), m[2].Value.Uint64()
	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		g := rand.New(rand.NewPCG(6, 6))
		ring := make([][]byte, 4096)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for i := 0; !stop.Load(); {
			<-tick.C
			for n := 0; n < 256<<10; i++ {
				ring[i%len(ring)] = make([]byte, 256+g.IntN(3841))
				n += len(ring[i%len(ring)])
			}
		}
	})
	i := 0
	for b.Loop() {
		k := i % entries * 8
		if _, ok := tr.Get(keys[k : k+8]); !ok {
			b.Fatal("missing key")
		}
		i += 7919
	}
	stop.Store(true)
	wg.Wait()
	metrics.Read(m)
	b.ReportMetric(100*(m[0].Value.Float64()-gc0)/(m[1].Value.Float64()-total0), "gc-cpu-%")
	b.ReportMetric(float64(m[2].Value.Uint64()-cycles0), "gc-cycles")
	runtime.KeepAlive(tr)
}
