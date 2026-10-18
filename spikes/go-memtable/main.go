// Go memtable spike: bytes per file, speed and garbage-collector cost of three stage-1 memtable
// layouts holding the MetaStore memory spike's file model.
//
// usage: go-memtable gen files=N names=PATH data=PATH probes=PATH
//
//	go-memtable run variant=slices|arena|pages data=PATH probes=PATH [churn_secs=10] [restore=1] [snap_updates=100000]
package main

import (
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func args() map[string]string {
	kv := map[string]string{}
	for _, a := range os.Args[2:] {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			panic("bad arg " + a)
		}
		kv[k] = v
	}
	return kv
}

func intArg(kv map[string]string, k string, def int) int {
	if v, ok := kv[k]; ok {
		n, err := strconv.Atoi(v)
		check(err)
		return n
	}
	return def
}

func rssBytes() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	check(err)
	f := strings.Fields(string(data))
	pages, _ := strconv.ParseInt(f[1], 10, 64)
	return pages * int64(os.Getpagesize())
}

var metricNames = []string{
	"/memory/classes/heap/objects:bytes",
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/total:cpu-seconds",
	"/gc/cycles/total:gc-cycles",
	"/sched/pauses/total/gc:seconds",
}

type snap struct {
	heap, gcCPU, totalCPU float64
	cycles                uint64
	pauses                *metrics.Float64Histogram
}

func read() snap {
	s := make([]metrics.Sample, len(metricNames))
	for i, n := range metricNames {
		s[i].Name = n
	}
	metrics.Read(s)
	return snap{
		heap:     float64(s[0].Value.Uint64()),
		gcCPU:    s[1].Value.Float64(),
		totalCPU: s[2].Value.Float64(),
		cycles:   s[3].Value.Uint64(),
		pauses:   s[4].Value.Float64Histogram(),
	}
}

// maxPause is the upper bound of the highest histogram bucket that gained counts between a and b.
func maxPause(a, b *metrics.Float64Histogram) float64 {
	for i := len(b.Counts) - 1; i >= 0; i-- {
		if b.Counts[i] > a.Counts[i] {
			return b.Buckets[i+1]
		}
	}
	return 0
}

func pct(v []time.Duration, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return float64(v[min(len(v)-1, int(float64(len(v))*q))].Nanoseconds()) / 1000
}

func newVariant(name string) memtable {
	switch name {
	case "slices":
		return newSliceTree()
	case "arena":
		return newArenaTree()
	case "pages":
		return newPageTree()
	}
	panic("variant " + name)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go-memtable gen|run key=value ...")
		os.Exit(2)
	}
	kv := args()
	switch os.Args[1] {
	case "gen":
		generate(intArg(kv, "files", 1_000_000), loadNames(kv["names"]), kv["data"], kv["probes"])
	case "run":
		run(kv)
	default:
		panic("unknown command " + os.Args[1])
	}
}

func run(kv map[string]string) {
	name := kv["variant"]
	exp := ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, st := range bi.Settings {
			if st.Key == "GOEXPERIMENT" {
				exp = st.Value
			}
		}
	}
	fmt.Printf("### variant=%s %s GOEXPERIMENT=%q GOGC=%q GOMAXPROCS=%d\n", name, runtime.Version(), exp,
		os.Getenv("GOGC"), runtime.GOMAXPROCS(0))
	pr := loadProbes(kv["probes"])
	runtime.GC()
	rss0, s0 := rssBytes(), read()

	t := newVariant(name)
	start := time.Now()
	forEach(kv["data"], func(k, v []byte) { t.put(k, v) })
	build := time.Since(start)
	runtime.GC()
	rss1, s1 := rssBytes(), read()
	files := float64(t.len()) / 2.09
	fmt.Printf("M1 build: entries=%d %.2f s (%.2f M entries/s) %s\n", t.len(), build.Seconds(),
		float64(t.len())/build.Seconds()/1e6, t.stats())
	fmt.Printf("M1 memory: heap objects %.0f B/file, RSS %.0f B/file (files = entries / 2.09)\n",
		(s1.heap-s0.heap)/files, float64(rss1-rss0)/files)

	gets(t, pr)
	scanBig(t)
	forcedGC()
	updates(t, pr)
	churn(t, pr, s1.heap, intArg(kv, "churn_secs", 10))
	if intArg(kv, "restore", 1) == 0 {
		return
	}
	t = restore(t, name, s0)
	if p, ok := t.(*pageTree); ok {
		snapshotTest(p, pr, intArg(kv, "snap_updates", 100_000))
	}
	runtime.KeepAlive(t)
}

// restore rebuilds the tree in key order, as loading a snapshot does, and drops the original.
func restore(t memtable, name string, s0 snap) memtable {
	r := newVariant(name)
	s := time.Now()
	t.scan(nil, func(k, v []byte) bool { r.put(k, v); return true })
	d := time.Since(s)
	files := float64(t.len()) / 2.09
	t = nil
	runtime.GC()
	debug.FreeOSMemory()
	s1 := read()
	fmt.Printf("M7 restore in key order: %.2f s (%.2f M entries/s), heap objects %.0f B/file, %s\n", d.Seconds(),
		float64(r.len())/d.Seconds()/1e6, (s1.heap-s0.heap)/files, r.stats())
	return r
}

func gets(t memtable, pr *probes) {
	r := rand.New(rand.NewPCG(3, 4))
	const n = 1_000_000
	lat := make([]time.Duration, 0, n)
	var miss int
	for range n {
		k := pr.key(r.IntN(pr.len()))
		s := time.Now()
		_, ok := t.get(k)
		lat = append(lat, time.Since(s))
		if !ok {
			miss++
		}
	}
	slices.Sort(lat)
	fmt.Printf("M4 get: p50 %.2f us p99 %.2f us (%d probes, %d misses)\n", pct(lat, 0.5), pct(lat, 0.99), n, miss)
}

func scanBig(t memtable) {
	prefix := direntPrefix(bigDir)
	lat := make([]time.Duration, 0, 1000)
	var got int
	for range 1000 {
		got = 0
		s := time.Now()
		t.scan(prefix, func(k, v []byte) bool { got++; return true })
		lat = append(lat, time.Since(s))
	}
	slices.Sort(lat)
	fmt.Printf("M4 scan: %d entries, p50 %.1f us\n", got, pct(lat, 0.5))
}

func forcedGC() {
	for i := range 3 {
		a := read()
		s := time.Now()
		runtime.GC()
		d := time.Since(s)
		b := read()
		fmt.Printf("M2 forced GC %d: wall %.1f ms, GC CPU %.1f ms, max pause %.3f ms\n", i, float64(d.Microseconds())/1000,
			(b.gcCPU-a.gcCPU)*1000, maxPause(a.pauses, b.pauses)*1000)
	}
}

// updates rewrites the values of a million existing keys with values of the same length.
func updates(t memtable, pr *probes) {
	r := rand.New(rand.NewPCG(5, 6))
	const n = 1_000_000
	buf := make([]byte, 0, 128)
	s := time.Now()
	for range n {
		k := pr.key(r.IntN(pr.len()))
		v, _ := t.get(k)
		buf = append(buf[:0], v...)
		buf[len(buf)-1] ^= 0xff
		t.put(k, buf)
	}
	d := time.Since(s)
	fmt.Printf("M4 update: %d same-length overwrites, %.2f us each, %s\n", n, d.Seconds()*1e6/n, t.stats())
}

// churn runs lookups on two goroutines while a third allocates short-lived garbage at about
// 256 MB/s, with the memory limit set 512 MiB above the tree, as a daemon would set GOMEMLIMIT.
func churn(t memtable, pr *probes, treeHeap float64, secs int) {
	runtime.GC()
	limit := int64(treeHeap) + 512<<20
	debug.SetMemoryLimit(limit)
	defer debug.SetMemoryLimit(-1)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var lookups atomic.Int64
	samples := make([][]time.Duration, 2)
	a := read()
	start := time.Now()
	for g := range 2 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(g), 7))
			lat := make([]time.Duration, 0, 1<<20)
			for i := 0; !stop.Load(); i++ {
				k := pr.key(r.IntN(pr.len()))
				if i%64 == 0 {
					s := time.Now()
					t.get(k)
					if len(lat) < cap(lat) {
						lat = append(lat, time.Since(s))
					}
				} else {
					t.get(k)
				}
				lookups.Add(1)
			}
			samples[g] = lat
		})
	}
	var allocated atomic.Int64
	wg.Go(func() {
		r := rand.New(rand.NewPCG(8, 9))
		ring := make([][]byte, 4096)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for i := 0; !stop.Load(); {
			<-tick.C
			for b := 0; b < 256<<10; i++ {
				n := 256 + r.IntN(3841)
				ring[i%len(ring)] = make([]byte, n)
				b += n
				allocated.Add(int64(n))
			}
		}
	})
	time.Sleep(time.Duration(secs) * time.Second)
	stop.Store(true)
	wg.Wait()
	wall := time.Since(start).Seconds()
	b := read()
	lat := append(samples[0], samples[1]...)
	slices.Sort(lat)
	fmt.Printf("M3 churn %ds: limit %.0f MiB, garbage %.0f MB/s, GC cycles %d, GC CPU %.3f cores (%.1f%% of process CPU), "+
		"max pause %.3f ms, lookups %.2f M/s, get p50 %.2f us p99 %.2f us p99.9 %.2f us max %.0f us\n",
		secs, float64(limit)/(1<<20), float64(allocated.Load())/wall/1e6, b.cycles-a.cycles, (b.gcCPU-a.gcCPU)/wall,
		100*(b.gcCPU-a.gcCPU)/(b.totalCPU-a.totalCPU), maxPause(a.pauses, b.pauses)*1000,
		float64(lookups.Load())/wall/1e6, pct(lat, 0.5), pct(lat, 0.99), pct(lat, 0.999),
		float64(lat[len(lat)-1].Microseconds()))
}

func snapshotTest(t *pageTree, pr *probes, n int) {
	before := t.inUse
	t.beginSnapshot()
	r := rand.New(rand.NewPCG(10, 11))
	buf := make([]byte, 0, 128)
	s := time.Now()
	for range n {
		k := pr.key(r.IntN(pr.len()))
		v, _ := t.get(k)
		buf = append(buf[:0], v...)
		buf[0] ^= 0xff
		t.put(k, buf)
	}
	d := time.Since(s)
	var frozen int
	t.scanFrozen(func(k, v []byte) bool { frozen++; return true })
	peak := t.inUse
	t.endSnapshot()
	fmt.Printf("M6 snapshot: %d updates while frozen, %.2f us each; frozen view %d entries (live %d); pages %d -> %d "+
		"(+%.1f%%) -> %d after end\n", n, d.Seconds()*1e6/float64(n), frozen, t.len(), before, peak,
		100*float64(peak-before)/float64(before), t.inUse)
}
