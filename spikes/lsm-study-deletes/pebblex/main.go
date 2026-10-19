// Command pebblex drives Pebble v2.1.7 with the chunk index's key shape and a MetaStore-like key
// range to count what range deletions, point deletions and delete-only and tombstone-density
// compactions cost: points stepped per floor, compactions by kind and bytes rewritten.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

var exp = flag.String("exp", "p1,p2,p3", "experiments: comma list of p1, p2, p3")

func main() {
	flag.Parse()
	want := func(n string) bool { return strings.Contains(","+*exp+",", ","+n+",") }
	if want("p1") {
		p1()
	}
	if want("p2") {
		p2()
	}
	if want("p3") {
		p3()
	}
}

func open(mod func(*pebble.Options)) *pebble.DB {
	o := &pebble.Options{FS: vfs.NewMem()}
	if mod != nil {
		mod(o)
	}
	db, err := pebble.Open("db", o)
	if err != nil {
		panic(err)
	}
	return db
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// waitIdle waits until no compaction runs or is due.
func waitIdle(db *pebble.DB) {
	idle := 0
	for idle < 5 {
		time.Sleep(20 * time.Millisecond)
		if db.Metrics().Compact.NumInProgress == 0 {
			idle++
		} else {
			idle = 0
		}
	}
}

// chunkKey is the chunk index's 38-byte extent key: pool, a hash of the chunk, the 16-byte chunk
// ID, the frozen version (0 for the live map) and the first block.
func chunkKey(dst []byte, chunk uint64, first uint16) []byte {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], chunk)
	h := fnv.New64a()
	h.Write(id[:])
	dst = binary.BigEndian.AppendUint32(dst[:0], 1)
	dst = binary.BigEndian.AppendUint64(dst, h.Sum64())
	dst = binary.BigEndian.AppendUint64(dst, 0)
	dst = binary.BigEndian.AppendUint64(dst, chunk)
	dst = binary.BigEndian.AppendUint64(dst, 0)
	return binary.BigEndian.AppendUint16(dst, first)
}

var extentVal = bytes.Repeat([]byte{7}, 26)

// p1 rewrites a chunk of 1,024 one-block extents whole, with 1,023 point deletions or one range
// deletion, and counts the points each floor of the chunk's blocks steps over.
func p1() {
	fmt.Println("== P1 chunk of 1,024 one-block extents rewritten whole among 20,000 chunks (Pebble v2.1.7)")
	fmt.Println("mode       phase      floors  points/floor  covered/floor  ns/floor  L0 tables  L6 tables")
	for _, mode := range []string{"point", "range"} {
		db := open(nil)
		var k []byte
		b := db.NewBatch()
		for c := uint64(1); c <= 20_000; c++ {
			for _, f := range []uint16{0, 256, 512, 768} {
				k = chunkKey(k, c, f)
				must(b.Set(k, extentVal, nil))
			}
			if b.Len() > 1<<20 {
				must(b.Commit(pebble.NoSync))
				b = db.NewBatch()
			}
		}
		const target = 1 << 40
		for f := uint16(0); f < 1024; f++ {
			k = chunkKey(k, target, f)
			must(b.Set(k, extentVal, nil))
		}
		must(b.Commit(pebble.NoSync))
		must(db.Flush())
		lo, hi := chunkKey(nil, 0, 0), bytes.Repeat([]byte{0xff}, 40)
		must(db.Compact(context.Background(), lo, hi, false))
		waitIdle(db)

		b = db.NewBatch()
		k = chunkKey(k, target, 0)
		must(b.Set(k, extentVal, nil))
		if mode == "point" {
			for f := uint16(1); f < 1024; f++ {
				must(b.Delete(chunkKey(nil, target, f), nil))
			}
		} else {
			must(b.DeleteRange(chunkKey(nil, target, 1), chunkKey(nil, target, 1024), nil))
		}
		must(b.Commit(pebble.NoSync))
		measure := func(phase string) {
			prefix := chunkKey(nil, target, 0)[:36]
			it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix})
			must(err)
			before := it.Stats().InternalStats
			start := time.Now()
			found := 0
			var succ []byte
			for f := uint16(0); f < 1024; f++ {
				succ = append(chunkKey(succ, target, f), 0)
				if it.SeekLT(succ) {
					found++
				}
			}
			el := time.Since(start)
			st := it.Stats().InternalStats
			must(it.Close())
			m := db.Metrics()
			fmt.Printf("%-10s %-10s %6d  %12.1f  %13.1f  %8d  %9d  %9d\n", mode, phase, found,
				float64(st.PointCount-before.PointCount)/1024,
				float64(st.PointsCoveredByRangeTombstones-before.PointsCoveredByRangeTombstones)/1024,
				el.Nanoseconds()/1024, m.Levels[0].TablesCount, m.Levels[6].TablesCount)
		}
		measure("memtable")
		must(db.Flush())
		waitIdle(db)
		measure("flushed")
		must(db.Compact(context.Background(), lo, hi, false))
		waitIdle(db)
		measure("compacted")
		must(db.Close())
	}
}

func fsKey(dst []byte, vol, inode int) []byte {
	return fmt.Appendf(dst[:0], "fs/%08d/%016x/attr", vol, inode)
}

// p2 drops one of four volumes of a MetaStore-like key range, with one range deletion or a point
// deletion per key, and counts the compactions by kind and the bytes they rewrite.
func p2() {
	fmt.Println("== P2 dropping one of four 250,000-key volumes (100-byte values), 256 KiB tables, automatic compactions")
	fmt.Println("mode         excise  default  delete-only  elision-only  tombstone-density  tables deleted  tables excised  bytes compacted  live bytes before  after")
	val := bytes.Repeat([]byte{1}, 100)
	for _, mode := range []string{"point", "range", "range+excise"} {
		db := open(func(o *pebble.Options) {
			for i := range o.TargetFileSizes {
				o.TargetFileSizes[i] = 256 << 10
			}
			if mode == "range+excise" {
				o.Experimental.EnableDeleteOnlyCompactionExcises = func() bool { return true }
			}
		})
		rng := rand.New(rand.NewPCG(2, 2))
		perm := rng.Perm(1_000_000)
		var k []byte
		b := db.NewBatch()
		for _, i := range perm {
			k = fsKey(k, i%4, i/4)
			must(b.Set(k, val, nil))
			if b.Len() > 1<<20 {
				must(b.Commit(pebble.NoSync))
				b = db.NewBatch()
			}
		}
		must(b.Commit(pebble.NoSync))
		must(db.Flush())
		must(db.Compact(context.Background(), []byte("fs/"), []byte("fs0"), false))
		waitIdle(db)
		m0 := db.Metrics()
		before := m0.Total()
		live0 := m0.DiskSpaceUsage()

		lo, hi := fsKey(nil, 2, 0)[:12], fsKey(nil, 3, 0)[:12]
		if mode == "point" {
			b = db.NewBatch()
			for i := 0; i < 250_000; i++ {
				must(b.Delete(fsKey(k, 2, i), nil))
				if b.Len() > 1<<20 {
					must(b.Commit(pebble.NoSync))
					b = db.NewBatch()
				}
			}
			must(b.Commit(pebble.NoSync))
		} else {
			must(db.DeleteRange(lo, hi, pebble.NoSync))
		}
		must(db.Flush())
		waitIdle(db)
		time.Sleep(500 * time.Millisecond)
		waitIdle(db)
		m := db.Metrics()
		after := m.Total()
		var deleted, excised uint64
		for _, l := range m.Levels {
			deleted += l.TablesDeleted
			excised += l.TablesExcised
		}
		fmt.Printf("%-12s %6v  %7d  %11d  %12d  %17d  %14d  %14d  %15d  %17d  %d (L6 tables %d)\n", mode, mode == "range+excise",
			m.Compact.DefaultCount-m0.Compact.DefaultCount, m.Compact.DeleteOnlyCount-m0.Compact.DeleteOnlyCount,
			m.Compact.ElisionOnlyCount-m0.Compact.ElisionOnlyCount, m.Compact.TombstoneDensityCount-m0.Compact.TombstoneDensityCount,
			deleted, excised, after.TableBytesCompacted-before.TableBytesCompacted, live0, m.DiskSpaceUsage(), m0.Levels[6].TablesCount)
		must(db.Close())
	}
}

// p3 runs a queue: each step appends a key at the tail and deletes the head, with a small
// memtable, and counts the points a seek to the queue's head steps over, with and without
// tombstone-density compactions.
func p3() {
	fmt.Println("== P3 queue of 10,000 keys, 300,000 append+delete steps, 64 KiB memtable, seek to head every 1,000 steps")
	fmt.Println("density      seeks  mean points/seek  max points/seek  tombstone-density compactions  default compactions")
	val := bytes.Repeat([]byte{3}, 64)
	for _, on := range []bool{true, false} {
		db := open(func(o *pebble.Options) {
			o.MemTableSize = 64 << 10
			if !on {
				o.Experimental.TombstoneDenseCompactionThreshold = -1
			}
		})
		qk := func(dst []byte, i int) []byte { return fmt.Appendf(dst[:0], "q/%012d", i) }
		var k []byte
		for i := 0; i < 10_000; i++ {
			must(db.Set(qk(k, i), val, pebble.NoSync))
		}
		// A large cold range sorts after the queue, as in Pebble's motivating workloads.
		for i := 0; i < 200_000; i++ {
			must(db.Set(fmt.Appendf(k[:0], "r/%012d", i), val, pebble.NoSync))
		}
		var sum, mx, n uint64
		for s := 0; s < 300_000; s++ {
			must(db.Set(qk(k, 10_000+s), val, pebble.NoSync))
			must(db.Delete(qk(k, s), pebble.NoSync))
			if s%1000 == 999 {
				it, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("q/"), UpperBound: []byte("q0")})
				must(err)
				it.SeekGE([]byte("q/"))
				pc := it.Stats().InternalStats.PointCount
				must(it.Close())
				sum += pc
				mx = max(mx, pc)
				n++
			}
		}
		m := db.Metrics()
		fmt.Printf("%-12v %5d  %16.0f  %15d  %29d  %19d\n", on, n, float64(sum)/float64(n), mx,
			m.Compact.TombstoneDensityCount, m.Compact.DefaultCount)
		must(db.Close())
	}
}
