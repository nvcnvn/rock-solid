// Command ours runs the deletion workloads of pebblex against the LSM runs spike's prototype tree
// (copied as lsmd, plus DropCovered): dropping a volume's key range, with and without dropping
// the run blocks a range removal covers, and a queue removed from its head.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/spikes/lsm-study-deletes/lsmd"
)

var exp = flag.String("exp", "o1,o2", "experiments: comma list of o1, o2")

const maxPiece = 1 << 20

func must(err error) {
	if err != nil {
		panic(err)
	}
}

type log struct {
	next    uint64
	buf     []byte
	written int
}

// checkpoint writes every piece the tree's flush asks for, each durable at once.
func (l *log) checkpoint(t *lsmd.Tree) {
	if l.buf == nil {
		l.buf = make([]byte, maxPiece)
	}
	_, err := t.BeginFlush()
	must(err)
	for {
		n, _, err := t.NextPiece(l.buf)
		must(err)
		if n == 0 {
			break
		}
		l.next++
		l.written += n
		must(t.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(l.next), LSN: l.next}))
	}
	_, err = t.CheckpointDone()
	must(err)
}

func compact(t *lsmd.Tree) {
	for c := t.PickCompaction(); c != nil; c = t.PickCompaction() {
		c.Run()
		must(t.Install(c))
	}
}

func main() {
	flag.Parse()
	want := func(n string) bool { return strings.Contains(","+*exp+",", ","+n+",") }
	if want("o1") {
		o1()
	}
	if want("o2") {
		o2()
	}
}

func fsKey(dst []byte, vol, inode int) []byte {
	return fmt.Appendf(dst[:0], "fs/%08d/%016x/attr", vol, inode)
}

// o1 drops one of four volumes with one range removal, then keeps creating files in another
// volume, a flush every 2,000 creates, and tracks the bytes the runs hold.
func o1() {
	fmt.Println("== O1 dropping one of four 250,000-file volumes (100-byte values), then 400 flushes of 2,000 creates in volume 1")
	fmt.Println("mode    runs before  run MB before  run MB after the flush  after 10 flushes  after 30  after 100  after 400  bytes merged MB  bytes rewritten by drops MB  blocks dropped")
	val := bytes.Repeat([]byte{1}, 100)
	for _, drop := range []bool{false, true} {
		t, err := lsmd.New(lsmd.Config{MaxPiece: maxPiece, CursorFloor: true})
		must(err)
		var l log
		rng := rand.New(rand.NewPCG(2, 2))
		var k []byte
		for n, i := range rng.Perm(1_000_000) {
			must(t.Put(fsKey(k, i%4, i/4), val))
			if n%20_000 == 19_999 {
				l.checkpoint(t)
				compact(t)
			}
		}
		l.checkpoint(t)
		compact(t)
		runs0, bytes0 := t.Runs(), t.RunBytes()
		merged0 := t.Stats.MergeOut
		must(t.DeleteRange(fsKey(nil, 2, 0)[:12], fsKey(nil, 3, 0)[:12]))
		next := 1_000_000
		var at []float64
		for f := 1; f <= 400; f++ {
			if f > 1 {
				for range 2_000 {
					must(t.Put(fsKey(k, 1, next), val))
					next++
				}
			}
			l.checkpoint(t)
			compact(t)
			if drop {
				t.DropCovered()
			}
			if f == 1 || f == 10 || f == 30 || f == 100 || f == 400 {
				at = append(at, float64(t.RunBytes())/1e6)
			}
		}
		mode := "none"
		if drop {
			mode = "drop"
		}
		fmt.Printf("%-6s  %11d  %13.1f  %22.1f  %16.1f  %8.1f  %9.1f  %9.1f  %15.1f  %27.1f  %14d\n", mode, runs0, float64(bytes0)/1e6,
			at[0], at[1], at[2], at[3], at[4], float64(t.Stats.MergeOut-merged0)/1e6, float64(t.Drop.BytesKept)/1e6, t.Drop.BlocksDrop)
	}
}

// o2 runs pebblex's queue: each step appends a key at the tail and removes the head, a flush every
// 1,000 steps, and a seek to the head every 1,000 steps counts the removals it steps over. The
// range mode removes the consumed prefix [q/, head] with one range removal instead.
func o2() {
	fmt.Println("== O2 queue of 10,000 keys beside 200,000 cold keys, 300,000 append+remove steps, flush every 1,000 steps")
	fmt.Println("mode    seeks  mean steps/seek  max steps/seek  runs at end")
	val := bytes.Repeat([]byte{3}, 64)
	qk := func(dst []byte, i int) []byte { return fmt.Appendf(dst[:0], "q/%012d", i) }
	for _, mode := range []string{"point", "range"} {
		t, err := lsmd.New(lsmd.Config{MaxPiece: maxPiece, CursorFloor: true})
		must(err)
		var l log
		var k []byte
		for i := 0; i < 10_000; i++ {
			must(t.Put(qk(k, i), val))
		}
		for i := 0; i < 200_000; i++ {
			must(t.Put(fmt.Appendf(k[:0], "r/%012d", i), val))
		}
		l.checkpoint(t)
		compact(t)
		var sum, mx, n int
		var it lsmd.Iter
		for s := 0; s < 300_000; s++ {
			must(t.Put(qk(k, 10_000+s), val))
			if mode == "point" {
				must(t.Delete(qk(k, s)))
			} else {
				must(t.DeleteRange([]byte("q/"), qk(k, s+1)))
			}
			if s%1000 == 999 {
				l.checkpoint(t)
				compact(t)
				t.Iter(&it)
				it.SeekGE([]byte("q/"))
				st := it.Steps()
				sum += st
				mx = max(mx, st)
				n++
			}
		}
		fmt.Printf("%-6s  %5d  %15.0f  %14d  %11d\n", mode, n, float64(sum)/float64(n), mx, t.Runs())
	}
}
