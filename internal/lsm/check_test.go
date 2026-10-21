package lsm

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// threeRuns holds three runs, oldest first: k00-k39, k20-k59 with k30-k34 removed by a range
// removal, then k10-k29 removed one by one, and m the map it reads as.
func threeRuns(t *testing.T) (*Tree, *fakeLog, model) {
	t.Helper()
	tr := newTree(t, 192)
	f := newFakeLog()
	m := model{}
	key := func(i int) string { return fmt.Sprintf("k%02d", i) }
	for i := range 40 {
		put(t, tr, m, key(i), "a"+key(i))
	}
	f.checkpoint(t, tr, nil)
	for i := 20; i < 60; i++ {
		put(t, tr, m, key(i), "b"+key(i))
	}
	delRange(t, tr, m, key(30), key(35))
	f.checkpoint(t, tr, nil)
	for i := 10; i < 30; i++ {
		del(t, tr, m, key(i))
	}
	f.checkpoint(t, tr, nil)
	return tr, f, m
}

// drainFlush builds pieces of the running flush, each made durable, until one fails or none is left.
func drainFlush(tr *Tree, f *fakeLog) error {
	buf := make([]byte, tr.cfg.MaxPiece)
	for {
		n, _, err := tr.NextPiece(buf)
		if err != nil || n == 0 {
			return err
		}
		if err := tr.PieceDurable(f.put(buf[:n])); err != nil {
			return err
		}
	}
}

// A flush whose run and drops hold one entry or one fragment fewer than the frozen memtable fails
// with ErrCheck; the frozen memtable is kept, reads go on, and the next flush writes it whole.
func TestFlushCountCheck(t *testing.T) {
	for _, short := range []string{"entry", "fragment"} {
		t.Run(short, func(t *testing.T) {
			tr, f, m := threeRuns(t)
			put(t, tr, m, "k70", "c")
			put(t, tr, m, "k71", "c")
			delRange(t, tr, m, "k50", "k55")
			if _, err := tr.BeginFlush(); err != nil {
				t.Fatal(err)
			}
			count := &tr.frozen.count
			if short == "fragment" {
				count = &tr.frozen.frags
			}
			*count++
			if err := drainFlush(tr, f); !errors.Is(err, ErrCheck) {
				t.Fatalf("flush: %v, want ErrCheck", err)
			}
			tr.FlushFailed()
			if !tr.Frozen() || tr.Runs() != 3 {
				t.Fatalf("after the failed flush: frozen %v, %d runs", tr.Frozen(), tr.Runs())
			}
			checkReads(t, tr, m, []string{"k52", "k70"})
			*count--
			f.checkpoint(t, tr, nil)
			if tr.Frozen() || tr.Runs() != 4 {
				t.Fatalf("after the next flush: frozen %v, %d runs", tr.Frozen(), tr.Runs())
			}
			checkReads(t, tr, m, []string{"k52", "k70"})
		})
	}
}

// A merge whose output and drops hold one entry or one fragment fewer than its inputs fails with
// ErrCheck at install, and nothing is installed.
func TestMergeCountCheck(t *testing.T) {
	for _, short := range []string{"entry", "fragment"} {
		t.Run(short, func(t *testing.T) {
			tr, _, m := threeRuns(t)
			before := slices.Clone(tr.runs)
			c := tr.pickSpan(0, 3, ruleNone)
			if short == "entry" {
				c.runs[1].entries++
			} else {
				c.plant.dropFrags = true
			}
			c.Run()
			if err := tr.Install(c); !errors.Is(err, ErrCheck) {
				t.Fatalf("install: %v, want ErrCheck", err)
			}
			if !slices.Equal(tr.runs, before) || slices.ContainsFunc(tr.runs, func(r *run) bool { return r.held }) {
				t.Fatal("a failed merge changed the run list or kept its inputs held")
			}
			checkReads(t, tr, m, []string{"k15", "k32"})
			compactSpan(t, tr, 0, 3)
			checkReads(t, tr, m, []string{"k15", "k32"})
		})
	}
}

// flipLastValue flips a bit in the value of the last entry of block b, so the block still decodes,
// and returns that entry's key.
func flipLastValue(b []byte) string {
	var it blockIter
	it.init(b)
	for ok := it.first(); ok && it.next < it.restarts; ok = it.advance() {
	}
	it.val[len(it.val)-1] ^= 1
	return string(it.key)
}

func mergeErr(tr *Tree, i, j int) error {
	c := tr.pickSpan(i, j, ruleNone)
	c.Run()
	return tr.Install(c)
}

// One bit flipped in an in-memory piece leaves reads going on and fails the first merge that reads
// the piece, as an input or to decide whether an older run holds a removal's key, not a merge that
// does not read it.
func TestMergeVerifiesPieceChecksums(t *testing.T) {
	t.Run("input", func(t *testing.T) {
		tr, _, m := threeRuns(t)
		oldest := tr.runs[2]
		last := oldest.blocks[len(oldest.blocks)-1]
		if string(firstKey(last)) <= "k29" {
			t.Fatalf("the oldest run's last block starts at %q, which the removals k10-k29 read", firstKey(last))
		}
		flipLastValue(last)
		checkReads(t, tr, m, nil)
		if n := testing.AllocsPerRun(10, func() { oldest.intact() }); n != 0 {
			t.Fatalf("checking a run's checksums allocates %v", n)
		}
		compactSpan(t, tr, 0, 2)
		if err := mergeErr(tr, 0, 2); !errors.Is(err, ErrCheck) {
			t.Fatalf("merge reading the flipped piece: %v, want ErrCheck", err)
		}
		if tr.Runs() != 2 {
			t.Fatalf("%d runs after the failed merge", tr.Runs())
		}
	})
	t.Run("older run", func(t *testing.T) {
		tr, _, _ := threeRuns(t)
		oldest := tr.runs[2]
		flipLastValue(oldest.blocks[oldest.blocks.blockFor([]byte("k10"), false)])
		if err := mergeErr(tr, 0, 2); !errors.Is(err, ErrCheck) {
			t.Fatalf("merge of removals over the flipped piece: %v, want ErrCheck", err)
		}
	})
	t.Run("fragment", func(t *testing.T) {
		tr, _, _ := threeRuns(t)
		frag := tr.runs[1].frags[0]
		frag[len(frag)-9] ^= 1
		if err := mergeErr(tr, 1, 3); !errors.Is(err, ErrCheck) {
			t.Fatalf("merge reading the flipped fragment: %v, want ErrCheck", err)
		}
	})
	t.Run("filter", func(t *testing.T) {
		tr, _, _ := threeRuns(t)
		tr.runs[1].parts[0].bits[1] ^= 0x80
		if err := mergeErr(tr, 1, 3); !errors.Is(err, ErrCheck) {
			t.Fatalf("merge of a run with a flipped filter: %v, want ErrCheck", err)
		}
	})
}

// A flush that writes a merge's output, flipped in memory after the merge, fails with ErrCheck
// rather than give the flipped block a fresh checksum in the log.
func TestFlushVerifiesRewrittenRuns(t *testing.T) {
	for _, part := range []string{"block", "filter"} {
		t.Run(part, func(t *testing.T) {
			tr, f, _ := threeRuns(t)
			compactSpan(t, tr, 0, 3)
			if part == "block" {
				flipLastValue(tr.runs[0].blocks[0])
			} else {
				tr.runs[0].parts[0].bits[filterKeyHdr] ^= 1
			}
			if _, err := tr.BeginFlush(); err != nil {
				t.Fatal(err)
			}
			if err := drainFlush(tr, f); !errors.Is(err, ErrCheck) {
				t.Fatalf("flush of the flipped run: %v, want ErrCheck", err)
			}
		})
	}
}

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), want) {
			t.Fatalf("panic %v, want one containing %q", r, want)
		}
	}()
	fn()
}

// With Check on, a planted encoder that drops an entry panics in the flush that built the piece.
func TestFlushDecodeCheck(t *testing.T) {
	tr := newTree(t, 512)
	tr.plant.skipEncoded = true
	put(t, tr, nil, "a", "1")
	put(t, tr, nil, "b", "2")
	put(t, tr, nil, "c", "3")
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "differs from the entries it took", func() { drainFlush(tr, newFakeLog()) })
}

// With Check on, a planted filter that leaves a key out panics in the flush that built the piece.
func TestFlushFilterCheck(t *testing.T) {
	tr := newTree(t, 512)
	tr.plant.skipFiltered = true
	put(t, tr, nil, "a", "1")
	put(t, tr, nil, "b", "2")
	if _, err := tr.BeginFlush(); err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "left a key out of its filter", func() { drainFlush(tr, newFakeLog()) })
}

// With Check on, a planted merge that emits keys again after later ones panics in that merge.
func TestMergeOrderCheck(t *testing.T) {
	tr, _, _ := threeRuns(t)
	tr.plant.repeatBlock = true
	c := tr.pickSpan(0, 3, ruleNone)
	mustPanic(t, "out of order", c.Run)
}

// With Check on, a planted install that puts a merge's output as the oldest run panics at install
// (the metamorphic testing spike's P2).
func TestRunOrderCheck(t *testing.T) {
	tr, _, _ := threeRuns(t)
	tr.plant.installOldest = true
	c := tr.pickSpan(0, 2, ruleNone)
	c.Run()
	mustPanic(t, "sequence order", func() { tr.Install(c) })
}

// BenchmarkMergeChecksums merges four runs of 26,214 random 24-byte keys with 12-byte values in
// 64 KiB pieces, the metamorphic testing spike's T6 workload, with and without checking the inputs'
// checksums and computing the output's.
func BenchmarkMergeChecksums(b *testing.B) {
	tr, err := New(Config{MaxPiece: 1 << 16})
	if err != nil {
		b.Fatal(err)
	}
	f := newFakeLog()
	r := rand.New(rand.NewPCG(1, 2))
	key := make([]byte, 24)
	for range 4 {
		for range 1 << 20 / 40 {
			for i := range key {
				key[i] = 'a' + byte(r.IntN(26))
			}
			if err := tr.Put(key, key[:12]); err != nil {
				b.Fatal(err)
			}
		}
		f.checkpoint(b, tr, nil)
	}
	for _, verify := range []bool{true, false} {
		b.Run(fmt.Sprintf("verify=%v", verify), func(b *testing.B) {
			b.SetBytes(int64(tr.RunBytes()))
			b.ReportAllocs()
			for b.Loop() {
				c := tr.pickSpan(0, 4, ruleNone)
				c.plant.noSums = !verify
				c.Run()
				if c.err != nil {
					b.Fatal(c.err)
				}
				tr.failMerge(c)
			}
		})
	}
}
