package chunkstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

var chunkC = core.ChunkID{Volume: 1, Inode: 7, Index: 3}

func fill(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }

func reads(h *harness, mark int) int {
	n := 0
	for _, a := range h.acts[mark:] {
		if a.Kind == core.ActDiskRead && a.IO&fetchIOBit != 0 && a.Len > 0 && a.IO&1 == 0 {
			n++
		}
	}
	return n
}

// Byte ranges become whole blocks: what each write or punch logs, how many base reads it makes,
// and what reads back.
func TestByteRangesToWholeBlocks(t *testing.T) {
	const v1, v2 = 1<<32 | 1, 1<<32 | 2
	cases := []struct {
		name   string
		setup  func(h *harness)
		op     func(h *harness) uint64
		logged []string
		reads  int
		want   func(b []byte)
	}{{
		name:   "whole block",
		op:     func(h *harness) uint64 { return h.write(chunkC, 0, fill(core.BlockSize, 0x11), 0) },
		logged: []string{"write 0+1 v0x100000001"},
		want:   func(b []byte) { copy(b, fill(core.BlockSize, 0x11)) },
	}, {
		name:   "one byte",
		op:     func(h *harness) uint64 { return h.write(chunkC, 100, []byte{0x22}, 0) },
		logged: []string{"write 0+1 v0x100000001"},
		want:   func(b []byte) { b[100] = 0x22 },
	}, {
		name:   "across a block edge",
		op:     func(h *harness) uint64 { return h.write(chunkC, 4000, fill(200, 0x33), 0) },
		logged: []string{"write 0+2 v0x100000001"},
		want:   func(b []byte) { copy(b[4000:], fill(200, 0x33)) },
	}, {
		name:   "two merged edges, two whole blocks",
		op:     func(h *harness) uint64 { return h.write(chunkC, 3*core.BlockSize+10, fill(3*core.BlockSize, 0x44), 0) },
		logged: []string{"write 3+4 v0x100000001"},
		want:   func(b []byte) { copy(b[3*core.BlockSize+10:], fill(3*core.BlockSize, 0x44)) },
	}, {
		name:   "punch of whole blocks",
		setup:  func(h *harness) { h.sync(h.write(chunkC, 0, fill(2*core.BlockSize, 0x55), 0)) },
		op:     func(h *harness) uint64 { return h.punch(chunkC, 0, 2*core.BlockSize, 0) },
		logged: []string{"punch 0+2 v0x100000002"},
	}, {
		name:   "punch with merged edges",
		setup:  func(h *harness) { h.sync(h.write(chunkC, 0, fill(3*core.BlockSize, 0x66), 0)) },
		op:     func(h *harness) uint64 { return h.punch(chunkC, 100, 2*core.BlockSize, 0) },
		logged: []string{"write 0+1 v0x100000002", "punch 1+1 v0x100000002", "write 2+1 v0x100000002"},
		reads:  2,
		want: func(b []byte) {
			copy(b, fill(3*core.BlockSize, 0x66))
			clear(b[100 : 100+2*core.BlockSize])
		},
	}, {
		name:   "punch of the whole chunk",
		setup:  func(h *harness) { h.sync(h.write(chunkC, 0, fill(core.BlockSize, 0x77), 0)) },
		op:     func(h *harness) uint64 { return h.punch(chunkC, 0, core.ChunkSize, 0) },
		logged: []string{"punch 0+1024 v0x100000002"},
	}, {
		name: "punch of a chunk that does not exist",
		op:   func(h *harness) uint64 { return h.punch(chunkC, 0, 100, 0) },
	}, {
		name: "write after a tombstone",
		setup: func(h *harness) {
			h.sync(h.write(chunkC, 0, fill(core.BlockSize, 0x77), 0))
			h.settle()
			h.sync(h.punch(chunkC, 0, core.ChunkSize, 0))
		},
		op:     func(h *harness) uint64 { return h.write(chunkC, 5*core.BlockSize, fill(core.BlockSize, 0x88), 0) },
		logged: []string{"write 5+1 v0x100000003"},
		want:   func(b []byte) { copy(b[5*core.BlockSize:], fill(core.BlockSize, 0x88)) },
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig())
			if tc.setup != nil {
				tc.setup(h)
				h.settle()
			}
			mark, rec := len(h.acts), len(h.records)
			id := tc.op(h)
			h.settle()
			v := h.ok(id)
			if got := h.logged(rec, chunkC); !slices.Equal(got, tc.logged) {
				t.Fatalf("logged %q, want %q", got, tc.logged)
			}
			if got := reads(h, mark); got != tc.reads {
				t.Fatalf("%d base reads, want %d", got, tc.reads)
			}
			if tc.logged == nil && (v != 0 || h.actions(mark, core.ActDiskWrite) != 0) {
				t.Fatalf("version %#x, %d writes, want nothing logged", v, h.actions(mark, core.ActDiskWrite))
			}
			if len(tc.logged) > 0 && v != v1 && v != v2 && v != 1<<32|3 {
				t.Fatalf("version %#x", v)
			}
			want := make([]byte, 8*core.BlockSize)
			if tc.want != nil {
				tc.want(want)
			}
			if got := h.readNow(chunkC, 0, uint32(len(want))); !bytes.Equal(got, want) {
				t.Fatalf("read back differs at byte %d", firstDiff(got, want))
			}
			if err := h.s.Index().Check(); err != nil {
				t.Fatal(err)
			}
		})
	}
	h := newHarness(t, testConfig())
	h.sync(h.write(chunkC, 0, fill(2*core.BlockSize, 0x99), 0))
	h.settle()
	if got := h.readNow(chunkC, 100, 10); !bytes.Equal(got, fill(10, 0x99)) {
		t.Fatalf("10 bytes at 100: %x", got)
	}
	if got := h.readNow(core.ChunkID{Volume: 9}, 0, core.ChunkSize); !bytes.Equal(got, make([]byte, core.ChunkSize)) {
		t.Fatal("a chunk never written does not read as zeros")
	}
	whole := h.s.Index()
	if _, ok := whole.Chunk(testPool, core.ChunkID{Volume: 9}); ok {
		t.Fatal("a read created a chunk entry")
	}
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// A whole-chunk punch keeps the chunk's entry, at the punch's version, as its tombstone.
func TestPunchWholeChunkLeavesTombstone(t *testing.T) {
	h := newHarness(t, testConfig())
	h.sync(h.write(chunkC, 0, fill(core.BlockSize, 1), 0))
	h.settle()
	id := h.punch(chunkC, 0, core.ChunkSize, 0)
	h.settle()
	v := h.ok(id)
	ch, ok := h.s.Index().Chunk(testPool, chunkC)
	if !ok || ch.Version != v || v != 1<<32|2 {
		t.Fatalf("entry %+v %v after a punch at %#x", ch, ok, v)
	}
	if exts := h.s.Index().Extents(nil, testPool, chunkC, 0); len(exts) != 0 {
		t.Fatalf("live map %+v after a whole-chunk punch", exts)
	}
}

// Requests outside a chunk, empty ones, and ones whose expectVer does not match are refused with
// no disk action; Seal exists in the API only.
func TestAPIEdges(t *testing.T) {
	h := newHarness(t, testConfig())
	h.sync(h.write(chunkC, 0, fill(core.BlockSize, 1), 0))
	h.settle()
	mark := len(h.acts)
	refused := []struct {
		name string
		id   uint64
		err  error
	}{
		{"write past 4 MiB", h.write(chunkC, core.ChunkSize-1, []byte{1, 2}, 0), ErrRange},
		{"punch past 4 MiB", h.punch(chunkC, core.ChunkSize-100, 200, 0), ErrRange},
		{"empty punch", h.punch(chunkC, 0, 0, 0), ErrRange},
		{"empty read", h.read(chunkC, 0, 0), ErrRange},
		{"read past 4 MiB", h.read(chunkC, core.ChunkSize, 1), ErrRange},
		{"expectVer behind", h.writeExpect(chunkC, 0, []byte{1}, 0), ErrVersion},
		{"expectVer 0 of an existing chunk", h.writeExpect(chunkC, 0, []byte{1}, 0), ErrVersion},
		{"atVer no snapshot pins", h.readAt(chunkC, 0, 1, 7), ErrNoVersion},
	}
	id := h.id()
	h.s.Write(&h.ctx, Request{ID: id, Chunk: chunkC, Off: 0, Len: 0, Buf: h.writeBuf(0, []byte{1})}, &h.out)
	h.take()
	refused = append(refused, struct {
		name string
		id   uint64
		err  error
	}{"empty write", id, ErrRange})
	for _, r := range refused {
		if got := h.reply(r.id).Err; !errors.Is(got, r.err) {
			t.Errorf("%s: %v, want %v", r.name, got, r.err)
		}
	}
	if n := len(h.acts) - mark; n != 0 || len(h.queue) != 0 {
		t.Fatalf("refused requests made %d disk actions", n)
	}
	if h.pool.Live() != h.liveBefore() {
		t.Fatalf("refused writes kept %d buffers", h.pool.Live()-h.liveBefore())
	}
	if err := h.s.Seal(chunkC); !errors.Is(err, ErrSeal) {
		t.Fatalf("Seal: %v", err)
	}
	other := core.ChunkID{Volume: 2}
	v := h.sync(h.writeExpect(other, 0, []byte{1}, 0))
	if v != 1<<32|1 {
		t.Fatalf("create-if-absent gave %#x", v)
	}
	h.settle()
	if v2 := h.sync(h.writeExpect(other, 0, []byte{2}, v)); v2 != v+1 {
		t.Fatalf("expectVer of the current version gave %#x", v2)
	}
}

// liveBefore is the pool buffers a settled harness holds: the log's own round buffers.
func (h *harness) liveBefore() int { return 2 * h.cfg.Log.CommitDepth }

// W1 = Write(off 0, 10 B 'a') pending, then W2 = Write(off 5, 10 B 'b'): W2's base is W1's
// merged block, so the block reads aaaaabbbbbbbbbb then zeros.
func TestMergeAgainstPendingWrite(t *testing.T) {
	h := newHarness(t, testConfig())
	mark := len(h.acts)
	w1 := h.write(chunkC, 0, fill(10, 'a'), 0)
	w2 := h.write(chunkC, 5, fill(10, 'b'), 0)
	if _, ok := h.replies[w1]; ok {
		t.Fatal("W1 answered before its record is durable")
	}
	h.settle()
	if v1, v2 := h.ok(w1), h.ok(w2); v2 != v1+1 {
		t.Fatalf("versions %#x then %#x", v1, v2)
	}
	if n := reads(h, mark); n != 0 {
		t.Fatalf("%d base reads, want none", n)
	}
	want := append(append(fill(5, 'a'), fill(10, 'b')...), make([]byte, core.BlockSize-15)...)
	if got := h.readNow(chunkC, 0, core.BlockSize); !bytes.Equal(got, want) {
		t.Fatalf("block reads %q", got[:20])
	}
}

// subBlockRun is the sub-block writes spike's merge model through the store: two clients with up
// to 8 writes pipelined each, 1 byte to 3 blocks at random offsets in the first 4 blocks of one
// chunk, every acknowledged write applied to the byte-range oracle in version order and the
// blocks checked after every step. It returns the first difference, if any.
func subBlockRun(t *testing.T, seed uint64, steps int, plant plants) (wrong string) {
	h := newHarness(t, testConfig())
	h.s.plant = plant
	r := core.NewRand(seed)
	o := newByteOracle()
	type sent struct {
		off    uint32
		data   []byte
		client int
	}
	inflight := map[uint64]sent{}
	var clients [2]int
	seen := 0
	for step := 0; step < steps || len(inflight) > 0; step++ {
		c := int(r.Below(2))
		if step < steps && clients[c] < 8 && r.Chance(1, 2) {
			n := uint32(1 + r.Below(3*core.BlockSize))
			off := uint32(r.Below(uint64(4*core.BlockSize - n + 1)))
			data := make([]byte, n)
			r.Fill(data)
			inflight[h.write(chunkC, off, data, 0)] = sent{off, data, c}
			clients[c]++
		} else if !h.step() && step >= steps {
			break
		}
		for ; seen < len(h.order); seen++ {
			id := h.order[seen]
			w := inflight[id]
			v := h.ok(id)
			delete(inflight, id)
			clients[w.client]--
			o.apply(t, chunkC, w.off, uint32(len(w.data)), w.data, v, 0)
		}
		if msg := o.diff(h, 0, 3); msg != "" {
			return msg
		}
	}
	if err := h.s.Index().Check(); err != nil {
		t.Fatal(err)
	}
	return ""
}

// [property] The spike's M1 and M9 as a regression bar: seeds 0-499, 400 steps each, 0 seeds
// wrong; merging against the committed version instead (M2) loses bytes on every one of the first
// 20 seeds.
func TestSubBlockWrites(t *testing.T) {
	seeds := 500
	if testing.Short() {
		seeds = 50
	}
	for seed := range seeds {
		if msg := subBlockRun(t, uint64(seed), 400, plants{}); msg != "" {
			t.Fatalf("seed %d: %s", seed, msg)
		}
	}
	for seed := range 20 {
		if subBlockRun(t, uint64(seed), 400, plants{mergeCommitted: true}) == "" {
			t.Fatalf("seed %d: merging against the committed version lost no bytes", seed)
		}
	}
}
