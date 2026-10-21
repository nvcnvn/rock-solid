package chunkstore

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// liveLBA is where block b of the chunk is stored.
func liveLBA(t *testing.T, h *harness, c core.ChunkID, b uint16) core.LBA {
	t.Helper()
	e, ok := h.s.Index().Lookup(testPool, c, b)
	if !ok || e.LBA == 0 {
		t.Fatalf("block %d of %v is not stored", b, c)
	}
	return e.BlockLBA(b)
}

// A base read from disk checks the block's CRC32C: a base that fails it, or whose read fails, is
// never merged into a new version, the write fails and nothing is logged for it, and the chunk's
// later operations go on.
func TestBaseReadFails(t *testing.T) {
	for _, fault := range []string{"checksum", "io"} {
		t.Run(fault, func(t *testing.T) {
			h := newHarness(t, testConfig())
			h.sync(h.write(chunkC, 0, fill(core.BlockSize, 0x11), 0))
			lba := liveLBA(t, h, chunkC, 0)
			want := error(disklog.ErrBadChecksum)
			if fault == "checksum" {
				h.disk.FlipBit(lba, 3000, 0x10)
			} else {
				h.disk.FailReads(lba)
				want = core.DiskIO
			}
			rec := len(h.records)
			bad := h.write(chunkC, 100, []byte{0x22}, 0)
			next := h.write(chunkC, core.BlockSize+5, []byte{0x33}, 0)
			h.settle()
			if err := h.reply(bad).Err; !errors.Is(err, want) {
				t.Fatalf("write over a bad base: %v, want %v", err, want)
			}
			v := h.ok(next)
			if got := h.logged(rec, chunkC); !slices.Equal(got, []string{"write 1+1 v0x100000002"}) {
				t.Fatalf("logged %q", got)
			}
			if v != 1<<32|2 {
				t.Fatalf("the next write took %#x", v)
			}
			id := h.read(chunkC, 0, 10)
			h.settle()
			if err := h.reply(id).Err; !errors.Is(err, want) {
				t.Fatalf("read of the bad block: %v, want %v", err, want)
			}
		})
	}
}

// A base read in flight holds the chunk's later operations: W1 partial (base on disk), W2
// whole-block to the same block, W3 to another chunk. W3 goes out at once; nothing of the chunk
// is written before W1's base read completes, W2 is ordered after W1, and the block ends as W2.
func TestBaseReadHoldsChunk(t *testing.T) {
	h := newHarness(t, testConfig())
	other := core.ChunkID{Volume: 2}
	h.sync(h.write(chunkC, 0, fill(core.BlockSize, 0x11), 0))
	rec := len(h.records)
	w1 := h.write(chunkC, 100, []byte{0x22}, 0)
	w2 := h.write(chunkC, 0, fill(core.BlockSize, 0x33), 0)
	w3 := h.write(other, 0, fill(core.BlockSize, 0x44), 0)
	baseReads, writes := 0, 0
	for _, a := range h.queue {
		switch a.Kind {
		case core.ActDiskRead:
			baseReads++
		case core.ActDiskWrite:
			writes++
		}
	}
	if writes != 1 {
		t.Fatalf("%d writes went out at once, want W3's round", writes)
	}
	if baseReads != 2 {
		t.Fatalf("%d reads queued, want the base block and its header", baseReads)
	}
	for baseReads > 0 {
		a := h.queue[0]
		if a.Kind == core.ActDiskRead {
			baseReads--
		}
		h.step()
		if baseReads > 0 && len(h.logged(rec, chunkC)) > 0 {
			t.Fatal("the chunk was written before its base read completed")
		}
	}
	h.settle()
	v1, v2 := h.ok(w1), h.ok(w2)
	h.ok(w3)
	if v2 != v1+1 {
		t.Fatalf("W1 at %#x, W2 at %#x", v1, v2)
	}
	if got := h.readNow(chunkC, 0, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, 0x33)) {
		t.Fatal("the block does not end as W2")
	}
}

// committed puts a committed whole-block write of version v at block 0 of c into the index.
func committed(t *testing.T, h *harness, c core.ChunkID, v uint64) {
	t.Helper()
	ev := disklog.Event{Kind: disklog.Durable, LSN: 1, LBA: 1 << 20, HeaderBlocks: 1,
		Entries: []disklog.Entry{disklog.WriteEntry(testPool, c, v, 0, 1)}, Payloads: []uint64{0}}
	if err := h.s.Index().Apply(&ev); err != nil {
		t.Fatal(err)
	}
}

// Versions are a 32-bit interval over a 32-bit counter per chunk: the newest ordered version plus
// one within the interval, or (interval, 1), the interval raised when a counter runs out.
func TestVersions(t *testing.T) {
	h := newHarness(t, testConfig())
	whole := func(c core.ChunkID) uint64 { return h.write(c, core.BlockSize, fill(core.BlockSize, 1), 0) }
	a, b, c, d, e := core.ChunkID{Index: 1}, core.ChunkID{Index: 2}, core.ChunkID{Index: 3}, core.ChunkID{Index: 4}, core.ChunkID{Index: 5}
	committed(t, h, a, 1<<32|7)
	committed(t, h, b, 1<<32|6)
	committed(t, h, c, 1<<32|0xffffffff)
	committed(t, h, d, 1<<32|5)
	ids := []uint64{whole(a), whole(b), whole(b), whole(c), whole(d), whole(e)}
	h.settle()
	want := []uint64{1<<32 | 8, 1<<32 | 7, 1<<32 | 8, 2<<32 | 1, 2<<32 | 1, 2<<32 | 1}
	for i, id := range ids {
		if got := h.ok(id); got != want[i] {
			t.Errorf("write %d: version %#x, want %#x", i, got, want[i])
		}
	}
}

// expectVer compares with the chunk's newest ordered version, pending writes included.
func TestExpectVerOrdered(t *testing.T) {
	h := newHarness(t, testConfig())
	v0 := h.sync(h.write(chunkC, 0, fill(core.BlockSize, 1), 0))
	w1 := h.write(chunkC, 0, []byte{2}, 0)
	stale := h.writeExpect(chunkC, 0, []byte{3}, v0)
	next := h.writeExpect(chunkC, 0, []byte{4}, v0+1)
	h.settle()
	if err := h.reply(stale).Err; !errors.Is(err, ErrVersion) {
		t.Fatalf("expectVer of the committed version behind a pending write: %v", err)
	}
	if v1, v2 := h.ok(w1), h.ok(next); v1 != v0+1 || v2 != v0+2 {
		t.Fatalf("versions %#x %#x after %#x", v1, v2, v0)
	}
}

// A read at a frozen version returns that version's bytes and never waits for a writer; a read
// without atVer while W is pending returns the previous version's blocks; a read at a version no
// snapshot pins is ErrNoVersion.
func TestReadVersions(t *testing.T) {
	h := newHarness(t, testConfig())
	v1 := h.sync(h.write(chunkC, 0, fill(core.BlockSize, 1), 1))
	v2 := h.sync(h.write(chunkC, 10, fill(10, 2), 2))
	mark := len(h.acts)
	w := h.write(chunkC, 0, fill(core.BlockSize, 3), 3)
	live := h.read(chunkC, 0, 30)
	old := h.readAt(chunkC, 0, 30, v1)
	absent := h.readAt(chunkC, 0, 30, 0)
	gone := h.readAt(chunkC, 0, 30, v1+5)
	if reads(h, mark) != 2 {
		t.Fatalf("%d block reads issued while W is pending, want 2 at once", reads(h, mark))
	}
	h.settle()
	h.ok(w)
	want2 := append(fill(10, 1), fill(10, 2)...)
	if got := h.bytesOf(live, 0, 30); !bytes.Equal(got, append(want2, fill(10, 1)...)) {
		t.Fatalf("live read while W pending: %x", got)
	}
	if got := h.bytesOf(old, 0, 30); !bytes.Equal(got, fill(30, 1)) {
		t.Fatalf("read at %#x: %x", v1, got)
	}
	if got := h.bytesOf(absent, 0, 30); !bytes.Equal(got, make([]byte, 30)) {
		t.Fatalf("read at the absent version: %x", got)
	}
	if err := h.reply(gone).Err; !errors.Is(err, ErrNoVersion) {
		t.Fatalf("read at an unpinned version: %v", err)
	}
	if r := h.reply(old); r.Version != v1 {
		t.Fatalf("read at %#x reports %#x", v1, r.Version)
	}
	id := h.readAt(chunkC, 0, 30, v2)
	h.settle()
	if got := h.bytesOf(id, 0, 30); !bytes.Equal(got, append(want2, fill(10, 1)...)) {
		t.Fatalf("read at frozen %#x: %x", v2, got)
	}
}

// Freezes on the writer side: the first write after a cut to a chunk at v puts freeze(v, epoch)
// before its blocks; a pending W1 at epoch 1 is frozen by W2 at epoch 2; a second write at epoch 2
// carries none; a chunk with no version freezes version 0; a punch of a chunk that does not exist
// logs nothing; in a volume whose snapshot epoch is 0 nothing freezes.
func TestFreezes(t *testing.T) {
	h := newHarness(t, testConfig())
	other, fresh := core.ChunkID{Volume: 3}, core.ChunkID{Volume: 4}
	rec := len(h.records)
	h.sync(h.write(other, 0, []byte{1}, 0))
	if got := h.logged(rec, other); !slices.Equal(got, []string{"write 0+1 v0x100000001"}) {
		t.Fatalf("epoch 0: logged %q", got)
	}
	rec = len(h.records)
	w1 := h.write(chunkC, 0, []byte{1}, 1)
	w2 := h.write(chunkC, 1, []byte{2}, 2)
	w3 := h.write(chunkC, 2, []byte{3}, 2)
	p := h.punch(fresh, 0, 10, 2)
	h.settle()
	for _, id := range []uint64{w1, w2, w3} {
		h.ok(id)
	}
	if v := h.ok(p); v != 0 {
		t.Fatalf("a punch of an absent chunk took %#x", v)
	}
	want := []string{
		"freeze v0x0 e1", "write 0+1 v0x100000001",
		"freeze v0x100000001 e2", "write 0+1 v0x100000002",
		"write 0+1 v0x100000003",
	}
	if got := h.logged(rec, chunkC); !slices.Equal(got, want) {
		t.Fatalf("logged %q, want %q", got, want)
	}
	if got := h.logged(rec, fresh); len(got) != 0 {
		t.Fatalf("a punch of an absent chunk logged %q", got)
	}
	if v, ok := h.s.Index().FrozenAt(testPool, chunkC, 1); !ok || v != 0 {
		t.Fatalf("snapshot 1 reads version %#x %v, want the absent chunk", v, ok)
	}
	if v, ok := h.s.Index().FrozenAt(testPool, chunkC, 2); !ok || v != 1<<32|1 {
		t.Fatalf("snapshot 2 reads version %#x %v", v, ok)
	}
}

// Flush completes at once with nothing pending, and otherwise once every write and punch of its
// chunks submitted before it is answered.
func TestFlush(t *testing.T) {
	h := newHarness(t, testConfig())
	mark := len(h.acts)
	h.s.Flush(&h.ctx, 900, []core.ChunkID{chunkC}, &h.out)
	h.take()
	if _, ok := h.replies[900]; !ok || len(h.acts) != mark {
		t.Fatal("a flush with nothing pending did not complete at once without disk actions")
	}
	w := h.write(chunkC, 0, []byte{1}, 0)
	h.s.Flush(&h.ctx, 901, []core.ChunkID{core.ChunkID{Volume: 5}, chunkC}, &h.out)
	h.take()
	later := h.write(chunkC, 1, []byte{2}, 0)
	if _, ok := h.replies[901]; ok {
		t.Fatal("the flush completed before the write before it")
	}
	h.settle()
	order := slices.Index(h.order, 901)
	if order < slices.Index(h.order, w) {
		t.Fatal("the flush completed before the write before it")
	}
	h.ok(later)
	h.ok(901)
}
