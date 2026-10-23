package chunkstore

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// A read issued before the cleaner moves its block, whose disk reads are held while the copy
// applies and the disk fills, as an NVMe queue may hold them: its victim is not freed until the
// read completes, so the read returns the block's own bytes, and the victim is freed after.
func TestAReadIssuedBeforeACopyNeverReadsTheReusedVictim(t *testing.T) {
	testAHeldReadKeepsItsVictim(t, false)
}

// The same, with every block the victim holds overwritten after the read was issued, so the
// cleaner finds nothing live in it to copy.
func TestAReadIssuedBeforeAnOverwriteNeverReadsTheReusedVictim(t *testing.T) {
	testAHeldReadKeepsItsVictim(t, true)
}

func testAHeldReadKeepsItsVictim(t *testing.T, kill bool) {
	h := primed(t)
	h.freeEmpty()
	log := h.s.Log()
	var x uint32
	for _, v := range h.s.chooseVictims() {
		if u, st := log.Usage(v); st != disklog.Meta && u.Live > 0 {
			x = v
			break
		}
	}
	lo, hi := cleanGeo.LBA(x, 0), cleanGeo.LBA(x+1, 0)
	type block struct {
		c core.ChunkID
		b uint16
	}
	var in []block
	for _, c := range []core.ChunkID{chunk(1), chunk(2)} {
		for b := range uint16(64) {
			if e, ok := h.s.Index().Lookup(testPool, c, b); ok && e.BlockLBA(b) >= lo && e.BlockLBA(b) < hi {
				in = append(in, block{c, b})
			}
		}
	}
	if len(in) == 0 {
		t.Fatal("no live block in the victim")
	}
	c, b := in[0].c, in[0].b
	e, _ := h.s.Index().Lookup(testPool, c, b)
	orig := append([]byte(nil), h.disk.Read(e.BlockLBA(b), 1)...)
	id := h.read(c, uint32(b)*core.BlockSize, core.BlockSize)
	held := h.queue
	h.queue = nil
	if kill {
		for _, k := range in {
			h.sync(h.write(k.c, uint32(k.b)*core.BlockSize, fill(core.BlockSize, 7), 0))
		}
		if u, _ := log.Usage(x); u.Live != 0 {
			t.Fatalf("segment %d still holds %d live blocks", x, u.Live)
		}
	}
	seq := h.seqOf[x]
	h.s.cfg.CleanFree = 100
	h.s.cl.tried = false
	r := core.NewRand(3)
	for range 500 {
		overwrite(h, r, []core.ChunkID{chunk(3)}, 64)
	}
	if e, _ := h.s.Index().Lookup(testPool, c, b); e.BlockLBA(b) >= lo && e.BlockLBA(b) < hi {
		t.Fatal("the block is still in the victim")
	}
	if h.seqOf[x] != seq {
		t.Fatalf("segment %d was freed and opened again under a read in flight", x)
	}
	h.queue = append(h.queue, held...)
	h.settle()
	if got := h.bytesOf(id, 0, core.BlockSize); string(got) != string(orig) {
		t.Fatalf("the read of block %d of %v returned other bytes", b, c)
	}
	for i := 0; h.seqOf[x] == seq && i < 500; i++ {
		overwrite(h, r, []core.ChunkID{chunk(3)}, 64)
	}
	if h.seqOf[x] == seq {
		t.Fatalf("segment %d was not freed once its read completed", x)
	}
}

// Reads and overwrites of six chunks, with the disk completing commands in any order while the
// cleaner moves the blocks under the reads: every read returns the bytes of the block's last write
// acknowledged before it was issued, or of a write in flight with it.
func TestReadsCompletingOutOfOrderReadWhatWasWritten(t *testing.T) {
	h := newHarnessOn(t, cleanConfig(), cleanGeo)
	h.merging = true
	r := core.NewRand(11)
	chunks := []core.ChunkID{chunk(1), chunk(2), chunk(3), chunk(4), chunk(5), chunk(6)}
	const blocks = 6 * 64
	var value, written [blocks][]byte
	var writing [blocks]uint64
	for i, c := range chunks {
		data := make([]byte, 64*core.BlockSize)
		r.Fill(data)
		for off := 0; off < len(data); off += 8 * core.BlockSize {
			h.sync(h.write(c, uint32(off), data[off:][:8*core.BlockSize], 0))
		}
		for b := range 64 {
			value[i*64+b] = data[b*core.BlockSize:][:core.BlockSize]
		}
	}
	type pending struct {
		id    uint64
		block int
		may   [][]byte
	}
	var reads []*pending
	check := func() {
		reads = slices.DeleteFunc(reads, func(rd *pending) bool {
			if _, ok := h.replies[rd.id]; !ok {
				return false
			}
			got := h.bytesOf(rd.id, 0, core.BlockSize)
			if !slices.ContainsFunc(rd.may, func(b []byte) bool { return string(b) == string(got) }) {
				t.Fatalf("a read of block %d of %v returned bytes no write wrote", rd.block%64, chunks[rd.block/64])
			}
			return true
		})
		for k, id := range writing {
			if _, ok := h.replies[id]; id != 0 && ok {
				h.ok(id)
				value[k], writing[k] = written[k], 0
			}
		}
	}
	h.shuffle = r.Fork()
	for range 8 * int(cleanGeo.DiskBlocks()) {
		k := int(r.Below(blocks))
		rd := &pending{h.read(chunks[k/64], uint32(k%64)*core.BlockSize, core.BlockSize), k, [][]byte{value[k]}}
		if writing[k] != 0 {
			rd.may = append(rd.may, written[k])
		}
		reads = append(reads, rd)
		if k := int(r.Below(blocks)); writing[k] == 0 {
			written[k] = make([]byte, core.BlockSize)
			r.Fill(written[k])
			writing[k] = h.write(chunks[k/64], uint32(k%64)*core.BlockSize, written[k], 0)
			for _, rd := range reads {
				if rd.block == k {
					rd.may = append(rd.may, written[k])
				}
			}
		}
		for len(h.queue) > 32 || r.Chance(1, 2) {
			h.step()
		}
		check()
	}
	h.settle()
	check()
	if st := h.s.CleanStats(); st.Freed < int(cleanGeo.SegmentCount) || st.Copied == 0 {
		t.Fatalf("the cleaner freed %d segments and copied %d blocks under the reads", st.Freed, st.Copied)
	}
}
