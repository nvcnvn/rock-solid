package chunkstore

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// benchStore is a store whose disk actions complete at once, on a simulated disk when disk is set.
type benchStore struct {
	b    *testing.B
	pool *sim.Pool
	ctx  core.Context
	disk *sim.Disk
	s    *Store
	out  Output
	acts []core.Action
	got  int
}

func newBenchStore(b *testing.B, disk *sim.Disk) *benchStore {
	bs := &benchStore{b: b, pool: sim.NewPool(64<<20, 0), disk: disk}
	bs.ctx = core.Context{Now: 1, Raw: 2, Pool: bs.pool}
	geo, err := disklog.GeometryFor(1 << 40)
	if disk != nil {
		geo, err = disklog.GeometryFor(disk.Blocks() * core.BlockSize)
	}
	if err != nil {
		b.Fatal(err)
	}
	cfg := testConfig()
	cfg.Check, cfg.Log.Check = false, false
	if bs.s, err = Format(&bs.ctx, cfg, disklog.Superblock{LogID: 1, Geometry: geo}, &bs.out); err != nil {
		b.Fatal(err)
	}
	bs.complete()
	return bs
}

// complete runs actions until none are left, and counts and frees the replies.
func (bs *benchStore) complete() {
	for len(bs.out.Actions) > 0 || len(bs.out.Replies) > 0 {
		bs.acts = append(bs.acts[:0], bs.out.Actions...)
		for _, r := range bs.out.Replies {
			if r.Err != nil {
				bs.b.Fatal(r.Err)
			}
			if r.Buf != core.NoBuf {
				bs.pool.Free(r.Buf)
			}
			bs.got++
		}
		bs.out.Reset()
		for i := range bs.acts {
			ev := core.Event{Kind: core.EvDiskDone, IO: bs.acts[i].IO}
			if bs.disk != nil {
				ev = bs.disk.Execute(bs.pool, &bs.acts[i])
			}
			bs.s.OnEvent(&bs.ctx, &ev, &bs.out)
		}
	}
}

func (bs *benchStore) write(c core.ChunkID, off, n uint32) {
	first, last := spanOf(off, n)
	buf, err := bs.pool.Alloc((int(last) - int(first) + 1) * core.BlockSize)
	if err != nil {
		bs.b.Fatal(err)
	}
	bs.s.Write(&bs.ctx, Request{Chunk: c, Off: off, Len: n, Buf: buf}, &bs.out)
}

// BenchmarkStoreWriteBlock writes one whole block to one of 8 chunks through its record's durability and mapping.
func BenchmarkStoreWriteBlock(b *testing.B) {
	bs := newBenchStore(b, nil)
	i := 0
	op := func() {
		bs.write(core.ChunkID{Index: uint32(i % 8)}, uint32(i%16)*core.BlockSize, core.BlockSize)
		bs.complete()
		i++
	}
	for range 1000 {
		op()
	}
	b.ReportAllocs()
	for b.Loop() {
		op()
	}
}

// BenchmarkStoreWriteByteOnPending writes a whole block and, while it is pending, one byte of it,
// whose base is the pending block.
func BenchmarkStoreWriteByteOnPending(b *testing.B) {
	bs := newBenchStore(b, nil)
	i := 0
	op := func() {
		c, off := core.ChunkID{Index: uint32(i % 8)}, uint32(i%16)*core.BlockSize
		bs.write(c, off, core.BlockSize)
		bs.write(c, off+100, 1)
		bs.complete()
		i++
	}
	for range 1000 {
		op()
	}
	b.ReportAllocs()
	for b.Loop() {
		op()
	}
}

// BenchmarkStoreReadBlock reads one block, checking its CRC32C against its record header.
func BenchmarkStoreReadBlock(b *testing.B) {
	bs := newBenchStore(b, sim.NewDisk(diskBytes/core.BlockSize))
	for i := range 16 {
		bs.write(chunkC, uint32(i)*core.BlockSize, core.BlockSize)
		bs.complete()
	}
	i := 0
	op := func() {
		bs.s.Read(&bs.ctx, Request{Chunk: chunkC, Off: uint32(i%16) * core.BlockSize, Len: core.BlockSize}, &bs.out)
		bs.complete()
		i++
	}
	for range 1000 {
		op()
	}
	b.ReportAllocs()
	for b.Loop() {
		op()
	}
}

// BenchmarkStoreWriteWholeChunk writes one of 8 chunks whole to a chunk slot, through its record's
// durability, mapping and the freeing of the slot it rewrote.
func BenchmarkStoreWriteWholeChunk(b *testing.B) {
	bs := newBenchStore(b, nil)
	if bs.s.Log().Slots() == 0 {
		b.Fatal("no chunk slots")
	}
	i := 0
	op := func() {
		bs.write(core.ChunkID{Index: uint32(i % 8)}, 0, core.ChunkSize)
		bs.complete()
		i++
	}
	for range 100 {
		op()
	}
	b.ReportAllocs()
	for b.Loop() {
		op()
	}
}

// BenchmarkStoreReadSlotBlock reads one block of a chunk in a slot, checking its CRC32C against
// its record header.
func BenchmarkStoreReadSlotBlock(b *testing.B) {
	bs := newBenchStore(b, sim.NewDisk(256<<30/core.BlockSize))
	bs.write(chunkC, 0, core.ChunkSize)
	bs.complete()
	i := 0
	op := func() {
		bs.s.Read(&bs.ctx, Request{Chunk: chunkC, Off: uint32(i%1024) * core.BlockSize, Len: core.BlockSize}, &bs.out)
		bs.complete()
		i++
	}
	for range 1000 {
		op()
	}
	b.ReportAllocs()
	for b.Loop() {
		op()
	}
}
