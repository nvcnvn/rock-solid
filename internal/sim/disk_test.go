package sim

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func filled(c byte, blocks int) []byte { return bytes.Repeat([]byte{c}, blocks*core.BlockSize) }

func writeAction(t *testing.T, p *Pool, io core.IOID, lba core.LBA, data []byte) core.Action {
	t.Helper()
	b := mustAlloc(t, p, len(data))
	copy(p.Bytes(b), data)
	return core.Action{Kind: core.ActDiskWrite, IO: io, LBA: lba, Vec: []core.IOVec{{Buf: b, Len: uint32(len(data))}}, Len: uint64(len(data))}
}

func TestDiskWritesStayVolatileUntilAFlush(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(16)
	w := writeAction(t, p, 1, 0, filled(1, 1))
	if ev := d.Execute(p, &w); ev.Kind != core.EvDiskDone || ev.IO != 1 || ev.Err != core.DiskOK {
		t.Fatalf("write completion %+v", ev)
	}
	d.Execute(p, &core.Action{Kind: core.ActDiskFsync, IO: 2})
	w = writeAction(t, p, 3, 0, filled(2, 3))
	d.Execute(p, &w)
	if !bytes.Equal(d.Read(0, 3), filled(2, 3)) {
		t.Fatal("reads do not see unflushed writes")
	}
	if d.Unflushed() != 3 {
		t.Fatalf("%d unflushed block writes, want 3", d.Unflushed())
	}
	d.Crash(func(_ int, lba core.LBA) bool { return lba == 1 })
	want := append(append(filled(1, 1), filled(2, 1)...), make([]byte, core.BlockSize)...)
	if !bytes.Equal(d.Read(0, 3), want) || d.Unflushed() != 0 {
		t.Fatal("crash with only block 1 landing: want LBA 0 = 1s, LBA 1 = 2s, LBA 2 = zeros")
	}
}

func TestDiskReadsZerosAndRefusesOutOfRange(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(4)
	buf := mustAlloc(t, p, 2*core.BlockSize)
	r := core.Action{Kind: core.ActDiskRead, IO: 1, LBA: 2, Buf: buf, Len: 2 * core.BlockSize}
	if ev := d.Execute(p, &r); ev.Err != core.DiskOK || !bytes.Equal(p.Bytes(buf), make([]byte, 2*core.BlockSize)) {
		t.Fatalf("unwritten blocks: %+v", ev)
	}
	r.LBA = 3
	if ev := d.Execute(p, &r); ev.Err != core.DiskInvalidRange {
		t.Fatalf("read past the end: %+v", ev)
	}
	w := writeAction(t, p, 2, 4, filled(1, 1))
	if ev := d.Execute(p, &w); ev.Err != core.DiskInvalidRange {
		t.Fatalf("write past the end: %+v", ev)
	}
}

func TestDiskFaults(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	d.Put(0, filled(7, 2))
	d.FlipBit(1, 10, 0x10)
	if got := d.Read(1, 1); got[10] != 7^0x10 || got[11] != 7 {
		t.Fatal("bit flip not applied to exactly one byte")
	}
	d.Crash(func(int, core.LBA) bool { return false })
	if d.Read(1, 1)[10] != 7^0x10 {
		t.Fatal("a flipped bit is durable")
	}
	d.FailReads(1)
	buf := mustAlloc(t, p, 2*core.BlockSize)
	r := core.Action{Kind: core.ActDiskRead, IO: 1, LBA: 0, Buf: buf, Len: 2 * core.BlockSize}
	if ev := d.Execute(p, &r); ev.Err != core.DiskIO {
		t.Fatalf("read of a failing block: %+v", ev)
	}
	r.Len = core.BlockSize
	if ev := d.Execute(p, &r); ev.Err != core.DiskOK {
		t.Fatalf("read of a good block: %+v", ev)
	}

	rd := NewDisk(1 << 10)
	for i := range 1000 {
		w := writeAction(t, p, core.IOID(10+i), core.LBA(i), filled(1, 1))
		rd.Execute(p, &w)
		p.Free(w.Vec[0].Buf)
	}
	rd.CrashRandom(core.NewRand(1))
	landed := 0
	for i := range 1000 {
		if rd.Read(core.LBA(i), 1)[0] == 1 {
			landed++
		}
	}
	if landed < 400 || landed > 600 {
		t.Fatalf("%d of 1000 blocks landed with p = 1/2", landed)
	}
}
