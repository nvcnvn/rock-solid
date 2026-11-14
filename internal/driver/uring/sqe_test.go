package uring

import (
	"slices"
	"testing"
	"unsafe"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const bs = core.BlockSize

func testPool(t testing.TB) (*core.Pool, [][]byte) {
	t.Helper()
	rs := [][]byte{make([]byte, 16*bs), make([]byte, 16*bs)}
	return core.NewPool(rs, 0), rs
}

func alloc(t testing.TB, p *core.Pool, n int) core.IOBuf {
	t.Helper()
	b, err := p.Alloc(n)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func addr(p core.BufPool, b core.IOBuf) uint64 {
	return uint64(uintptr(unsafe.Pointer(&p.Bytes(b)[0])))
}

func TestEntriesMatchTheKernelLayout(t *testing.T) {
	if s := unsafe.Sizeof(sqe{}); s != 64 {
		t.Errorf("sqe is %d bytes, want 64", s)
	}
}

func TestPrepareBuildsEachSubmission(t *testing.T) {
	p, rs := testPool(t)
	g := newRegions(rs)
	g.zero = 0x7000
	fixed := g
	fixed.fixed = true
	dev := target{fd: 3, fixed: true, blocks: 100}
	file := target{fd: 7, blocks: 100, file: true}
	fua := target{fd: 3, blocks: 100, fua: true}
	one := alloc(t, p, 2*bs)
	for range 15 {
		alloc(t, p, bs)
	}
	second := alloc(t, p, bs)
	v1, v2 := alloc(t, p, bs), alloc(t, p, 2*bs)
	for _, c := range []struct {
		name  string
		a     core.Action
		t     target
		g     regions
		ok    bool
		err   core.DiskError
		check func(t *testing.T, o *op)
	}{
		{"fixed read", core.Action{Kind: core.ActDiskRead, IO: 9, LBA: 4, Buf: one, Len: 2 * bs}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			want := sqe{opcode: opReadFixed, flags: sqeFixedFile, fd: 3, off: 4 * bs, addr: addr(p, one), len: 2 * bs}
			if o.sqe != want || o.want != 2*bs || o.io != 9 {
				t.Errorf("got %+v want %+v", o.sqe, want)
			}
			if o.sent = true; !slices.Equal(o.held(), []core.IOBuf{one}) {
				t.Errorf("sent, holds %v", o.held())
			}
		}},
		{"fixed write in the second region", core.Action{Kind: core.ActDiskWrite, Buf: second, Len: bs}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			if o.sqe.opcode != opWriteFixed || o.sqe.bufIndex != 1 || o.sqe.addr != addr(p, second) {
				t.Errorf("got %+v", o.sqe)
			}
		}},
		{"plain read without registration", core.Action{Kind: core.ActDiskRead, Buf: one, Len: bs}, file, g, true, 0, func(t *testing.T, o *op) {
			if o.sqe.opcode != opRead || o.sqe.flags != 0 || o.sqe.fd != 7 || o.sqe.bufIndex != 0 {
				t.Errorf("got %+v", o.sqe)
			}
		}},
		{"a vector of one is a fixed write", core.Action{Kind: core.ActDiskWrite, Vec: []core.IOVec{{Buf: v2, Len: 2 * bs}}, Len: 2 * bs}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			if o.sqe.opcode != opWriteFixed || o.sqe.addr != addr(p, v2) || o.sqe.len != 2*bs {
				t.Errorf("got %+v", o.sqe)
			}
		}},
		{"a vector writes its buffers in order", core.Action{Kind: core.ActDiskWrite, LBA: 1, Vec: []core.IOVec{{Buf: v2, Len: bs}, {Buf: v1, Len: bs}, {Buf: v2, Len: 2 * bs}}, Len: 4 * bs}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			if o.sqe.opcode != opWritev || o.sqe.len != 3 || o.sqe.off != bs || o.sqe.addr != uint64(uintptr(unsafe.Pointer(&o.iov[0]))) {
				t.Errorf("got %+v", o.sqe)
			}
			for i, w := range []struct {
				b core.IOBuf
				n uint64
			}{{v2, bs}, {v1, bs}, {v2, 2 * bs}} {
				if uint64(uintptr(unsafe.Pointer(o.iov[i].Base))) != addr(p, w.b) || o.iov[i].Len != w.n {
					t.Errorf("iov %d: %+v", i, o.iov[i])
				}
			}
			if len(o.held()) != 0 {
				t.Errorf("unsent, holds %v", o.held())
			}
			if o.sent = true; !slices.Equal(o.held(), []core.IOBuf{v2, v1, v2}) {
				t.Errorf("sent, holds %v", o.held())
			}
		}},
		{"fua write", core.Action{Kind: core.ActDiskWrite, Buf: one, Len: bs}, fua, g, true, 0, func(t *testing.T, o *op) {
			if o.sqe.opcode != opWrite || o.sqe.opFlags != rwfDsync {
				t.Errorf("got %+v", o.sqe)
			}
		}},
		{"flush", core.Action{Kind: core.ActDiskFsync, IO: 4}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			want := sqe{opcode: opFsync, flags: sqeFixedFile, fd: 3, opFlags: fsyncDatasync}
			if o.sqe != want {
				t.Errorf("got %+v want %+v", o.sqe, want)
			}
		}},
		{"device trim", core.Action{Kind: core.ActDiskTrim, LBA: 10, Len: 20 * bs}, dev, fixed, true, 0, func(t *testing.T, o *op) {
			want := sqe{opcode: opURingCmd, flags: sqeFixedFile | sqeIOHardlink, fd: 3, off: blockCmdDiscard, addr: 10 * bs, addr3: 20 * bs}
			if o.sqe != want || !o.quiet {
				t.Errorf("got %+v want %+v, quiet %v", o.sqe, want, o.quiet)
			}
		}},
		{"trim of a file writes zeros over the segment header", core.Action{Kind: core.ActDiskTrim, IO: 6, LBA: 10, Len: 20 * bs}, file, g, true, 0, func(t *testing.T, o *op) {
			want := sqe{opcode: opWrite, fd: 7, off: 10 * bs, addr: 0x7000, len: bs, userData: 0}
			if o.sqe != want || o.quiet || o.kind != core.ActDiskWrite || o.want != bs || o.io != 6 {
				t.Errorf("got %+v want %+v, op %+v", o.sqe, want, o)
			}
			if o.sent = true; len(o.held()) != 0 {
				t.Errorf("a header write holds %v", o.held())
			}
		}},
		{"flush under fua submits nothing", core.Action{Kind: core.ActDiskFsync}, fua, g, false, core.DiskOK, nil},
		{"trim off the disk", core.Action{Kind: core.ActDiskTrim, LBA: 99, Len: 2 * bs}, file, g, false, core.DiskInvalidRange, nil},
		{"zero length", core.Action{Kind: core.ActDiskRead, Buf: one}, dev, g, false, core.DiskInvalidRange, nil},
		{"part of a block", core.Action{Kind: core.ActDiskRead, Buf: one, Len: bs + 512}, dev, g, false, core.DiskInvalidRange, nil},
		{"past the end", core.Action{Kind: core.ActDiskWrite, LBA: 99, Buf: one, Len: 2 * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"start past the end", core.Action{Kind: core.ActDiskRead, LBA: 100, Buf: one, Len: bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"longer than the buffer", core.Action{Kind: core.ActDiskRead, Buf: second, Len: 2 * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"vector lengths short of Len", core.Action{Kind: core.ActDiskWrite, Vec: []core.IOVec{{Buf: v1, Len: bs}, {Buf: v2, Len: bs}}, Len: 3 * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"vector of one short of Len", core.Action{Kind: core.ActDiskWrite, Vec: []core.IOVec{{Buf: v2, Len: bs}}, Len: 2 * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"vector element past its buffer", core.Action{Kind: core.ActDiskWrite, Vec: []core.IOVec{{Buf: v1, Len: 2 * bs}, {Buf: v2, Len: bs}}, Len: 3 * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"vector element of part of a block", core.Action{Kind: core.ActDiskWrite, Vec: []core.IOVec{{Buf: v1, Len: 512}, {Buf: v2, Len: bs - 512}}, Len: bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"vector longer than UIO_MAXIOV", core.Action{Kind: core.ActDiskWrite, Vec: make([]core.IOVec, maxIOV+1), Len: (maxIOV + 1) * bs}, dev, g, false, core.DiskInvalidRange, nil},
		{"not a disk action", core.Action{Kind: core.ActNetSend, Len: bs}, dev, g, false, core.DiskInvalidRange, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			var o op
			ok, err := prepare(&o, 0, &c.a, &c.t, &c.g, p)
			if ok != c.ok || err != c.err || ok && !o.live {
				t.Fatalf("prepare = %v, %v, live %v; want %v, %v", ok, err, o.live, c.ok, c.err)
			}
			if c.check != nil {
				c.check(t, &o)
			}
		})
	}
}

func TestCompleteDecodesEachResult(t *testing.T) {
	for _, c := range []struct {
		name string
		kind core.ActionKind
		want uint32
		res  int32
		err  core.DiskError
	}{
		{"write in full", core.ActDiskWrite, 8192, 8192, core.DiskOK},
		{"short write", core.ActDiskWrite, 8192, 4096, core.DiskIO},
		{"write of nothing", core.ActDiskWrite, 8192, 0, core.DiskIO},
		{"write error", core.ActDiskWrite, 8192, -5, core.DiskIO},
		{"read in full", core.ActDiskRead, 4096, 4096, core.DiskOK},
		{"short read", core.ActDiskRead, 8192, 4096, core.DiskIO},
		{"flush", core.ActDiskFsync, 0, 0, core.DiskOK},
		{"flush error", core.ActDiskFsync, 0, -5, core.DiskIO},
		{"flush cancelled behind a failed write", core.ActDiskFsync, 0, -125, core.DiskIO},
		{"flush with a positive result", core.ActDiskFsync, 0, 1, core.DiskIO},
		{"trim", core.ActDiskTrim, 0, 0, core.DiskOK},
		{"trim unsupported", core.ActDiskTrim, 0, -95, core.DiskIO},
	} {
		evs := complete(nil, &op{io: 3, kind: c.kind, want: c.want, then: 4, hasThen: true}, c.res)
		want := []core.Event{{Kind: core.EvDiskDone, IO: 3, Err: c.err}, {Kind: core.EvDiskDone, IO: 4, Err: c.err}}
		if len(evs) != 2 || evs[0] != want[0] || evs[1] != want[1] {
			t.Errorf("%s: %+v", c.name, evs)
		}
	}
	if evs := complete(nil, &op{io: 3, kind: core.ActDiskTrim, quiet: true}, -95); len(evs) != 0 {
		t.Errorf("a discard before its header write completed %+v", evs)
	}
}

func TestADeviceTrimIsADiscardThenAHeaderWrite(t *testing.T) {
	p, rs := testPool(t)
	g := newRegions(rs)
	g.zero = 0x7000
	dev := target{fd: 3, fixed: true, blocks: 100}
	a := core.Action{Kind: core.ActDiskTrim, IO: 8, LBA: 32, Len: 32 * bs}
	var discard, hdr op
	if ok, err := prepare(&discard, 1, &a, &dev, &g, p); !ok || err != core.DiskOK {
		t.Fatalf("prepare = %v, %v", ok, err)
	}
	header(&hdr, 2, &a, &dev, &g)
	want := sqe{opcode: opWrite, flags: sqeFixedFile, fd: 3, off: 32 * bs, addr: 0x7000, len: bs, userData: 2}
	if hdr.sqe != want || !hdr.live || hdr.io != 8 {
		t.Fatalf("header %+v, want %+v", hdr, want)
	}
	for _, res := range []int32{0, -95, -5} {
		evs := complete(complete(nil, &discard, res), &hdr, bs)
		if len(evs) != 1 || evs[0] != (core.Event{Kind: core.EvDiskDone, IO: 8}) {
			t.Errorf("discard result %d: %+v", res, evs)
		}
	}
	if evs := complete(nil, &hdr, -5); len(evs) != 1 || evs[0].Err != core.DiskIO {
		t.Errorf("a failed header write completed %+v", evs)
	}
}

func FuzzComplete(f *testing.F) {
	f.Add(uint8(core.ActDiskWrite), uint32(4096), int32(4096))
	f.Add(uint8(core.ActDiskRead), uint32(8192), int32(4096))
	f.Add(uint8(core.ActDiskFsync), uint32(0), int32(-125))
	f.Fuzz(func(t *testing.T, kind uint8, want uint32, res int32) {
		k := core.ActionKind(kind%4) + core.ActDiskRead
		evs := complete(nil, &op{io: 1, kind: k, want: want}, res)
		ev := evs[0]
		success := res == 0
		if k == core.ActDiskRead || k == core.ActDiskWrite {
			success = res >= 0 && uint32(res) == want
		}
		if len(evs) != 1 || ev.Kind != core.EvDiskDone || ev.IO != 1 || (ev.Err == core.DiskOK) != success || ev.Err != core.DiskOK && ev.Err != core.DiskIO {
			t.Fatalf("kind %d want %d res %d: %+v", k, want, res, ev)
		}
	})
}

func FuzzPrepare(f *testing.F) {
	f.Add(uint8(core.ActDiskWrite), uint64(3), uint64(2*bs), uint8(2), uint16(1), uint16(1), true, false, false)
	f.Add(uint8(core.ActDiskRead), uint64(99), uint64(bs), uint8(0), uint16(0), uint16(0), false, true, false)
	f.Add(uint8(core.ActDiskWrite), uint64(1), uint64(bs), uint8(0), uint16(0), uint16(0), true, false, true)
	f.Add(uint8(core.ActDiskFsync), uint64(0), uint64(0), uint8(0), uint16(0), uint16(0), false, false, true)
	f.Fuzz(func(t *testing.T, kind uint8, lba, n uint64, nvec uint8, l1, l2 uint16, fixed, isFile, fua bool) {
		p, rs := testPool(t)
		g := newRegions(rs)
		g.fixed, g.zero = fixed, 0x7000
		tg := target{fd: 5, blocks: 64, file: isFile, fua: fua}
		bufs := []core.IOBuf{alloc(t, p, 4*bs), alloc(t, p, 2*bs), alloc(t, p, bs)}
		a := core.Action{Kind: core.ActionKind(kind%4) + core.ActDiskRead, LBA: core.LBA(lba), Len: n, Buf: bufs[0]}
		lens := []uint32{uint32(l1) * 512, uint32(l2) * 512, bs}
		for i := range int(nvec % 4) {
			a.Vec = append(a.Vec, core.IOVec{Buf: bufs[i], Len: lens[i]})
		}
		var o op
		ok, err := prepare(&o, 0, &a, &tg, &g, p)
		valid := n > 0 && n%bs == 0 && lba < 64 && n/bs <= 64-lba
		switch a.Kind {
		case core.ActDiskFsync:
			valid = true
		case core.ActDiskRead, core.ActDiskWrite:
			if a.Kind == core.ActDiskRead || len(a.Vec) == 0 {
				valid = valid && n <= uint64(len(p.Bytes(bufs[0])))
			} else {
				var sum uint64
				fits := true
				for i, v := range a.Vec {
					sum += uint64(v.Len)
					fits = fits && v.Len > 0 && v.Len%bs == 0 && int(v.Len) <= len(p.Bytes(bufs[i]))
				}
				valid = valid && fits && sum == n
			}
		}
		if (err == core.DiskOK) != valid {
			t.Fatalf("%+v: prepare = %v, %v; valid %v", a, ok, err, valid)
		}
		if fua && a.Kind == core.ActDiskFsync {
			if ok || err != core.DiskOK {
				t.Fatalf("%+v under fua: prepare = %v, %v; want nothing submitted", a, ok, err)
			}
			return
		}
		if !ok {
			if err == core.DiskOK {
				t.Fatalf("%+v: nothing to submit", a)
			}
			return
		}
		if dsync := o.sqe.opFlags == rwfDsync; dsync != (fua && a.Kind == core.ActDiskWrite) {
			t.Fatalf("%+v under fua %v: op flags %#x", a, fua, o.sqe.opFlags)
		}
		if !o.live {
			t.Fatalf("%+v: not live", a)
		}
		if a.Kind == core.ActDiskFsync {
			return
		}
		if a.Kind == core.ActDiskTrim && isFile {
			if o.sqe.opcode != opWrite || o.sqe.off != lba*bs || o.sqe.addr != 0x7000 || o.sqe.len != bs || o.quiet {
				t.Fatalf("%+v: header write %+v", a, o.sqe)
			}
			return
		}
		if o.sqe.off != lba*bs && a.Kind != core.ActDiskTrim {
			t.Fatalf("offset %d for LBA %d", o.sqe.off, lba)
		}
		var total uint64
		switch o.sqe.opcode {
		case opWritev:
			for _, v := range o.iov {
				total += v.Len
			}
		case opURingCmd:
			total = o.sqe.addr3
		default:
			total = uint64(o.sqe.len)
			if fixed != (o.sqe.opcode == opReadFixed || o.sqe.opcode == opWriteFixed) {
				t.Fatalf("fixed %v, opcode %d", fixed, o.sqe.opcode)
			}
		}
		if total != n {
			t.Fatalf("%+v submits %d bytes", a, total)
		}
	})
}

func TestAnOpIsOnTheDiskItsSubmissionNames(t *testing.T) {
	o := op{sqe: sqe{fd: 1, flags: sqeFixedFile | sqeIOLink}}
	for _, c := range []struct {
		t  target
		on bool
	}{
		{target{fd: 1, fixed: true}, true},
		{target{fd: 1}, false},
		{target{fd: 2, fixed: true}, false},
	} {
		if o.on(&c.t) != c.on {
			t.Errorf("on %+v = %v", c.t, !c.on)
		}
	}
}
