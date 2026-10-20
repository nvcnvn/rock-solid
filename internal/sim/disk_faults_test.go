package sim

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func readAction(t *testing.T, p *Pool, io core.IOID, lba core.LBA, blocks int) core.Action {
	t.Helper()
	return core.Action{Kind: core.ActDiskRead, IO: io, LBA: lba, Buf: mustAlloc(t, p, blocks*core.BlockSize), Len: uint64(blocks * core.BlockSize)}
}

func flushAction(io core.IOID) core.Action { return core.Action{Kind: core.ActDiskFsync, IO: io} }

func trimAction(io core.IOID, lba core.LBA, blocks int) core.Action {
	return core.Action{Kind: core.ActDiskTrim, IO: io, LBA: lba, Len: uint64(blocks * core.BlockSize)}
}

type scriptRun struct {
	failed []int
	reads  map[int][]byte
}

func runScript(t *testing.T, seed uint64, faults ...Fault) scriptRun {
	t.Helper()
	p := NewPool(1<<20, 0)
	d := NewDiskWith(256, DiskSettings{Seed: seed, Latency: DefaultDiskLatency})
	for _, f := range faults {
		d.Inject(f)
	}
	script := []core.Action{
		readAction(t, p, 1, 0, 1),
		writeAction(t, p, 2, 64, filled(2, 3)),
		flushAction(3),
		writeAction(t, p, 4, 67, filled(4, 1)),
		flushAction(5),
		readAction(t, p, 6, 64, 4),
		writeAction(t, p, 7, 128, filled(7, 1)),
		flushAction(8),
		readAction(t, p, 9, 128, 1),
		flushAction(10),
	}
	run := scriptRun{reads: map[int][]byte{}}
	for i := range script {
		a := &script[i]
		if ev := d.Execute(p, a); ev.Err != core.DiskOK {
			run.failed = append(run.failed, i+1)
		}
		if a.Kind == core.ActDiskRead {
			run.reads[i+1] = slices.Clone(p.Bytes(a.Buf)[:a.Len])
		}
	}
	return run
}

func TestDiskInjectedErrorPredicates(t *testing.T) {
	for _, tc := range []struct {
		name string
		when Pred
		want []int
	}{
		{"kind = write", OnKind(core.ActDiskWrite), []int{2, 4, 7}},
		{"kind = flush", OnKind(core.ActDiskFsync), []int{3, 5, 8, 10}},
		{"kind = read, LBA 64 to 65", And(OnKind(core.ActDiskRead), OnBlocks(64, 65)), []int{6}},
		{"index = 4", OnIndex(4), []int{4}},
		{"kind = write, LBA 128 to 128", And(OnKind(core.ActDiskWrite), OnBlocks(128, 128)), []int{7}},
		{"index 4 or 9", Or(OnIndex(4), OnIndex(9)), []int{4, 9}},
		{"flush, not index 3", And(OnKind(core.ActDiskFsync), Not(OnIndex(3))), []int{5, 8, 10}},
		{"first write only", Once(OnKind(core.ActDiskWrite)), []int{2}},
	} {
		if got := runScript(t, 1, Fault{Effect: FailIO, When: tc.when}).failed; !slices.Equal(got, tc.want) {
			t.Errorf("%s: actions %v fail, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDiskRandomErrorsReplayBySeed(t *testing.T) {
	half := func() Fault { return Fault{Effect: FailIO, When: Randomly(1, 2)} }
	first := runScript(t, 3, half()).failed
	if again := runScript(t, 3, half()).failed; !slices.Equal(first, again) {
		t.Fatalf("seed 3 fails %v, then %v", first, again)
	}
	subsets := map[string]bool{}
	for seed := range uint64(20) {
		subsets[fmt.Sprint(runScript(t, seed, half()).failed)] = true
	}
	if len(subsets) < 2 {
		t.Fatal("probability 1/2 fails the same actions on 20 seeds")
	}
}

func TestDiskDamagedReadChangesOneByte(t *testing.T) {
	good := runScript(t, 1)
	bad := runScript(t, 1, Fault{Effect: DamageRead, When: And(OnKind(core.ActDiskRead), OnIndex(9))})
	if len(bad.failed) != 0 {
		t.Fatalf("actions %v fail, want none", bad.failed)
	}
	for i, want := range good.reads {
		diff := 0
		for j := range want {
			if bad.reads[i][j] != want[j] {
				diff++
			}
		}
		if i == 9 && diff != 1 || i != 9 && diff != 0 {
			t.Fatalf("read %d differs in %d bytes", i, diff)
		}
	}

	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	d.Put(0, filled(5, 1))
	d.Inject(Fault{Effect: DamageRead, When: OnIndex(1)})
	r := readAction(t, p, 1, 0, 1)
	d.Execute(p, &r)
	d.Execute(p, &r)
	if !bytes.Equal(p.Bytes(r.Buf), filled(5, 1)) || !bytes.Equal(d.Read(0, 1), filled(5, 1)) {
		t.Fatal("a damaged read changed the disk")
	}
}

func TestDiskFailedReadFillsNothingTrusted(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	d.Put(0, filled(5, 2))
	d.Inject(Fault{Effect: FailIO, When: OnKind(core.ActDiskRead)})
	r := readAction(t, p, 1, 0, 2)
	clear(p.Bytes(r.Buf))
	if ev := d.Execute(p, &r); ev.Err != core.DiskIO {
		t.Fatalf("failed read: %+v", ev)
	}
	if got := p.Bytes(r.Buf); bytes.Equal(got, filled(5, 2)) || bytes.Equal(got, make([]byte, len(got))) {
		t.Fatal("a failed read returned the blocks or left the buffer as it was")
	}
}

func TestDiskFailedWriteLeavesBlocksUndecided(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	d.Put(0, filled(1, 2))
	d.Inject(Fault{Effect: FailIO, When: OnIndex(1)})
	w := writeAction(t, p, 1, 0, filled(2, 2))
	if ev := d.Execute(p, &w); ev.Err != core.DiskIO {
		t.Fatalf("failed write: %+v", ev)
	}
	if !bytes.Equal(d.Read(0, 2), filled(2, 2)) || d.Unflushed() != 2 {
		t.Fatal("a failed write's blocks are not undecided like unflushed ones")
	}
	d.Crash(func(i int, _ core.LBA) bool { return i == 1 })
	if !bytes.Equal(d.Read(0, 2), append(filled(1, 1), filled(2, 1)...)) {
		t.Fatal("crash after a failed write: want LBA 0 = 1s, LBA 1 = 2s")
	}
}

func TestDiskFailedFlushLeavesUnflushedBlocksUndecided(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	d.Inject(Fault{Effect: FailIO, When: OnKind(core.ActDiskFsync)})
	w := writeAction(t, p, 1, 0, filled(2, 3))
	d.Execute(p, &w)
	f := flushAction(2)
	if ev := d.Execute(p, &f); ev.Err != core.DiskIO {
		t.Fatalf("failed flush: %+v", ev)
	}
	if !bytes.Equal(d.Read(0, 3), filled(2, 3)) || d.Unflushed() != 3 {
		t.Fatal("after a failed flush reads must still see the unflushed blocks")
	}
	d.Crash(func(_ int, lba core.LBA) bool { return lba == 1 })
	want := append(append(make([]byte, core.BlockSize), filled(2, 1)...), make([]byte, core.BlockSize)...)
	if !bytes.Equal(d.Read(0, 3), want) {
		t.Fatal("crash after a failed flush: want LBA 0 = zeros, LBA 1 = 2s, LBA 2 = zeros")
	}
}

func TestDiskFlushDecidesUndecidedBlocksBySeed(t *testing.T) {
	outcomes := map[byte]bool{}
	for seed := range uint64(32) {
		p := NewPool(1<<20, 0)
		d := NewDiskWith(8, DiskSettings{Seed: seed, Latency: DefaultDiskLatency})
		d.Put(0, filled(1, 1))
		d.Inject(Fault{Effect: FailIO, When: OnIndex(1)})
		w := writeAction(t, p, 1, 0, filled(2, 1))
		d.Execute(p, &w)
		f := flushAction(2)
		if ev := d.Execute(p, &f); ev.Err != core.DiskOK {
			t.Fatalf("flush: %+v", ev)
		}
		got := d.Read(0, 1)
		d.Crash(func(int, core.LBA) bool { return false })
		if !bytes.Equal(d.Read(0, 1), got) || !bytes.Equal(got, filled(got[0], 1)) {
			t.Fatalf("seed %d: a flushed block changed at a crash or holds two writes", seed)
		}
		outcomes[got[0]] = true
	}
	if !outcomes[1] || !outcomes[2] {
		t.Fatalf("a flush after a failed write lands it always or never: %v", outcomes)
	}
}

func TestDiskMisdirectsWrites(t *testing.T) {
	land := func(seed uint64, f Fault) (*Disk, MisdirectedWrite) {
		p := NewPool(1<<20, 0)
		d := NewDiskWith(64, DiskSettings{Seed: seed, Latency: DefaultDiskLatency})
		d.Put(10, filled(1, 2))
		d.Inject(f)
		w := writeAction(t, p, 7, 10, filled(2, 2))
		if ev := d.Execute(p, &w); ev.Err != core.DiskOK {
			t.Fatalf("misdirected write: %+v", ev)
		}
		m := d.Misdirected()
		if len(m) != 1 {
			t.Fatalf("%d misdirected writes reported, want 1", len(m))
		}
		if !bytes.Equal(d.Read(10, 2), filled(1, 2)) || !bytes.Equal(d.Read(m[0].Landed, 2), filled(2, 2)) {
			t.Fatalf("write to LBA 10 reported at LBA %d: the intended blocks changed or the landed ones did not", m[0].Landed)
		}
		return d, m[0]
	}
	seeded := Fault{Effect: Misdirect, When: OnKind(core.ActDiskWrite)}
	_, m := land(5, seeded)
	if m.Index != 1 || m.IO != 7 || m.Intended != 10 || m.Landed == 10 || uint64(m.Landed)+2 > 64 || m.Blocks != 2 {
		t.Fatalf("misdirected write %+v", m)
	}
	if _, again := land(5, seeded); again != m {
		t.Fatalf("seed 5 misdirects to %+v, then %+v", m, again)
	}
	d, m := land(5, Fault{Effect: Misdirect, When: OnIndex(1), To: func(DiskOp) core.LBA { return 40 }})
	if m.Landed != 40 {
		t.Fatalf("a rule naming LBA 40 landed the write at %d", m.Landed)
	}
	d.Crash(func(int, core.LBA) bool { return true })
	if !bytes.Equal(d.Read(40, 2), filled(2, 2)) || !bytes.Equal(d.Read(10, 2), filled(1, 2)) {
		t.Fatal("a crash moved the misdirected write")
	}
}

func TestDiskTearsSectorsInsideABlock(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDiskWith(8, DiskSettings{Latency: DefaultDiskLatency, TearSectors: true})
	d.Put(0, filled(1, 1))
	w := writeAction(t, p, 1, 0, filled(2, 1))
	d.Execute(p, &w)
	d.CrashSectors(func(_ int, _ core.LBA, s int) bool { return s == 0 || s == 3 })
	got := d.Read(0, 1)
	for s := range core.BlockSize / SectorSize {
		want := byte(1)
		if s == 0 || s == 3 {
			want = 2
		}
		if !bytes.Equal(got[s*SectorSize:(s+1)*SectorSize], bytes.Repeat([]byte{want}, SectorSize)) {
			t.Fatalf("sector %d: want %ds", s, want)
		}
	}

	const blocks = 256
	td := NewDiskWith(blocks, DiskSettings{Latency: DefaultDiskLatency, TearSectors: true})
	td.Put(0, filled(1, blocks))
	for i := range blocks {
		w := writeAction(t, p, core.IOID(i), core.LBA(i), filled(2, 1))
		td.Execute(p, &w)
		p.Free(w.Vec[0].Buf)
	}
	td.CrashRandom(core.NewRand(9))
	torn := 0
	for i := range blocks {
		blk := td.Read(core.LBA(i), 1)
		for s := 0; s < core.BlockSize; s += SectorSize {
			if !bytes.Equal(blk[s:s+SectorSize], bytes.Repeat(blk[s:s+1], SectorSize)) {
				t.Fatalf("block %d: sector at %d is not whole", i, s)
			}
		}
		if !bytes.Equal(blk, filled(blk[0], 1)) {
			torn++
		}
	}
	if torn == 0 {
		t.Fatal("tearing on, and no block landed in part")
	}
}

func TestDiskFlipsSeededBitsOverDurableBlocks(t *testing.T) {
	flip := func(seed uint64) (*Disk, []core.LBA) {
		p := NewPool(1<<20, 0)
		d := NewDiskWith(256, DiskSettings{Seed: seed, Latency: DefaultDiskLatency})
		d.Put(0, filled(7, 200))
		w := writeAction(t, p, 1, 0, filled(9, 1))
		d.Execute(p, &w)
		return d, d.FlipRandomBits(1, 2)
	}
	d, flipped := flip(4)
	if len(flipped) < 70 || len(flipped) > 130 {
		t.Fatalf("%d of 200 blocks flipped with p = 1/2", len(flipped))
	}
	if _, again := flip(4); !slices.Equal(flipped, again) {
		t.Fatal("seed 4 flips different blocks on a second run")
	}
	if !bytes.Equal(d.Read(0, 1), filled(9, 1)) {
		t.Fatal("a bit flip under an unflushed write shows through it")
	}
	d.Crash(func(int, core.LBA) bool { return false })
	for lba := range core.LBA(200) {
		bits := 0
		for _, b := range d.Read(lba, 1) {
			for x := b ^ 7; x != 0; x &= x - 1 {
				bits++
			}
		}
		if want := btoi(slices.Contains(flipped, lba)); bits != want {
			t.Fatalf("block %d differs in %d bits, want %d", lba, bits, want)
		}
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestDiskLatencyAndFailSlow(t *testing.T) {
	p := NewPool(1<<20, 0)
	d := NewDisk(8)
	actions := func() []core.Action {
		return []core.Action{readAction(t, p, 1, 0, 1), writeAction(t, p, 2, 0, filled(1, 1)), flushAction(3), trimAction(4, 1, 1)}
	}
	check := func(factor core.Tick) {
		t.Helper()
		want := []core.Tick{80, 20, 1000, 20}
		for i, a := range actions() {
			ev, took := d.ExecuteTimed(p, &a)
			if ev.Err != core.DiskOK || took != want[i]*factor {
				t.Fatalf("slow x%d, action %d: %+v in %d µs, want OK in %d µs", factor, i, ev, took, want[i]*factor)
			}
		}
	}
	check(1)
	d.SetSlow(100)
	check(100)
	d.SetSlow(1)
	check(1)

	custom := NewDiskWith(8, DiskSettings{Latency: DiskLatency{Read: 3, Write: 5, Flush: 7, Trim: 11}})
	f := flushAction(1)
	if _, took := custom.ExecuteTimed(p, &f); took != 7 {
		t.Fatalf("flush with a 7 µs setting took %d µs", took)
	}
}

func TestDiskTrim(t *testing.T) {
	outcomes := map[byte]bool{}
	for seed := range uint64(20) {
		p := NewPool(1<<20, 0)
		d := NewDiskWith(8, DiskSettings{Seed: seed, Latency: DefaultDiskLatency})
		d.Put(0, filled(1, 4))
		tr := trimAction(1, 1, 2)
		if ev := d.Execute(p, &tr); ev.Err != core.DiskOK {
			t.Fatalf("trim: %+v", ev)
		}
		got := d.Read(1, 2)
		if !bytes.Equal(got, filled(got[0], 2)) || got[0] != 0 && got[0] != 1 {
			t.Fatalf("seed %d: trimmed blocks read neither all zeros nor their old contents", seed)
		}
		if !bytes.Equal(d.Read(0, 1), filled(1, 1)) || !bytes.Equal(d.Read(3, 1), filled(1, 1)) {
			t.Fatalf("seed %d: a trim reached past its range", seed)
		}
		outcomes[got[0]] = true
		d.Crash(func(int, core.LBA) bool { return false })
		if !bytes.Equal(d.Read(1, 2), filled(1, 2)) {
			t.Fatalf("seed %d: a trim survived a crash without a flush", seed)
		}
		d.Execute(p, &tr)
		got = d.Read(1, 2)
		f := flushAction(2)
		d.Execute(p, &f)
		d.Crash(func(int, core.LBA) bool { return false })
		if !bytes.Equal(d.Read(1, 2), got) {
			t.Fatalf("seed %d: a flushed trim was undone by a crash", seed)
		}
	}
	if !outcomes[0] || !outcomes[1] {
		t.Fatalf("trims on 20 seeds read only as %v", outcomes)
	}
	p := NewPool(1<<20, 0)
	tr := trimAction(1, 7, 2)
	if ev := NewDisk(8).Execute(p, &tr); ev.Err != core.DiskInvalidRange {
		t.Fatalf("trim past the end: %+v", ev)
	}
}
