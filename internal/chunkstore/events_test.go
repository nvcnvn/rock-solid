package chunkstore

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// A write larger than one record goes to the log as several items of one version, the freeze in
// the first: on the 1 GiB test disk (64-block rounds) a whole chunk is 17 items, 16 of 62 blocks
// and one of 32. A 1-block W2 to the chunk right behind it merges against W1's pending block and
// is mapped and answered after W1.
func TestWriteOfSeveralItems(t *testing.T) {
	h := newHarness(t, testConfig())
	if n := h.s.Log().MaxWriteBlocks(); n != 62 {
		t.Fatalf("an item holds %d blocks", n)
	}
	rec := len(h.records)
	data := make([]byte, core.ChunkSize)
	core.NewRand(3).Fill(data)
	w1 := h.write(chunkC, 0, data, 1)
	w2 := h.write(chunkC, 5*core.BlockSize+7, []byte{0xee}, 1)
	h.settle()
	v1, v2 := h.ok(w1), h.ok(w2)
	if slices.Index(h.order, w2) < slices.Index(h.order, w1) || v2 != v1+1 {
		t.Fatalf("W2 at %#x answered before W1 at %#x", v2, v1)
	}
	var want []string
	want = append(want, "freeze v0x0 e1")
	for i := range 16 {
		want = append(want, fmt.Sprintf("write %d+62 v%#x", i*62, v1))
	}
	want = append(want, fmt.Sprintf("write 992+32 v%#x", v1), fmt.Sprintf("write 5+1 v%#x", v2))
	if got := h.logged(rec, chunkC); !slices.Equal(got, want) {
		t.Fatalf("logged %q", got)
	}
	data[5*core.BlockSize+7] = 0xee
	if got := h.readNow(chunkC, 0, core.ChunkSize); !bytes.Equal(got, data) {
		t.Fatalf("chunk differs at byte %d", firstDiff(got, data))
	}
}

// While a write of several items is partly durable, the replay point stays short of its first
// item's record, and a crash then leaves the durable items mapped under its version, so the next
// write's version continues past it.
func TestPartlyDurableWrite(t *testing.T) {
	h := newHarness(t, testConfig())
	rec := len(h.records)
	w := h.write(chunkC, 0, fill(core.ChunkSize, 0x5a), 0)
	first := uint64(0)
	for {
		h.step()
		if first == 0 && len(h.records) > rec {
			first = h.records[rec].lsn
		}
		if first != 0 && h.s.Log().DurableLSN() >= first+2 {
			break
		}
	}
	if _, ok := h.replies[w]; ok {
		t.Fatal("answered before every item is durable")
	}
	if got := h.s.replayPoint(); got != first-1 {
		t.Fatalf("replay point %d with the first item at LSN %d", got, first)
	}
	durable := h.s.Log().DurableLSN()
	h.disk.Crash(func(int, core.LBA) bool { return false })
	h.recoverStore(disklog.ModeOpen)
	if h.s.Log().DurableLSN() != durable {
		t.Fatalf("recovered to %d, durable %d", h.s.Log().DurableLSN(), durable)
	}
	ch, ok := h.s.Index().Chunk(testPool, chunkC)
	if !ok || ch.Version != 1<<32|1 {
		t.Fatalf("entry %+v after the crash", ch)
	}
	if got := h.readNow(chunkC, 0, core.BlockSize); !bytes.Equal(got, fill(core.BlockSize, 0x5a)) {
		t.Fatal("the durable items of the unacknowledged write are not mapped")
	}
	if got := h.readNow(chunkC, core.ChunkSize-core.BlockSize, core.BlockSize); !bytes.Equal(got, make([]byte, core.BlockSize)) {
		t.Fatal("an item that never became durable is mapped")
	}
	if v := h.sync(h.write(chunkC, 0, []byte{1}, 0)); v != 1<<32|2 {
		t.Fatalf("the next write took %#x", v)
	}
}

// smallDisk is a harness on a 16 MiB disk of 4-block segments.
func smallDisk(t *testing.T, cfg Config) *harness {
	h := &harness{t: t, pool: sim.NewPool(16<<20, 0), disk: sim.NewDisk(4096), cfg: cfg,
		replies: map[uint64]Reply{}, seqOf: map[uint32]uint64{}, crashAt: -1}
	h.ctx = core.Context{Now: 1, Raw: 2, Pool: h.pool}
	geo, err := disklog.GeometryFor(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if h.s, err = Format(&h.ctx, cfg, disklog.Superblock{LogID: 0x5eed, Geometry: geo}, &h.out); err != nil {
		t.Fatal(err)
	}
	h.take()
	h.settle()
	return h
}

// With only the reserve free and no cleaner to free a segment, the writes waiting for room fail
// with NoSpace to their callers in submit order, and later writes of their chunks may not carry
// their bytes.
func TestNoSpaceFailsInSubmitOrder(t *testing.T) {
	cfg := testConfig()
	cfg.Log.NoReserve, cfg.Log.Reserve = false, 1000
	h := smallDisk(t, cfg)
	var ids []uint64
	for i := range 60 {
		ids = append(ids, h.write(chunkC, uint32(i%8)*core.BlockSize+1, []byte{byte(i)}, 0))
	}
	h.settle()
	failedFrom := -1
	for i, id := range ids {
		err := h.reply(id).Err
		switch {
		case err == nil && failedFrom >= 0:
			t.Fatalf("write %d succeeded after write %d failed", i, failedFrom)
		case err != nil && !errors.Is(err, disklog.ErrNoSpace):
			t.Fatalf("write %d: %v", i, err)
		case err != nil && failedFrom < 0:
			failedFrom = i
		}
	}
	if failedFrom <= 0 {
		t.Fatalf("first failure at %d", failedFrom)
	}
	o := newByteOracle()
	for i := range failedFrom {
		v := h.ok(ids[i])
		o.apply(t, chunkC, uint32(i%8)*core.BlockSize+1, 1, []byte{byte(i)}, v, 0)
	}
	if msg := o.diff(h, 0, 7); msg != "" {
		t.Fatal(msg)
	}
	if h.pool.Live() != h.liveBefore() {
		t.Fatalf("%d buffers outstanding", h.pool.Live()-h.liveBefore())
	}
}

// A checkpoint that fails keeps the index's frozen memtable for the next, and reads go on.
func TestCheckpointFailedKeepsFrozenMemtable(t *testing.T) {
	cfg := testConfig()
	h := smallDisk(t, cfg)
	o := newByteOracle()
	for i := 0; ; i++ {
		id := h.write(chunkC, uint32(i%8)*core.BlockSize, fill(core.BlockSize, byte(i)), 0)
		h.settle()
		r := h.reply(id)
		if r.Err != nil {
			break
		}
		o.apply(t, chunkC, uint32(i%8)*core.BlockSize, core.BlockSize, fill(core.BlockSize, byte(i)), r.Version, 0)
	}
	tree := h.s.Index().Tree()
	runs := tree.Runs()
	h.s.beginCheckpoint(&h.ctx, &h.out)
	h.s.pump(&h.ctx, &h.out)
	h.take()
	h.settle()
	if !tree.Frozen() || tree.Runs() != runs || h.s.checkpointing {
		t.Fatalf("after the failed checkpoint: frozen %v, %d runs (had %d)", tree.Frozen(), tree.Runs(), runs)
	}
	if msg := o.diff(h, 0, 7); msg != "" {
		t.Fatal(msg)
	}
	if got := h.readNow(chunkC, 0, 1); len(got) != 1 {
		t.Fatal("reads stopped")
	}
}

// Items in flight when the disk fails are never acknowledged: they report ErrInDoubt, queued ones
// the disk's error, and the store refuses writes until it is opened again.
func TestInDoubt(t *testing.T) {
	h := newHarness(t, testConfig())
	h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: sim.OnKind(core.ActDiskFsync)})
	a := h.write(chunkC, 0, []byte{1}, 0)
	b := h.write(chunkC, 1, []byte{2}, 0)
	h.settle()
	if err := h.reply(a).Err; !errors.Is(err, ErrInDoubt) {
		t.Fatalf("write in flight: %v", err)
	}
	if err := h.reply(b).Err; err == nil {
		t.Fatal("write queued behind a failed round succeeded")
	}
	if err := h.reply(h.write(chunkC, 2, []byte{3}, 0)).Err; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("write after the disk failed: %v", err)
	}
	h.disk.ClearFaults()
	h.disk.Crash(func(int, core.LBA) bool { return false })
	h.recoverStore(disklog.ModeOpen)
	h.sync(h.write(chunkC, 2, []byte{3}, 0))
}

// A flush whose run check fails (lsm.ErrCheck) makes the store rebuild its index from the log:
// it refuses operations meanwhile, and the rebuilt index equals the byte-range oracle's.
func TestRunCheckRebuilds(t *testing.T) {
	cfg := testConfig()
	cfg.Log.MaxReplayBytes = 32 * core.BlockSize
	h := newHarness(t, cfg)
	o := newByteOracle()
	r := core.NewRand(9)
	write := func(i int) {
		off := uint32(r.Below(16 * core.BlockSize))
		data := fill(1+int(r.Below(3*core.BlockSize)), byte(i))
		id := h.write(chunkC, off, data, uint64(i/10))
		h.settle()
		o.apply(t, chunkC, off, uint32(len(data)), data, h.ok(id), uint64(i/10))
	}
	for i := range 30 {
		write(i)
	}
	h.s.plant.failCheck = true
	gen, refused := h.s.logGen, 0
	var ids []uint64
	for h.s.plant.failCheck || !h.s.Serving() {
		id := h.write(chunkC, 0, []byte{byte(len(ids))}, 3)
		ids = append(ids, id)
		if r, ok := h.replies[id]; ok && errors.Is(r.Err, ErrUnavailable) {
			refused++
		}
		h.step()
	}
	h.settle()
	for i, id := range ids {
		if r := h.reply(id); r.Err == nil {
			o.apply(t, chunkC, 0, 1, []byte{byte(i)}, r.Version, 3)
		}
	}
	if refused == 0 || h.s.logGen != gen+1 {
		t.Fatalf("%d writes refused, %d restarts of the log", refused, h.s.logGen-gen)
	}
	o.check(t, h, 0, 20, 3)
	for i := 40; i < 60; i++ {
		write(i)
	}
	o.check(t, h, 0, 20, 5)
}

// The store hands the log one run piece at a time, each once the one before is durable.
func TestOnePieceAtATime(t *testing.T) {
	cfg := testConfig()
	cfg.Log.MaxRecordBytes = 4 * core.BlockSize
	cfg.Log.MaxReplayBytes = 64 * core.BlockSize
	h := newHarness(t, cfg)
	r := core.NewRand(4)
	pieces := 0
	for range 400 {
		c := core.ChunkID{Index: uint32(r.Below(40))}
		h.write(c, uint32(r.Below(1000))*core.BlockSize+1, []byte{1}, 0)
		for h.step() {
			if h.s.piecesOut > 1 {
				t.Fatalf("%d pieces in flight", h.s.piecesOut)
			}
			if h.s.inPiece {
				pieces++
			}
		}
	}
	if h.s.Log().Manifest().IsNone() || h.s.Index().Tree().Runs() == 0 {
		t.Fatal("no checkpoint ran")
	}
	if pieces < 10 {
		t.Fatalf("%d steps with a piece in flight", pieces)
	}
}
