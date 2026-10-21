package disklog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func TestCheckpointBackpressure(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 8 * core.BlockSize
	h := formatted(t, gib, cfg)
	for i := range 3 {
		h.put(disklog.Hot, h.write(uint32(i), 3, uint64(i)))
	}
	h.put(disklog.Hot, h.write(3, 2, 3))
	if h.log.ReplayBytes() != 15*core.BlockSize {
		t.Fatalf("%d bytes to replay, want 15 blocks", h.log.ReplayBytes())
	}
	h.put(disklog.Hot, h.write(4, 1, 4))
	if len(h.eventsOf(disklog.Durable)) != 5 {
		t.Fatalf("a write at 15 blocks was held: %+v", h.events)
	}
	writes := len(h.trace)
	held := []disklog.Ticket{h.submit(disklog.Hot, h.write(5, 1, 5)), h.submit(disklog.Cold, h.write(6, 1, 6))}
	if len(h.trace) != writes || h.log.Stalled() || h.log.Queued() != 2 {
		t.Fatalf("at 17 blocks: trace %v, stalled %v, %d queued", h.trace[writes:], h.log.Stalled(), h.log.Queued())
	}
	h.wantLive(2)

	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 5}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 100, 1)
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 100, true, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.round()
	d := h.eventsOf(disklog.Durable)
	if len(d) != 6 || !slices.Equal(d[5].Tickets, []disklog.Ticket{piece}) || h.log.ReplayBytes() != 19*core.BlockSize {
		t.Fatalf("the piece waited or was not counted: %d bytes to replay, events %+v", h.log.ReplayBytes(), h.events)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.round()
	if len(h.eventsOf(disklog.CheckpointDone)) != 1 || len(h.pending) == 0 {
		t.Fatalf("events %+v", h.events)
	}
	h.run()
	d = h.eventsOf(disklog.Durable)
	if len(d) != 9 || !slices.Equal(d[7].Tickets, held[:1]) || !slices.Equal(d[8].Tickets, held[1:]) {
		t.Fatalf("held items after the manifest: %+v", d[6:])
	}
	h.wantLive(0)
	h.close()
}

func TestStalledCheckpointBoundsReplay(t *testing.T) {
	var holds int
	for seed := uint64(1); seed <= 10; seed++ {
		r := core.NewRand(seed)
		cfg := testConfig(r.Range(3, 16))
		cfg.CommitDepth = int(r.Range(1, 3))
		cfg.MaxReplayBytes = r.Range(8, 40) * core.BlockSize
		h := formatted(t, gib, cfg)
		n := newNode(h, int(r.Range(100, 2000)))
		maxData := min(cfg.MaxRecordBytes/core.BlockSize-2, 8)
		ordinary := map[disklog.Ticket]bool{}
		type lsnBytes struct{ lsn, bytes uint64 }
		var since []lsnBytes
		var after uint64
		bound := 2*cfg.MaxReplayBytes + cfg.MaxRecordBytes
		h.onEvent = func(ev disklog.Event) {
			n.onEvent(ev)
			switch ev.Kind {
			case disklog.Durable:
				var b uint64
				for i, tk := range ev.Tickets {
					if ordinary[tk] {
						b += uint64(ev.Entries[i].Count) * core.BlockSize
					}
				}
				since = append(since, lsnBytes{ev.LSN, b})
				after += b
			case disklog.CheckpointDone:
				for len(since) > 0 && since[0].lsn <= n.begun.BaseLSN {
					after -= since[0].bytes
					since = since[1:]
				}
			}
			if after > bound {
				t.Fatalf("seed %d: %d bytes of ordinary records after the newest manifest's base, over %d", seed, after, bound)
			}
		}
		withhold := false
		for range 1000 {
			if r.Chance(1, 40) {
				withhold = !withhold
			}
			if !withhold {
				for n.step() {
				}
			}
			if len(h.pending) == 0 || r.Chance(1, 2) {
				tk := h.submit(disklog.Stream(r.Below(2)), h.chunkWrite(uint32(r.Below(64)), 0, uint16(r.Range(1, maxData)), r.Uint64()))
				ordinary[tk] = true
				if len(h.pending) == 0 && h.log.Queued() > 0 {
					holds++
				}
				continue
			}
			h.step(0)
		}
		for i := 0; h.log.Queued() > 0 || len(h.pending) > 0; i++ {
			if i > 100000 {
				t.Fatalf("seed %d: %d items still queued", seed, h.log.Queued())
			}
			for n.step() {
			}
			h.run()
		}
		if len(h.eventsOf(disklog.Failed)) != 0 {
			t.Fatalf("seed %d: %+v", seed, h.eventsOf(disklog.Failed))
		}
	}
	if holds == 0 {
		t.Fatal("no seed held a write behind its checkpoint")
	}
}

func TestHeldItemsFailWithTheirCheckpointForWantOfSpace(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := formatted(t, 16*core.BlockSize, cfg)
	h.victim = true
	for i := range 3 {
		h.put(disklog.Hot, h.write(uint32(i), 2, uint64(i)))
	}
	held := h.submit(disklog.Hot, h.write(3, 1, 3))
	if h.log.Queued() != 1 || h.log.Stalled() {
		t.Fatalf("held behind the checkpoint: %d queued, stalled %v", h.log.Queued(), h.log.Stalled())
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 3}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 10, 1)
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 10, true, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if !h.log.Stalled() || len(h.pending) != 0 {
		t.Fatalf("a piece with no segment free: stalled %v, %v", h.log.Stalled(), h.trace)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	failed, cpFailed := h.eventsOf(disklog.Failed), h.eventsOf(disklog.CheckpointFailed)
	if len(cpFailed) != 1 || len(failed) != 2 || !slices.Equal(failed[0].Tickets, []disklog.Ticket{piece}) ||
		!slices.Equal(failed[1].Tickets, []disklog.Ticket{held}) {
		t.Fatalf("no victim: %+v", h.events)
	}

	h.submit(disklog.Hot, h.write(4, 1, 4))
	if !h.log.Stalled() {
		t.Fatal("a write behind a checkpoint that failed for want of space is not stalled")
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 3}}); err != nil {
		t.Fatal(err)
	}
	if h.log.Stalled() {
		t.Fatal("a new checkpoint left the held write stalled")
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.log.FailStalled(&h.ctx, &h.out)
	h.absorb()
	if len(h.eventsOf(disklog.CheckpointFailed)) != 2 || len(h.eventsOf(disklog.Failed)) != 4 {
		t.Fatalf("events %+v", h.events)
	}
	h.wantLive(0)
	h.close()
}

func TestMixedRoundCountsOrdinaryBlocks(t *testing.T) {
	for _, c := range []struct {
		name string
		cold func(h *harness) []disklog.Item
		want uint64
	}{
		{"two 1-block writes", func(h *harness) []disklog.Item { return []disklog.Item{h.write(2, 1, 2), h.write(3, 1, 3)} }, 3},
		{"a blob sharing the piece's block", func(h *harness) []disklog.Item { return []disklog.Item{h.blob(disklog.BlobRaftEntries, 7, 100, 2)} }, 2},
	} {
		if got := mixedRoundHold(t, c.cold); got != c.want*core.BlockSize {
			t.Errorf("%s: a mixed round adds %d bytes to the hold, want %d blocks", c.name, got, c.want)
		}
	}
}

// mixedRoundHold queues a 100-byte checkpoint piece behind cold items while a hot write is in
// flight, so that one round carries them all, and is what that round adds to the hold.
func mixedRoundHold(t *testing.T, cold func(h *harness) []disklog.Item) uint64 {
	t.Helper()
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 64 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Cold, h.write(0, 1, 0))
	h.submit(disklog.Hot, h.write(1, 1, 1))
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 1}}); err != nil {
		t.Fatal(err)
	}
	var want []disklog.Ticket
	for _, it := range cold(h) {
		want = append(want, h.submit(disklog.Cold, it))
	}
	it := h.blob(disklog.BlobRun, 5, 100, 1)
	piece, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 100, false, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.absorb()
	var before, after uint64
	h.onEvent = func(ev disklog.Event) {
		if ev.Kind == disklog.Durable {
			before, after = after, disklog.HeldBytes(h.log)
		}
	}
	h.run()
	d := h.eventsOf(disklog.Durable)
	if mixed := d[len(d)-1]; !slices.Equal(mixed.Tickets, append([]disklog.Ticket{piece}, want...)) {
		t.Fatalf("round %+v, want the piece then the cold items", mixed)
	}
	h.close()
	h.reopen()
	if got := disklog.HeldBytes(h.log); got != after {
		t.Fatalf("recovery restored a hold of %d bytes, the log had %d", got, after)
	}
	h.close()
	return after - before
}

func TestCheckpointLargerThanTheHoldReleasesIt(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.write(0, 3, 0))
	h.put(disklog.Hot, h.write(1, 3, 1))
	held := h.submit(disklog.Hot, h.write(2, 1, 2))
	if h.log.Queued() != 1 {
		t.Fatal("a write at 8 blocks was not held")
	}
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}}); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		it := h.blob(disklog.BlobRun, 5, 2*core.BlockSize, uint64(i))
		if _, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 2*core.BlockSize, i == 3, &h.out); err != nil {
			t.Fatal(err)
		}
		h.absorb()
		h.run()
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	if h.log.ReplayBytes() < 2*cfg.MaxReplayBytes || h.log.Queued() != 0 {
		t.Fatalf("%d bytes to replay, %d queued: a checkpoint of %d blocks kept the write held", h.log.ReplayBytes(), h.log.Queued(), h.log.ReplayBytes()/core.BlockSize)
	}
	if d := h.eventsOf(disklog.Durable); !slices.Equal(d[len(d)-1].Tickets, []disklog.Ticket{held}) {
		t.Fatalf("events %+v", d)
	}
	h.close()
	h.reopen()
	if h.log.ReplayBytes() != 24*core.BlockSize || disklog.HeldBytes(h.log) != 10*core.BlockSize {
		t.Fatalf("after a restart replaying from the start: %d bytes to replay, %d held; want 24 blocks and the 10 of the three writes",
			h.log.ReplayBytes(), disklog.HeldBytes(h.log))
	}
	h.close()
}

func TestHoldBoundThatOverflowsIsRefused(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 1 << 63
	h := blankHarness(t, gib, 1<<20, cfg)
	if _, err := disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out); !errors.Is(err, disklog.ErrBadConfig) {
		t.Fatalf("format with max replay 2^63: %v", err)
	}
	if got := disklog.ReserveSegments(1<<63, 1<<63, 2, 4); got != 1<<32-1 {
		t.Fatalf("an overflowing reserve is %d, want the largest", got)
	}
}

func TestCloseFailsItemsHeldBehindACheckpoint(t *testing.T) {
	cfg := testConfig(0)
	cfg.MaxReplayBytes = 4 * core.BlockSize
	h := formatted(t, gib, cfg)
	h.put(disklog.Hot, h.write(0, 3, 0))
	h.put(disklog.Hot, h.write(1, 3, 1))
	held := h.submit(disklog.Cold, h.write(2, 1, 2))
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: 5, ReplayLSN: 2}}); err != nil {
		t.Fatal(err)
	}
	it := h.blob(disklog.BlobRun, 5, 100, 1)
	if _, err := h.log.CheckpointPiece(&h.ctx, 5, it.Buf, 100, true, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	if err := h.log.Close(&h.ctx, &h.out); !errors.Is(err, disklog.ErrBusy) {
		t.Fatalf("close with the piece in flight: %v", err)
	}
	h.run()
	if err := h.log.Close(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	failed, cpFailed := h.eventsOf(disklog.Failed), h.eventsOf(disklog.CheckpointFailed)
	if len(failed) != 1 || !errors.Is(failed[0].Err, disklog.ErrClosed) || !slices.Equal(failed[0].Tickets, []disklog.Ticket{held}) ||
		len(cpFailed) != 1 || !errors.Is(cpFailed[0].Err, disklog.ErrClosed) {
		t.Fatalf("close with a write held: %+v", h.events)
	}
	if h.pool.Live() != 0 {
		t.Fatalf("%d buffers live after close", h.pool.Live())
	}
	if _, err := h.log.Submit(&h.ctx, disklog.Hot, h.write(3, 1, 3), &h.out); !errors.Is(err, disklog.ErrClosed) {
		t.Fatalf("submit after close: %v", err)
	}
}
