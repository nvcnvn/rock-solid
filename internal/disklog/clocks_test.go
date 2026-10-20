package disklog_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

// clockRun formats a log with raw deriving ctx.Raw, writes, checkpoints, crashes with a round in
// flight and recovers, returning every action and event in order.
func clockRun(t *testing.T, raw func(core.Tick) core.Tick) []string {
	h := blankHarness(t, gib, 64<<20, testConfig(0))
	h.raw = raw
	h.ctx.Raw = raw(h.ctx.Now)
	var err error
	h.log, err = disklog.Format(&h.ctx, h.cfg, disklog.Superblock{LogID: testLogID, Geometry: h.geo}, firstIO, &h.out)
	if err != nil {
		t.Fatal(err)
	}
	h.own = h.pool.Live()
	h.absorb()
	h.run()
	h.twoBlockWrites(5)
	if err := h.log.BeginCheckpoint([]disklog.CheckpointIndex{{Owner: indexOwner, ReplayLSN: 5}}); err != nil {
		t.Fatal(err)
	}
	if err := h.log.EndCheckpoint(&h.ctx, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.run()
	h.submit(disklog.Hot, h.chunkWrite(9, 0, 1, 9))
	h.step(0)
	h.disk.Crash(func(int, core.LBA) bool { return true })
	res := h.recover(disklog.ModeOpen)
	log := slices.Clone(res.trace)
	for _, ev := range h.events {
		log = append(log, fmt.Sprintf("event %d lsn %d", ev.Kind, ev.LSN))
	}
	return append(log, fmt.Sprintf("recovered %d %v", res.durable, res.err))
}

func TestRawClockChangesNoTrace(t *testing.T) {
	want := clockRun(t, func(now core.Tick) core.Tick { return now })
	for name, raw := range map[string]func(core.Tick) core.Tick{
		"apart":  func(now core.Tick) core.Tick { return now*1226/1000 + 7 },
		"×1.226": func(now core.Tick) core.Tick { return now * 1226 / 1000 },
		"behind": func(now core.Tick) core.Tick { return now / 2 },
		"frozen": func(core.Tick) core.Tick { return 3 },
	} {
		if got := clockRun(t, raw); !slices.Equal(got, want) {
			t.Fatalf("Raw %s: trace\n%v\nwant\n%v", name, got, want)
		}
	}
}

func TestResumedKeepsRoundsInFlight(t *testing.T) {
	cfg := testConfig(0)
	cfg.CommitDepth = 2
	h := formatted(t, gib, cfg)
	h.submit(disklog.Hot, h.chunkWrite(1, 0, 1, 1))
	h.submit(disklog.Hot, h.chunkWrite(2, 0, 1, 2))
	inFlight, pending, trace := h.log.InFlight(), len(h.pending), len(h.trace)
	h.feed(core.Event{Kind: core.EvResumed})
	if h.log.InFlight() != inFlight || len(h.pending) != pending || len(h.trace) != trace {
		t.Fatalf("Resumed changed group commit: %d rounds, %d actions, trace %v", h.log.InFlight(), len(h.pending), h.trace)
	}
	h.run()
	if h.log.DurableLSN() != 2 {
		t.Fatalf("durable LSN %d after Resumed, want 2", h.log.DurableLSN())
	}
}
