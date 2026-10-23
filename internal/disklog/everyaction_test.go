package disklog_test

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

var errCrash = errors.New("crash")

// sweep is one run of the crash-at-every-action workload: format, 40 one- and two-block writes
// with a cold blob every fifth, and the node's checkpoints, the second carrying a merge step.
// Actions run one at a time, writes before flushes, so commit depth 2 keeps two rounds' writes
// unflushed together.
type sweep struct {
	h *harness
	n *node
	// executed counts the actions run; the one that would run action crashAt panics with errCrash
	// instead, and action failAt completes with Io.
	executed, crashAt, failAt int
	// failedEvents and failedTrace are the events and trace lines there were when the failed
	// action ran, or -1.
	failedEvents, failedTrace int
	// formatted is the actions format took, or -1 until they ran.
	formatted int
}

func newSweep(t *testing.T, crashAt, failAt int) *sweep {
	cfg := testConfig(4)
	cfg.MaxReplayBytes = 40 * core.BlockSize
	cfg.CommitDepth = 2
	h := newHarness(t, 1024*core.BlockSize, 64<<20, cfg)
	n := newNode(h, 64)
	n.mergeAt = 2
	h.watch(n.live)
	return &sweep{h: h, n: n, crashAt: crashAt, failAt: failAt, failedEvents: -1, failedTrace: -1, formatted: -1}
}

// exec runs the first ready write or trim, or else the first ready flush, and reports whether
// there was one.
func (s *sweep) exec() bool {
	h := s.h
	i := slices.IndexFunc(h.pending, func(p pendingIO) bool { return p.act.Kind != core.ActDiskFsync })
	for j := range h.pending {
		if i < 0 && h.eligible(j) {
			i = j
		}
	}
	if i < 0 {
		return false
	}
	if s.executed == s.crashAt {
		panic(errCrash)
	}
	if s.executed == s.failAt {
		h.disk.Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(onIO(h.pending[i].act.IO))})
		s.failedEvents, s.failedTrace = len(h.events), len(h.trace)
	}
	s.executed++
	h.step(i)
	return true
}

// offer submits an item unless the log has failed.
func (s *sweep) offer(stream disklog.Stream, it disklog.Item) {
	h := s.h
	t, err := h.log.Submit(&h.ctx, stream, it, &h.out)
	if err != nil {
		h.pool.Free(it.Buf)
		return
	}
	h.tickets.track(t, it.Entry)
	h.absorb()
}

func (s *sweep) drain() {
	for {
		for s.n.step() {
		}
		if !s.exec() {
			return
		}
	}
}

func (s *sweep) workload() {
	h := s.h
	s.drain()
	s.formatted = s.executed
	for i := range 40 {
		s.offer(disklog.Hot, h.chunkWrite(uint32(i%8), 0, uint16(1+i%2), uint64(i+1)))
		if i%5 == 0 {
			s.offer(disklog.Cold, h.blob(disklog.BlobRaftEntries, userBlobOwner, 100+i, uint64(i)))
		}
		for range 2 {
			s.n.step()
			s.exec()
		}
	}
	s.drain()
}

// run runs the workload to its crash point.
func (s *sweep) run() {
	defer func() {
		if r := recover(); r != nil && r != errCrash {
			panic(r)
		}
	}()
	s.workload()
}

// crash crashes the disk with the unflushed and undecided blocks landing as landing says.
func (s *sweep) crash(landing string, seed uint64) {
	d := s.h.disk
	d.ClearFaults()
	switch landing {
	case "none":
		d.Crash(func(int, core.LBA) bool { return false })
	case "half":
		d.CrashRandom(core.NewRand(seed))
	case "all":
		d.Crash(func(int, core.LBA) bool { return true })
	}
}

// recoverChecked recovers in mode and reports how the recovery fails the crash oracle; a
// recovery before format finished may find no log.
func (s *sweep) recoverChecked(mode disklog.Mode, crash func(core.Action) bool) (*recovered, error) {
	res := s.h.recoverCrashing(mode, crash)
	switch {
	case errors.Is(res.err, disklog.ErrNotFormatted) && (s.formatted < 0 || s.failAt >= 0 && s.failAt < s.formatted):
		return nil, nil
	case res.err != nil:
		return nil, fmt.Errorf("recovery in mode %d: %w", mode, res.err)
	}
	if err := oracleCheck(res); err != nil {
		return nil, fmt.Errorf("recovery in mode %d: %w", mode, err)
	}
	s.h.readBack(res.index)
	return res, nil
}

// failureFinal reports an acknowledgment, write or flush from the open log after its failed action.
func (s *sweep) failureFinal() error {
	if s.failedEvents < 0 {
		return nil
	}
	h := s.h
	for _, ev := range h.events[s.failedEvents:] {
		if ev.Kind == disklog.Durable {
			return fmt.Errorf("LSN %d acknowledged after action %d failed", ev.LSN, s.failAt)
		}
	}
	for _, line := range h.trace[s.failedTrace:] {
		if strings.HasPrefix(line, "write") || strings.HasPrefix(line, "fsync") {
			return fmt.Errorf("%q issued after action %d failed", line, s.failAt)
		}
	}
	if h.log.Err() == nil {
		return fmt.Errorf("the log outlived its failed action %d", s.failAt)
	}
	return nil
}

// sweepStats counts the rare cases the sweep reached.
type sweepStats struct {
	runs, notFormatted, inRecovery, keptInFlight, droppedInFlight int
}

// crashRun crashes the workload at action crashAt, or fails its action failAt and crashes it at
// its end, lands the unflushed blocks as landing says, and recovers in open mode, then in rebuild
// mode; with recoveryCrash, the first recovery is crashed before its write, trim or flush
// recoveryCrash, counting from 0, and landing applies again. A crash before a read leaves the disk
// as one before the next of those does. It reports the first check the recoveries fail.
func crashRun(t *testing.T, crashAt, failAt int, landing string, recoveryCrash int, plant func(*disklog.Recovery), st *sweepStats) (*recovered, error) {
	s := newSweep(t, crashAt, failAt)
	s.run()
	if err := s.failureFinal(); err != nil {
		return nil, err
	}
	acked, inFlight := s.h.log.DurableLSN(), s.h.log.InFlight()
	s.h.plant = plant
	seed := uint64(max(crashAt, failAt))
	s.crash(landing, seed)
	st.runs++
	var crash func(core.Action) bool
	if recoveryCrash >= 0 {
		n := 0
		crash = func(a core.Action) bool {
			if a.Kind == core.ActDiskRead {
				return false
			}
			n++
			if n-1 != recoveryCrash {
				return false
			}
			st.inRecovery++
			s.crash(landing, seed+1)
			return true
		}
	}
	res, err := s.recoverChecked(disklog.ModeOpen, crash)
	if err != nil || res == nil {
		if res == nil && err == nil {
			st.notFormatted++
		}
		return nil, err
	}
	switch {
	case res.durable > acked:
		st.keptInFlight++
	case inFlight > 0:
		st.droppedInFlight++
	}
	reb, err := s.recoverChecked(disklog.ModeRebuild, nil)
	if err != nil {
		return nil, err
	}
	if reb.durable != res.durable || !maps.Equal(reb.index, res.index) {
		return nil, fmt.Errorf("rebuild recovered LSN %d, open %d: %s", reb.durable, res.durable, modelDiff(reb.index, res.index))
	}
	return res, nil
}

// fullSweep returns the actions of the workload run to its end, failing unless it wrote two
// checkpoints, one with a merge step.
func fullSweep(t *testing.T) int {
	s := newSweep(t, -1, -1)
	s.run()
	if s.n.checkpoints < 2 || s.n.mergeRuns == nil {
		t.Fatalf("the workload finished %d checkpoints, merged %v", s.n.checkpoints, s.n.mergeRuns)
	}
	return s.executed
}

// sweepEveryAction crashes the workload at every action with each landing, and fails each action
// with each landing, with plant in every recovery, and reports the first run whose recoveries fail.
func sweepEveryAction(t *testing.T, total int, plant func(*disklog.Recovery), st *sweepStats) error {
	for k := 0; k <= total; k++ {
		for _, landing := range []string{"none", "half", "all"} {
			if _, err := crashRun(t, k, -1, landing, -1, plant, st); err != nil {
				return fmt.Errorf("crash at action %d, %s landing: %w", k, landing, err)
			}
			if k == total {
				continue
			}
			if _, err := crashRun(t, -1, k, landing, -1, plant, st); err != nil {
				return fmt.Errorf("action %d failed, %s landing: %w", k, landing, err)
			}
		}
	}
	return nil
}

// [property] The disk log's workload, crashed at its k-th action for every k with no unflushed
// block landing, a seeded half and all of them, and separately failed at its k-th action with Io
// and then crashed the same ways, recovers a prefix of what was submitted that holds every
// acknowledged record, in open and rebuild mode alike, and the failed log acknowledges, writes and
// flushes nothing after its failure. A recovery crashed before each of its own writes and flushes
// recovers again to the LSN and index it would have found.
func TestCrashAtEveryAction(t *testing.T) {
	total := fullSweep(t)
	var st sweepStats
	if err := sweepEveryAction(t, total, nil, &st); err != nil {
		t.Fatal(err)
	}
	for k := 0; k <= total; k++ {
		want, err := crashRun(t, k, -1, "half", -1, nil, &st)
		if err != nil {
			t.Fatal(err)
		}
		if want == nil {
			continue
		}
		for j := 0; ; j++ {
			before := st.inRecovery
			got, err := crashRun(t, k, -1, "half", j, nil, &st)
			if err != nil {
				t.Fatalf("crash at %d, recovery crashed before its write or flush %d: %v", k, j, err)
			}
			if st.inRecovery == before {
				break
			}
			if got.durable != want.durable || !maps.Equal(got.index, want.index) {
				t.Fatalf("crash at %d, recovery crashed before its write or flush %d: recovered LSN %d, %d uncrashed: %s", k, j, got.durable, want.durable, modelDiff(got.index, want.index))
			}
		}
	}
	if st.notFormatted == 0 || st.inRecovery == 0 || st.keptInFlight == 0 || st.droppedInFlight == 0 {
		t.Fatalf("the sweep missed a case: %+v", st)
	}
	t.Logf("%d actions, %+v", total, st)
}

// The sweep catches recovery that stops one LSN early, dropping the last acknowledged record,
// and recovery that replays past a gap, keeping a record no recovery may keep.
func TestCrashAtEveryActionCatchesPlantedRecoveryBugs(t *testing.T) {
	total := fullSweep(t)
	for _, p := range []struct {
		name  string
		plant func(*disklog.Recovery)
	}{{"stop early", disklog.StopEarly}, {"replay past gaps", disklog.ReplayPastGaps}} {
		err := sweepEveryAction(t, total, p.plant, &sweepStats{})
		if err == nil {
			t.Fatalf("%s: every recovery passed", p.name)
		}
		t.Logf("%s: %v", p.name, err)
	}
}
