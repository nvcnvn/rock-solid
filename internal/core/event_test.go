package core

import (
	"errors"
	"testing"
	"time"
)

func TestInlineMsgLimit(t *testing.T) {
	var m Msg
	if err := m.SetInline(make([]byte, 256)); err != nil || m.Len != 256 {
		t.Fatalf("256 bytes: %v", err)
	}
	if err := m.SetInline(nil); err != nil || len(m.Inline()) != 0 {
		t.Fatalf("empty: %v", err)
	}
	err := m.SetInline(make([]byte, 257))
	if !errors.Is(err, ErrMsgTooLong) || err.Error() != "inline message too long: 257 bytes" {
		t.Fatalf("257 bytes: %v", err)
	}
}

func TestTick(t *testing.T) {
	if TickFromDuration(2*time.Second) != 2_000_000 || Tick(2_000_000).Millis() != 2000 {
		t.Fatal("2 s")
	}
	if TickFromDuration(15*time.Millisecond) != 15_000 || Tick(950).Millis() != 0 {
		t.Fatal("15 ms, 950 ticks")
	}
	if Tick(15_000).Duration() != 15*time.Millisecond {
		t.Fatal("Duration")
	}
}

func TestPoolExhaustedError(t *testing.T) {
	err := error(&PoolExhaustedError{Requested: 1, Available: 0})
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatal("errors.Is")
	}
	if err.Error() != "buffer pool exhausted: requested 1 bytes, 0 available" {
		t.Fatal(err.Error())
	}
}

func TestEventAndActionAllocateNothing(t *testing.T) {
	out := make([]Action, 0, 8)
	var now Tick
	allocs := testing.AllocsPerRun(1000, func() {
		out = out[:0]
		ctx := Context{Now: 1000 + now, Raw: 1226 + now}
		ev := Event{Kind: EvTimer, Timer: 7}
		resumed := Event{Kind: EvResumed}
		if resumed.Kind == EvResumed {
			out = append(out, Action{Kind: ActSetTimer, Timer: ev.Timer, At: ctx.Now + 100})
		}
		out = append(out, Action{Kind: ActDiskFsync, IO: 3, At: ctx.Raw})
		now++
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per call", allocs)
	}
}

func TestContextCarriesTwoClocks(t *testing.T) {
	for _, c := range []struct{ now, raw Tick }{{1000, 1226}, {5_000_000, 4_000_000}, {7, 0}, {0, 7}} {
		ctx := Context{Now: c.now, Raw: c.raw}
		if ctx.Now != c.now || ctx.Raw != c.raw {
			t.Fatalf("context %+v, want Now %d and Raw %d", ctx, c.now, c.raw)
		}
	}
}

func TestResumedNeedsNoOtherField(t *testing.T) {
	ev := Event{Kind: EvResumed}
	if ev != (Event{Kind: EvResumed, Peer: NodeID{}, Msg: Msg{}, IO: 0, Err: DiskOK, Timer: 0}) {
		t.Fatalf("Resumed %+v", ev)
	}
	for _, k := range []EventKind{EvNone, EvNetRecv, EvPeerLost, EvDiskDone, EvTimer} {
		if k == EvResumed {
			t.Fatalf("Resumed shares kind %d", k)
		}
	}
}
