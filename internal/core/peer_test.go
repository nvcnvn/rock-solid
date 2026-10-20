package core

import (
	"reflect"
	"strings"
	"testing"
)

var (
	peerA = NodeID{Node: 1, Loop: 0}
	peerB = NodeID{Node: 2, Loop: 3}
)

func send(t *testing.T, p *PeerRequests, now Tick, id RequestID, peer NodeID, timer TimerID, timeout Tick, buf IOBuf) {
	t.Helper()
	var msg Msg
	out := p.Send(&Context{Now: now}, id, peer, timer, timeout, &msg, buf, nil)
	if len(out) != 2 {
		t.Fatalf("Send(%d): %d actions", id, len(out))
	}
}

func expectFailures(t *testing.T, got, want []Failure) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSendEmitsMessageThenTimer(t *testing.T) {
	p := NewPeerRequests()
	var msg Msg
	if err := msg.SetInline([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := IOBuf{Slot: 4, Gen: 2}
	out := p.Send(&Context{Now: 100}, 1, peerA, 7, 50, &msg, buf, nil)
	wantMsg := msg
	wantMsg.Buf = buf
	want := []Action{
		{Kind: ActNetSend, Peer: peerA, Msg: wantMsg},
		{Kind: ActSetTimer, Timer: 7, At: 150},
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("got %+v, want %+v", out, want)
	}
	if p.InFlight() != 1 {
		t.Fatalf("in flight %d", p.InFlight())
	}
	r, ok := p.Complete(1)
	if !ok || r != (Request{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Buf: buf}) {
		t.Fatalf("Complete: %+v, %v", r, ok)
	}
}

func TestSendInFlightIDOrTimerPanics(t *testing.T) {
	tests := []struct {
		name  string
		id    RequestID
		timer TimerID
		want  string
	}{
		{name: "id", id: 1, timer: 8, want: "request 1 is already in flight"},
		{name: "timer", id: 2, timer: 7, want: "timer 7 already guards request 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPeerRequests()
			send(t, p, 100, 1, peerA, 7, 50, NoBuf)
			defer func() {
				r := recover()
				if msg, _ := r.(string); !strings.Contains(msg, tc.want) {
					t.Fatalf("panic %v, want %q", r, tc.want)
				}
			}()
			var msg Msg
			p.Send(&Context{Now: 100}, tc.id, peerA, tc.timer, 50, &msg, NoBuf, nil)
		})
	}
}

func TestPeerLostFailsEveryRequestToThatPeer(t *testing.T) {
	p := NewPeerRequests()
	buf := IOBuf{Slot: 3, Gen: 1}
	send(t, p, 100, 2, peerA, 12, 50, buf)
	send(t, p, 110, 1, peerA, 11, 50, NoBuf)
	send(t, p, 120, 3, peerB, 13, 50, buf)
	got := p.OnPeerLost(peerA, nil)
	expectFailures(t, got, []Failure{
		{ID: 1, Peer: peerA, Timer: 11, Deadline: 160, Buf: NoBuf, Reason: ReasonPeerLost},
		{ID: 2, Peer: peerA, Timer: 12, Deadline: 150, Buf: buf, Reason: ReasonPeerLost},
	})
	if p.InFlight() != 1 {
		t.Fatalf("in flight %d", p.InFlight())
	}
	expectFailures(t, p.OnTimer(&Context{Now: 200}, 12, nil), nil)
	expectFailures(t, p.OnTimer(&Context{Now: 200}, 13, nil), []Failure{
		{ID: 3, Peer: peerB, Timer: 13, Deadline: 170, Buf: buf, Reason: ReasonTimeout},
	})
}

func TestTimeoutKeepsTheBuffer(t *testing.T) {
	p := NewPeerRequests()
	buf := IOBuf{Slot: 5, Gen: 9}
	send(t, p, 100, 1, peerA, 7, 50, buf)
	expectFailures(t, p.OnTimer(&Context{Now: 149}, 7, nil), nil)
	want := []Failure{{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Buf: buf, Reason: ReasonTimeout}}
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), want)
	if p.InFlight() != 1 {
		t.Fatalf("in flight %d after timeout", p.InFlight())
	}
	expectFailures(t, p.OnTimer(&Context{Now: 151}, 7, nil), nil)
	r, ok := p.Complete(1)
	if !ok || r.Buf != buf {
		t.Fatalf("Complete: %+v, %v", r, ok)
	}
	if p.InFlight() != 0 {
		t.Fatalf("in flight %d after completion", p.InFlight())
	}
}

func TestTimedOutRequestIsFailedByPeerLost(t *testing.T) {
	p := NewPeerRequests()
	buf := IOBuf{Slot: 5, Gen: 9}
	send(t, p, 100, 1, peerA, 7, 50, buf)
	send(t, p, 100, 2, peerA, 8, 500, NoBuf)
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), []Failure{
		{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Buf: buf, Reason: ReasonTimeout},
	})
	expectFailures(t, p.OnPeerLost(peerA, nil), []Failure{
		{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Buf: buf, Reason: ReasonPeerLost},
		{ID: 2, Peer: peerA, Timer: 8, Deadline: 600, Buf: NoBuf, Reason: ReasonPeerLost},
	})
	if p.InFlight() != 0 {
		t.Fatalf("in flight %d", p.InFlight())
	}
}

func TestTimerReusedAfterTimeout(t *testing.T) {
	p := NewPeerRequests()
	send(t, p, 100, 1, peerA, 7, 50, NoBuf)
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), []Failure{
		{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Reason: ReasonTimeout},
	})
	send(t, p, 150, 2, peerA, 7, 50, NoBuf)
	if p.InFlight() != 2 {
		t.Fatalf("in flight %d", p.InFlight())
	}
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), nil)
	expectFailures(t, p.OnTimer(&Context{Now: 200}, 7, nil), []Failure{
		{ID: 2, Peer: peerA, Timer: 7, Deadline: 200, Reason: ReasonTimeout},
	})
	expectFailures(t, p.OnPeerLost(peerA, nil), []Failure{
		{ID: 1, Peer: peerA, Timer: 7, Deadline: 150, Reason: ReasonPeerLost},
		{ID: 2, Peer: peerA, Timer: 7, Deadline: 200, Reason: ReasonPeerLost},
	})
}

func TestCompletedRequest(t *testing.T) {
	p := NewPeerRequests()
	send(t, p, 100, 1, peerA, 7, 50, NoBuf)
	if _, ok := p.Complete(1); !ok {
		t.Fatal("first completion")
	}
	if r, ok := p.Complete(1); ok {
		t.Fatalf("second completion returned %+v", r)
	}
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), nil)
	expectFailures(t, p.OnPeerLost(peerA, nil), nil)
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 99, nil), nil)
	if p.InFlight() != 0 {
		t.Fatalf("in flight %d", p.InFlight())
	}
}

func TestStaleTimerIsMatchedToTheCurrentRequest(t *testing.T) {
	p := NewPeerRequests()
	send(t, p, 100, 1, peerA, 7, 50, NoBuf)
	if _, ok := p.Complete(1); !ok {
		t.Fatal("Complete(1)")
	}
	send(t, p, 130, 2, peerA, 7, 50, NoBuf)
	expectFailures(t, p.OnTimer(&Context{Now: 150}, 7, nil), nil)
	expectFailures(t, p.OnTimer(&Context{Now: 180}, 7, nil), []Failure{
		{ID: 2, Peer: peerA, Timer: 7, Deadline: 180, Reason: ReasonTimeout},
	})
}

func TestSendAndCompleteAllocateNothing(t *testing.T) {
	p := NewPeerRequests()
	ctx := &Context{Now: 100}
	var msg Msg
	out := make([]Action, 0, 2)
	allocs := testing.AllocsPerRun(1000, func() {
		out = p.Send(ctx, 1, peerA, 7, 50, &msg, NoBuf, out[:0])
		if _, ok := p.Complete(1); !ok {
			t.Fatal("Complete")
		}
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per call", allocs)
	}
}

func BenchmarkSendComplete(b *testing.B) {
	p := NewPeerRequests()
	ctx := &Context{Now: 100}
	var msg Msg
	out := make([]Action, 0, 2)
	out = p.Send(ctx, 1, peerA, 7, 50, &msg, NoBuf, out[:0])
	p.Complete(1)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		out = p.Send(ctx, RequestID(i), peerA, 7, 50, &msg, NoBuf, out[:0])
		p.Complete(RequestID(i))
	}
}

func TestPeerDeadlinesIgnoreRaw(t *testing.T) {
	type step struct {
		now, raw Tick
		failures []Failure
	}
	run := func(raw func(now Tick) Tick) []step {
		p := NewPeerRequests()
		var msg Msg
		p.Send(&Context{Now: 100, Raw: raw(100)}, 1, peerA, 7, 50, &msg, NoBuf, nil)
		var steps []step
		for _, now := range []Tick{149, 150} {
			ctx := &Context{Now: now, Raw: raw(now)}
			steps = append(steps, step{now, ctx.Raw, p.OnTimer(ctx, 7, nil)})
		}
		return steps
	}
	sameRaw := run(func(now Tick) Tick { return now })
	if len(sameRaw[0].failures) != 0 || len(sameRaw[1].failures) != 1 || sameRaw[1].failures[0].Reason != ReasonTimeout {
		t.Fatalf("Raw = Now: %+v", sameRaw)
	}
	for name, raw := range map[string]func(Tick) Tick{
		"Raw 10":        func(Tick) Tick { return 10 },
		"Raw Now×1.226": func(now Tick) Tick { return now * 1226 / 1000 },
	} {
		got := run(raw)
		for i := range got {
			if !reflect.DeepEqual(got[i].failures, sameRaw[i].failures) {
				t.Fatalf("%s at Now %d: %+v, want %+v", name, got[i].now, got[i].failures, sameRaw[i].failures)
			}
		}
	}
}
