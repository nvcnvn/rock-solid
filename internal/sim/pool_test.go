package sim

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

func mustAlloc(t *testing.T, p *Pool, n int) core.IOBuf {
	t.Helper()
	b, err := p.Alloc(n)
	if err != nil {
		t.Fatalf("Alloc(%d): %v", n, err)
	}
	return b
}

func expectExhausted(t *testing.T, err error, requested, available int) {
	t.Helper()
	var e *core.PoolExhaustedError
	if !errors.As(err, &e) || !errors.Is(err, core.ErrPoolExhausted) {
		t.Fatalf("got %v, want PoolExhaustedError", err)
	}
	if e.Requested != requested || e.Available != available {
		t.Fatalf("got requested %d available %d, want %d and %d", e.Requested, e.Available, requested, available)
	}
}

func TestExplicitCapacity(t *testing.T) {
	p := NewPool(2*core.BlockSize, 0)
	a := mustAlloc(t, p, core.BlockSize)
	b := mustAlloc(t, p, core.BlockSize)
	if a.Slot == b.Slot {
		t.Fatalf("both buffers in slot %d", a.Slot)
	}
	if len(p.Bytes(a)) != core.BlockSize || len(p.Bytes(b)) != core.BlockSize {
		t.Fatal("buffer length")
	}
	if p.Used() != 2*core.BlockSize || p.Available() != 0 || p.Live() != 2 {
		t.Fatalf("used %d available %d live %d", p.Used(), p.Available(), p.Live())
	}
	_, err := p.Alloc(1)
	expectExhausted(t, err, 1, 0)
}

func TestRequestsRoundUpToWholeBlocks(t *testing.T) {
	p := NewPool(3*core.BlockSize, 0)
	a := mustAlloc(t, p, core.BlockSize+1)
	if len(p.Bytes(a)) != 2*core.BlockSize || p.Used() != 2*core.BlockSize {
		t.Fatalf("%d bytes, used %d", len(p.Bytes(a)), p.Used())
	}
	_, err := p.Alloc(2 * core.BlockSize)
	expectExhausted(t, err, 2*core.BlockSize, core.BlockSize)
	b := mustAlloc(t, p, 1)
	if b.Slot != 2 || len(p.Bytes(b)) != core.BlockSize {
		t.Fatalf("slot %d, %d bytes", b.Slot, len(p.Bytes(b)))
	}
}

func TestFreeReturnsBytesAndRenewsGeneration(t *testing.T) {
	p := NewPool(core.BlockSize, 0)
	a := mustAlloc(t, p, core.BlockSize)
	p.Free(a)
	if p.Used() != 0 || p.Available() != core.BlockSize || p.Live() != 0 {
		t.Fatalf("after free: used %d available %d live %d", p.Used(), p.Available(), p.Live())
	}
	b := mustAlloc(t, p, core.BlockSize)
	if b.Slot != a.Slot || b.Gen == a.Gen {
		t.Fatalf("old handle %+v, new handle %+v", a, b)
	}
	if a != (core.IOBuf{Slot: 0, Gen: 1}) || b != (core.IOBuf{Slot: 0, Gen: 2}) {
		t.Fatalf("handles %+v and %+v", a, b)
	}
	p.Free(b)
	if p.Used() != 0 || p.Live() != 0 {
		t.Fatalf("used %d live %d", p.Used(), p.Live())
	}
}

func TestAllocPoisonsAndAllocZeroedZeros(t *testing.T) {
	p := NewPool(core.BlockSize, 0)
	a := mustAlloc(t, p, core.BlockSize)
	for i := range p.Bytes(a) {
		p.Bytes(a)[i] = 0x11
	}
	p.Free(a)
	b := mustAlloc(t, p, core.BlockSize)
	if i := slices.IndexFunc(p.Bytes(b), func(c byte) bool { return c != poison }); i >= 0 {
		t.Fatalf("byte %d is %#x, want poison %#x", i, p.Bytes(b)[i], poison)
	}
	p.Free(b)
	z, err := p.AllocZeroed(core.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(p.Bytes(z), func(c byte) bool { return c != 0 }); i >= 0 {
		t.Fatalf("byte %d is %#x, want 0", i, p.Bytes(z)[i])
	}
}

func TestLiveLimit(t *testing.T) {
	p := NewPool(2*core.BlockSize, 1)
	a := mustAlloc(t, p, core.BlockSize)
	_, err := p.Alloc(core.BlockSize)
	expectExhausted(t, err, core.BlockSize, 0)
	if p.Available() != core.BlockSize {
		t.Fatalf("available %d", p.Available())
	}
	p.Free(a)
	mustAlloc(t, p, core.BlockSize)
}

func mustPanic(t *testing.T, f func()) (msg string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("no panic")
		}
		msg = r.(string)
	}()
	f()
	return ""
}

func TestFreedAndStaleHandlesPanic(t *testing.T) {
	p := NewPool(core.BlockSize, 0)
	freed := mustAlloc(t, p, core.BlockSize)
	p.Free(freed)
	for _, name := range []string{"Bytes", "Free"} {
		msg := mustPanic(t, func() { use(p, name, freed) })
		if !strings.Contains(msg, "freed") || !strings.Contains(msg, "slot 0 generation 1") {
			t.Fatalf("%s on a freed handle: %q", name, msg)
		}
	}
	stale := mustAlloc(t, p, core.BlockSize)
	p.Free(stale)
	mustAlloc(t, p, core.BlockSize)
	for _, name := range []string{"Bytes", "Free"} {
		msg := mustPanic(t, func() { use(p, name, stale) })
		if !strings.Contains(msg, "stale") || !strings.Contains(msg, "slot 0 generation 2") || !strings.Contains(msg, "generation 3") {
			t.Fatalf("%s on a stale handle: %q", name, msg)
		}
	}
	if p.Live() != 1 || p.Used() != core.BlockSize {
		t.Fatalf("live %d used %d after rejected frees", p.Live(), p.Used())
	}
	if msg := mustPanic(t, func() { p.Bytes(core.NoBuf) }); !strings.Contains(msg, "zero buffer handle") {
		t.Fatalf("NoBuf: %q", msg)
	}
	if msg := mustPanic(t, func() { p.Bytes(core.IOBuf{Slot: 9, Gen: 1}) }); !strings.Contains(msg, "slot 9") {
		t.Fatalf("out of range: %q", msg)
	}
}

func use(p *Pool, method string, b core.IOBuf) {
	if method == "Bytes" {
		p.Bytes(b)
	} else {
		p.Free(b)
	}
}

const (
	retryTimer core.TimerID = 1
	retryDelay core.Tick    = 10
)

type pendingWrite struct {
	io  core.IOID
	buf core.IOBuf
}

type writer struct {
	queued  int
	lastIO  core.IOID
	pending []pendingWrite
}

func (w *writer) Write(ctx core.Context, out []core.Action) []core.Action {
	w.queued++
	return w.drain(ctx, out)
}

func (w *writer) OnEvent(ctx core.Context, ev core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvTimer:
		return w.drain(ctx, out)
	case core.EvDiskDone:
		i := slices.IndexFunc(w.pending, func(p pendingWrite) bool { return p.io == ev.IO })
		ctx.Pool.Free(w.pending[i].buf)
		w.pending = slices.Delete(w.pending, i, i+1)
		return w.drain(ctx, out)
	}
	return out
}

func (w *writer) drain(ctx core.Context, out []core.Action) []core.Action {
	for w.queued > 0 {
		buf, err := ctx.Pool.Alloc(core.BlockSize)
		if errors.Is(err, core.ErrPoolExhausted) {
			return append(out, core.Action{Kind: core.ActSetTimer, Timer: retryTimer, At: ctx.Now + retryDelay})
		}
		w.queued--
		w.lastIO++
		w.pending = append(w.pending, pendingWrite{io: w.lastIO, buf: buf})
		out = append(out, core.Action{Kind: core.ActDiskWrite, IO: w.lastIO, Bufs: []core.IOBuf{buf}, Len: core.BlockSize})
	}
	return out
}

func TestExhaustionIsAnOrdinaryOutcome(t *testing.T) {
	p := NewPool(core.BlockSize, 0)
	w := &writer{}
	steps := []struct {
		now   core.Tick
		event core.Event
		want  []core.Action
	}{
		{now: 100, want: []core.Action{{Kind: core.ActDiskWrite, IO: 1, Bufs: []core.IOBuf{{Slot: 0, Gen: 1}}, Len: core.BlockSize}}},
		{now: 105, want: []core.Action{{Kind: core.ActSetTimer, Timer: retryTimer, At: 115}}},
		{now: 115, event: core.Event{Kind: core.EvTimer, Timer: retryTimer}, want: []core.Action{{Kind: core.ActSetTimer, Timer: retryTimer, At: 125}}},
		{now: 120, event: core.Event{Kind: core.EvDiskDone, IO: 1}, want: []core.Action{{Kind: core.ActDiskWrite, IO: 2, Bufs: []core.IOBuf{{Slot: 0, Gen: 2}}, Len: core.BlockSize}}},
		{now: 125, event: core.Event{Kind: core.EvTimer, Timer: retryTimer}},
		{now: 130, event: core.Event{Kind: core.EvDiskDone, IO: 2}},
	}
	for i, s := range steps {
		ctx := core.Context{Now: s.now, Pool: p}
		var got []core.Action
		if s.event.Kind == core.EvNone {
			got = w.Write(ctx, nil)
		} else {
			got = w.OnEvent(ctx, s.event, nil)
		}
		if !reflect.DeepEqual(got, s.want) {
			t.Fatalf("step %d at t=%d: got %+v, want %+v", i, s.now, got, s.want)
		}
	}
	if p.Live() != 0 || p.Used() != 0 {
		t.Fatalf("live %d used %d after the last DiskDone", p.Live(), p.Used())
	}
}
