package loop_test

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/loop/looptest"
	"github.com/nvcnvn/rock-solid/internal/trace"
	"golang.org/x/sys/unix"
)

// resumes counts Resumed events and, with spin set, arms timer 1 again at Now each time it fires.
type resumes struct {
	n    int
	spin bool
}

func (m *resumes) OnEvent(ctx *core.Context, ev *core.Event, out []core.Action) []core.Action {
	switch ev.Kind {
	case core.EvResumed:
		m.n++
	case core.EvTimer:
		if m.spin {
			return append(out, core.Action{Kind: core.ActSetTimer, Timer: 1, At: ctx.Now})
		}
	}
	return out
}

func bootRaw(t *testing.T) (boot, raw core.Tick) {
	t.Helper()
	var b, r unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &b); err != nil {
		t.Fatal(err)
	}
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC_RAW, &r); err != nil {
		t.Fatal(err)
	}
	return core.Tick(b.Nano() / 1000), core.Tick(r.Nano() / 1000)
}

func TestAPassReadsBothClocksAfterItsWait(t *testing.T) {
	r := newRealLoop(t, 1)
	r.l.Inbox().Post(&core.Event{Kind: core.EvNetRecv})
	r.l.Pass()
	prev := r.l.Context().Now
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = unix.Write(r.ends[0], frame(nil, 1))
	}()
	boot0, raw0 := bootRaw(t)
	r.l.Pass()
	boot1, raw1 := bootRaw(t)
	c := r.l.Context()
	if c.Now < boot0 || c.Now > boot1 || c.Raw < raw0 || c.Raw > raw1 {
		t.Fatalf("a pass read Now %d and Raw %d; the test read %d to %d and %d to %d around it", c.Now, c.Raw, boot0, boot1, raw0, raw1)
	}
	if c.Raw-raw0 < 200_000 {
		t.Fatalf("Raw was read %d µs into a 200 ms wait", c.Raw-raw0)
	}
	var got trace.Record
	for _, rec := range r.records() {
		if rec.Name == trace.EvNetRecv && rec.Pass == r.l.Passes() {
			got = rec
		}
	}
	if got.Name != trace.EvNetRecv || got.Tick < prev+200_000 {
		t.Fatalf("the NetRecv that ended a 200 ms wait came at %d, %d µs after the pass before\n%s", got.Tick, got.Tick-prev, r.ring.Dump(nil))
	}
}

// TestNoResumedUnderThrottling makes a million passes beside a CPU hog when run in a container
// with a CPU limit that sets RS_WANT_THROTTLE: docker run --cpus 0.5 -e RS_WANT_THROTTLE=1 ...
func TestNoResumedUnderThrottling(t *testing.T) {
	passes, throttle := 100_000, os.Getenv("RS_WANT_THROTTLE") != ""
	if throttle {
		passes = 1_000_000
	}
	k, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	m := &resumes{spin: true}
	l := loop.New(loop.Config{}, k, m, &looptest.Wire{})
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range runtime.NumCPU() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
			}
		}()
	}
	var before int
	if throttle {
		before = throttled(t)
	}
	l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 1})
	start := time.Now()
	for range passes {
		l.Pass()
	}
	el := time.Since(start)
	stop.Store(true)
	wg.Wait()
	if m.n != 0 {
		t.Fatalf("%d Resumed in %d passes", m.n, passes)
	}
	if !throttle {
		t.Logf("%d passes in %v beside %d spinning goroutines, no Resumed", passes, el, runtime.NumCPU())
		return
	}
	n := throttled(t) - before
	if n == 0 {
		t.Fatal("no period was throttled")
	}
	t.Logf("%d passes in %v beside %d spinning goroutines, %d throttled periods, no Resumed", passes, el, runtime.NumCPU(), n)
}

func BenchmarkClocks(b *testing.B) {
	k, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		b.Fatal(err)
	}
	defer k.Close()
	b.ReportAllocs()
	var sum core.Tick
	for b.Loop() {
		sum += k.Clocks().Boot
	}
	_ = sum
}

func TestTheTimerfdIsAnAbsoluteBoottimeTimer(t *testing.T) {
	r := newRealLoop(t, 0)
	r.m.wait = 10_000
	r.l.Inbox().Post(&core.Event{Kind: core.EvTimer, Timer: 0})
	r.l.Pass()
	if d := r.l.Pending().Deadline; d != r.m.at {
		t.Fatalf("the heap's earliest is %d; the timer was set at %d", d, r.m.at)
	}
	fd := r.k.TimerFD()
	boot, _ := bootRaw(t)
	var cur unix.ItimerSpec
	if err := unix.TimerfdGettime(fd, &cur); err != nil {
		t.Fatal(err)
	}
	if left := cur.Value.Nano(); left <= 0 || left > int64(r.m.at-boot)*1000 {
		t.Fatalf("the timerfd has %d ns left, %d µs before a deadline at %d", left, r.m.at-boot, r.m.at)
	}
	info, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{fmt.Sprintf("clockid: %d\n", unix.CLOCK_BOOTTIME), "settime flags: 01\n"} {
		if !strings.Contains(string(info), want) {
			t.Fatalf("timerfd fdinfo lacks %q:\n%s", want, info)
		}
	}
	for r.m.fired.Load() == 0 {
		r.l.Pass()
	}
	if r.l.Passes() > 4 {
		t.Fatalf("%d passes to wait 10 ms: the loop spun", r.l.Passes())
	}
	var settimes int
	for _, rec := range r.records() {
		switch {
		case rec.Name == trace.SysTimerfdSettime:
			settimes++
			if core.Tick(rec.Fields[0]) != r.m.at {
				t.Fatalf("timerfd_settime at %d; the heap's earliest was %d", rec.Fields[0], r.m.at)
			}
		case rec.Name == trace.EvTimer && rec.Fields[0] == 1 && rec.Tick < r.m.at:
			t.Fatalf("timer handled at %d, before its deadline %d", rec.Tick, r.m.at)
		}
	}
	if settimes != 1 {
		t.Fatalf("%d timerfd_settime for one timer\n%s", settimes, r.ring.Dump(nil))
	}
	if ready := r.k.Wait(nil, false); len(ready) != 0 {
		t.Fatalf("the fired timerfd is still reported: %+v", ready)
	}
}

func TestAVMClockMarkerChangeDeliversResumed(t *testing.T) {
	path := vmclockFile(t, 1)
	k, err := loop.NewEpoll(path)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if err := k.VMClock(); err != nil {
		t.Fatalf("the crafted device was not mapped: %v", err)
	}
	m := &resumes{}
	l := loop.New(loop.Config{}, k, m, &looptest.Wire{})
	pass := func() {
		l.Inbox().Post(&core.Event{Kind: core.EvNetRecv})
		l.Pass()
	}
	pass()
	pass()
	if m.n != 0 {
		t.Fatalf("%d Resumed with the marker unchanged", m.n)
	}
	setMarker(t, path, 2)
	pass()
	pass()
	if m.n != 1 {
		t.Fatalf("%d Resumed after one marker change", m.n)
	}
}

func TestALoopWithoutAVMClockRuns(t *testing.T) {
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	if err := k.VMClock(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing device reported %v", err)
	}
	if c := k.Clocks(); c.Marker != 0 || c.Boot == 0 {
		t.Fatalf("clocks without a device: %+v", c)
	}
	host, err := loop.NewEpoll(loop.VMClockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	t.Logf("%s: %v", loop.VMClockPath, host.VMClock())
}

func TestAVMClockThatCannotBeMappedFailsNewEpoll(t *testing.T) {
	foreign := t.TempDir() + "/zeros"
	if err := os.WriteFile(foreign, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{foreign, t.TempDir()} {
		if k, err := loop.NewEpoll(path); err == nil {
			k.Close()
			t.Fatalf("NewEpoll took %s as its VMClock device", path)
		}
	}
}

// TestAStopInsideTheClockReadIsBracketedOut stops a child process that reads its clocks in a loop
// with SIGSTOP for 20 ms, 200 times. A stop between the CLOCK_MONOTONIC and CLOCK_BOOTTIME reads
// would move CLOCK_BOOTTIME − CLOCK_MONOTONIC by 20 ms, as a suspend does; the bracket must retry
// such a read, so every reading stays within 2 ms of the first, and the child must see retries.
func TestAStopInsideTheClockReadIsBracketedOut(t *testing.T) {
	if os.Getenv("RS_BRACKET_CHILD") != "" {
		bracketChild()
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAStopInsideTheClockReadIsBracketedOut$")
	cmd.Env = append(os.Environ(), "RS_BRACKET_CHILD=1")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(out)
	if line, err := r.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("child said %q, %v", line, err)
	}
	for i := range 200 {
		time.Sleep(time.Duration(i%7) * 300 * time.Microsecond)
		if err := cmd.Process.Signal(unix.SIGSTOP); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		if err := cmd.Process.Signal(unix.SIGCONT); err != nil {
			t.Fatal(err)
		}
	}
	_ = in.Close()
	rest, _ := io.ReadAll(r)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v: %s", err, rest)
	}
	t.Logf("%s", rest)
}

// bracketChild reads the clocks until its stdin closes, then reports and exits.
func bracketChild() {
	k, err := loop.NewEpoll("/nonexistent/vmclock0")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var done atomic.Bool
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		done.Store(true)
	}()
	first := k.Clocks()
	base := first.Boot - first.Mono
	var reads, worst int64
	fmt.Println("ready")
	for !done.Load() {
		c := k.Clocks()
		d := int64(c.Boot-c.Mono) - int64(base)
		worst = max(worst, d, -d)
		reads++
	}
	retries, widest := k.Bracket()
	fmt.Printf("%d reads, %d wider than ε (widest %v), BOOTTIME − MONOTONIC moved at most %d µs\n", reads, retries, time.Duration(widest), worst)
	if worst > 2000 || retries == 0 {
		os.Exit(1)
	}
	os.Exit(0)
}
