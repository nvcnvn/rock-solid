package bubble

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
	"unsafe"
)

const (
	clockMonotonicRaw = 4
	clockBoottime     = 7
)

func rawNow(clock uintptr) time.Duration {
	var ts syscall.Timespec
	syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, clock, uintptr(unsafe.Pointer(&ts)), 0)
	return time.Duration(ts.Nano())
}

type poller struct{ epfd, efd int }

func newPoller(t *testing.T) poller {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	efd, _, errno := syscall.RawSyscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(efd)}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, int(efd), &ev); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(epfd); syscall.Close(int(efd)) })
	return poller{epfd, int(efd)}
}

// wait blocks in epoll_pwait with no timeout until the eventfd is ready and returns how many
// times the call came back with EINTR first.
func (p poller) wait(raw bool) (eintr int) {
	var evs [8]syscall.EpollEvent
	call := syscall.Syscall6
	if raw {
		call = syscall.RawSyscall6
	}
	for {
		n, _, errno := call(syscall.SYS_EPOLL_PWAIT, uintptr(p.epfd), uintptr(unsafe.Pointer(&evs[0])), 8, ^uintptr(0), 0, 0)
		if errno == syscall.EINTR {
			eintr++
			continue
		}
		if int(n) > 0 {
			return eintr
		}
	}
}

func (p poller) wake() {
	one := uint64(1)
	syscall.RawSyscall(syscall.SYS_WRITE, uintptr(p.efd), uintptr(unsafe.Pointer(&one)), 8)
}

// E1: inside a bubble time.Now and time.Sleep are fake, the raw clocks are not.
func TestRawClocksIgnoreBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w0, b0, r0 := time.Now(), rawNow(clockBoottime), rawNow(clockMonotonicRaw)
		time.Sleep(time.Hour)
		fake, boot, raw := time.Since(w0), rawNow(clockBoottime)-b0, rawNow(clockMonotonicRaw)-r0
		t.Logf("E1 start=%s fake=%v CLOCK_BOOTTIME=%v CLOCK_MONOTONIC_RAW=%v", w0.UTC().Format(time.RFC3339), fake, boot, raw)
		if fake != time.Hour || boot > time.Second || raw > time.Second {
			t.Fatal("expected fake time to move an hour and the raw clocks almost nothing")
		}
	})
}

// E2: a goroutine in epoll_wait is not durably blocked, so the bubble's clock and
// synctest.Wait wait for the kernel. An unbubbled goroutine wakes it after 300 ms of real time.
func TestEpollWaitHoldsBubble(t *testing.T) {
	for _, tc := range []struct {
		name     string
		raw      bool
		viaSleep bool
	}{
		{"Syscall/time.Sleep", false, true},
		{"RawSyscall/time.Sleep", true, true},
		{"Syscall/synctest.Wait", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPoller(t)
			const realDelay = 300 * time.Millisecond
			go func() { time.Sleep(realDelay); p.wake() }()
			var real, fake time.Duration
			var eintr int
			synctest.Test(t, func(t *testing.T) {
				done := make(chan struct{})
				go func() { eintr = p.wait(tc.raw); close(done) }()
				start, w0 := rawNow(clockBoottime), time.Now()
				if tc.viaSleep {
					time.Sleep(time.Millisecond)
				} else {
					synctest.Wait()
				}
				real, fake = rawNow(clockBoottime)-start, time.Since(w0)
				<-done
			})
			t.Logf("E2 %s: a fake step of %v took %v of CLOCK_BOOTTIME; epoll_pwait returned EINTR %d times", tc.name, fake, real.Round(time.Millisecond), eintr)
			if real < realDelay*9/10 {
				t.Fatalf("bubble advanced while a goroutine was in epoll_wait (%v)", real)
			}
		})
	}
}

// E3: a timerfd on CLOCK_BOOTTIME armed in a bubble does not fire when fake time passes its deadline.
func TestTimerfdIgnoresBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tfd, _, errno := syscall.RawSyscall(syscall.SYS_TIMERFD_CREATE, clockBoottime, syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
		if errno != 0 {
			t.Fatal(errno)
		}
		defer syscall.Close(int(tfd))
		spec := [4]int64{0, 0, 1, 0} // it_interval 0, it_value 1 s
		if _, _, errno := syscall.RawSyscall6(syscall.SYS_TIMERFD_SETTIME, tfd, 0, uintptr(unsafe.Pointer(&spec)), 0, 0, 0); errno != 0 {
			t.Fatal(errno)
		}
		time.Sleep(2 * time.Second)
		var n uint64
		_, _, errno = syscall.RawSyscall(syscall.SYS_READ, tfd, uintptr(unsafe.Pointer(&n)), 8)
		t.Logf("E3 after 2 s of fake time a 1 s timerfd reads: expirations=%d errno=%v", n, errno)
		if errno != syscall.EAGAIN {
			t.Fatal("timerfd fired on fake time")
		}
	})
}

// E4: what a bubble does fake: a worker goroutine that backs off with time.Sleep and hands
// results to a loop through a channel-free queue and a non-blocking eventfd write.
func TestWorkerInBubble(t *testing.T) {
	p := newPoller(t)
	synctest.Test(t, func(t *testing.T) {
		var attempts [6]time.Duration
		n := 0
		start := time.Now()
		go func() {
			for d := 10 * time.Millisecond; n < len(attempts); d *= 2 {
				time.Sleep(d)
				attempts[n] = time.Since(start)
				n++
				p.wake()
			}
		}()
		time.Sleep(time.Second)
		synctest.Wait()
		var count uint64
		syscall.RawSyscall(syscall.SYS_READ, uintptr(p.efd), uintptr(unsafe.Pointer(&count)), 8)
		t.Logf("E4 attempts at %v, eventfd count %d", attempts[:n], count)
		if n != 6 || attempts[5] != 630*time.Millisecond || count != 6 {
			t.Fatal("expected 6 attempts by 1 s of fake time, the last at 630 ms")
		}
	})
}

// E5: not a bubble: what a loop stepped from one goroutine sees. After a write returns, does a
// zero-timeout epoll_wait on the reader already report it ready, every time?
func TestReadyAfterWrite(t *testing.T) {
	const rounds = 10000
	unixPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	tcpPair := tcpLoopback(t)
	for _, tc := range []struct {
		name string
		w, r int
	}{
		{"AF_UNIX socketpair", unixPair[0], unixPair[1]},
		{"TCP loopback", tcpPair[0], tcpPair[1]},
	} {
		epfd, _ := syscall.EpollCreate1(0)
		ev := syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(tc.r)}
		syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, tc.r, &ev)
		var evs [1]syscall.EpollEvent
		buf := make([]byte, 64)
		ready := 0
		for range rounds {
			syscall.Write(tc.w, buf)
			if n, _ := syscall.EpollWait(epfd, evs[:], 0); n == 1 {
				ready++
			}
			for {
				if n, _ := syscall.Read(tc.r, buf); n <= 0 {
					break
				}
			}
		}
		syscall.Close(epfd)
		t.Logf("E5 %s: ready at once after %d of %d writes", tc.name, ready, rounds)
	}
}

func tcpLoopback(t *testing.T) [2]int {
	ln, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	sa := &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}
	if err := syscall.Bind(ln, sa); err != nil {
		t.Fatal(err)
	}
	syscall.Listen(ln, 1)
	bound, _ := syscall.Getsockname(ln)
	c, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err := syscall.Connect(c, bound); err != nil {
		t.Fatal(err)
	}
	s, _, err := syscall.Accept(ln)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(ln)
	syscall.SetsockoptInt(c, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	syscall.SetNonblock(s, true)
	return [2]int{c, s}
}

// E6: not a bubble: what a container gives the real-kernel tier. Logs the cgroup's cpu.max and
// whether io_uring_setup is allowed; run.sh runs it under different docker options.
func TestContainer(t *testing.T) {
	const sysIOUringSetup = 425 // the same number on amd64 and arm64
	cpuMax, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		t.Logf("E6 cpu.max: %v", err)
	}
	var params [120]byte // struct io_uring_params
	fd, _, errno := syscall.RawSyscall(sysIOUringSetup, 4, uintptr(unsafe.Pointer(&params)), 0)
	if errno == 0 {
		syscall.Close(int(fd))
	}
	t.Logf("E6 uid=%d cpu.max=%q io_uring_setup errno=%v", os.Getuid(), strings.TrimSpace(string(cpuMax)), errno)
}
