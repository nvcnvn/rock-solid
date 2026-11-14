package uring

import (
	"fmt"
	"io"
	_ "log"      // want `data path package imports "log": the data path emits no free-form logs; record a trace event instead`
	_ "log/slog" // want `data path package imports "log/slog": the data path emits no free-form logs; record a trace event instead`
	"math/rand/v2"
	_ "os"
	_ "sync"
	_ "sync/atomic"
	_ "syscall"
	"time"
	_ "unsafe"

	"golang.org/x/sys/unix"
)

func clocks() {
	_ = unix.ClockGettime(1, nil) // want `loop package calls unix.ClockGettime: read the clocks through the kernel seam`
	_ = unix.Gettimeofday(nil)    // want `loop package calls unix.Gettimeofday: read the clocks through the kernel seam`
	_, _ = unix.Time(nil)         // want `loop package calls unix.Time: read the clocks through the kernel seam`
}

func allowed(m map[string]int, keys []string) {
	_ = rand.Uint64()
	go func() {}()
	var d time.Duration
	_ = d
	for _, k := range keys {
		_ = m[k]
	}
}

func waits(in chan int, m map[string]int) { // want `loop package uses a channel: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
	_ = time.Now()               // want `loop package calls time.Now: read the clocks through the kernel seam`
	_ = time.Since(time.Time{})  // want `loop package calls time.Since: read the clocks through the kernel seam`
	time.Sleep(time.Millisecond) // want `loop package calls time.Sleep: block only in the kernel seam's Wait`
	var t *time.Timer            // want `loop package uses time.Timer: arm a timer in the loop's heap`
	_ = t
	time.AfterFunc(0, func() {}) // want `loop package calls time.AfterFunc: arm a timer in the loop's heap`
	ch := make(chan int, 1)      // want `loop package uses a channel: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
	ch <- 1                      // want `loop package sends on a channel: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
	<-ch                         // want `loop package receives from a channel: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
	select {                     // want `loop package uses a select statement: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
	case v := <-in: // want `loop package receives from a channel: a loop waits only in the kernel seam's Wait; hand work over through an inbox`
		_ = v
	default:
	}
	for k := range m { // want `loop package ranges over a map: iteration order is random; iterate sorted keys`
		_ = k
	}
}

func logs(w io.Writer) {
	fmt.Print("a")       // want `data path package calls fmt.Print: the data path emits no free-form logs; record a trace event instead`
	fmt.Println("a")     // want `data path package calls fmt.Println: the data path emits no free-form logs; record a trace event instead`
	fmt.Printf("a")      // want `data path package calls fmt.Printf: the data path emits no free-form logs; record a trace event instead`
	fmt.Fprint(w, "a")   // want `data path package calls fmt.Fprint: the data path emits no free-form logs; record a trace event instead`
	fmt.Fprintln(w, "a") // want `data path package calls fmt.Fprintln: the data path emits no free-form logs; record a trace event instead`
	fmt.Fprintf(w, "a")  // want `data path package calls fmt.Fprintf: the data path emits no free-form logs; record a trace event instead`
	_ = fmt.Sprintf("a")
}
