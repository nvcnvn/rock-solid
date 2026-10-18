package loop

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

	_ "golang.org/x/sys/unix"
)

func allowed(in chan int, m map[string]int) {
	_ = time.Now()
	time.Sleep(time.Millisecond)
	_ = rand.Uint64()
	ch := make(chan int, 1)
	go func() { ch <- 1 }()
	<-ch
	select {
	case v := <-in:
		_ = v
	default:
	}
	for k := range m {
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
