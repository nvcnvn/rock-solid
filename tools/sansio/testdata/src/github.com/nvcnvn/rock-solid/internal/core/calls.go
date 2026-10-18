package core

import (
	"fmt"
	"io"
	"math/rand/v2"
	"time"
)

func clock() {
	_ = time.Now()                             // want `sans-IO package calls time.Now: use ctx.Now`
	_ = time.Since(time.Time{})                // want `sans-IO package calls time.Since: use ctx.Now`
	_ = time.Until(time.Time{})                // want `sans-IO package calls time.Until: use ctx.Now`
	time.Sleep(time.Millisecond)               // want `sans-IO package calls time.Sleep: emit an Action with a TimerID`
	_ = time.After(time.Second)                // want `sans-IO package calls time.After: emit an Action with a TimerID`
	_ = time.AfterFunc(time.Second, func() {}) // want `sans-IO package calls time.AfterFunc: emit an Action with a TimerID`
	_ = time.NewTimer(time.Second)             // want `sans-IO package calls time.NewTimer: emit an Action with a TimerID`
	_ = time.NewTicker(time.Second)            // want `sans-IO package calls time.NewTicker: emit an Action with a TimerID`
	_ = time.Tick(time.Second)                 // want `sans-IO package calls time.Tick: emit an Action with a TimerID`
	now := time.Now                            // want `sans-IO package calls time.Now: use ctx.Now`
	_ = now
}

func globalRandom() {
	_ = rand.Uint64()                  // want `sans-IO package calls rand.Uint64: it draws from the global source; use a seeded core.Rand`
	_ = rand.IntN(10)                  // want `sans-IO package calls rand.IntN: it draws from the global source; use a seeded core.Rand`
	_ = rand.N(10)                     // want `sans-IO package calls rand.N: it draws from the global source; use a seeded core.Rand`
	_ = rand.Float64()                 // want `sans-IO package calls rand.Float64: it draws from the global source; use a seeded core.Rand`
	rand.Shuffle(2, func(i, j int) {}) // want `sans-IO package calls rand.Shuffle: it draws from the global source; use a seeded core.Rand`
}

func logs(w io.Writer) {
	fmt.Print("a")       // want `sans-IO package calls fmt.Print: record a trace event instead of a free-form log`
	fmt.Println("a")     // want `sans-IO package calls fmt.Println: record a trace event instead of a free-form log`
	fmt.Printf("a")      // want `sans-IO package calls fmt.Printf: record a trace event instead of a free-form log`
	fmt.Fprint(w, "a")   // want `sans-IO package calls fmt.Fprint: record a trace event instead of a free-form log`
	fmt.Fprintln(w, "a") // want `sans-IO package calls fmt.Fprintln: record a trace event instead of a free-form log`
	fmt.Fprintf(w, "a")  // want `sans-IO package calls fmt.Fprintf: record a trace event instead of a free-form log`
}
