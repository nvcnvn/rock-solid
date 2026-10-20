package main

import (
	"fmt"
	_ "log"
	_ "log/slog"
	"math/rand/v2"
	_ "os"
	_ "sync"
	"time"
	_ "unsafe"
)

func allowed(in chan int, m map[string]int) {
	_ = time.Now()
	_ = rand.Uint64()
	fmt.Println("ordinary Go")
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
