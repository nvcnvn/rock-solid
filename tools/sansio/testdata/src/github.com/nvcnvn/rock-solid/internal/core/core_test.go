package core

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"
)

func TestExempt(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	_ = time.Now()
	_ = rand.Uint64()
	fmt.Println("test files may do anything")
	ch := make(chan int, 1)
	go func() { ch <- 1 }()
	<-ch
	select {
	default:
	}
	for k := range map[string]int{"a": 1} {
		_ = k
	}
	_, _ = os.ReadFile("testdata/golden.txt")
}
