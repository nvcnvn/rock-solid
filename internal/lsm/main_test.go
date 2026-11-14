package lsm

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

// TestMain runs every tree in the package's tests on the event loops' off-heap slabs, except under
// the seed flags: a slab never gets a dropped tree's pages back, so thousands of seeds would map
// gigabytes the Go heap would have collected.
func TestMain(m *testing.M) {
	flag.Parse()
	if !seeds.Default() {
		os.Exit(m.Run())
	}
	slab := loop.NewSlab(64 << 20)
	var mu sync.Mutex
	heapPages = func(n int) []byte {
		mu.Lock()
		defer mu.Unlock()
		return slab.Carve(n)
	}
	code := m.Run()
	if err := slab.Close(); err != nil && code == 0 {
		code = 1
	}
	os.Exit(code)
}

func TestATreeTakesItsPagesFromConfigPages(t *testing.T) {
	slab := loop.NewSlab(4 << 20)
	t.Cleanup(func() {
		if err := slab.Close(); err != nil {
			t.Error(err)
		}
	})
	carves := 0
	tr, err := New(Config{MaxPiece: 1 << 20, Check: true, Pages: func(n int) []byte {
		carves++
		return slab.Carve(n)
	}})
	if err != nil {
		t.Fatal(err)
	}
	m := model{}
	for i := range 20000 {
		put(t, tr, m, fmt.Sprintf("key-%08d", i*7919%20000), fmt.Sprintf("value-%064d", i))
	}
	if carves < 2 || slab.Mapped() < carves<<20 {
		t.Fatalf("%d carves, %d bytes mapped: the tree did not take its pages from Config.Pages", carves, slab.Mapped())
	}
	checkReads(t, tr, m, []string{"", "key-00010000", "z"})
}
