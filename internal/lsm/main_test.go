package lsm

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/loop"
)

// lockedSlab is a slab the package's parallel tests share.
type lockedSlab struct {
	mu   sync.Mutex
	slab *loop.Slab
}

func (s *lockedSlab) Carve(n int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.slab.Carve(n)
}

func (s *lockedSlab) Return(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slab.Return(b)
}

// TestMain runs every tree in the package's tests on the event loops' off-heap slabs. The seeded
// tests close each seed's trees, which a slab needs back, as thousands of seeds would map gigabytes.
func TestMain(m *testing.M) {
	flag.Parse()
	slab := &lockedSlab{slab: loop.NewSlab(64 << 20)}
	heapPages = slab
	code := m.Run()
	if err := slab.slab.Close(); err != nil && code == 0 {
		code = 1
	}
	os.Exit(code)
}

// countedPages counts the bytes of page slabs a tree holds of heapPages, so a seed can check that
// closing its trees gave every one back.
type countedPages struct{ out int }

func (p *countedPages) Carve(n int) []byte {
	p.out += n
	return heapPages.Carve(n)
}

func (p *countedPages) Return(b []byte) {
	p.out -= len(b)
	heapPages.Return(b)
}

// countedSlab counts the carves a tree makes.
type countedSlab struct {
	*loop.Slab
	carves int
}

func (s *countedSlab) Carve(n int) []byte {
	s.carves++
	return s.Slab.Carve(n)
}

// A tree takes its pages from Config.Pages and gives every slab back when it closes, which the
// next tree carves again without mapping more.
func TestATreeTakesItsPagesFromConfigPagesAndGivesThemBack(t *testing.T) {
	slab := &countedSlab{Slab: loop.NewSlab(4 << 20)}
	t.Cleanup(func() {
		if err := slab.Close(); err != nil {
			t.Error(err)
		}
	})
	tr, err := New(Config{MaxPiece: 1 << 20, Check: true, Pages: slab})
	if err != nil {
		t.Fatal(err)
	}
	m := model{}
	for i := range 20000 {
		put(t, tr, m, fmt.Sprintf("key-%08d", i*7919%20000), fmt.Sprintf("value-%064d", i))
	}
	if slab.carves < 2 || slab.Out() != slab.carves<<20 {
		t.Fatalf("%d carves, %d bytes out: the tree did not take its pages from Config.Pages", slab.carves, slab.Out())
	}
	checkReads(t, tr, m, []string{"", "key-00010000", "z"})
	mapped := slab.Mapped()
	tr.Close()
	if slab.Out() != 0 {
		t.Fatalf("%d bytes still out after the tree closed", slab.Out())
	}
	again, err := New(Config{MaxPiece: 1 << 20, Check: true, Pages: slab})
	if err != nil {
		t.Fatal(err)
	}
	m = model{}
	for i := range 20000 {
		put(t, again, m, fmt.Sprintf("key-%08d", i), "v")
	}
	again.Close()
	if slab.Mapped() != mapped || slab.Out() != 0 {
		t.Fatalf("mapped %d, then %d; %d out", mapped, slab.Mapped(), slab.Out())
	}
}
