package sim

import (
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/core/pooltest"
)

func TestPool(t *testing.T) {
	pooltest.Run(t, func(_ *testing.T, capacity, maxLive int) pooltest.Pool { return NewPool(capacity, maxLive) })
}

func mustAlloc(t *testing.T, p *Pool, n int) core.IOBuf {
	t.Helper()
	b, err := p.Alloc(n)
	if err != nil {
		t.Fatalf("Alloc(%d): %v", n, err)
	}
	return b
}

func TestAllocPoisons(t *testing.T) {
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
}
