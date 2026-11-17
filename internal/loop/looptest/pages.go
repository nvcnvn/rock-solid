package looptest

import (
	"sync"

	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

var shared struct {
	sync.Mutex
	slab *loop.Slab
}

// Pages is a tree's page source in tests, counting the bytes out: the test binary's off-heap slab,
// or under the seed flags the Go heap, since a seeded run drops its last trees unclosed and
// thousands of seeds would map gigabytes.
type Pages struct {
	// Out is the bytes carved and not given back.
	Out int
}

// Carve returns n bytes.
func (p *Pages) Carve(n int) []byte {
	p.Out += n
	if !seeds.Default() {
		return make([]byte, n)
	}
	shared.Lock()
	defer shared.Unlock()
	if shared.slab == nil {
		shared.slab = loop.NewSlab(64 << 20)
	}
	return shared.slab.Carve(n)
}

// Return takes back b.
func (p *Pages) Return(b []byte) {
	p.Out -= len(b)
	if seeds.Default() {
		shared.Lock()
		defer shared.Unlock()
		shared.slab.Return(b)
	}
}
