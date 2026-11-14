package core

import "fmt"

// Pool is a BufPool over memory its caller supplies, in regions that a buffer never crosses,
// panicking on any freed or stale handle before it touches a byte.
type Pool struct {
	regions   [][]byte
	perRegion int
	blocks    []poolBlock
	low       int
	maxLive   int
	used      int
	live      int
}

type poolBlock struct {
	gen uint32
	run uint32
}

// NewPool hands out the whole blocks of regions, each at least a block and every one but the last
// of the same size, the last no larger, with at most maxLive buffers out, 0 meaning no cap.
func NewPool(regions [][]byte, maxLive int) *Pool {
	p := &Pool{regions: regions, maxLive: maxLive}
	n := 0
	for i, r := range regions {
		blocks := len(r) / BlockSize
		switch {
		case blocks == 0:
			panic(fmt.Sprintf("pool: region %d holds %d bytes, under a block", i, len(r)))
		case i == 0:
			p.perRegion = blocks
		case blocks > p.perRegion:
			panic(fmt.Sprintf("pool: region %d holds %d blocks, more than region 0's %d", i, blocks, p.perRegion))
		case len(regions[i-1])/BlockSize != p.perRegion:
			panic(fmt.Sprintf("pool: region %d follows a short region", i))
		}
		n += blocks
	}
	p.blocks = make([]poolBlock, n)
	return p
}

// Alloc hands out n bytes rounded up to whole blocks, with whatever they last held.
func (p *Pool) Alloc(n int) (IOBuf, error) {
	if n <= 0 {
		panic(fmt.Sprintf("pool: allocating %d bytes", n))
	}
	if p.maxLive > 0 && p.live >= p.maxLive {
		return NoBuf, &PoolExhaustedError{Requested: n, Available: 0}
	}
	want := (n + BlockSize - 1) / BlockSize
	start, ok := p.findRun(want)
	if !ok {
		return NoBuf, &PoolExhaustedError{Requested: n, Available: p.Available()}
	}
	s := &p.blocks[start]
	s.gen++
	s.run = uint32(want)
	p.used += want * BlockSize
	p.live++
	return IOBuf{Slot: uint32(start), Gen: s.gen}, nil
}

// AllocZeroed hands out n bytes rounded up to whole blocks, zeroed.
func (p *Pool) AllocZeroed(n int) (IOBuf, error) {
	b, err := p.Alloc(n)
	if err != nil {
		return NoBuf, err
	}
	clear(p.Bytes(b))
	return b, nil
}

// findRun is first fit from low, below which every block is taken, skipping a buffer at a time.
// ponytail: O(live buffers above low) per allocation; keep a free-run index if allocation shows in profiles.
func (p *Pool) findRun(want int) (int, bool) {
	free, first := 0, -1
	for i := p.low; i < len(p.blocks); {
		if run := int(p.blocks[i].run); run > 0 {
			free = 0
			i += run
			continue
		}
		if first < 0 {
			first = i
		}
		if i%p.perRegion == 0 {
			free = 0
		}
		free++
		i++
		if free == want {
			if start := i - want; start != first {
				p.low = first
			} else {
				p.low = i
			}
			return i - want, true
		}
	}
	if first >= 0 {
		p.low = first
	}
	return 0, false
}

// Bytes is the buffer's memory, every block of it.
func (p *Pool) Bytes(b IOBuf) []byte {
	s := p.slot(b)
	r, off := int(b.Slot)/p.perRegion, int(b.Slot)%p.perRegion*BlockSize
	return p.regions[r][off : off+int(s.run)*BlockSize]
}

// Free returns the buffer to the pool.
func (p *Pool) Free(b IOBuf) {
	s := p.slot(b)
	run := int(s.run)
	s.run = 0
	p.low = min(p.low, int(b.Slot))
	p.used -= run * BlockSize
	p.live--
}

func (p *Pool) slot(b IOBuf) *poolBlock {
	if b.Gen == 0 {
		panic("pool: using the zero buffer handle")
	}
	if int(b.Slot) >= len(p.blocks) {
		panic(fmt.Sprintf("pool: buffer slot %d is beyond the pool's %d blocks", b.Slot, len(p.blocks)))
	}
	s := &p.blocks[b.Slot]
	if b.Gen != s.gen {
		panic(fmt.Sprintf("pool: stale buffer handle: slot %d generation %d, the slot is at generation %d", b.Slot, b.Gen, s.gen))
	}
	if s.run == 0 {
		panic(fmt.Sprintf("pool: using a freed buffer: slot %d generation %d", b.Slot, b.Gen))
	}
	return s
}

// Valid reports whether b is a buffer handed out and not freed.
func (p *Pool) Valid(b IOBuf) bool {
	return b.Gen != 0 && int(b.Slot) < len(p.blocks) && p.blocks[b.Slot].gen == b.Gen && p.blocks[b.Slot].run != 0
}

// Used is the bytes handed out.
func (p *Pool) Used() int { return p.used }

// Available is the bytes not handed out.
func (p *Pool) Available() int { return len(p.blocks)*BlockSize - p.used }

// Live is the buffers outstanding.
func (p *Pool) Live() int { return p.live }
