package sim

import (
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
)

const poison = 0xA5

// Pool is the simulator's buffer pool: whole blocks of one backing slice, a generation
// per slot, poisoned Alloc memory and a panic on any freed or stale handle.
type Pool struct {
	mem     []byte
	blocks  []block
	maxLive int
	used    int
	live    int
}

type block struct {
	gen   uint32
	run   uint32
	taken bool
}

// NewPool makes a pool of capacityBytes rounded up to whole blocks; maxLive caps the
// buffers outstanding at once, 0 meaning no cap.
func NewPool(capacityBytes int, maxLive int) *Pool {
	n := (capacityBytes + core.BlockSize - 1) / core.BlockSize
	return &Pool{
		mem:     make([]byte, n*core.BlockSize),
		blocks:  make([]block, n),
		maxLive: maxLive,
	}
}

// Alloc hands out n bytes rounded up to whole blocks, filled with the poison byte.
func (p *Pool) Alloc(n int) (core.IOBuf, error) {
	b, err := p.alloc(n)
	if err != nil {
		return core.NoBuf, err
	}
	mem := p.Bytes(b)
	for i := range mem {
		mem[i] = poison
	}
	return b, nil
}

// AllocZeroed hands out n bytes rounded up to whole blocks, zeroed.
func (p *Pool) AllocZeroed(n int) (core.IOBuf, error) {
	b, err := p.alloc(n)
	if err != nil {
		return core.NoBuf, err
	}
	clear(p.Bytes(b))
	return b, nil
}

func (p *Pool) alloc(n int) (core.IOBuf, error) {
	if n <= 0 {
		panic(fmt.Sprintf("sim: allocating %d bytes", n))
	}
	if p.maxLive > 0 && p.live >= p.maxLive {
		return core.NoBuf, &core.PoolExhaustedError{Requested: n, Available: 0}
	}
	want := (n + core.BlockSize - 1) / core.BlockSize
	start, ok := p.findRun(want)
	if !ok {
		return core.NoBuf, &core.PoolExhaustedError{Requested: n, Available: p.Available()}
	}
	for i := start; i < start+want; i++ {
		p.blocks[i].taken = true
	}
	s := &p.blocks[start]
	s.gen++
	s.run = uint32(want)
	p.used += want * core.BlockSize
	p.live++
	return core.IOBuf{Slot: uint32(start), Gen: s.gen}, nil
}

func (p *Pool) findRun(want int) (int, bool) {
	free := 0
	for i, b := range p.blocks {
		if b.taken {
			free = 0
			continue
		}
		free++
		if free == want {
			return i - want + 1, true
		}
	}
	return 0, false
}

// Bytes is the buffer's memory, every block of it.
func (p *Pool) Bytes(b core.IOBuf) []byte {
	s := p.slot(b)
	start := int(b.Slot) * core.BlockSize
	return p.mem[start : start+int(s.run)*core.BlockSize]
}

// Free returns the buffer to the pool.
func (p *Pool) Free(b core.IOBuf) {
	s := p.slot(b)
	run := int(s.run)
	for i := int(b.Slot); i < int(b.Slot)+run; i++ {
		p.blocks[i].taken = false
	}
	s.run = 0
	p.used -= run * core.BlockSize
	p.live--
}

func (p *Pool) slot(b core.IOBuf) *block {
	if b.Gen == 0 {
		panic("sim: using the zero buffer handle")
	}
	if int(b.Slot) >= len(p.blocks) {
		panic(fmt.Sprintf("sim: buffer slot %d is beyond the pool's %d blocks", b.Slot, len(p.blocks)))
	}
	s := &p.blocks[b.Slot]
	if b.Gen != s.gen {
		panic(fmt.Sprintf("sim: stale buffer handle: slot %d generation %d, the slot is at generation %d", b.Slot, b.Gen, s.gen))
	}
	if s.run == 0 {
		panic(fmt.Sprintf("sim: using a freed buffer: slot %d generation %d", b.Slot, b.Gen))
	}
	return s
}

// Used is the bytes handed out.
func (p *Pool) Used() int { return p.used }

// Available is the bytes not handed out.
func (p *Pool) Available() int { return len(p.mem) - p.used }

// Live is the buffers outstanding.
func (p *Pool) Live() int { return p.live }
