package sim

import "github.com/nvcnvn/rock-solid/internal/core"

const poison = 0xA5

// Pool is the simulator's buffer pool: core.Pool over one Go slice, poisoning every fresh buffer.
type Pool struct{ *core.Pool }

// NewPool makes a pool of capacityBytes in whole blocks with at most maxLive buffers out, 0 meaning no cap.
func NewPool(capacityBytes int, maxLive int) *Pool {
	n := (capacityBytes + core.BlockSize - 1) / core.BlockSize
	return &Pool{core.NewPool([][]byte{make([]byte, n*core.BlockSize)}, maxLive)}
}

// Alloc hands out n bytes rounded up to whole blocks, filled with the poison byte.
func (p *Pool) Alloc(n int) (core.IOBuf, error) {
	b, err := p.Pool.Alloc(n)
	if err != nil {
		return core.NoBuf, err
	}
	mem := p.Bytes(b)
	for i := range mem {
		mem[i] = poison
	}
	return b, nil
}
