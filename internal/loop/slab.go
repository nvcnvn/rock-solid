// Package loop runs the state machines on the event loops, and holds the slabs their bulk memory lives in.
package loop

import (
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
	"golang.org/x/sys/unix"
)

// MaxRegion is the largest region a slab maps at once, the most io_uring registers as one buffer.
const MaxRegion = 1 << 30

// Slab is memory mapped outside the Go heap as regions in a table that only grows, so a region
// never moves once mapped. It hands out bytes only, so it never holds a Go pointer.
type Slab struct {
	regions [][]byte
	size    int
	left    int
}

// NewSlab makes a slab that maps regions of size bytes, rounded up to whole blocks; size is at most MaxRegion.
func NewSlab(size int) *Slab {
	if size <= 0 || size > MaxRegion {
		panic(fmt.Sprintf("loop: slab region of %d bytes", size))
	}
	return &Slab{size: roundBlocks(size)}
}

func roundBlocks(n int) int { return (n + core.BlockSize - 1) / core.BlockSize * core.BlockSize }

func (s *Slab) grow(size int) ([]byte, error) {
	r, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		return nil, fmt.Errorf("loop: mapping a %d-byte slab region: %w", size, err)
	}
	s.regions = append(s.regions, r)
	return r, nil
}

// Carve returns n bytes, rounded up to whole blocks, from the last region, mapping a new region
// when it has fewer left. It panics when the mapping fails, as the Go heap does when out of memory.
func (s *Slab) Carve(n int) []byte {
	n = roundBlocks(n)
	if n <= 0 || n > s.size {
		panic(fmt.Sprintf("loop: carving %d bytes from %d-byte regions", n, s.size))
	}
	if n > s.left {
		if _, err := s.grow(s.size); err != nil {
			panic(err)
		}
		s.left = s.size
	}
	r := s.regions[len(s.regions)-1]
	off := s.size - s.left
	s.left -= n
	return r[off : off+n : off+n]
}

// Mapped is the bytes the slab has mapped.
func (s *Slab) Mapped() int {
	n := 0
	for _, r := range s.regions {
		n += len(r)
	}
	return n
}

// Close unmaps every region; nothing carved from them may be used after.
func (s *Slab) Close() error {
	var first error
	for _, r := range s.regions {
		if err := unix.Munmap(r); err != nil && first == nil {
			first = err
		}
	}
	s.regions, s.left = nil, 0
	return first
}

// Pool is the default drivers' buffer pool, core.Pool over a slab of a fixed capacity.
type Pool struct {
	*core.Pool
	slab *Slab
}

// NewPool maps capacityBytes, rounded up to whole blocks, in regions of at most MaxRegion, with at
// most maxLive buffers out, 0 meaning no cap.
func NewPool(capacityBytes, maxLive int) (*Pool, error) {
	return newPool(capacityBytes, maxLive, MaxRegion)
}

func newPool(capacityBytes, maxLive, region int) (*Pool, error) {
	s := &Slab{size: region}
	for left := roundBlocks(capacityBytes); left > 0; left -= region {
		if _, err := s.grow(min(left, region)); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return &Pool{Pool: core.NewPool(s.regions, maxLive), slab: s}, nil
}

// Close unmaps the pool's slab and empties the pool in place, so any later use of a handle, live or
// not, through the pool or a kept *core.Pool, panics as past the pool's end; a slice taken from
// Bytes must not be kept past its handle.
func (p *Pool) Close() error {
	*p.Pool = core.Pool{}
	return p.slab.Close()
}
