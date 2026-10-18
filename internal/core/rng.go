package core

import (
	"encoding/binary"
	"math/rand/v2"
)

// Rand is the only source of randomness a state machine may use: ChaCha8 from a 64-bit
// seed, so a run replays from its seed. It is not safe for concurrent use.
type Rand struct {
	r *rand.Rand
}

// NewRand seeds a generator. Two generators with one seed yield one stream.
func NewRand(seed uint64) *Rand {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	return &Rand{r: rand.New(rand.NewChaCha8(key))}
}

// Uint64 draws 64 random bits.
func (r *Rand) Uint64() uint64 { return r.r.Uint64() }

// Uint32 draws 32 random bits.
func (r *Rand) Uint32() uint32 { return r.r.Uint32() }

// Below draws uniformly from [0, n); n of 0 or 1 gives 0.
func (r *Rand) Below(n uint64) uint64 {
	if n < 2 {
		return 0
	}
	return r.r.Uint64N(n)
}

// Range draws uniformly from [lo, hi].
func (r *Rand) Range(lo, hi uint64) uint64 {
	if hi <= lo {
		return lo
	}
	return lo + r.Below(hi-lo+1)
}

// Chance is true with probability num/den.
func (r *Rand) Chance(num, den uint64) bool { return r.Below(den) < num }

// Fill overwrites b with random bytes.
func (r *Rand) Fill(b []byte) {
	for len(b) >= 8 {
		binary.LittleEndian.PutUint64(b, r.r.Uint64())
		b = b[8:]
	}
	if len(b) > 0 {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], r.r.Uint64())
		copy(b, tail[:])
	}
}

// Fork draws a seed and returns a generator for it, so a child's stream is fixed by the
// parent's state and independent of what the child draws.
func (r *Rand) Fork() *Rand { return NewRand(r.r.Uint64()) }

// Shuffle permutes n elements with Fisher-Yates.
func (r *Rand) Shuffle(n int, swap func(i, j int)) { r.r.Shuffle(n, swap) }
