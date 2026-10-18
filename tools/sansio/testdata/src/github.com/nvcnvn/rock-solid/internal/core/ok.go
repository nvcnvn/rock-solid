package core

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"
)

type Rand struct {
	r *rand.Rand
}

func NewRand(seed uint64) *Rand {
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], seed)
	return &Rand{r: rand.New(rand.NewChaCha8(key))}
}

func NewPCGRand(seed uint64) *Rand {
	return &Rand{r: rand.New(rand.NewPCG(seed, seed))}
}

func (r *Rand) Uint64() uint64 { return r.r.Uint64() }

func (r *Rand) Below(n uint64) uint64 { return r.r.Uint64N(n) }

func (r *Rand) Shuffle(n int, swap func(i, j int)) { r.r.Shuffle(n, swap) }

func sortedSum(m map[string]int, xs []int) (int, error) {
	sum := 0
	for _, k := range slices.Sorted(maps.Keys(m)) {
		sum += m[k]
	}
	for k := range maps.Keys(m) {
		sum += len(k)
	}
	for _, x := range xs {
		sum += x
	}
	for i := range 3 {
		sum += i
	}
	if sum < 0 {
		return 0, fmt.Errorf("negative sum %s", fmt.Sprint(sum))
	}
	return sum, nil
}

func deadline(now, tick time.Duration) time.Duration {
	if tick == 0 {
		panic("broken invariant: zero tick")
	}
	return now + 3*tick
}
