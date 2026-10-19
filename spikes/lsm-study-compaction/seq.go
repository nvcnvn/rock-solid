package main

import (
	"math"
	"math/rand/v2"
)

// workload is a sequence of checkpoint flushes over an index of keys live keys, each flush
// holding n uniform random updates of existing keys, as the chunk index sees from user writes
// and the cleaner's moves.
type workload struct {
	name    string
	keys    int
	entryB  float64
	flushes []int
}

// sequences builds the flush-size sequences. small and large are entries per flush; the
// defaults put small at 1.3 MB and large at 33 MB of 40 B entries in a 44 MB index, the
// cleaner streams spike's S4 shape (64 MiB of 4 KiB writes, 2.06 index entries each, and a
// cleaner pass about 25 times that).
func sequences(keys int, entryB float64, small, large, flushes int, seed uint64) []workload {
	rng := rand.New(rand.NewPCG(seed, 1))
	mk := func(name string, f func(i int) int) workload {
		w := workload{name: name, keys: keys, entryB: entryB}
		for i := range flushes {
			w.flushes = append(w.flushes, f(i))
		}
		return w
	}
	lo, hi := math.Log(float64(small)/10), math.Log(float64(large))
	return []workload{
		mk("steady", func(int) int { return small }),
		mk("alternating", func(i int) int {
			if i%2 == 0 {
				return small
			}
			return large
		}),
		mk("bursty", func(int) int {
			if rng.Float64() < 0.2 {
				return large
			}
			return small
		}),
		mk("loguniform", func(int) int { return int(math.Exp(lo + rng.Float64()*(hi-lo))) }),
	}
}
