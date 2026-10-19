package lsmx

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// The tree reads as a map under random puts, removals and range removals, across flushes and
// compactions, with every floor variant and with density merges.
func TestRangeModel(t *testing.T) {
	merges, rangedRuns := 0, 0
	defer func() { t.Logf("merges %d, runs holding range removals at the end %d", merges, rangedRuns) }()
	for _, cfg := range []Config{
		{MaxPiece: 4096},
		{MaxPiece: 4096, CursorFloor: true},
		{MaxPiece: 4096, CursorFloor: true, DensityMerge: 0.5, DensityMinRemovals: 4},
		{MaxPiece: 4096, SmallRun: 1},
		{MaxPiece: 4096, CursorFloor: true, SmallRun: 1, DensityMerge: 0.5, DensityMinRemovals: 4},
	} {
		for seed := range uint64(60) {
			t.Run(fmt.Sprintf("%+v/%d", cfg, seed), func(t *testing.T) {
				tr, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				r := rand.New(rand.NewPCG(seed, 7))
				m := model{}
				f := newFakeLog()
				key := func() string { return fmt.Sprintf("k%03d", r.IntN(300)) }
				var probes []string
				for i := range 300 {
					probes = append(probes, fmt.Sprintf("k%03d", i), fmt.Sprintf("k%03d~", i))
				}
				for step := range 3000 {
					switch x := r.IntN(100); {
					case x < 55:
						k, v := key(), fmt.Sprintf("v%d", step)
						if err := tr.Put([]byte(k), []byte(v)); err != nil {
							t.Fatal(err)
						}
						m[k] = v
					case x < 75:
						k := key()
						if err := tr.Delete([]byte(k)); err != nil {
							t.Fatal(err)
						}
						delete(m, k)
					case x < 80:
						lo := r.IntN(300)
						hi := lo + 1 + r.IntN(40)
						a, b := fmt.Sprintf("k%03d", lo), fmt.Sprintf("k%03d", hi)
						if err := tr.DeleteRange([]byte(a), []byte(b)); err != nil {
							t.Fatal(err)
						}
						for k := range m {
							if k >= a && k < b {
								delete(m, k)
							}
						}
					case x < 84:
						f.checkpoint(t, tr, nil)
					case x < 87:
						if c := tr.PickCompaction(); c != nil {
							c.Run()
							if err := tr.Install(c); err != nil {
								t.Fatal(err)
							}
						}
					}
					if step%100 == 99 {
						checkReads(t, tr, m, probes)
					}
				}
				checkReads(t, tr, m, probes)
				merges += tr.Stats.Merges
				ranged := 0
				for _, r := range tr.runs {
					if len(r.rdels) > 0 {
						ranged++
					}
				}
				rangedRuns += ranged
			})
		}
	}
}
