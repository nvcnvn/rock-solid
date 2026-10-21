package seeds

import (
	"slices"
	"testing"
)

func TestShardsTogetherRunEachSeedOnce(t *testing.T) {
	for _, c := range []struct{ lo, hi, k uint64 }{{0, 10000, 14}, {1, 101, 3}, {5, 8, 8}, {7, 7, 2}} {
		var got []uint64
		for i := range c.k {
			lo, hi := part(c.lo, c.hi, i, c.k)
			for s := lo; s < hi; s++ {
				got = append(got, s)
			}
		}
		var want []uint64
		for s := c.lo; s < c.hi; s++ {
			want = append(want, s)
		}
		if !slices.Equal(got, want) {
			t.Errorf("[%d, %d) in %d shards: %v", c.lo, c.hi, c.k, got)
		}
	}
}

func TestRunTakesTheTestsOwnSeedsByDefault(t *testing.T) {
	if !Default() {
		t.Skip("a seed flag is set")
	}
	var got []uint64
	Run(t, 5, 3, func(seed uint64) { got = append(got, seed) })
	if !slices.Equal(got, []uint64{5, 6, 7}) {
		t.Fatalf("ran %v", got)
	}
}

func TestReplayNamesThePackageTestAndSeed(t *testing.T) {
	t.Run("a sub test", func(t *testing.T) {
		const want = "go test ./internal/sim/seeds -run '^TestReplayNamesThePackageTestAndSeed$/^a_sub_test$' -seed 42"
		if got := Replay(t, 42); got != want {
			t.Fatalf("got  %s\nwant %s", got, want)
		}
	})
}
