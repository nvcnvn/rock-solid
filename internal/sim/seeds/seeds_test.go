package seeds

import (
	"os"
	"os/exec"
	"slices"
	"strings"
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

// A seed that panics logs its replay line before the panic ends the test binary, so the run is
// checked from a second process of this binary.
func TestAPanickingSeedLogsItsReplay(t *testing.T) {
	if os.Getenv("SEEDS_TEST_PANIC") != "" {
		Run(t, 1, 5, func(seed uint64) {
			if seed == 3 {
				panic("broken invariant")
			}
		})
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run", "^TestAPanickingSeedLogsItsReplay$")
	cmd.Env = append(os.Environ(), "SEEDS_TEST_PANIC=1")
	out, err := cmd.CombinedOutput()
	const want = "replay: go test ./internal/sim/seeds -run '^TestAPanickingSeedLogsItsReplay$' -seed 3"
	if err == nil || !strings.Contains(string(out), want) || !strings.Contains(string(out), "broken invariant") {
		t.Fatalf("exit %v, output:\n%s", err, out)
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
