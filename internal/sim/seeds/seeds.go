// Package seeds picks the seeds a seeded test runs, from its own range or the seed flags, and logs
// the command that replays a failing seed.
package seeds

import (
	"flag"
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"
)

var (
	one   = flag.Int64("seed", -1, "run only this seed, to replay a failure")
	count = flag.Int("seeds", 0, "seeds each seeded test runs, in place of its own count")
	from  = flag.Int64("seeds.first", -1, "the first seed each seeded test runs, in place of its own")
	shard = flag.String("seeds.shard", "", "i/k: run only the i-th of k equal contiguous parts of the seeds")
)

// Default reports whether no seed flag is set, so each test runs its own seeds and a check across them holds.
func Default() bool {
	return *one < 0 && *count == 0 && *from < 0 && *shard == ""
}

// Run calls f with each seed of the test's own n from first, as the seed flags change them, and
// once f fails the test logs the command that replays that seed and stops.
func Run(t *testing.T, first uint64, n int, f func(seed uint64)) {
	t.Helper()
	if *from >= 0 {
		first = uint64(*from)
	}
	if *count > 0 {
		n = *count
	}
	lo, hi := first, first+uint64(n)
	if *one >= 0 {
		lo, hi = uint64(*one), uint64(*one)+1
	}
	if *shard != "" {
		var i, k uint64
		if _, err := fmt.Sscanf(*shard, "%d/%d", &i, &k); err != nil || i >= k {
			t.Fatalf("-seeds.shard %q is not i/k with i below k", *shard)
		}
		lo, hi = part(lo, hi, i, k)
	}
	for seed := lo; seed < hi && !t.Failed(); seed++ {
		func() {
			defer func() {
				if t.Failed() {
					t.Logf("replay: %s", Replay(t, seed))
				}
			}()
			f(seed)
		}()
	}
}

// part is the i-th of k contiguous parts of [lo, hi), so the k parts together hold each seed once.
func part(lo, hi, i, k uint64) (uint64, uint64) {
	n := hi - lo
	return lo + n*i/k, lo + n*(i+1)/k
}

// Replay is the go test command that runs t on seed alone, with every other flag this run was given.
func Replay(t testing.TB, seed uint64) string {
	pkg := "."
	if bi, ok := debug.ReadBuildInfo(); ok {
		if p, ok := strings.CutPrefix(strings.TrimSuffix(bi.Path, ".test"), bi.Main.Path); ok {
			pkg += p
		}
	}
	names := strings.Split(t.Name(), "/")
	for i, n := range names {
		names[i] = "^" + regexp.QuoteMeta(n) + "$"
	}
	cmd := fmt.Sprintf("go test %s -run '%s' -seed %d", pkg, strings.Join(names, "/"), seed)
	flag.Visit(func(f *flag.Flag) {
		if !strings.HasPrefix(f.Name, "test.") && !strings.HasPrefix(f.Name, "seed") {
			cmd += fmt.Sprintf(" -%s=%s", f.Name, f.Value)
		}
	})
	return cmd
}
