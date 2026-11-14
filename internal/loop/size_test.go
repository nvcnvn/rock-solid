package loop

import "testing"

func TestSizeFollowsTheCPULimit(t *testing.T) {
	for _, c := range []struct {
		procs, cpus int
		want        Sizing
	}{
		{4, 4, Sizing{Loops: 4, Procs: 5}},
		{3, 4, Sizing{Loops: 2}},
		{2, 4, Sizing{Loops: 1}},
		{1, 2, Sizing{Loops: 1}},
		{1, 1, Sizing{Loops: 1, Procs: 2}},
	} {
		if got := Size(c.procs, c.cpus); got != c.want {
			t.Errorf("Size(%d, %d) = %+v, want %+v", c.procs, c.cpus, got, c.want)
		}
	}
}
