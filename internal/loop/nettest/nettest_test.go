package nettest_test

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/loop/nettest"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

func TestSuiteOnTheSimulatedNetwork(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 4
	}
	seeds.Run(t, 0, n, func(seed uint64) {
		nettest.Run(t, func(t *testing.T) nettest.Net {
			set := sim.DefaultSettings()
			set.NetJitter = 200
			return nettest.NewSimNet(t, seed, set)
		})
		for _, c := range nettest.SimCases {
			t.Run(c.Name, func(t *testing.T) { c.Run(t, seed) })
		}
	})
}
