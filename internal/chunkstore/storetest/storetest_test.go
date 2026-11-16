package storetest_test

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim/seeds"
)

func TestSuiteOnTheSimulatedDisk(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Disk { return storetest.NewSimDisk(1) })
}

func TestFreedSlotReadsAsZerosOnTheSimulatedDisk(t *testing.T) {
	storetest.FreedSlotReadsAsZeros(t, storetest.NewSimDiskOf(1, storetest.SlotDiskBytes))
}

func TestFaultsOnTheSimulatedDisk(t *testing.T) {
	n := 20
	if testing.Short() {
		n = 4
	}
	unflushed := 0
	seeds.Run(t, 0, n, func(seed uint64) {
		for _, c := range storetest.SimCases {
			t.Run(c.Name, func(t *testing.T) {
				d := storetest.NewSimDisk(seed)
				c.Run(storetest.NewRig(t, d, storetest.Config()), d, core.NewRand(seed))
				unflushed += d.Unflushed
			})
		}
	})
	if seeds.Default() && unflushed == 0 {
		t.Fatal("no crash fell while a written block was unflushed")
	}
}
