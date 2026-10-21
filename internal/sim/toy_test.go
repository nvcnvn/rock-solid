package sim_test

import (
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/toy"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

func writeMsg(seq uint64) core.Msg { return toy.Msg(trace.MsgWrite, 0, seq, 0) }

func chaosSettings() sim.Settings {
	set := sim.DefaultSettings()
	set.DropNum, set.DropDen = 1, 8
	set.CrashNum, set.CrashDen = 1, 300
	set.StaleNum, set.StaleDen = 1, 4
	return set
}

func seedTrace(seed uint64) []byte {
	c := toy.NewCluster(seed, chaosSettings(), 40, toy.NoBug)
	c.Scheduler().Run(3000)
	return c.Scheduler().Trace()
}
