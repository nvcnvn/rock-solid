package chunkstore

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

// ZZN seeds from ZZFIRST of the read run with plant ZZPLANT checked against ZZINV.
func TestZZReads(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("ZZN"))
	first, _ := strconv.Atoi(os.Getenv("ZZFIRST"))
	name := os.Getenv("ZZPLANT")
	inv := map[string]string{"reader": invOneReader, "inc": invIncarnation, "late": invLateLog, "same": invSameInterval, "fwd": invForward,
		"acts": invActivations, "drop": invKeptAfterDrop, "ids": invDiskIDs}[os.Getenv("ZZINV")]
	p := &readPlant{inv: inv}
	switch name {
	case "instant":
		p.instant = true
	default:
		p.set = map[string]func(r *readRun){
			"fence":   func(r *readRun) { r.d.replicaPlants.fenceOnArrival = true },
			"reorder": func(r *readRun) { r.d.replicaPlants.reorderAfterOther = true },
			"stop":    func(r *readRun) { r.d.replicaPlants.stopPreviousOnly = true },
			"report":  func(r *readRun) { r.d.replicaPlants.reportDecided = true },
			"none":    func(r *readRun) {},
		}[name]
	}
	fails, firstFail, other := 0, -1, 0
	for seed := uint64(first); seed < uint64(first+n); seed++ {
		var err error
		done := false
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			err = runReads(t, seed, p)
			done = true
		})
		switch {
		case !done:
			other++
		case err != nil:
			fails++
			if firstFail < 0 {
				firstFail = int(seed)
				t.Logf("first: %v", err)
			}
		}
	}
	t.Logf("reads %s %s: %d of %d, first %d, test failures %d", name, os.Getenv("ZZINV"), fails, n, firstFail, other)
}

var zzlog = func(f string, a ...any) { fmt.Printf(f+"\n", a...) }
