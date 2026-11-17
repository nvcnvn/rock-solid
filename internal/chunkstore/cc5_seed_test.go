package chunkstore

import "testing"

func TestCC5Plant(t *testing.T) {
	for _, mod := range []bool{false, true} {
		err := runPrimaries(t, 94, &peerPlant{invKeptWritten, func(r *primaryRun) { r.c.replicaPlants.mergeOnDropped = mod }})
		t.Logf("plant %v: %v", mod, err)
	}
}
