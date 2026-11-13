package chunkstore

import (
	"bytes"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// imageAfterLoss is rep2 with no restart and no move: Y reaches the primary alone as the interval ends, so the next peering
// drops it and has the primary list the chunk while its image is X. The other member's disk dies before the primary recovers
// its own entry and is declared lost, and the peering after that decides with the primary as X's one holder.
func imageAfterLoss(t *testing.T, plant func(*replicaPlants)) (Response, []byte) {
	r := newSoleRun(t, 2, 2, 5)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	c.until(id)
	c.settle()
	if plant != nil {
		plantAll(c, plant)
	}
	ms := r.members()
	p, m := ms[0].Disk, ms[1].Disk
	pn, mn := uint32(c.node(p)), uint32(c.node(m))
	r.recovery(false)
	c.s.Partition(pn, mn, true)
	c.write(pg0, ch, core.BlockSize, bytes.Repeat([]byte{0x22}, core.BlockSize))
	c.runFor(5_000)
	_, e := c.table.entry(pg0)
	r.a.asks = []PGInterval{{pg0, e.Interval}}
	r.publish()
	c.s.Heal(pn, mn)
	r.tick(5_000_000)
	if l := c.listed(c.node(p), ch); l.Listed == 0 || l.Version != l.Listed {
		t.Fatalf("primary %+v: the shape needs it listing X for its dropped Y", l)
	}
	r.lose(m)
	r.tick(DegradedRefillAfter + 2_000_000)
	r.declare(t, m)
	r.tick(5_000_000)
	r.recovery(true)
	r.tick(60_000_000)
	return r.read(ch)
}

func TestAPrimaryListingOnlyItsDroppedWriteServesPastADiskLoss(t *testing.T) {
	if rp, data := imageAfterLoss(t, nil); rp.Status != StatusOK || len(data) == 0 || data[0] != 0x11 {
		t.Fatalf("X read %+v %x", rp, data)
	}
}

// With forgetImage the next decision takes the primary's listing for a copy it lacks, and X, whole on the primary, is unfound.
func TestForgetImageLeavesTheChunkUnfound(t *testing.T) {
	if rp, _ := imageAfterLoss(t, func(p *replicaPlants) { p.forgetImage = true }); rp.Status != StatusUnfound {
		t.Fatalf("X read %+v", rp)
	}
}

// rep2 with min_live 1: X lands on the other member while the primary is down, so when it returns it lists nothing of the
// chunk yet lacks X. Its decision carries no image of X: its copy is not the version kept.
func TestAPrimaryLackingTheKeptVersionCarriesNoImage(t *testing.T) {
	r := newSoleRun(t, 2, 1, 2)
	c := r.c
	ch := chunkIn(pg0, 1, 0)
	ms := r.members()
	p := ms[0].Disk
	r.recovery(false)
	r.lose(p)
	r.tick(3_000_000)
	id := c.write(pg0, ch, 0, bytes.Repeat([]byte{0x11}, core.BlockSize))
	rp, ok := r.answered(id, 5_000_000)
	if !ok || rp.Status != StatusOK {
		t.Fatalf("X with the primary down: %+v", rp)
	}
	r.back(p)
	r.tick(5_000_000)
	n := c.node(p)
	i := c.reps[n].lead(pg0)
	if i < 0 || !c.reps[n].leads[i].active {
		t.Fatalf("disk %d does not lead pg0 after it came back: members %v", p, r.members())
	}
	if x, _ := c.reps[n].Store().Index().Chunk(testPool, ch); x.Whole == rp.Version {
		t.Fatalf("primary %+v: the shape needs it lacking X", x)
	}
	if img := c.reps[n].leads[i].image; slices.Contains(img, ChunkVersion{Chunk: ch, Version: rp.Version}) {
		t.Fatalf("the primary lacks %#x of %v but carries it as its image: %v", rp.Version, ch, img)
	}
}
