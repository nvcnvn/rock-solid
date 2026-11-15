package storetest

import (
	"errors"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Cases are the suite's behaviours, the Single-node ChunkStore item's.
var Cases = []Case{
	{"WritesFromOneByteToThreeBlocksReadBack", writesReadBack},
	{"PipelinedWritesToOneBlockMergeInOrder", pipelinedWrites},
	{"ExpectVerSucceedsAtTheCurrentVersionOnly", expectVer},
	{"AFlushAnswersOnceTheChunksWritesAreDurable", flush},
	{"AWholeChunkPunchLeavesATombstone", tombstone},
	{"AWholeChunkWriteIsOneVersion", wholeChunk},
	{"ASnapshotCutFreezesTheOlderVersion", snapshotCut},
	{"ARestartRecoversEveryAcknowledgedChange", restart},
	{"ACheckpointThenReplayReadsTheSame", checkpointReplay},
}

// SimCases are the cases that inject faults, which only the simulated disk can.
var SimCases = []SimCase{
	{"ACrashKeepsEveryAcknowledgedWrite", crash},
	{"AFailedWriteIsAnsweredWithAnErrorAndLosesNothingAcknowledged", failedWrite},
	{"AFlippedBitFailsTheRead", flippedBit},
}

// SimCase is a case that needs the simulated disk's fault injection.
type SimCase struct {
	Name string
	Run  func(r *Rig, d *SimDisk, rnd *core.Rand)
}

// Chunk is chunk i of the suite's volume.
func Chunk(i uint32) core.ChunkID { return core.ChunkID{Volume: 1, Inode: 1, Index: i} }

// Fill is n bytes of v, v+1, ... a byte per block.
func Fill(n int, v byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = v + byte(i/core.BlockSize)
	}
	return b
}

func writesReadBack(r *Rig) {
	c := Chunk(1)
	v1 := r.OK(r.Write(c, 100, []byte{0x22}, 0))
	v2 := r.OK(r.Write(c, 4000, Fill(200, 0x33), 0))
	v3 := r.OK(r.Write(c, 3*core.BlockSize+10, Fill(3*core.BlockSize, 0x44), 0))
	v4 := r.OK(r.Punch(c, 3*core.BlockSize+100, 2*core.BlockSize, 0))
	if !(v1 < v2 && v2 < v3 && v3 < v4) {
		r.T.Fatalf("versions %#x %#x %#x %#x do not rise", v1, v2, v3, v4)
	}
	if got, ver, err := r.ReadNow(chunkstore.Request{Chunk: c, Off: 90, Len: 20}); err != nil || ver != v4 || Diff(got, r.Model(c)[90:110]) != nil {
		r.T.Fatalf("unaligned read: version %#x %v, %v", ver, err, Diff(got, r.Model(c)[90:110]))
	}
	r.Check()
}

func pipelinedWrites(r *Rig) {
	c := Chunk(1)
	r.OK(r.Write(c, 0, Fill(core.BlockSize, 0x10), 0))
	ids := make([]uint64, 8)
	for i := range ids {
		ids[i] = r.Write(c, uint32(100+i), []byte{byte(i + 1), byte(i + 1)}, 0)
	}
	r.Settle()
	r.InOrder(ids...)
	var last uint64
	for _, id := range ids {
		v := r.OK(id)
		if v <= last {
			r.T.Fatalf("pipelined writes answered versions out of order: %#x after %#x", v, last)
		}
		last = v
	}
	r.Check()
}

func expectVer(r *Rig) {
	c := Chunk(1)
	v1 := r.OK(r.Write(c, 0, Fill(core.BlockSize, 1), 0))
	v2 := r.OK(r.WriteExpect(c, 10, []byte{9}, v1))
	r.Fails(r.WriteExpect(c, 20, []byte{8}, v1), chunkstore.ErrVersion)
	if n := r.S.Newest(c); n != v2 {
		r.T.Fatalf("newest version %#x, want %#x", n, v2)
	}
	r.Check()
}

func flush(r *Rig) {
	c, d := Chunk(1), Chunk(2)
	w1, w2 := r.Write(c, 0, Fill(2*core.BlockSize, 1), 0), r.Write(d, 0, Fill(core.BlockSize, 5), 0)
	f := r.Flush(c, d)
	if _, ok := r.Reply(f); ok {
		r.T.Fatal("a flush was answered before its chunks' writes were durable")
	}
	r.OK(f)
	r.OK(w1)
	r.OK(w2)
	r.InOrder(w1, f)
	r.InOrder(w2, f)
	r.Check()
}

func tombstone(r *Rig) {
	c := Chunk(1)
	r.OK(r.Write(c, 0, Fill(3*core.BlockSize, 1), 0))
	v := r.OK(r.Punch(c, 0, core.ChunkSize, 0))
	if n := r.S.Newest(c); n != v {
		r.T.Fatalf("newest version %#x after a whole-chunk punch at %#x", n, v)
	}
	if exts := r.S.Index().Extents(nil, pool, c, 0); len(exts) != 0 {
		r.T.Fatalf("live map %+v after a whole-chunk punch", exts)
	}
	r.Check()
	r.Restart()
	if n := r.S.Newest(c); n != v {
		r.T.Fatalf("newest version %#x after a restart, want the tombstone's %#x", n, v)
	}
	r.Check()
}

func wholeChunk(r *Rig) {
	c := Chunk(1)
	data := make([]byte, core.ChunkSize)
	core.NewRand(7).Fill(data)
	id := r.Write(c, 0, data, 0)
	if _, ok := r.Reply(id); ok {
		r.T.Fatal("a whole-chunk write was answered before it ran")
	}
	v := r.OK(id)
	exts := r.S.Index().Extents(nil, pool, c, 0)
	if len(exts) < 2 {
		r.T.Fatalf("a whole-chunk write mapped as %d extents, want several items", len(exts))
	}
	for _, e := range exts {
		if e.Version != v {
			r.T.Fatalf("extent %+v of a write at version %#x", e, v)
		}
	}
	r.Check()
}

func snapshotCut(r *Rig) {
	c, fresh := Chunk(1), Chunk(2)
	old := Fill(core.BlockSize, 0x10)
	v1 := r.OK(r.Write(c, 0, old, 0))
	v2 := r.OK(r.Write(c, 0, Fill(core.BlockSize, 0x20), 1))
	got, ver, err := r.ReadNow(chunkstore.Request{Chunk: c, Len: core.BlockSize, HasVer: true, Ver: v1})
	if err != nil || ver != v1 || Diff(got, old) != nil {
		r.T.Fatalf("read at the cut's version %#x: version %#x %v, %v", v1, ver, err, Diff(got, old))
	}
	if _, ver, err := r.ReadNow(chunkstore.Request{Chunk: c, Len: core.BlockSize}); err != nil || ver != v2 {
		r.T.Fatalf("live read: version %#x %v, want %#x", ver, err, v2)
	}
	r.OK(r.Write(fresh, 0, Fill(core.BlockSize, 0x30), 1))
	got, _, err = r.ReadNow(chunkstore.Request{Chunk: fresh, Len: core.BlockSize, HasVer: true})
	if err != nil || Diff(got, make([]byte, core.BlockSize)) != nil {
		r.T.Fatalf("a chunk first written after the cut reads %v at version 0, %v", err, Diff(got, make([]byte, core.BlockSize)))
	}
	r.Restart()
	if got, _, err := r.ReadNow(chunkstore.Request{Chunk: c, Len: core.BlockSize, HasVer: true, Ver: v1}); err != nil || Diff(got, old) != nil {
		r.T.Fatalf("read at the cut's version after a restart: %v, %v", err, Diff(got, old))
	}
	r.Check()
}

// churn writes and punches n changes at random over chunks 1 to 4, a few of them in flight at once.
func churn(r *Rig, rnd *core.Rand, n int) {
	for i := range n {
		c := Chunk(1 + uint32(rnd.Below(4)))
		off := uint32(rnd.Below(core.ChunkSize - 1))
		size := uint32(min(rnd.Range(1, 3*core.BlockSize), core.ChunkSize-uint64(off)))
		if rnd.Chance(1, 6) {
			r.Punch(c, off, size, 0)
		} else {
			data := make([]byte, size)
			rnd.Fill(data)
			r.Write(c, off, data, 0)
		}
		if i%4 == 3 {
			r.Settle()
		}
	}
	r.Settle()
}

func restart(r *Rig) {
	churn(r, core.NewRand(1), 64)
	r.Check()
	newest := make([]uint64, 4)
	for i := range newest {
		newest[i] = r.S.Newest(Chunk(1 + uint32(i)))
	}
	r.Restart()
	for i, v := range newest {
		if n := r.S.Newest(Chunk(1 + uint32(i))); n != v {
			r.T.Fatalf("chunk %d at version %#x after a restart, was %#x", i+1, n, v)
		}
	}
	r.Check()
	for i, v := range newest {
		if n := r.OK(r.Write(Chunk(1+uint32(i)), 0, []byte{1}, 0)); n <= v {
			r.T.Fatalf("chunk %d written at version %#x after a restart, was %#x", i+1, n, v)
		}
	}
	r.Check()
}

func checkpointReplay(r *Rig) {
	r.Cfg.Log.MaxReplayBytes = 256 << 10
	r.Restart()
	churn(r, core.NewRand(2), 400)
	if r.S.Log().Manifest().LSN == 0 {
		r.T.Fatal("no checkpoint was written")
	}
	before := make([][]byte, 4)
	for i := range before {
		before[i] = r.Chunk(Chunk(1 + uint32(i)))
	}
	r.Restart()
	for i, b := range before {
		if err := Diff(r.Chunk(Chunk(1+uint32(i))), b); err != nil {
			r.T.Fatalf("chunk %d after a checkpoint and replay: %v", i+1, err)
		}
	}
	r.Check()
}

func crash(r *Rig, d *SimDisk, rnd *core.Rand) {
	churn(r, rnd, 16)
	inFlight := make([]uint64, 4)
	data := make([][]byte, 4)
	for i := range inFlight {
		data[i] = make([]byte, rnd.Range(1, 3*core.BlockSize))
		rnd.Fill(data[i])
		inFlight[i] = r.Write(Chunk(10+uint32(i)), 0, data[i], 0)
	}
	for k := rnd.Below(6); k > 0 && r.Step(); k-- {
	}
	acked := make([]bool, len(inFlight))
	for i, id := range inFlight {
		p, ok := r.Reply(id)
		acked[i] = ok && p.Err == nil
	}
	d.Crash(rnd)
	r.Reopen()
	r.Check()
	for i := range inFlight {
		if !acked[i] {
			r.allOrNothing(Chunk(10+uint32(i)), data[i])
		}
	}
}

// allOrNothing checks that chunk c, written data at 0 by a write never acknowledged, holds all of
// it or none.
func (r *Rig) allOrNothing(c core.ChunkID, data []byte) {
	r.T.Helper()
	got := r.Chunk(c)[:len(data)]
	if Diff(got, data) != nil && Diff(got, make([]byte, len(got))) != nil {
		r.T.Fatalf("chunk %v, written by a write never acknowledged, recovered as neither all of it nor none", c)
	}
}

func failedWrite(r *Rig, d *SimDisk, rnd *core.Rand) {
	churn(r, rnd, 16)
	d.D.Inject(sim.Fault{Effect: sim.FailIO, When: sim.Once(sim.OnKind(core.ActDiskWrite))})
	data := Fill(core.BlockSize, 0x77)
	id := r.Write(Chunk(20), 0, data, 0)
	r.Settle()
	if p, ok := r.Reply(id); !ok || p.Err == nil {
		r.T.Fatalf("a write whose disk write failed was answered %+v %v", p, ok)
	}
	d.D.ClearFaults()
	r.Restart()
	r.Check()
	r.allOrNothing(Chunk(20), data)
}

func flippedBit(r *Rig, d *SimDisk, rnd *core.Rand) {
	c := Chunk(1)
	r.OK(r.Write(c, 0, Fill(core.BlockSize, 0x55), 0))
	e, ok := r.S.Index().Lookup(pool, c, 0)
	if !ok || e.LBA == 0 {
		r.T.Fatalf("block 0 of a written chunk is not mapped: %+v", e)
	}
	d.D.FlipBit(e.BlockLBA(0), int(rnd.Below(core.BlockSize)), 1<<rnd.Below(8))
	if _, _, err := r.ReadNow(chunkstore.Request{Chunk: c, Len: core.BlockSize}); !errors.Is(err, disklog.ErrBadChecksum) {
		r.T.Fatalf("a read of a block with a flipped bit: %v, want %v", err, disklog.ErrBadChecksum)
	}
}
