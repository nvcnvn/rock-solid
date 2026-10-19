package main

import (
	"encoding/binary"
	"math/rand/v2"

	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/lsm"
)

const maxPiece = 1 << 20

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// keyOf spreads key ids over the key space so updates land at random positions.
func keyOf(buf []byte, id uint64) []byte {
	binary.BigEndian.PutUint64(buf[0:8], id*0x9E3779B97F4A7C15)
	binary.BigEndian.PutUint64(buf[8:16], id)
	return buf[:16]
}

func valueOf(buf []byte, n int, v uint64) []byte {
	for i := 0; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], v+uint64(i))
	}
	return buf[:n]
}

// realTree replays a workload through internal/lsm at the module's commit: puts, a checkpoint
// per flush whose pieces count as written, then every compaction PickCompaction offers, which
// the next checkpoint writes.
func realTree(w workload, valueB int, seed uint64) result {
	t, err := lsm.New(lsm.Config{MaxPiece: maxPiece})
	must(err)
	rng := rand.New(rand.NewPCG(seed, 2))
	kb, vb := make([]byte, 16), make([]byte, valueB)
	buf := make([]byte, maxPiece)
	var next uint64
	var ver uint64
	checkpoint := func() (all, mem int) {
		froze, err := t.BeginFlush()
		must(err)
		var runBytes []int
		cur := 0
		for {
			n, root, err := t.NextPiece(buf)
			must(err)
			if n == 0 {
				break
			}
			next++
			cur += n
			must(t.PieceDurable(disklog.BlobRef{Segment: 1, Block: uint32(next), LSN: next}))
			if root {
				runBytes = append(runBytes, cur)
				cur = 0
			}
		}
		_, err = t.CheckpointDone()
		must(err)
		for _, b := range runBytes {
			all += b
		}
		if froze && len(runBytes) > 0 {
			mem = runBytes[len(runBytes)-1]
		}
		return all, mem
	}
	for id := range uint64(w.keys) {
		ver++
		must(t.Put(keyOf(kb, id), valueOf(vb, valueB, ver)))
	}
	checkpoint()
	res := result{policy: "bucket (internal/lsm)", workload: w.name, liveB: float64(t.RunBytes())}
	half := len(w.flushes) / 2
	samples := 0
	for fi, n := range w.flushes {
		measure := fi >= half
		for range n {
			ver++
			must(t.Put(keyOf(kb, rng.Uint64N(uint64(w.keys))), valueOf(vb, valueB, ver)))
		}
		all, mem := checkpoint()
		if measure {
			res.flushedB += float64(mem)
			res.flushes++
		}
		if fi > half {
			res.compactedB += float64(all - mem)
		}
		res.runsMaxEver = max(res.runsMaxEver, t.Runs())
		for {
			c := t.PickCompaction()
			if c == nil {
				break
			}
			c.Run()
			must(t.Install(c))
			if measure {
				res.compactions++
			}
		}
		if measure {
			samples++
			res.runsMean += float64(t.Runs())
			res.runsMax = max(res.runsMax, t.Runs())
			sp := float64(t.RunBytes())
			res.spaceMean += sp
			res.spaceMax = max(res.spaceMax, sp)
		}
	}
	all, mem := checkpoint()
	res.compactedB += float64(all - mem)
	res.runsMean /= float64(samples)
	res.spaceMean /= float64(samples) * res.liveB
	res.spaceMax /= res.liveB
	return res
}
