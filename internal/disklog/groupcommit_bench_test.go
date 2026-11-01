package disklog_test

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// BenchmarkGroupCommitRound runs rounds of eight 1-block writes and a 300-byte Raft blob
// through submit, write completion and flush completion, with no checkpoint ever due, and
// reports each blob dead once durable.
func BenchmarkGroupCommitRound(b *testing.B) {
	pool := sim.NewPool(4<<20, 0)
	ctx := core.Context{Now: 1000, Raw: 1226, Pool: pool}
	geo, err := disklog.GeometryFor(1 << 40)
	if err != nil {
		b.Fatal(err)
	}
	var out disklog.Output
	cfg := disklog.DefaultConfig()
	cfg.MaxReplayBytes = 1 << 60
	cfg.NoReserve = true
	log, err := disklog.Format(&ctx, cfg, disklog.Superblock{LogID: 1, Geometry: geo}, 1, &out)
	if err != nil {
		b.Fatal(err)
	}
	var acts []core.Action
	var dead []disklog.BlobRef
	complete := func() {
		for len(out.Actions) > 0 {
			acts = append(acts[:0], out.Actions...)
			out.Reset()
			for i := range acts {
				ev := core.Event{Kind: core.EvDiskDone, IO: acts[i].IO}
				log.OnEvent(&ctx, &ev, &out)
			}
			for _, ev := range out.Events {
				for i, e := range ev.Entries {
					if ev.Kind == disklog.Durable && e.Kind == disklog.EntryBlob {
						dead = append(dead, disklog.BlobRef{Segment: ev.Segment, Block: ev.Block, LSN: ev.LSN, Entry: uint16(i)})
					}
				}
			}
		}
		out.Reset()
		if err := log.RaftBlobsDead(dead); err != nil {
			b.Fatal(err)
		}
		dead = dead[:0]
	}
	round := func() {
		for i := range 9 {
			it := disklog.Item{Entry: disklog.BlobEntry(disklog.BlobRaftEntries, 1, 300)}
			if i < 8 {
				it.Entry = disklog.WriteEntry(1, core.ChunkID{Index: uint32(i)}, 1, 0, 1)
			}
			it.Buf, _ = pool.Alloc(max(int(it.Entry.Length), core.BlockSize))
			if _, err := log.Submit(&ctx, disklog.Hot, it, &out); err != nil {
				b.Fatal(err)
			}
		}
		complete()
	}
	complete()
	for range 100 {
		round()
	}
	b.ReportAllocs()
	for b.Loop() {
		round()
	}
}
