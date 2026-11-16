package storetest_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/chunkstore/storetest"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
	"github.com/nvcnvn/rock-solid/internal/sim"
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

// A device formatted again over its log and crashed after one of format's steps, then started and
// crashed after as many steps of the format that start runs, comes back at each start with its old
// log whole, or formatted under a log ID no format drew before, which the record takes as a new disk.
func TestADeviceCrashedTwiceMidFormatComesBackWholeOrNew(t *testing.T) {
	cfg, data := storetest.Config(), storetest.Fill(3*core.BlockSize, 7)
	total := formatSteps(t)
	ks := []int{1, 2, 3, 4, 5, 6, 7, 8, total / 2}
	for k := total - 8; k <= total; k++ {
		ks = append(ks, k)
	}
	var old, fresh int
	for _, k := range ks {
		d, rnd := storetest.NewSimDisk(uint64(k)), core.NewRand(uint64(k))
		r := storetest.NewRig(t, d, cfg)
		r.OK(r.Write(storetest.Chunk(1), 0, data, 0))
		var rec chunkstore.DiskRecord
		id, _ := rec.Report(0, 1, 0x5eed, "")
		logs := []uint64{0x5eed}
		cut := func(log uint64) {
			logs = append(logs, log)
			r.Format(log)
			for i := 0; i < k && r.Step(); i++ {
			}
			d.Crash(rnd)
		}
		cut(rnd.Uint64())
		for start := 0; ; start++ {
			var act chunkstore.DeviceStart
			var log uint64
			r, act, log = storetest.StartRig(t, d, cfg, rnd)
			if act == chunkstore.StartFormat {
				if slices.Contains(logs, log) {
					t.Fatalf("crash after %d steps: start %d formats under log %#x again", k, start, log)
				}
				if start == 0 {
					cut(log)
					continue
				}
				logs = append(logs, log)
				r.Format(log)
				r.Settle()
			} else if act != chunkstore.StartOpened {
				t.Fatalf("crash after %d steps: start %d left the device to the operator: %v", k, start, r.S.Err())
			}
			got := r.S.Log().Superblock().LogID
			nid, err := rec.Report(0, 1, got, "")
			switch {
			case err != nil:
				t.Fatalf("crash after %d steps: start %d opened log %#x, refused: %v", k, start, got, err)
			case got == 0x5eed && nid == id && storetest.Diff(r.Chunk(storetest.Chunk(1))[:len(data)], data) == nil:
				old++
			case got != 0x5eed && slices.Contains(logs, got) && nid > id && storetest.Diff(r.Chunk(storetest.Chunk(1)), make([]byte, core.ChunkSize)) == nil:
				fresh++
			default:
				t.Fatalf("crash after %d steps: start %d opened log %#x as disk %d, old log %#x was disk %d, formats drew %#x", k, start, got, nid, uint64(0x5eed), id, logs)
			}
			break
		}
	}
	if old == 0 || fresh == 0 {
		t.Fatalf("old log %d, new log %d", old, fresh)
	}
}

// formatSteps counts the disk completions of a format on the simulated disk.
func formatSteps(t *testing.T) int {
	r, act, log := storetest.StartRig(t, storetest.NewSimDisk(0), storetest.Config(), core.NewRand(0))
	if act != chunkstore.StartFormat {
		t.Fatalf("a blank device: %v, %v", act, r.S.Err())
	}
	r.Format(log)
	n := 0
	for r.Step() {
		n++
	}
	return n
}

// A blank device too small for any geometry, and one whose format fails a write, are formatted and
// then reported failed: the node hands StartDevice how the format ended.
func TestADeviceWhoseFormatFailsIsReportedFailed(t *testing.T) {
	rng := core.NewRand(1)
	small := storetest.NewSimDiskOf(1, 8*core.BlockSize)
	r, act, _ := storetest.StartRig(t, small, storetest.Config(), rng)
	if act != chunkstore.StartFormat {
		t.Fatalf("a blank device too small for a geometry: %v, %v", act, r.S.Err())
	}
	_, err := disklog.GeometryFor(small.Blocks() * core.BlockSize)
	if act, _ := chunkstore.StartDevice(rng, err); act != chunkstore.StartFailed {
		t.Fatalf("a format with no geometry, %v: %v", err, act)
	}
	d := storetest.NewSimDisk(1)
	r, act, log := storetest.StartRig(t, d, storetest.Config(), rng)
	if act != chunkstore.StartFormat {
		t.Fatalf("a blank device: %v, %v", act, r.S.Err())
	}
	d.D.Inject(sim.Fault{Effect: sim.FailIO, When: func(op sim.DiskOp, _ *core.Rand) bool { return op.Kind == core.ActDiskWrite }})
	r.Format(log)
	r.Settle()
	if act, _ := chunkstore.StartDevice(rng, r.S.Err()); act != chunkstore.StartFailed {
		t.Fatalf("a format whose write failed, %v: %v", r.S.Err(), act)
	}
}

// A device whose superblock and copy both fail their checksums is left to the operator, byte for
// byte as it was.
func TestADeviceWithNoValidSuperblockIsLeftAsItWas(t *testing.T) {
	d := storetest.NewSimDisk(1)
	r := storetest.NewRig(t, d, storetest.Config())
	r.OK(r.Write(storetest.Chunk(1), 0, storetest.Fill(core.BlockSize, 7), 0))
	geo := r.S.Log().Superblock().Geometry
	d.D.FlipBit(geo.SuperblockLBA(), 16, 1)
	d.D.FlipBit(geo.SuperblockCopyLBA(), 16, 1)
	before := diskSum(d, 0)
	d.Restart()
	r, act, _ := storetest.StartRig(t, d, storetest.Config(), core.NewRand(1))
	if act != chunkstore.StartFailed || !errors.Is(r.S.Err(), disklog.ErrUnrecognized) {
		t.Fatalf("a device with no valid superblock: %v, %v", act, r.S.Err())
	}
	if diskSum(d, 0) != before {
		t.Fatal("starting a device with no valid superblock changed it")
	}
}

// A device reported failed after recovery repaired block 0 from its valid copy, then failed a read,
// differs from what it was only in block 0, now the copy's superblock.
func TestAFailedDeviceIsWrittenOnlyByItsSuperblockRepair(t *testing.T) {
	d := storetest.NewSimDisk(1)
	r := storetest.NewRig(t, d, storetest.Config())
	r.OK(r.Write(storetest.Chunk(1), 0, storetest.Fill(core.BlockSize, 7), 0))
	geo := r.S.Log().Superblock().Geometry
	if geo.SuperblockLBA() != 0 {
		t.Fatalf("block 0 at LBA %d", geo.SuperblockLBA())
	}
	d.D.FlipBit(0, 16, 1)
	b0, rest := d.D.Read(0, 1), diskSum(d, 1)
	d.Restart()
	d.D.Inject(sim.Fault{Effect: sim.FailIO, When: func(op sim.DiskOp, _ *core.Rand) bool {
		return op.Kind == core.ActDiskRead && op.LBA > geo.SuperblockCopyLBA()
	}})
	r, act, _ := storetest.StartRig(t, d, storetest.Config(), core.NewRand(1))
	if act != chunkstore.StartFailed {
		t.Fatalf("a device whose reads fail past its copy: %v, %v", act, r.S.Err())
	}
	if diskSum(d, 1) != rest {
		t.Fatal("starting the device changed a block other than block 0")
	}
	if after := d.D.Read(0, 1); bytes.Equal(after, b0) || !bytes.Equal(after, d.D.Read(geo.SuperblockCopyLBA(), 1)) {
		t.Fatal("block 0 is not the copy's superblock")
	}
}

func diskSum(d *storetest.SimDisk, from uint64) [sha256.Size]byte {
	h := sha256.New()
	for lba := from; lba < d.Blocks(); lba += 1024 {
		h.Write(d.D.Read(core.LBA(lba), min(1024, d.Blocks()-lba)))
	}
	return [sha256.Size]byte(h.Sum(nil))
}
