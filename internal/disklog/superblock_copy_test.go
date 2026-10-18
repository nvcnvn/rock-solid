package disklog_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func (h *harness) wantSuperblockAt(lba core.LBA, want disklog.Superblock) {
	h.t.Helper()
	sb, err := disklog.DecodeSuperblock(h.read(lba, 1))
	if err != nil || sb != want {
		h.t.Fatalf("superblock at LBA %d: %+v, %v; want %+v", lba, sb, err, want)
	}
}

// superblockTrace runs a recovery and returns its trace up to the first segment-header read.
func superblockTrace(res *recovered) (*recovered, []string) {
	i := slices.IndexFunc(res.trace, func(s string) bool { return strings.HasPrefix(s, "read 64+2 ") })
	if i < 0 {
		return res, res.trace
	}
	return res, res.trace[:i+1]
}

func TestSuperblockCopyRepairsADamagedCopy(t *testing.T) {
	cases := []struct {
		name  string
		flip  core.LBA
		trace []string
	}{
		{"bit flip in block 0's log ID", 0, []string{"read 0+1 io100", "read 63+1 io101", "write 0+1 io102", "fsync io103", "read 64+2 io104"}},
		{"bit flip in the copy's log ID", 63, []string{"read 0+1 io100", "read 63+1 io101", "write 63+1 io102", "fsync io103", "read 64+2 io104"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := formatted(t, gib, testConfig(0))
			h.twoBlockWrites(3)
			want := h.log.Superblock()
			h.disk.FlipBit(c.flip, 17, 0x04)
			res, trace := superblockTrace(h.reopen())
			if !slices.Equal(trace, c.trace) {
				t.Fatalf("trace %q, want %q", trace, c.trace)
			}
			h.wantLSNs(res, lsnRange(1, 3))
			h.wantSuperblockAt(0, want)
			h.wantSuperblockAt(63, want)
		})
	}
}

func TestSuperblockCopyRestoresAnUnreadableBlock0(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(2)
	want := h.log.Superblock()
	h.disk.Put(0, make([]byte, core.BlockSize))
	h.disk.FailReads(0)
	res, trace := superblockTrace(h.reopen())
	if wantTrace := []string{"read 0+1 io100", "read 63+1 io101", "write 0+1 io102", "fsync io103", "read 64+2 io104"}; !slices.Equal(trace, wantTrace) {
		t.Fatalf("trace %q, want %q", trace, wantTrace)
	}
	h.wantLSNs(res, lsnRange(1, 2))
	h.wantSuperblockAt(0, want)
}

func TestSuperblockCopyNeverOverwritesAReusedBlock0(t *testing.T) {
	efi := bytes.Repeat([]byte("EFI PART"), core.BlockSize/8)
	cases := []struct {
		name   string
		damage func(h *harness)
		block0 error
	}{
		{"zeros", func(h *harness) { h.disk.Put(0, make([]byte, core.BlockSize)) }, disklog.ErrBadMagic},
		{"another format", func(h *harness) { h.disk.Put(0, efi) }, disklog.ErrBadMagic},
		{"bit flip in the magic", func(h *harness) { h.disk.FlipBit(0, 3, 1) }, disklog.ErrBadMagic},
		{"bit flip in the version", func(h *harness) { h.disk.FlipBit(0, 8, 2) }, disklog.ErrBadVersion},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := formatted(t, gib, testConfig(0))
			h.twoBlockWrites(2)
			want := h.log.Superblock()
			c.damage(h)
			block0 := h.read(0, 1)
			res, trace := superblockTrace(h.recover(disklog.ModeOpen))
			var co *disklog.CopyOnlyError
			if !errors.As(res.err, &co) || !errors.Is(res.err, disklog.ErrCopyOnly) || co.Copy != want || !errors.Is(co.Block0, c.block0) {
				t.Fatalf("recovery: %v, want CopyOnly(%v, copy %+v)", res.err, c.block0, want)
			}
			if wantTrace := []string{"read 0+1 io100", "read 63+1 io101"}; !slices.Equal(trace, wantTrace) {
				t.Fatalf("trace %q, want only the two superblock reads", trace)
			}
			if !bytes.Equal(h.read(0, 1), block0) {
				t.Fatal("block 0 was written")
			}
		})
	}
}

func TestSuperblockCopyBothDamagedIsNotFormatted(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.disk.FlipBit(0, 17, 1)
	h.disk.FlipBit(63, 17, 1)
	if res := h.recover(disklog.ModeOpen); !errors.Is(res.err, disklog.ErrNotFormatted) {
		t.Fatalf("both superblocks damaged: %v", res.err)
	}
}

func TestSuperblockCopyAfterAReformatThatCrashed(t *testing.T) {
	h := formatted(t, gib, testConfig(0))
	h.twoBlockWrites(3)
	old := h.log.Superblock()
	h.resetIO()
	h.logID = 4
	newSB := disklog.Superblock{LogID: 4, Geometry: h.geo}
	var err error
	if h.log, err = disklog.Format(&h.ctx, h.cfg, newSB, firstIO, &h.out); err != nil {
		t.Fatal(err)
	}
	h.absorb()
	h.round()
	h.wantTrace("write 0+1 io100", "fsync io101", "write 63+1 io102", "fsync io103")
	h.crashLanding()
	h.wantSuperblockAt(63, old)
	res := h.reopen()
	h.wantLSNs(res, nil)
	if res.durable != 0 || h.log.Superblock() != newSB {
		t.Fatalf("recovered LSN %d of %+v", res.durable, h.log.Superblock())
	}
	h.wantSuperblockAt(0, newSB)
	h.wantSuperblockAt(63, newSB)
	if free := disklog.FreeSegments(h.log); free != int(h.geo.SegmentCount)-1 {
		t.Fatalf("%d segments free, want every one of the old log's", free)
	}
}
