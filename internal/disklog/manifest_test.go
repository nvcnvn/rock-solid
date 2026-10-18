package disklog_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func sampleManifest() disklog.Manifest {
	return disklog.Manifest{
		BaseLSN: 77,
		Indexes: []disklog.ManifestIndex{
			{Owner: 1, ReplayLSN: 70, Runs: []disklog.BlobRef{{Segment: 4, Block: 2, LSN: 78, Entry: 3}}},
			{Owner: 2, ReplayLSN: 77},
		},
		Segments: []disklog.SegmentUsage{
			{Segment: 1, State: disklog.SegmentUsed, Live: 40, Newest: 77},
			{Segment: 3, State: disklog.SegmentQuarantined, Live: 12, Newest: 9},
			{Segment: 4, State: disklog.SegmentUsed},
		},
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := sampleManifest()
	b := disklog.AppendManifest(nil, &m)
	if len(b) != disklog.ManifestSize(&m) {
		t.Fatalf("encoded %d bytes, size says %d", len(b), disklog.ManifestSize(&m))
	}
	got, err := disklog.DecodeManifest(b)
	if err != nil || !manifestsEqual(got, m) {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	if _, err := disklog.DecodeManifest(append(slices.Clone(b), 0)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("trailing byte: %v", err)
	}
	if _, err := disklog.DecodeManifest(b[:10]); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("truncated: %v", err)
	}
	more := m
	more.Indexes = slices.Clone(m.Indexes)
	more.Indexes[1].Runs = []disklog.BlobRef{{Segment: 9, Block: 1, LSN: 80}}
	if n := len(disklog.AppendManifest(nil, &more)) - len(b); n != 18 {
		t.Fatalf("a reference adds %d bytes", n)
	}
	more = m
	more.Segments = append(slices.Clone(m.Segments), disklog.SegmentUsage{Segment: 9, State: disklog.SegmentUsed, Live: 1, Newest: 70})
	if n := len(disklog.AppendManifest(nil, &more)) - len(b); n != 17 {
		t.Fatalf("a segment adds %d bytes", n)
	}
	b[0]++
	if _, err := disklog.DecodeManifest(b); !errors.Is(err, disklog.ErrBadVersion) {
		t.Fatalf("other version: %v", err)
	}
}

func TestManifestPieceRoundTrip(t *testing.T) {
	earlier := []disklog.BlobRef{{Segment: 2, Block: 1, LSN: 4}, {Segment: 2, Block: 4, LSN: 5, Entry: 9}}
	b, err := disklog.AppendManifestPiece(nil, earlier, []byte("rest"))
	if err != nil || len(b) != disklog.ManifestPieceSize(2, 4) {
		t.Fatalf("%d bytes, %v", len(b), err)
	}
	refs, rest, err := disklog.DecodeManifestPiece(b, nil)
	if err != nil || !slices.Equal(refs, earlier) || string(rest) != "rest" {
		t.Fatalf("decoded %+v %q %v", refs, rest, err)
	}
	if _, _, err := disklog.DecodeManifestPiece(b[:20], nil); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("truncated: %v", err)
	}
	b[0]++
	if _, _, err := disklog.DecodeManifestPiece(b, nil); !errors.Is(err, disklog.ErrBadVersion) {
		t.Fatalf("other version: %v", err)
	}
}

func TestManifestRejectsDuplicateOwnersAndLateReplay(t *testing.T) {
	m := sampleManifest()
	m.Indexes[1].Owner = 1
	if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &m)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("duplicate owner: %v", err)
	}
	m = sampleManifest()
	m.Indexes[0].ReplayLSN = m.BaseLSN + 1
	if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &m)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("replay LSN past the base: %v", err)
	}
}

func TestManifestRejectsBadSegmentRows(t *testing.T) {
	for name, change := range map[string]func(*disklog.Manifest){
		"segment 0":       func(m *disklog.Manifest) { m.Segments[0].Segment = 0 },
		"out of order":    func(m *disklog.Manifest) { m.Segments[1].Segment = 5 },
		"listed twice":    func(m *disklog.Manifest) { m.Segments[1].Segment = 1 },
		"free":            func(m *disklog.Manifest) { m.Segments[0].State = disklog.SegmentFree },
		"unknown state":   func(m *disklog.Manifest) { m.Segments[0].State = 3 },
		"newer than base": func(m *disklog.Manifest) { m.Segments[0].Newest = m.BaseLSN + 1 },
	} {
		m := sampleManifest()
		change(&m)
		if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &m)); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func FuzzDecodeManifest(f *testing.F) {
	m := sampleManifest()
	f.Add(disklog.AppendManifest(nil, &m))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := disklog.DecodeManifest(b)
		if err != nil {
			return
		}
		if again := disklog.AppendManifest(nil, &m); !bytes.Equal(again, b) {
			t.Fatalf("re-encoding %+v differs", m)
		}
	})
}

func FuzzDecodeManifestPiece(f *testing.F) {
	b, _ := disklog.AppendManifestPiece(nil, []disklog.BlobRef{{Segment: 2, Block: 1, LSN: 4}}, []byte("rest"))
	f.Add(b)
	f.Add([]byte{1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		refs, rest, err := disklog.DecodeManifestPiece(b, nil)
		if err != nil {
			return
		}
		again, err := disklog.AppendManifestPiece(nil, refs, rest)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("re-encoding %+v %q differs: %v", refs, rest, err)
		}
	})
}
