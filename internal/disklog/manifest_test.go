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
		BaseSeq: 5,
		Starts:  [2]disklog.StreamStart{{Segment: 3, Block: 17}, {}},
		Indexes: []disklog.ManifestIndex{
			{Owner: 1, ReplayLSN: 70, Runs: []disklog.BlobRef{{Segment: 4, Block: 2, LSN: 78, Entry: 3}}},
			{Owner: 2, ReplayLSN: 77},
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
	f.Add([]byte{0, 0})
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
