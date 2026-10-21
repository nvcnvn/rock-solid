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
		NextSeq: 9,
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

func sampleEdit() disklog.ManifestEdit {
	return disklog.ManifestEdit{
		BaseLSN: 90,
		NextSeq: 11,
		Indexes: []disklog.IndexEdit{
			{
				Owner: 2, ReplayLSN: 88,
				Added:  []disklog.BlobRef{{Segment: 6, Block: 1, LSN: 91}},
				Bounds: []disklog.MergeBound{{Run: disklog.BlobRef{Segment: 6, Block: 1, LSN: 91}, Key: []byte("k"), Dead: 3}},
			},
			{Owner: 1, ReplayLSN: 90, Removed: []disklog.BlobRef{{Segment: 4, Block: 2, LSN: 78, Entry: 3}}},
		},
		Segments: []disklog.SegmentUsage{
			{Segment: 1, State: disklog.SegmentFree},
			{Segment: 5, State: disklog.SegmentUsed, Live: 3, Newest: 90},
		},
	}
}

func editsEqual(a, b disklog.ManifestEdit) bool {
	return a.BaseLSN == b.BaseLSN && a.NextSeq == b.NextSeq && slices.Equal(a.Segments, b.Segments) &&
		slices.EqualFunc(a.Indexes, b.Indexes, func(x, y disklog.IndexEdit) bool {
			return x.Owner == y.Owner && x.ReplayLSN == y.ReplayLSN && slices.Equal(x.Added, y.Added) && slices.Equal(x.Removed, y.Removed) &&
				boundsEqual(x.Bounds, y.Bounds)
		})
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
	if _, err := disklog.DecodeManifestEdit(b); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a snapshot decoded as an edit: %v", err)
	}
	b[0]++
	if _, err := disklog.DecodeManifest(b); !errors.Is(err, disklog.ErrBadVersion) {
		t.Fatalf("other version: %v", err)
	}
}

func TestManifestEditRoundTrip(t *testing.T) {
	e := sampleEdit()
	b := disklog.AppendManifestEdit(nil, &e)
	if len(b) != disklog.ManifestEditSize(&e) {
		t.Fatalf("encoded %d bytes, size says %d", len(b), disklog.ManifestEditSize(&e))
	}
	got, err := disklog.DecodeManifestEdit(b)
	if err != nil || !editsEqual(got, e) {
		t.Fatalf("decoded %+v, %v", got, err)
	}
	if _, err := disklog.DecodeManifestEdit(append(slices.Clone(b), 0)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("trailing byte: %v", err)
	}
	if _, err := disklog.DecodeManifest(b); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("an edit decoded as a snapshot: %v", err)
	}
	none := disklog.ManifestEdit{BaseLSN: 5, NextSeq: 1}
	if got, err := disklog.DecodeManifestEdit(disklog.AppendManifestEdit(nil, &none)); err != nil || !editsEqual(got, none) {
		t.Fatalf("an empty edit decoded as %+v, %v", got, err)
	}
	b[0]++
	if _, err := disklog.DecodeManifestEdit(b); !errors.Is(err, disklog.ErrBadVersion) {
		t.Fatalf("other version: %v", err)
	}
}

func TestManifestNextSeqIsEightBytesAndNeverZero(t *testing.T) {
	m := sampleManifest()
	e := sampleEdit()
	for name, enc := range map[string]func(uint64) []byte{
		"snapshot": func(n uint64) []byte { m.NextSeq = n; return disklog.AppendManifest(nil, &m) },
		"edit":     func(n uint64) []byte { e.NextSeq = n; return disklog.AppendManifestEdit(nil, &e) },
	} {
		b := enc(1 << 40)
		if !bytes.Equal(b[11:19], []byte{0, 0, 0, 0, 0, 1, 0, 0}) {
			t.Fatalf("%s: NextSeq encoded as %x", name, b[11:19])
		}
		zero := enc(0)
		_, errS := disklog.DecodeManifest(zero)
		_, errE := disklog.DecodeManifestEdit(zero)
		if !errors.Is(errS, disklog.ErrMalformed) || !errors.Is(errE, disklog.ErrMalformed) {
			t.Fatalf("%s: NextSeq 0 decoded: %v, %v", name, errS, errE)
		}
	}
}

func TestManifestPieceRoundTrip(t *testing.T) {
	lists := [][]disklog.BlobRef{
		{{Segment: 2, Block: 1, LSN: 4}, {Segment: 2, Block: 4, LSN: 5, Entry: 9}},
		{{Segment: 3, Block: 7, LSN: 9}},
		nil,
	}
	b, err := disklog.AppendManifestPiece(nil, lists, []byte("rest"))
	if err != nil || len(b) != disklog.ManifestPieceSize(lists, 4) {
		t.Fatalf("%d bytes, %v", len(b), err)
	}
	got, rest, err := disklog.DecodeManifestPiece(b)
	if err != nil || len(got) != 3 || !slices.Equal(got[0], lists[0]) || !slices.Equal(got[1], lists[1]) || len(got[2]) != 0 || string(rest) != "rest" {
		t.Fatalf("decoded %+v %q %v", got, rest, err)
	}
	if _, _, err := disklog.DecodeManifestPiece(b[:20]); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("truncated: %v", err)
	}
	if _, err := disklog.AppendManifestPiece(nil, nil, nil); !errors.Is(err, disklog.ErrTooLarge) {
		t.Fatalf("a piece listing no manifest: %v", err)
	}
	if _, _, err := disklog.DecodeManifestPiece([]byte{1, 0, 0, 0}); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a piece listing no manifest decoded: %v", err)
	}
	b[0]++
	if _, _, err := disklog.DecodeManifestPiece(b); !errors.Is(err, disklog.ErrBadVersion) {
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
	e := sampleEdit()
	e.Indexes[1].Owner = 2
	if _, err := disklog.DecodeManifestEdit(disklog.AppendManifestEdit(nil, &e)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("an edit naming an index twice: %v", err)
	}
	e = sampleEdit()
	e.Indexes[0].ReplayLSN = e.BaseLSN + 1
	if _, err := disklog.DecodeManifestEdit(disklog.AppendManifestEdit(nil, &e)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("an edit's replay LSN past its base: %v", err)
	}
}

func TestManifestRejectsBadSegmentRows(t *testing.T) {
	for name, change := range map[string]func([]disklog.SegmentUsage, uint64){
		"segment 0":       func(u []disklog.SegmentUsage, _ uint64) { u[0].Segment = 0 },
		"out of order":    func(u []disklog.SegmentUsage, _ uint64) { u[0].Segment = 7 },
		"listed twice":    func(u []disklog.SegmentUsage, _ uint64) { u[1].Segment = u[0].Segment },
		"unknown state":   func(u []disklog.SegmentUsage, _ uint64) { u[1].State = disklog.SegmentSlot + 1 },
		"newer than base": func(u []disklog.SegmentUsage, base uint64) { u[1].Newest = base + 1 },
		"free with a count": func(u []disklog.SegmentUsage, _ uint64) {
			u[1].State, u[1].Live, u[1].Newest = disklog.SegmentFree, 1, 0
		},
		"free with an LSN": func(u []disklog.SegmentUsage, _ uint64) {
			u[1].State, u[1].Live, u[1].Newest = disklog.SegmentFree, 0, 1
		},
	} {
		m := sampleManifest()
		change(m.Segments, m.BaseLSN)
		if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &m)); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("snapshot, %s: %v", name, err)
		}
		e := sampleEdit()
		change(e.Segments, e.BaseLSN)
		if _, err := disklog.DecodeManifestEdit(disklog.AppendManifestEdit(nil, &e)); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("edit, %s: %v", name, err)
		}
	}
	m := sampleManifest()
	m.Segments[0] = disklog.SegmentUsage{Segment: 1, State: disklog.SegmentFree}
	if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &m)); !errors.Is(err, disklog.ErrMalformed) {
		t.Fatalf("a free row in a snapshot: %v", err)
	}
	e := sampleEdit()
	if got, err := disklog.DecodeManifestEdit(disklog.AppendManifestEdit(nil, &e)); err != nil || got.Segments[0].State != disklog.SegmentFree {
		t.Fatalf("a free row in an edit: %+v, %v", got.Segments, err)
	}
}

func TestManifestEditApplies(t *testing.T) {
	m, e := sampleManifest(), sampleEdit()
	got, err := disklog.ApplyManifestEdit(&m, &e)
	if err != nil {
		t.Fatal(err)
	}
	want := disklog.Manifest{
		BaseLSN: 90,
		NextSeq: 11,
		Indexes: []disklog.ManifestIndex{
			{Owner: 2, ReplayLSN: 88, Runs: []disklog.BlobRef{{Segment: 6, Block: 1, LSN: 91}}, Bounds: e.Indexes[0].Bounds},
			{Owner: 1, ReplayLSN: 90},
		},
		Segments: []disklog.SegmentUsage{
			{Segment: 3, State: disklog.SegmentQuarantined, Live: 12, Newest: 9},
			{Segment: 4, State: disklog.SegmentUsed},
			{Segment: 5, State: disklog.SegmentUsed, Live: 3, Newest: 90},
		},
	}
	if !manifestsEqual(got, want) {
		t.Fatalf("applied\n got %+v\nwant %+v", got, want)
	}
	if !manifestsEqual(m, sampleManifest()) {
		t.Fatalf("applying changed its base: %+v", m)
	}
	dropped := disklog.ManifestEdit{BaseLSN: 80, NextSeq: 9, Indexes: []disklog.IndexEdit{{Owner: 2, ReplayLSN: 80}}}
	if got, err := disklog.ApplyManifestEdit(&m, &dropped); err != nil || len(got.Indexes) != 1 || got.Indexes[0].Owner != 2 || !slices.Equal(got.Segments, m.Segments) {
		t.Fatalf("an edit leaving index 1 out: %+v, %v", got, err)
	}

	for name, change := range map[string]func(*disklog.ManifestEdit){
		"removes a run its base does not hold": func(e *disklog.ManifestEdit) {
			e.Indexes[0].Removed = []disklog.BlobRef{{Segment: 4, Block: 2, LSN: 78}}
		},
		"removes a run twice": func(e *disklog.ManifestEdit) {
			e.Indexes[1].Removed = append(e.Indexes[1].Removed, e.Indexes[1].Removed[0])
		},
		"adds a run its base holds": func(e *disklog.ManifestEdit) {
			e.Indexes[1].Removed, e.Indexes[1].Added = nil, e.Indexes[1].Removed
		},
		"frees a quarantined segment": func(e *disklog.ManifestEdit) { e.Segments[1] = disklog.SegmentUsage{Segment: 3} },
		"uses a quarantined segment":  func(e *disklog.ManifestEdit) { e.Segments[1].Segment = 3 },
		"goes back in base LSN":       func(e *disklog.ManifestEdit) { e.BaseLSN = m.BaseLSN - 1 },
		"goes back in NextSeq":        func(e *disklog.ManifestEdit) { e.NextSeq = m.NextSeq - 1 },
	} {
		e := sampleEdit()
		change(&e)
		if _, err := disklog.ApplyManifestEdit(&m, &e); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("an edit that %s: %v", name, err)
		}
	}
}

func TestLoadManifestStartsFromASnapshot(t *testing.T) {
	m, e := sampleManifest(), sampleEdit()
	snap, edit := disklog.AppendManifest(nil, &m), disklog.AppendManifestEdit(nil, &e)
	want, _ := disklog.ApplyManifestEdit(&m, &e)
	if got, err := disklog.LoadManifest([][]byte{snap, edit}); err != nil || !manifestsEqual(got, want) {
		t.Fatalf("loaded %+v, %v", got, err)
	}
	for name, bodies := range map[string][][]byte{
		"nothing":                     nil,
		"an edit with no snapshot":    {edit},
		"a snapshot after a snapshot": {snap, snap},
		"an edit applied twice":       {snap, edit, edit},
	} {
		if _, err := disklog.LoadManifest(bodies); !errors.Is(err, disklog.ErrMalformed) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func FuzzDecodeManifest(f *testing.F) {
	m := sampleManifest()
	f.Add(disklog.AppendManifest(nil, &m))
	m.Indexes[0].Bounds = []disklog.MergeBound{{Run: m.Indexes[0].Runs[0], Key: []byte("bound"), Dead: 2}}
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

func FuzzDecodeManifestEdit(f *testing.F) {
	e := sampleEdit()
	f.Add(disklog.AppendManifestEdit(nil, &e))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := disklog.DecodeManifestEdit(b)
		if err != nil {
			return
		}
		if again := disklog.AppendManifestEdit(nil, &e); !bytes.Equal(again, b) {
			t.Fatalf("re-encoding %+v differs", e)
		}
		base := sampleManifest()
		if got, err := disklog.ApplyManifestEdit(&base, &e); err == nil {
			if _, err := disklog.DecodeManifest(disklog.AppendManifest(nil, &got)); err != nil {
				t.Fatalf("applying %+v gave %+v, which does not decode: %v", e, got, err)
			}
		}
	})
}

func FuzzDecodeManifestPiece(f *testing.F) {
	b, _ := disklog.AppendManifestPiece(nil, [][]disklog.BlobRef{{{Segment: 2, Block: 1, LSN: 4}}, nil}, []byte("rest"))
	f.Add(b)
	f.Add([]byte{1, 0, 1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		lists, rest, err := disklog.DecodeManifestPiece(b)
		if err != nil {
			return
		}
		again, err := disklog.AppendManifestPiece(nil, lists, rest)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("re-encoding %+v %q differs: %v", lists, rest, err)
		}
	})
}
