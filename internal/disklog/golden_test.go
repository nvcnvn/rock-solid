package disklog

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// log_format_golden.txt is frozen disk format; the test reads it, never the reverse.
func readGolden(t testing.TB) map[string][]byte {
	t.Helper()
	f, err := os.Open("testdata/log_format_golden.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	golden := make(map[string][]byte)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, hexBytes, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("bad golden line %q", line)
		}
		b, err := hex.DecodeString(hexBytes)
		if err != nil {
			t.Fatalf("golden %s: %v", name, err)
		}
		golden[name] = b
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return golden
}

func goldenImages(t testing.TB) map[string][]byte {
	t.Helper()
	sb, err := AppendSuperblock(nil, testSuperblock(t))
	if err != nil {
		t.Fatal(err)
	}
	images := map[string][]byte{"superblock": sb, "record_header": goldenRecord(t).image[:core.BlockSize]}
	input, output := BlobRef{Segment: 4, Block: 2, LSN: 78, Entry: 3}, BlobRef{Segment: 6, Block: 1, LSN: 91}
	images["manifest_snapshot"] = AppendManifest(nil, &Manifest{BaseLSN: 77, NextSeq: 9,
		Indexes:  []ManifestIndex{{Owner: 1, ReplayLSN: 70, Runs: []BlobRef{input, output}, Bounds: []MergeBound{{Run: input, Key: []byte("bound"), Dead: 2}}}},
		Segments: []SegmentUsage{{Segment: 1, State: SegmentUsed, Live: 40, Newest: 77}}})
	images["manifest_edit"] = AppendManifestEdit(nil, &ManifestEdit{BaseLSN: 90, NextSeq: 11,
		Indexes:  []IndexEdit{{Owner: 1, ReplayLSN: 88, Added: []BlobRef{output}, Removed: []BlobRef{input}, Bounds: []MergeBound{{Run: output, Key: []byte("k"), Dead: 3}}}},
		Segments: []SegmentUsage{{Segment: 1, State: SegmentFree}, {Segment: 5, State: SegmentUsed, Live: 3, Newest: 90}}})
	for name, h := range map[string]SegmentHeader{"segment_header": testSegmentHeader(), "segment_header_meta": testMetaHeader(), "segment_header_merge": testMergeHeader()} {
		if images[name], err = AppendSegmentHeader(nil, h); err != nil {
			t.Fatal(err)
		}
	}
	return images
}

func TestGoldenBytes(t *testing.T) {
	golden := readGolden(t)
	images := goldenImages(t)
	for _, name := range []string{"superblock", "segment_header", "segment_header_meta", "segment_header_merge", "record_header", "manifest_snapshot", "manifest_edit"} {
		t.Run(name, func(t *testing.T) {
			want, ok := golden[name]
			if !ok {
				t.Fatal("missing from the fixture")
			}
			got := images[name]
			if !bytes.Equal(got[:len(want)], want) {
				t.Fatalf("encoded\n%x\nwant\n%x", got[:len(want)], want)
			}
			if !isZero(got[len(want):]) {
				t.Fatal("bytes after the fixture are not zero")
			}
		})
	}
}
