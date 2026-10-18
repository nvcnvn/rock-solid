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
	sg, err := AppendSegmentHeader(nil, testSegmentHeader())
	if err != nil {
		t.Fatal(err)
	}
	return map[string][]byte{"superblock": sb, "segment_header": sg, "record_header": goldenRecord(t).image[:core.BlockSize]}
}

func TestGoldenBytes(t *testing.T) {
	golden := readGolden(t)
	images := goldenImages(t)
	for _, name := range []string{"superblock", "segment_header", "record_header"} {
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
