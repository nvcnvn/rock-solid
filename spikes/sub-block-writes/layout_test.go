package subblock

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/disklog"
)

func TestLayoutMatchesCodec(t *testing.T) {
	chunk := core.ChunkID{Volume: 1, Inode: 2, Index: 3}
	cases := []struct {
		name    string
		entries []disklog.Entry
		model   recLayout
	}{
		{"one 4 KiB write (an append flushed as a block)", []disklog.Entry{disklog.WriteEntry(1, chunk, 1, 0, 1)}, recLayout{headerBytes: writeEntry, dataBytes: blockSize}},
		{"16 writes of one block", repeat(disklog.WriteEntry(1, chunk, 1, 0, 1), 16), recLayout{headerBytes: 16 * writeEntry, dataBytes: 16 * blockSize}},
		{"one 100-byte blob", []disklog.Entry{disklog.BlobEntry(disklog.BlobRaftEntries, 1, 100)}, recLayout{headerBytes: blobEntry, dataBytes: 100}},
		{"15 writes and one 100-byte blob", append(repeat(disklog.WriteEntry(1, chunk, 1, 0, 1), 15), disklog.BlobEntry(disklog.BlobRaftEntries, 1, 100)), recLayout{headerBytes: 15*writeEntry + blobEntry, dataBytes: 15*blockSize + 100}},
	}
	for _, c := range cases {
		got, err := disklog.LayoutOf(c.entries)
		if err != nil {
			t.Fatal(err)
		}
		c.model.finish()
		tag := "[OK ]"
		if int(got.HeaderBlocks) != c.model.headerBlocks || int(got.DataBlocks) != c.model.dataBlocks {
			tag = "[FAIL]"
			t.Fail()
		}
		t.Logf("%s L0 %s: got=%d+%d blocks (codec) want=%d+%d (model)", tag, c.name, got.HeaderBlocks, got.DataBlocks, c.model.headerBlocks, c.model.dataBlocks)
	}
}

func repeat(e disklog.Entry, n int) []disklog.Entry {
	out := make([]disklog.Entry, n)
	for i := range out {
		out[i] = e
	}
	return out
}
