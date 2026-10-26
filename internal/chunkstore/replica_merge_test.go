package chunkstore

import (
	"bytes"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
)

// diskBlock is block b of chunk ch as disk d's index maps it, read off its disk; zeros if unmapped.
func (c *rcluster) diskBlock(d int, ch core.ChunkID, b uint16) []byte {
	e, ok := c.reps[d].Store().Index().Lookup(testPool, ch, b)
	if !ok || e.LBA == 0 {
		return make([]byte, core.BlockSize)
	}
	return c.s.Node(uint32(d)).Disk(0).Read(e.BlockLBA(b), 1)
}

// sinceMark counts the actions disk d emitted from pass mark on that match.
func (c *rcluster) sinceMark(d, mark int, match func(a *core.Action) bool) int {
	n := 0
	for _, acts := range c.passes[d][mark:] {
		for i := range acts {
			if match(&acts[i]) {
				n++
			}
		}
	}
	return n
}

func isRead(a *core.Action) bool { return a.Kind == core.ActDiskRead }

func (c *rcluster) writeWhole(pg core.PGID, ch core.ChunkID, first uint16, n int, fill byte) {
	c.t.Helper()
	w := c.write(pg, ch, uint32(first)*core.BlockSize, bytes.Repeat([]byte{fill}, n*core.BlockSize))
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK {
		c.t.Fatalf("whole write answered %+v", rp)
	}
}

func TestPartialBlocksAreMergedAtThePrimary(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 5, 1, 0xaa)

	mark := len(c.passes[0])
	w := c.write(pg0, ch, 5*core.BlockSize, bytes.Repeat([]byte{1}, 100))
	c.settle()
	if rp := c.reply(w); rp.Status != StatusOK {
		t.Fatal(rp)
	}
	sent := c.sinceMark(0, mark, func(a *core.Action) bool {
		var u Update
		return a.Kind == core.ActNetSend && !a.Reply && DecodeUpdate(a.Msg.Inline(), core.BlockSize, &u) == nil && u.Off == 5*core.BlockSize && u.Len == core.BlockSize
	})
	if sent != 2 || c.sinceMark(0, mark, isRead) != 2 {
		t.Fatalf("%d whole blocks sent, %d base reads (block and header)", sent, c.sinceMark(0, mark, isRead))
	}
	for d := range 3 {
		got := c.diskBlock(d, ch, 5)
		if !bytes.Equal(got[:100], bytes.Repeat([]byte{1}, 100)) || got[100] != 0xaa || got[4095] != 0xaa {
			t.Fatalf("disk %d holds % x ... % x", d+1, got[98:102], got[4094:])
		}
	}

	mark = len(c.passes[0])
	w = c.write(pg0, ch, 5*core.BlockSize+100, bytes.Repeat([]byte{2}, 100))
	c.settle()
	if c.reply(w).Status != StatusOK || c.sinceMark(0, mark, isRead) != 0 {
		t.Fatalf("a base in the merged-block cache was read from disk %d times", c.sinceMark(0, mark, isRead))
	}

	c.s.Partition(0, 2, false)
	c.write(pg0, ch, 5*core.BlockSize+200, bytes.Repeat([]byte{3}, 100))
	c.write(pg0, ch, 5*core.BlockSize+300, bytes.Repeat([]byte{4}, 100))
	c.runFor(100_000)
	got := c.diskBlock(1, ch, 5)
	if got[0] != 1 || got[100] != 2 || got[200] != 3 || got[300] != 4 || got[400] != 0xaa {
		t.Fatalf("the second pending write was not merged on the first: % x", []byte{got[0], got[100], got[200], got[300], got[400]})
	}
	if got := c.readNow(pg0, ch, 5*core.BlockSize+200, 1); got[0] != 0xaa {
		t.Fatalf("read %x of an uncommitted write", got[0])
	}
}

func TestEdgesAloneNeedABase(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 0, 3, 0xaa)
	mark := len(c.passes[0])
	w := c.write(pg0, ch, 100, bytes.Repeat([]byte{1}, 8192))
	c.settle()
	if c.reply(w).Status != StatusOK || c.sinceMark(0, mark, isRead) != 4 {
		t.Fatalf("bytes 100 to 8,291 made %d base reads, want 2 blocks and their headers", c.sinceMark(0, mark, isRead))
	}
	mark = len(c.passes[0])
	c.writeWhole(pg0, ch, 3, 2, 0xbb)
	if c.sinceMark(0, mark, isRead) != 0 {
		t.Fatal("whole blocks read a base")
	}
	mark = len(c.passes[0])
	w = c.write(pg0, ch, 10*core.BlockSize, []byte{9})
	p := c.update(c.pgFields(pg0), ch, 3*core.BlockSize+100, nil, core.BlockSize-100+core.BlockSize, nil)
	c.settle()
	if c.reply(w).Status != StatusOK || c.reply(p).Status != StatusOK || c.sinceMark(0, mark, isRead) != 2 {
		t.Fatalf("a never-written block and a punch's edge read %d blocks", c.sinceMark(0, mark, isRead))
	}
	for d := range 3 {
		b10, b3, b4 := c.diskBlock(d, ch, 10), c.diskBlock(d, ch, 3), c.diskBlock(d, ch, 4)
		if b10[0] != 9 || b10[1] != 0 || b3[99] != 0xbb || b3[100] != 0 || b4[0] != 0 {
			t.Fatalf("disk %d: block 10 % x, block 3 % x, block 4 % x", d+1, b10[:2], b3[99:101], b4[:1])
		}
	}
}

// Once warm, a replicated write allocates nothing in any replica's events: the primary's ordering, fan-out, commit and reply, and each member's logging and acknowledgment.
func TestReplicatedWritesAllocateNothing(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	write := func(i int) {
		w := c.write(pg0, ch, uint32(i%8)*core.BlockSize+100, []byte{byte(i)})
		c.until(w)
		if c.reply(w).Status != StatusOK {
			t.Fatal(c.reply(w))
		}
	}
	for i := range 200 {
		write(i)
	}
	c.counting = true
	for i := range 100 {
		write(i)
	}
	if c.mallocs != 0 {
		t.Fatalf("%d heap allocations over 100 writes' events", c.mallocs)
	}
}

// The primary's copy of a base fails its CRC32C: the base comes from another member, and the write merges on that good copy.
func TestARottenBaseIsFetchedFromAnotherMember(t *testing.T) {
	c := newRCluster(t, rtable(4, 2))
	ch := chunkIn(pg0, 4, 0)
	c.writeWhole(pg0, ch, 2, 1, 0xaa)
	e, _ := c.reps[0].Store().Index().Lookup(testPool, ch, 2)
	c.s.Node(0).Disk(0).FlipBit(e.BlockLBA(2), 4000, 1)
	w := c.write(pg0, ch, 2*core.BlockSize, []byte{1})
	next := c.write(pg0, ch, 2*core.BlockSize+1, []byte{2})
	c.settle()
	if c.reply(w).Status != StatusOK || c.reply(next).Status != StatusOK || c.reply(next).Version != c.reply(w).Version+1 {
		t.Fatalf("answered %+v and %+v", c.reply(w), c.reply(next))
	}
	for d := range 3 {
		if got := c.diskBlock(d, ch, 2); got[0] != 1 || got[1] != 2 || got[4000] != 0xaa {
			t.Fatalf("disk %d merged on % x", d+1, []byte{got[0], got[1], got[4000]})
		}
	}
	e1, _ := c.reps[1].Store().Index().Lookup(testPool, ch, 2)
	e2, _ := c.reps[2].Store().Index().Lookup(testPool, ch, 2)
	ep, _ := c.reps[0].Store().Index().Lookup(testPool, ch, 2)
	c.s.Node(0).Disk(0).FlipBit(ep.BlockLBA(2), 4000, 1)
	c.s.Node(1).Disk(0).FlipBit(e1.BlockLBA(2), 4000, 1)
	c.s.Node(2).Disk(0).FlipBit(e2.BlockLBA(2), 4000, 1)
	c.reps[0].Store().cache = nil
	w = c.write(pg0, ch, 2*core.BlockSize+5, []byte{3})
	c.settle()
	if rp := c.reply(w); rp.Status != StatusRefused {
		t.Fatalf("with every copy rotten the write answered %+v", rp)
	}
}
