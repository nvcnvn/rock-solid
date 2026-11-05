package chunkstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/trace"
)

// conformBegin starts a conformance trace: the table now is the spec's Init, so interval 1's bootstrap peering before this record is no part of it.
func (c *rcluster) conformBegin() {
	c.conforming = true
	c.noteTable(&c.table)
}

func (c *rcluster) noteTable(next *Table) {
	if !c.conforming {
		return
	}
	var prev Table
	if c.noted {
		prev = c.table
	}
	c.noted = true
	for _, r := range TableRecords(nil, &prev, next) {
		c.s.Node(c.client.Node).Output(c.client, r)
	}
}

// noteSends records the client writes in acts.
func (c *rcluster) noteSends(acts []core.Action) {
	if !c.conforming {
		return
	}
	for i := range acts {
		a := &acts[i]
		if a.Kind != core.ActNetSend || a.Msg.Len < headSize || Kind(a.Msg.Body[1]) != KindWrite {
			continue
		}
		data := 0
		if a.Msg.Buf != core.NoBuf {
			data = len(c.s.Node(c.client.Node).Pool().Bytes(a.Msg.Buf))
		}
		var u Update
		if DecodeUpdate(a.Msg.Inline(), data, &u) != nil {
			continue
		}
		c.s.Node(c.client.Node).Output(c.client, trace.Record{Name: trace.OutSend, Fields: [trace.MaxFields]uint64{
			uint64(u.ID), u.Chunk.Hash(), pgField(u.PG.PG), uint64(u.Off)<<32 | uint64(u.Len), uint64(u.Incarnation)}})
	}
}

// conformActor records what a client actor sends.
type conformActor struct {
	sim.Actor
	c *rcluster
}

func (a conformActor) Next(ctx *core.Context, acts []core.Action) []core.Action {
	n := len(acts)
	acts = a.Actor.Next(ctx, acts)
	a.c.noteSends(acts[n:])
	return acts
}

// conformWrite keeps a run's trace for scripts/conform.sh.
func conformWrite(t *testing.T, name string, c *rcluster) {
	t.Helper()
	dir := os.Getenv("CONFORM_DIR")
	if dir == "" {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, name+".trace"), c.s.Trace(), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A clean run, one PG on three disks: writes, then a primary change, then writes under the new primary.
func TestConformScriptedPrimaryChange(t *testing.T) {
	c := newRCluster(t, rtable(1, 2))
	c.conformBegin()
	ch := chunkIn(pg0, 1, 0)
	block := make([]byte, core.BlockSize)
	for i := range 2 {
		block[0] = byte(i + 1)
		c.until(c.write(pg0, ch, 0, block))
	}
	conformWrite(t, "scripted-clean", c)
	next := c.table
	next.Epoch++
	next.Pools = []PoolTable{next.Pools[0]}
	next.Pools[0].PGs = []PGEntry{next.Pools[0].PGs[0]}
	e := &next.Pools[0].PGs[0]
	e.Past = append(e.Past, PastInterval{Interval: e.Interval, MinLive: 2, Members: e.Members})
	e.Interval++
	e.Members = []Member{e.Members[1], e.Members[0], e.Members[2]}
	c.publish(next, true)
	for i := range 2 {
		block[0] = byte(i + 3)
		c.until(c.write(pg0, ch, 0, block))
	}
	c.settle()
	conformWrite(t, "scripted-primary-change", c)
}
