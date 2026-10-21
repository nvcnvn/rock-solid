package toy

import (
	"encoding/binary"
	"fmt"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/sim"
)

// Nodes is the number of nodes in a cluster: a primary and two replicas.
const Nodes = 3

// Cluster is the protocol on three simulated nodes, node 0 the primary, whose client writes values 1 to ops, two at most in flight, into a history that outlives crashes.
type Cluster struct {
	s       *sim.Scheduler
	history *sim.History
	nodes   [Nodes]*Machine
	boots   [Nodes]uint64
	next    uint64
	ops     uint64
	acked   []uint64
	checked int
}

// NewCluster makes a cluster on a scheduler seeded with seed and set, with bug planted in every node.
func NewCluster(seed uint64, set sim.Settings, ops int, bug Bug) *Cluster {
	s := sim.New(seed, set)
	c := &Cluster{s: s, history: sim.NewHistory(s, ops), next: 1, ops: uint64(ops)}
	disk := []sim.DiskSpec{{Blocks: uint64(ops) + 1, Settings: sim.DefaultDiskSettings()}}
	for id := range uint32(Nodes) {
		s.AddNode(sim.DefaultClock, disk, func(n *sim.Node) {
			c.boots[id]++
			cfg := Config{Self: core.NodeID{Node: id}, Epoch: c.boots[id], Bug: bug}
			if id == 0 {
				cfg.Peers = []core.NodeID{{Node: 1}, {Node: 2}}
				cfg.Ack = c.ack
			}
			m := New(n.Fork(), cfg)
			c.nodes[id] = m
			loop := n.Start(m, n.Disk(0))
			if id == 0 {
				n.AddActor(loop, &client{c: c, m: m})
			}
		})
	}
	return c
}

// Scheduler is the cluster's scheduler.
func (c *Cluster) Scheduler() *sim.Scheduler { return c.s }

// History is the client's history.
func (c *Cluster) History() *sim.History { return c.history }

// Node is the machine of node id's current incarnation.
func (c *Cluster) Node(id int) *Machine { return c.nodes[id] }

// Boots is the number of times each node has started.
func (c *Cluster) Boots() [Nodes]uint64 { return c.boots }

// Acked is the number of values acknowledged.
func (c *Cluster) Acked() int { return len(c.acked) }

func (c *Cluster) ack(value uint64) {
	c.history.Complete(int(value-1), [sim.OpWords]uint64{})
	c.acked = append(c.acked, value)
}

// LostWriteError reports an acknowledged value fewer than two disks hold durably, which a crash of every node and the loss of one disk could lose.
type LostWriteError struct {
	Value  uint64
	Step   uint64
	Copies int
}

// Error names the value, the step and the copies.
func (e *LostWriteError) Error() string {
	return fmt.Sprintf("value %d acknowledged by step %d has %d durable copies, not 2", e.Value, e.Step, e.Copies)
}

// Check reports the first value acknowledged since the last Check that fewer than two disks hold durably.
func (c *Cluster) Check() error {
	// Each value goes to a block of its own and the cluster's disks have no faults, so a value
	// once durable stays so and is checked only once; disk faults would need every value rechecked.
	for ; c.checked < len(c.acked); c.checked++ {
		v := c.acked[c.checked]
		copies := 0
		for id := range uint32(Nodes) {
			if b := c.s.Node(id).Disk(0).Durable(core.LBA(v)); b != nil && binary.LittleEndian.Uint64(b) == v {
				copies++
			}
		}
		if copies < 2 {
			return &LostWriteError{Value: v, Step: c.s.Steps(), Copies: copies}
		}
	}
	return nil
}

// Run takes up to max steps, checking after each, and stops at the first violation.
func (c *Cluster) Run(max int) error {
	for range max {
		if !c.s.Step() {
			return nil
		}
		if err := c.Check(); err != nil {
			return err
		}
	}
	return nil
}

type client struct {
	c *Cluster
	m *Machine
}

func (cl *client) Ready() bool { return cl.c.next <= cl.c.ops && cl.m.Pending() < 2 }

func (cl *client) Next(ctx *core.Context, out []core.Action) []core.Action {
	v := cl.c.next
	cl.c.next++
	cl.c.history.Invoke(0, [sim.OpWords]uint64{v})
	return cl.m.Propose(ctx, v, out)
}
