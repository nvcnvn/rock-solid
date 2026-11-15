package nettest

import (
	"testing"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/loop"
	"github.com/nvcnvn/rock-solid/internal/sim"
	"github.com/nvcnvn/rock-solid/internal/sim/clock"
)

// maxSteps bounds a Run on the simulated network.
const maxSteps = 1_000_000

// SimNet is the simulated network under the suite: node i is the scheduler's node i, its one loop
// loop 0.
type SimNet struct {
	T        testing.TB
	S        *sim.Scheduler
	machines []loop.Machine
	stopped  []bool
}

// NewSimNet makes a simulated network with set, its pools of 64 MiB.
func NewSimNet(t testing.TB, seed uint64, set sim.Settings) *SimNet {
	set.PoolBytes = 64 << 20
	return &SimNet{T: t, S: sim.New(seed, set)}
}

// Start implements Net.
func (n *SimNet) Start(i int, m loop.Machine) core.NodeID {
	if i == len(n.machines) {
		n.machines, n.stopped = append(n.machines, m), append(n.stopped, false)
		n.S.AddNode(sim.DefaultClock, nil, func(nd *sim.Node) {
			if !n.stopped[i] {
				nd.Start(n.machines[i], nil)
			}
		})
		return core.NodeID{Node: uint32(i)}
	}
	n.machines[i], n.stopped[i] = m, false
	if nd := n.S.Node(uint32(i)); nd.Up() {
		nd.Start(m, nil)
	} else {
		n.S.Restart(uint32(i))
	}
	return core.NodeID{Node: uint32(i)}
}

// Stop implements Net, as a crash that leaves what the process sent with its kernel; the node
// comes back with no loop, as a host whose process is down does.
func (n *SimNet) Stop(i int) {
	n.stopped[i] = true
	n.S.CrashRandom(uint32(i))
}

// Pool implements Net.
func (n *SimNet) Pool(i int) Pool { return n.S.Node(uint32(i)).Pool() }

// Call implements Net.
func (n *SimNet) Call(i int, f func(ctx *core.Context, out []core.Action) []core.Action) {
	n.S.Node(uint32(i)).Call(core.NodeID{Node: uint32(i)}, f)
}

// Hold implements Net, as a pause of a second that keeps the messages sent to the node.
func (n *SimNet) Hold(i int) { n.S.Pause(uint32(i), clock.PauseFrozen, 1_000_000, false) }

// Run implements Net.
func (n *SimNet) Run(done func() bool) bool {
	defer n.contract()
	for range maxSteps {
		if done != nil && done() {
			return true
		}
		if !n.S.Step() {
			return done == nil || done()
		}
	}
	return false
}

// Sleep implements Net.
func (n *SimNet) Sleep(d core.Tick) {
	n.Run(nil)
	n.S.Advance(n.S.Time() + d)
}

// Backoff implements Net: the simulated network dials again at once.
func (n *SimNet) Backoff() core.Tick { return 0 }

func (n *SimNet) contract() {
	if err := n.S.DriverContract(); err != nil {
		n.T.Fatal(err)
	}
}
