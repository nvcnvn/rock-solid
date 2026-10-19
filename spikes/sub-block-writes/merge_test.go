package subblock

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// The merge model: one chunk of nblk 4 KiB blocks, written by clients with byte-range writes
// (sub-block, unaligned, some crossing a block boundary). A strategy turns each write into
// whole blocks somewhere; the store keeps only whole blocks, as the record format does. The
// oracle applies every write that was acknowledged, as a byte range, in the order the primary
// gave it, and the committed image must equal it after every step.

const (
	bs   = 4096
	nblk = 4
)

type strategy int

const (
	primaryOverlay   strategy = iota // primary merges against committed + pending, in version order
	primaryCommitted                 // primary merges against the committed image only
	clientRMW                        // client reads committed blocks, merges, sends whole blocks
	clientCAS                        // as clientRMW, with compare-and-set on the chunk version
)

func (s strategy) String() string {
	return [...]string{"primary/overlay", "primary/committed", "client/RMW", "client/RMW+CAS"}[s]
}

type dropMode int

const (
	dropSuffix dropMode = iota // peering drops the newest pending versions only
	dropAny                    // peering drops any subset of pending versions
)

type failover int

const (
	noFailover failover = iota
	recoverFirst
	serveFirst // new primary merges before it holds a version peering kept
)

type mcfg struct {
	strat    strategy
	clients  int
	depth    int // writes a client keeps outstanding
	failProb float64
	drop     dropMode
	fo       failover
	steps    int
	maxLen   int
}

type pend struct {
	ver    uint64
	off, n int
	data   []byte
	blocks map[int][]byte
	client int
}

type op struct {
	off, n int
	data   []byte
	read   map[int][]byte // client-side merge: blocks read, nil until read
	expect uint64
}

type model struct {
	cfg       mcfg
	rng       *rand.Rand
	committed []byte
	oracle    []byte
	ver       uint64 // newest version ordered (committed or pending)
	cver      uint64 // newest committed version
	pending   []pend
	recovery  map[int][]byte // blocks a recovery must still write (serveFirst)
	outst     [][]*op
	acked     int
	retries   int
	lostBytes int
	failures  int
	firstBad  int
}

func newModel(c mcfg, seed uint64) *model {
	m := &model{cfg: c, rng: rand.New(rand.NewPCG(seed, 0x5eed)), committed: make([]byte, nblk*bs), oracle: make([]byte, nblk*bs), firstBad: -1}
	m.outst = make([][]*op, c.clients)
	return m
}

func (m *model) newestBlock(b int) []byte {
	for i := len(m.pending) - 1; i >= 0; i-- {
		if blk, ok := m.pending[i].blocks[b]; ok {
			return blk
		}
	}
	return m.committed[b*bs : (b+1)*bs]
}

func touched(off, n int) (int, int) { return off / bs, (off + n - 1) / bs }

func (m *model) order(client int, o *op) bool {
	if m.cfg.strat == clientCAS && o.expect != m.ver {
		m.retries++
		return false
	}
	p := pend{off: o.off, n: o.n, data: o.data, blocks: map[int][]byte{}, client: client}
	first, last := touched(o.off, o.n)
	for b := first; b <= last; b++ {
		var base []byte
		switch m.cfg.strat {
		case primaryOverlay:
			base = m.newestBlock(b)
		case primaryCommitted:
			base = m.committed[b*bs : (b+1)*bs]
		case clientRMW, clientCAS:
			base = o.read[b]
		}
		blk := bytes.Clone(base)
		lo, hi := max(o.off, b*bs), min(o.off+o.n, (b+1)*bs)
		copy(blk[lo-b*bs:hi-b*bs], o.data[lo-o.off:hi-o.off])
		p.blocks[b] = blk
	}
	m.ver++
	p.ver = m.ver
	m.pending = append(m.pending, p)
	return true
}

func (m *model) commitOldest() {
	p := m.pending[0]
	m.pending = m.pending[1:]
	for b, blk := range p.blocks {
		copy(m.committed[b*bs:], blk)
	}
	copy(m.oracle[p.off:], p.data)
	m.cver = p.ver
	m.acked++
}

func (m *model) fail() {
	m.failures++
	n := len(m.pending)
	keep := make([]bool, n)
	switch m.cfg.drop {
	case dropSuffix:
		k := m.rng.IntN(n + 1)
		for i := range k {
			keep[i] = true
		}
	case dropAny:
		for i := range keep {
			keep[i] = m.rng.IntN(2) == 0
		}
	}
	var kept []pend
	for i, p := range m.pending {
		if keep[i] {
			kept = append(kept, p)
		}
	}
	before := bytes.Clone(m.committed)
	for _, p := range kept {
		for b, blk := range p.blocks {
			copy(m.committed[b*bs:], blk)
		}
		copy(m.oracle[p.off:], p.data)
		m.cver = p.ver
		m.acked++
	}
	m.pending = nil
	if m.cfg.fo == serveFirst && len(kept) > 0 {
		m.recovery = map[int][]byte{}
		for b := range nblk {
			if !bytes.Equal(before[b*bs:(b+1)*bs], m.committed[b*bs:(b+1)*bs]) {
				m.recovery[b] = bytes.Clone(m.committed[b*bs : (b+1)*bs])
				copy(m.committed[b*bs:], before[b*bs:(b+1)*bs])
			}
		}
	}
	for c := range m.outst {
		m.outst[c] = nil
	}
}

func (m *model) recover() {
	for b := range nblk {
		if blk, ok := m.recovery[b]; ok {
			copy(m.committed[b*bs:], blk)
		}
	}
	m.recovery = nil
}

func (m *model) step() {
	c := m.cfg
	switch r := m.rng.Float64(); {
	case r < c.failProb && len(m.pending) > 0:
		m.fail()
	case r < 0.45 && len(m.pending) > 0:
		m.commitOldest()
	case m.recovery != nil && r < 0.5:
		m.recover()
	default:
		cl := m.rng.IntN(c.clients)
		if len(m.outst[cl]) > 0 && (c.strat == clientRMW || c.strat == clientCAS) {
			o := m.outst[cl][0]
			if o.read == nil {
				o.read = map[int][]byte{}
				first, last := touched(o.off, o.n)
				for b := first; b <= last; b++ {
					o.read[b] = bytes.Clone(m.committed[b*bs : (b+1)*bs])
				}
				o.expect = m.cver
				return
			}
			m.outst[cl] = m.outst[cl][1:]
			if !m.order(cl, o) {
				o.read = nil
				m.outst[cl] = append([]*op{o}, m.outst[cl]...)
			}
			return
		}
		if len(m.outst[cl]) >= c.depth && c.strat != primaryOverlay && c.strat != primaryCommitted {
			return
		}
		inflight := 0
		for _, p := range m.pending {
			if p.client == cl {
				inflight++
			}
		}
		if inflight >= c.depth {
			return
		}
		n := 1 + m.rng.IntN(c.maxLen)
		off := m.rng.IntN(nblk*bs - n + 1)
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(1 + m.rng.IntN(255))
		}
		o := &op{off: off, n: n, data: data}
		if c.strat == primaryOverlay || c.strat == primaryCommitted {
			m.order(cl, o)
			return
		}
		m.outst[cl] = append(m.outst[cl], o)
	}
}

func (m *model) check(step int) {
	if m.recovery != nil {
		return
	}
	if d := diffBytes(m.committed, m.oracle); d > 0 && m.firstBad < 0 {
		m.firstBad, m.lostBytes = step, d
	}
}

func diffBytes(a, b []byte) int {
	n := 0
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func runMerge(t *testing.T, id, desc string, c mcfg, seeds int, wantBad bool) {
	bad, acked, retries, failures := 0, 0, 0, 0
	example := ""
	for s := range seeds {
		m := newModel(c, uint64(s))
		for i := range c.steps {
			m.step()
			m.check(i)
		}
		for len(m.pending) > 0 {
			m.commitOldest()
		}
		if m.recovery != nil {
			m.recover()
		}
		m.check(c.steps)
		acked += m.acked
		retries += m.retries
		failures += m.failures
		if m.firstBad >= 0 {
			bad++
			if example == "" {
				example = fmt.Sprintf("seed %d step %d: %d bytes differ", s, m.firstBad, m.lostBytes)
			}
		}
	}
	tag := "[OK ]"
	if (bad > 0) != wantBad {
		tag = "[FAIL]"
	}
	want := "0 seeds wrong"
	if wantBad {
		want = "some seeds wrong"
	}
	extra := ""
	if c.strat == clientCAS {
		extra = fmt.Sprintf(", %.3f CAS retries per acknowledged write", float64(retries)/float64(acked))
	}
	if c.failProb > 0 {
		extra += fmt.Sprintf(", %d failovers", failures)
	}
	t.Logf("%s %s %s (%s): got=%d of %d seeds wrong want=%s; %d writes acknowledged%s %s", tag, id, desc, c.strat, bad, seeds, want, acked, extra, example)
	if tag == "[FAIL]" {
		t.Fail()
	}
}

func TestMerge(t *testing.T) {
	base := mcfg{clients: 2, depth: 1, steps: 400, maxLen: 300}
	with := func(f func(*mcfg)) mcfg { c := base; f(&c); return c }
	runMerge(t, "M1", "two clients, one write each in flight", with(func(c *mcfg) { c.strat = primaryOverlay }), 500, false)
	runMerge(t, "M2", "one client, 4 pipelined writes", with(func(c *mcfg) { c.strat = primaryCommitted; c.clients = 1; c.depth = 4 }), 500, true)
	runMerge(t, "M3", "two clients, one write each in flight", with(func(c *mcfg) { c.strat = primaryCommitted }), 500, true)
	runMerge(t, "M4", "two clients, one write each in flight", with(func(c *mcfg) { c.strat = clientRMW }), 500, true)
	runMerge(t, "M5", "two clients, one write each in flight", with(func(c *mcfg) { c.strat = clientCAS }), 500, false)
	runMerge(t, "M5b", "four clients, one write each in flight", with(func(c *mcfg) { c.strat = clientCAS; c.clients = 4 }), 500, false)
	runMerge(t, "M6", "two clients, 8 pipelined each, failovers dropping a suffix", with(func(c *mcfg) {
		c.strat, c.depth, c.failProb, c.drop, c.fo = primaryOverlay, 8, 0.02, dropSuffix, recoverFirst
	}), 500, false)
	runMerge(t, "M7", "two clients, 8 pipelined each, failovers dropping any subset", with(func(c *mcfg) {
		c.strat, c.depth, c.failProb, c.drop, c.fo = primaryOverlay, 8, 0.02, dropAny, recoverFirst
	}), 500, true)
	runMerge(t, "M8", "failovers whose new primary merges before recovering a kept version", with(func(c *mcfg) {
		c.strat, c.depth, c.failProb, c.drop, c.fo = primaryOverlay, 8, 0.02, dropSuffix, serveFirst
	}), 500, true)
	runMerge(t, "M9", "two clients, writes up to 3 blocks long, 8 pipelined, failovers", with(func(c *mcfg) {
		c.strat, c.depth, c.failProb, c.drop, c.fo, c.maxLen = primaryOverlay, 8, 0.02, dropSuffix, recoverFirst, 3*bs
	}), 500, false)
}
