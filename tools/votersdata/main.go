// Command votersdata turns a simulator trace of a Raft group into the data module the voter
// replacement model's trace spec, models/voters/trace_voters.tla, reads: the run's constants and
// one line per Raft step. It decides no protocol step. Its judgement is in the mapping alone: a
// command entry is no entry of the spec's log, a term no replica led in is none of its terms
// unless a halted leader's hand-off failed in it, state names which change of a replica's
// RaftState record each line stands for, replicate places an append at its send, and support
// writes the lapses a fortification's lines need.
//
//	votersdata [-module trace_data] run.trace > trace_data.tla
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"os"
	"slices"
	"strings"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// conf is a configuration as bit sets of replica IDs: incoming voters, outgoing voters, learners.
type conf struct{ in, out, lrn uint64 }

type entry struct {
	term, kind, change uint64
}

// deadFor is the pause, in ticks, the replacement run stands a node still for good with.
const deadFor = 100_000_000

// simFortified and simGone are the message types raftshard's harness writes for a follower's
// answer to a fortification and a node's answer to an ask.
const (
	simFortified = 1<<9 | 2
	simGone      = 1<<9 | 6
)

const (
	kindCommand = 0
	kindEmpty   = 1
	kindConf    = 2
)

// apply is the configuration a ConfChangeV2 entry leads to from c.
func (e entry) apply(c conf) conf {
	n := e.change & 0xff
	if n == 0 {
		c.out = 0
		return c
	}
	if n > 1 {
		c.out = c.in
	}
	for i := range n {
		ch := e.change >> (8 * (i + 1)) & 0xff
		b := uint64(1) << (ch & 0x3f)
		switch ch >> 6 {
		case 0:
			c.in, c.lrn = c.in|b, c.lrn&^b
		case 1:
			c.in, c.lrn = c.in&^b, c.lrn&^b
		case 3:
			c.in, c.lrn = c.in&^b, c.lrn|b
		default:
			fatal("change type %d", ch>>6)
		}
	}
	return c
}

type replica struct {
	ents       map[uint64]entry
	last, base uint64
	baseConf   conf
	state      [trace.MaxFields]uint64
	// noted is the spec terms of the log at the last RaftState.
	noted []int
	// hist is each change of whether the replica leads, its term and its applied configuration's
	// members, from the line it takes effect at, and lastLine the key of the replica's last line
	// that is no stutter.
	hist     []sent
	lastLine key
	// applyAt is the place in lines of the replica's last apply line, or -1, and haltAt the spec
	// index of its halt line, or 0.
	applyAt int
	haltAt  int
	// fresh is whether the next RaftState is a restart's, and props the configuration changes
	// the replica's rounds logged since its last RaftState.
	fresh bool
	props []uint64
	// peer is the replica that sent the event of the pass the replica's records are in, and pass
	// that pass; nowPass is the pass of the last MsgTimeoutNow it took, and nowTerm the term that
	// MsgTimeoutNow had it campaign in.
	peer, pass       uint64
	nowPass, nowTerm uint64
	down             bool
	// forced is whether a test asked a leader to hand the replica its leadership, which the
	// replica's next campaign on a MsgTimeoutNow carries out.
	forced bool
	// sup is the leader the replica supports, fort the peers it counts as leader, both as its last
	// RaftSupport had them, and gonePass and fortPass the passes of the last answer to an ask and
	// to a fortification it took.
	sup, fort, gonePass, fortPass uint64
}

// key orders the lines: a is the line's place among those written in record order, and a line
// placed before line a takes b below 0, in the order placed.
type key struct{ a, b int }

func (k key) less(o key) bool { return k.a < o.a || k.a == o.a && k.b < o.b }

// line is a line of the data, id the replica it is of, 0 for a stutter, and lead, lg and c a
// replicate line's leader, log and commit.
type line struct {
	k    key
	s    string
	id   int
	lead uint64
	lg   []int
	c    int
}

// sent is whether a replica leads, in which spec term, and to which members of the configuration
// it applied, from line at on.
type sent struct {
	at      key
	leads   bool
	term    int
	members uint64
}

type win struct {
	term int
	at   key
}

type gen struct {
	reps    []*replica
	chosen  map[uint64]entry
	elected []uint64
	// failed is each term of elected no replica led, a halted leader's failed hand-off, and
	// leaders the replica that led each term of elected a replica led.
	failed  map[uint64]bool
	leaders map[uint64]uint64
	// wins is the spec term and key of each elect or transfer line.
	wins    []win
	lines   []line
	placed  int
	boot    conf
	plan    uint64
	crashes int
	halts   int
	deaths  int
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "votersdata: "+format+"\n", args...)
	os.Exit(1)
}

// handOffFails reports whether a RaftState with fields f, after one with flags was, is a halted
// leader stepping down, on the term its hand-off's target failed an election in when no replica
// led that term.
func handOffFails(was uint64, f [trace.MaxFields]uint64) bool {
	return was&1 != 0 && f[4]&1 == 0 && f[4]&4 != 0
}

// term is the spec's term for code term t: how many terms up to t a replica led in, or failed a
// halted leader's hand-off in.
func (g *gen) term(t uint64) int {
	n, _ := slices.BinarySearch(g.elected, t+1)
	return n
}

// led reports whether a replica led code term t.
func (g *gen) led(t uint64) bool {
	_, ok := slices.BinarySearch(g.elected, t)
	return ok && !g.failed[t]
}

// at is replica r's entry at index k.
func (g *gen) at(r *replica, k uint64) (entry, bool) {
	if k > r.base {
		e, ok := r.ents[k]
		return e, ok
	}
	e, ok := g.chosen[k]
	return e, ok
}

// index is the spec's index for r's code index k: the entries up to k that are not commands,
// past the boot snapshot at index 1.
func (g *gen) index(r *replica, k uint64) int {
	n := 0
	for i := uint64(2); i <= k && i <= r.last; i++ {
		e, ok := g.at(r, i)
		if !ok {
			fatal("replica has no entry %d below its last, %d", i, r.last)
		}
		if e.kind != kindCommand {
			n++
		}
	}
	return n
}

// terms is the spec's term of each entry of r's log that is not a command, past the boot
// snapshot at index 1.
func (g *gen) terms(r *replica) []int {
	var ts []int
	for i := uint64(2); i <= r.last; i++ {
		e, ok := g.at(r, i)
		if !ok {
			fatal("replica has no entry %d below its last, %d", i, r.last)
		}
		if e.kind != kindCommand {
			ts = append(ts, g.term(e.term))
		}
	}
	return ts
}

func seq(ts []int) string {
	s := make([]string, len(ts))
	for i, t := range ts {
		s[i] = fmt.Sprint(t)
	}
	return "<<" + strings.Join(s, ", ") + ">>"
}

// confAt is r's configuration at index k.
func (g *gen) confAt(r *replica, k uint64) conf {
	c := g.boot
	if r.base > 1 {
		c = r.baseConf
	}
	for i := max(r.base, 1) + 1; i <= k && i <= r.last; i++ {
		if e, _ := g.at(r, i); e.kind == kindConf {
			c = e.apply(c)
		}
	}
	return c
}

func set(b uint64) string {
	var ids []string
	for b != 0 {
		id := bits.TrailingZeros64(b)
		ids = append(ids, fmt.Sprint(id))
		b &^= 1 << id
	}
	return "{" + strings.Join(ids, ", ") + "}"
}

func (c conf) tla() string {
	return fmt.Sprintf("[in |-> %s, out |-> %s, lrn |-> %s]", set(c.in), set(c.out), set(c.lrn))
}

func (g *gen) next() key { return key{len(g.lines) - g.placed, 0} }

// line writes a line of replica id at the end.
func (g *gen) line(id int, format string, args ...any) {
	g.stutter(format, args...)
	g.lines[len(g.lines)-1].id = id
	g.reps[id-1].lastLine = g.lines[len(g.lines)-1].k
}

// stutter writes a line no spec state differs at, which a line placed before it or a halt that
// takes the place of an earlier apply passes.
func (g *gen) stutter(format string, args ...any) {
	g.lines = append(g.lines, line{k: g.next(), s: fmt.Sprintf(format, args...)})
}

// note records whether replica id leads, its term and its applied configuration's members, from
// the lines its record writes on.
func (g *gen) note(id int, at key) {
	r := g.reps[id-1]
	c := g.confAt(r, r.state[3])
	s := sent{at: at, leads: r.state[4]&1 != 0 && !r.down, term: g.term(r.state[0]), members: c.in | c.out | c.lrn}
	if s.members&(1<<id) == 0 {
		// A replica past its removal keeps its joint configuration.
		s.members = ^uint64(0)
	}
	if n := len(r.hist); n > 0 && r.hist[n-1] == (sent{r.hist[n-1].at, s.leads, s.term, s.members}) {
		return
	}
	r.hist = append(r.hist, s)
}

// replicate writes follower id's line for an append from lead in code term t that left it holding
// lg. An append is the spec's step when its leader sends it: one that arrives once its leader
// stopped leading the term, or applied a configuration without the follower, is placed before
// the line where that happened, unless a line of the follower's is later that the append's
// earlier place could fail (passes).
func (g *gen) replicate(id int, lead, t uint64, lg []int, c int) {
	s := fmt.Sprintf(`[k |-> "replicate", i |-> %d, j |-> %d, lg |-> %s, c |-> %d]`, lead, id, seq(lg), c)
	r, l := g.reps[id-1], g.reps[lead-1]
	sends := func(h sent) bool { return h.leads && h.term == g.term(t) && h.members&(1<<id) != 0 }
	if n := len(l.hist); n == 0 || !sends(l.hist[n-1]) {
		for i := n - 2; i >= 0; i-- {
			end := l.hist[i+1].at
			if end.b != 0 || !r.lastLine.less(end) && !g.passes(id, end, lead, lg, c) {
				break
			}
			if sends(l.hist[i]) {
				g.placed++
				k := key{end.a, g.placed - 1<<40}
				g.lines = append(g.lines, line{k, s, id, lead, lg, c})
				if r.lastLine.less(k) {
					r.lastLine = k
				}
				return
			}
		}
	}
	g.line(id, "%s", s)
	n := &g.lines[len(g.lines)-1]
	n.lead, n.lg, n.c = lead, lg, c
}

// passes reports whether every line of follower id from end on is an apply, or a replicate of
// lead's whose log is a prefix of lg and whose commit is at most c: lines an append from lead
// leaving the follower lg and c, taken before end, still holds, since the follower's later
// appends from lead move none of it and its applies are checked as at most the spec's.
func (g *gen) passes(id int, end key, lead uint64, lg []int, c int) bool {
	for _, l := range g.lines {
		if l.id != id || l.k.less(end) {
			continue
		}
		if !strings.HasPrefix(l.s, `[k |-> "apply"`) &&
			(l.lead != lead || len(l.lg) > len(lg) || !slices.Equal(l.lg, lg[:len(l.lg)]) || l.c > c) {
			return false
		}
	}
	return true
}

// sender is the leader and code term of an append a follower took as its term went from from
// to to: the latest of those terms whose leader led it by now.
func (g *gen) sender(from, to uint64) (uint64, uint64) {
	for t := to; t >= max(from, 1); t-- {
		if l := g.leaders[t]; l != 0 && slices.ContainsFunc(g.reps[l-1].hist, func(h sent) bool { return h.leads && h.term == g.term(t) }) {
			return l, t
		}
	}
	return 0, to
}

// elect writes replica id's line s for its win of spec term t. etcd-raft wins on the last vote's
// answer, which can arrive after a voter that gave it won a later term: the win is placed before
// the first win of a later term, unless a line of the replica's is later.
func (g *gen) elect(id int, s string, t int) {
	r := g.reps[id-1]
	var first *win
	for i := range g.wins {
		if w := &g.wins[i]; w.term > t && (first == nil || w.at.less(first.at)) {
			first = w
		}
	}
	if first == nil || first.at.b != 0 || !r.lastLine.less(first.at) {
		g.line(id, "%s", s)
	} else {
		g.placed++
		k := key{first.at.a, g.placed - 1<<40}
		g.lines = append(g.lines, line{k: k, s: s, id: id})
		r.lastLine = k
	}
	g.wins = append(g.wins, win{t, r.lastLine})
}

// state writes the lines of replica id's RaftState record f: a restart, then a lead taken, its
// log or commit index moved by a leader, its commit index moved as leader, the entries it
// applied, or halted at, the configuration changes it proposed, as etcd-raft proposes the leave
// on applying a joint configuration, and its removal learned or acted on: learned by applying
// when the configuration it applied holds it and the one it knows committed does not, from a
// notice otherwise.
func (g *gen) state(id int, pass uint64, f [trace.MaxFields]uint64) {
	r := g.reps[id-1]
	defer g.note(id, g.next())
	prev := r.state
	r.state = f
	lg := g.terms(r)
	term, lead, commit, applied, flags := f[0], f[1], f[2], f[3], f[4]
	if commit < prev[2] || applied < prev[3] {
		fatal("replica %d's commit and applied indexes dropped outside a restart, from %d and %d to %d and %d", id, prev[2], prev[3], commit, applied)
	}
	leads := flags&1 != 0
	appTerm := term
	if !leads && lead == 0 && !slices.Equal(lg, r.noted) {
		// A follower that took an append and then started a pre-vote, or took a later term's
		// vote, in the same pass knows no leader, yet holds the entries a leader sent.
		if lead, appTerm = g.sender(prev[0], term); lead == 0 {
			fatal("replica %d's log moved with no leader of terms %d to %d", id, prev[0], term)
		}
	}
	if pass == r.nowPass && term > prev[0] {
		r.nowTerm = term
	}
	if commit > prev[2] {
		for k := prev[2] + 1; k <= commit && k <= r.last; k++ {
			if e, ok := g.at(r, k); ok {
				g.chosen[k] = e
			}
		}
	}
	if r.fresh {
		r.fresh = false
		a := g.index(r, applied)
		if r.haltAt == 0 || a < r.haltAt {
			g.line(id, `[k |-> "restart", r |-> %d, c |-> %d, a |-> %d]`, id, g.index(r, commit), a)
			return
		}
		// A halted replica restarts below the entry it cannot decode, and halts again applying it.
		g.line(id, `[k |-> "restart", r |-> %d, c |-> %d, a |-> %d]`, id, g.index(r, commit), r.haltAt-1)
		g.line(id, `[k |-> "apply", r |-> %d, a |-> %d]`, id, a)
		return
	}
	if leads && (prev[4]&1 == 0 || prev[0] != term) {
		kind := "elect"
		if term == r.nowTerm && !r.forced {
			kind = "transfer"
		}
		if term == r.nowTerm {
			r.forced = false
		}
		g.elect(id, fmt.Sprintf(`[k |-> "%s", i |-> %d, t |-> %d]`, kind, id, g.term(term)), g.term(term))
	}
	switch {
	case leads && commit > prev[2] && g.index(r, commit) <= g.index(r, prev[2]):
		g.stutter(`[k |-> "commit", i |-> %d, c |-> %d]`, id, g.index(r, commit))
	case leads && commit > prev[2]:
		g.line(id, `[k |-> "commit", i |-> %d, c |-> %d]`, id, g.index(r, commit))
	case !leads && lead != 0 && (commit != prev[2] || !slices.Equal(lg, r.noted)):
		g.replicate(id, lead, appTerm, lg, g.index(r, commit))
	case term != prev[0] && handOffFails(prev[4], f) && g.failed[term]:
		g.line(id, `[k |-> "handofffails", l |-> %d, t |-> %d]`, id, g.term(term))
	case term != prev[0]:
		g.stutter(`[k |-> "term", r |-> %d]`, id)
	}
	halts := flags&4 != 0 && prev[4]&4 == 0
	if halts {
		g.halts++
	}
	moved := g.index(r, applied) > g.index(r, prev[3])
	switch {
	case moved && halts:
		r.haltAt = g.index(r, applied)
		g.line(id, `[k |-> "halt", r |-> %d, a |-> %d]`, id, r.haltAt)
	case applied > prev[3] && !moved:
		g.stutter(`[k |-> "apply", r |-> %d, a |-> %d]`, id, g.index(r, applied))
	case applied > prev[3]:
		g.line(id, `[k |-> "apply", r |-> %d, a |-> %d]`, id, g.index(r, applied))
		r.applyAt = len(g.lines) - 1
	}
	for _, k := range r.props {
		if !leads {
			break
		}
		c, was := g.confAt(r, k), g.confAt(r, k-1)
		kind := "addlearner"
		switch {
		case c.out != 0:
			kind = "swap"
		case was.out != 0:
			kind = "leave"
		}
		g.line(id, `[k |-> "%s", i |-> %d, cfg |-> %s]`, kind, id, c.tla())
	}
	r.props = r.props[:0]
	r.noted = lg
	if flags&2 != 0 && prev[4]&2 == 0 {
		if was, c := g.confAt(r, applied), g.confAt(r, commit); (was.in|was.out|was.lrn)&^(c.in|c.out|c.lrn)&(1<<id) != 0 {
			g.line(id, `[k |-> "removed", r |-> %d]`, id)
		} else {
			g.line(id, `[k |-> "notice", r |-> %d, p |-> %d]`, id, r.peer)
		}
	}
	if halts && !moved {
		// A test halts a replica with no entry it failed to apply, which meta's halt needs, or
		// past a command, which moves no spec index:
		// the halt is the replica's last apply, unless a line of the replica's is later.
		r.haltAt = g.index(r, applied)
		h := fmt.Sprintf(`[k |-> "halt", r |-> %d, a |-> %d]`, id, r.haltAt)
		if r.applyAt >= 0 && g.lines[r.applyAt].k == r.lastLine {
			g.lines[r.applyAt].s = h
		} else {
			g.line(id, "%s", h)
		}
	}
	if flags&8 != 0 && prev[4]&8 == 0 {
		g.line(id, `[k |-> "delete", r |-> %d]`, id)
	}
}

// support writes the lines of replica id's RaftSupport record f: its support of a leader ended,
// by the leader's node's answer to its ask when the pass took one, and by a lapse when it ended in
// the support's term, or on a vote the support lets through in a term no replica led, as the spec
// has no failed election, or after a test's forced hand-off, which is the spec's Elect. Any other
// end moved the replica's term, and the Run or Replicate of the term's leader ends the spec's
// support, which fails where the code's replica voted while it supported a leader. And its count of fortified voters of the configuration it applied grown by an answer the pass took,
// a Fortify, then a lapse of each replica's support the code's
// lacks, since Fortify has every voter of the term support the leader. They are written as
// stutters, as no append placed before them reads what they move, and a test's halt that takes
// the place of an earlier apply goes before them.
func (g *gen) support(id int, pass uint64, f [trace.MaxFields]uint64) {
	r := g.reps[id-1]
	lead, fort := f[0], f[1]
	if r.sup != 0 && lead != r.sup {
		switch {
		case r.gonePass == pass:
			g.stutter(`[k |-> "askleader", r |-> %d, l |-> %d]`, id, r.sup)
		case f[2] == trace.SupportInTerm || f[2] == trace.SupportForced || f[2] == trace.SupportVote && !g.led(r.state[0]):
			g.stutter(`[k |-> "lapse", j |-> %d, i |-> %d]`, id, r.sup)
		}
	}
	r.sup = lead
	c := g.confAt(r, r.state[3])
	grown := fort &^ r.fort & (c.in | c.out)
	r.fort = fort
	if grown == 0 || r.fortPass != pass {
		return
	}
	g.stutter(`[k |-> "fortify", i |-> %d, f |-> %s]`, id, set(grown))
	for j, o := range g.reps {
		if j+1 != id && o.sup != uint64(id) {
			g.stutter(`[k |-> "lapse", j |-> %d, i |-> %d]`, j+1, id)
		}
	}
}

func (g *gen) run(recs []trace.Record) {
	for _, rec := range recs {
		id := int(rec.Node) + 1
		if id > len(g.reps) {
			continue
		}
		r := g.reps[id-1]
		f := rec.Fields
		switch rec.Name {
		case trace.EvNetRecv:
			r.peer, r.pass = uint64(trace.PeerID(f[0]).Node)+1, rec.Pass
			switch f[1] {
			case uint64(raftpb.MsgTimeoutNow):
				r.nowPass = rec.Pass
			case simFortified:
				r.fortPass = rec.Pass
			case simGone:
				r.gonePass = rec.Pass
			}
		case trace.EvCrash:
			g.crashes++
			at := g.next()
			g.line(id, `[k |-> "crash", r |-> %d]`, id)
			r.down, r.sup, r.fort = true, 0, 0
			g.note(id, at)
		case trace.EvPause:
			if f[2] >= deadFor {
				g.deaths++
				g.line(id, `[k |-> "die", r |-> %d]`, id)
			}
		case trace.EvRestart:
			r.fresh, r.down = true, false
			r.state = [trace.MaxFields]uint64{}
		case trace.OutRaftSnap:
			r.base, r.last = f[0], f[0]
			r.baseConf = conf{f[2], f[3], f[4]}
			clear(r.ents)
		case trace.OutRaftEntry:
			for k := range r.ents {
				if k >= f[0] {
					delete(r.ents, k)
				}
			}
			e := entry{f[1], f[2], f[3]}
			if e.kind == kindConf && e.change&0xff == 0 && g.confAt(r, f[0]-1).out == 0 {
				// The leave a leader past its removal proposes again changes nothing, and the
				// spec leaves it out of its log.
				e.kind = kindCommand
			}
			r.ents[f[0]] = e
			r.last = f[0]
			if e.kind == kindConf {
				r.props = append(r.props, f[0])
			}
		case trace.OutRaftState:
			g.state(id, rec.Pass, f)
		case trace.OutRaftSupport:
			g.support(id, rec.Pass, f)
		case trace.OutRaftPlan:
			g.plan = f[0]
		case trace.OutRaftForced:
			if to := int(f[0]); to <= len(g.reps) {
				g.reps[to-1].forced = true
			}
		}
	}
}

func main() {
	module := flag.String("module", "trace_data", "module name")
	flag.Parse()
	if flag.NArg() != 1 {
		fatal("usage: votersdata [-module name] run.trace")
	}
	file, err := os.Open(flag.Arg(0))
	if err != nil {
		fatal("%v", err)
	}
	var recs []trace.Record
	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		rec, err := trace.Parse(sc.Bytes())
		if err != nil {
			fatal("%v", err)
		}
		recs = append(recs, rec)
	}
	if err := sc.Err(); err != nil {
		fatal("%v", err)
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	write(w, *module, recs)
}

// write writes the data module of the trace's records recs.
func write(w io.Writer, module string, recs []trace.Record) {
	nodes := 0
	for _, rec := range recs {
		nodes = max(nodes, int(rec.Node)+1)
	}
	g := &gen{chosen: map[uint64]entry{}, failed: map[uint64]bool{}, leaders: map[uint64]uint64{}, boot: conf{in: 0b1110}, plan: 0b1110}
	var failed []uint64
	prev := make([]uint64, nodes)
	for _, rec := range recs {
		if rec.Name != trace.OutRaftState {
			continue
		}
		f := rec.Fields
		if f[4]&1 != 0 {
			g.leaders[f[0]] = uint64(rec.Node) + 1
			if i, ok := slices.BinarySearch(g.elected, f[0]); !ok {
				g.elected = slices.Insert(g.elected, i, f[0])
			}
		} else if handOffFails(prev[rec.Node], f) {
			failed = append(failed, f[0])
		}
		prev[rec.Node] = f[4]
	}
	for _, t := range failed {
		if i, ok := slices.BinarySearch(g.elected, t); !ok {
			g.elected = slices.Insert(g.elected, i, t)
			g.failed[t] = true
		}
	}
	for id := range nodes {
		r := &replica{ents: map[uint64]entry{}, applyAt: -1}
		if g.boot.in&(1<<(id+1)) != 0 {
			r.last, r.base = 1, 1
			r.state[2], r.state[3] = 1, 1
		}
		g.reps = append(g.reps, r)
	}
	g.run(recs)
	if len(g.lines) == 0 {
		fatal("the trace has no Raft step")
	}
	fmt.Fprintf(w, "---- MODULE %s ----\nEXTENDS Integers\n", module)
	fmt.Fprintf(w, "TReplicas == 1..%d\nTBaseVoters == %s\nTPlan == %s\n", nodes, set(g.boot.in), set(g.plan))
	fmt.Fprintf(w, "TMaxTerm == %d\nTMaxCrashes == %d\nTMaxHalts == %d\nTMaxDeaths == %d\n", len(g.elected), g.crashes, g.halts, g.deaths)
	slices.SortStableFunc(g.lines, func(x, y line) int {
		if x.k.less(y.k) {
			return -1
		}
		if y.k.less(x.k) {
			return 1
		}
		return 0
	})
	ls := make([]string, len(g.lines))
	for i, l := range g.lines {
		ls[i] = l.s
	}
	fmt.Fprintf(w, "Trace == <<\n%s\n>>\n====\n", strings.Join(ls, ",\n"))
}
