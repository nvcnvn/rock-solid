// Command tracedata turns a simulator trace into the data module the epoch model's trace spec reads: the run's constants and one line per protocol step. It decides no protocol step, and holds the mapping's judgement in two places: classify names the spec's changes for a table, and landOpen and crash name the versions a node may have made durable unrecorded.
//
//	tracedata [-module trace_data] run.trace > trace_data.tla
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/nvcnvn/rock-solid/internal/trace"
)

// verBase is the spec's version packing, TVer in models/epoch/trace_epoch.tla.
const verBase = 1 << 20

type member struct {
	disk, mark uint64
	leaving    bool
}

type past struct {
	interval, minLive uint64
	members           []member
}

type entry struct {
	interval, activated, minLive uint64
	members                      []member
	past                         []past
}

type refusal struct {
	line string
	node int
}

type write struct {
	chunk, block, inc int
	whole             bool
	identity, request uint64
}

type reply struct {
	decided uint64
	chunks  map[uint64][3]uint64
}

type gen struct {
	pgs     []uint64
	chunks  []uint64
	chunkPG []int
	blocks  []int
	entries map[uint64]*entry
	initial map[uint64]entry
	// nodes counts the replicas' nodes: the client, which records the tables, is numbered after them,
	// though a run may add a node of its own later, such as the real control plane's.
	nodes  int
	writes []write
	// wid is each write's spec number by its identity, the session and incarnation as OutSend packs them, and its request ID; byID by its request ID alone, 0 for one two identities share.
	wid  map[[2]uint64]int
	byID map[uint64]int
	// verWrite is the write each ordered [chunk, version] took.
	verWrite map[[2]uint64]int
	// answer is each write's newest answer line's token, which its client hearing an answer marks heard; a token not marked is a lost answer.
	answer  map[int]string
	heardOK map[string]bool
	// unanswered counts each write's sends since its last order, reanswer or stale line: the spec's sends of a write the
	// primary holds are one, so a later answer with none since repeats the last.
	unanswered map[int]int
	// mergedAt is the line of each write's newest send while the primary held an attempt, which the spec's pin, a set,
	// merges with it, and takenAt the line of its newest order, reanswer or stale.
	mergedAt, takenAt map[int]int
	// watermark is each [spec disk, pg, identity]'s newest oldest outstanding request ID a trim named.
	watermark map[[3]uint64]uint64
	plan      []string
	tables    int
	epochs    map[uint64]int
	lines     []string
	replies   map[[3]uint64]map[uint64]*reply
	cur       *reply
	// decisions holds each [node, pg, interval] an OutDecide named.
	decisions map[[3]uint64]bool
	// listedBy holds each [pg, interval, disk, chunk] a decision listed.
	listedBy map[[4]uint64]bool
	// disks maps a disk ID to the spec's: one formatted again takes the next past the nodes.
	diskIDs map[uint64]uint64
	fmts    int
	// primary is each PG's newest activated node; ownOpen a node's versions it ordered and has not logged, memberOpen those a member has not.
	primary    map[uint64]int
	ownOpen    map[int][][2]uint64
	memberOpen map[int][][2]uint64
	// landAt is the line of the first maydeliver of each [node, chunk, version] a member has open.
	landAt map[[3]uint64]int
	// lists holds each [disk, pg, interval] a Persisted record named.
	lists map[[3]uint64]bool
	// operatorAsk holds each PG the harness, no primary, asked a new interval of: its next table is an Edit that changes nothing.
	operatorAsk map[uint64]bool
	// restarted holds each [node, pg] whose node restarted and has not activated the PG since.
	restarted map[[2]uint64]bool
	// activated is each PG's newest activated interval, tableIv the interval of each PG at each epoch.
	activated map[uint64]uint64
	tableIv   map[uint64]map[uint64]uint64
	// left is the line where a [node, pg] stopped being in the PG's activated interval, formatAt the line where a spec disk's node formatted it, and nodeDisk a node's spec disk once formatted.
	left     map[[2]uint64]int
	formatAt map[uint64]int
	// outOfPool is the spec disks formatted or declared lost.
	outOfPool map[uint64]bool
	// refused holds each member's refusal the primary has not heard, by chunk: its line, and the node that refused.
	refused map[uint64][]refusal
	// past holds the lines of members' refusals of a version of an interval they left, by chunk and version, until the primary fails that version; one it never fails changed nothing there.
	past map[[2]uint64][]string
	// orderAt is the line of each recovery a primary ordered, by chunk and interval, and served each [spec disk, chunk, interval] a member read for a recovery.
	orderAt  map[[2]uint64]int
	served   map[[3]uint64]bool
	nodeDisk map[int]uint64
	// recovering holds each node's recovery writes its store took and has not reported: chunk, spec source, interval, version.
	recovering map[int][][4]uint64
}

func ver(v uint64) string { return strconv.FormatUint((v>>32)*verBase+v&(1<<32-1), 10) }

func (g *gen) pg(f uint64) int {
	i := slices.Index(g.pgs, f)
	if i < 0 {
		fatal("unknown PG %d", f)
	}
	return i + 1
}

func (g *gen) chunk(hash uint64) int {
	i := slices.Index(g.chunks, hash)
	if i < 0 {
		fatal("chunk %d was never written", hash)
	}
	return i + 1
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tracedata: "+format+"\n", args...)
	os.Exit(1)
}

// chunkField is the PG of a chunk, by its trace field.
func (g *gen) chunkField(hash uint64) uint64 { return g.pgs[g.chunkPG[g.chunk(hash)-1]-1] }

func (g *gen) disk(id uint64) uint64 {
	if d, ok := g.diskIDs[id]; ok {
		return d
	}
	return id
}

// source is the spec's disk for a Recovered record's source, 0 for none.
func (g *gen) source(id uint64) uint64 {
	if id == 0 {
		return 0
	}
	return g.disk(id)
}

func dropVersion(l [][2]uint64, c, v uint64) [][2]uint64 {
	return slices.DeleteFunc(l, func(x [2]uint64) bool { return x[0] == c && x[1] == v })
}

// landOpen lets the versions node n's store took and has not reported durable land now: the spec logs a version as a member takes it, and a table or a crash may come before the record.
func (g *gen) landOpen(n int) {
	for _, x := range g.memberOpen[n] {
		if k := [3]uint64{uint64(n), x[0], x[1]}; g.landAt[k] == 0 {
			g.landAt[k] = len(g.lines) + 1
		}
		g.line(`k |-> "maydeliver", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(x[0]), ver(x[1]))
	}
}

// crash writes a process crash: a write in flight may have reached the disk, so the lines before the restart let the versions a member had not reported durable land, and those the node's own log had not lose their record.
func (g *gen) crash(n int) {
	g.landOpen(n)
	for _, x := range g.recovering[n] {
		f := g.chunkField(x[0])
		if x[2] == g.activated[f] {
			g.lineAt(g.recoveredAt(f, x[0], x[2], n, x[1], x[3]), `k |-> "mayrecover", n |-> %d, l |-> %d, c |-> %d, v |-> %s, s |-> %d, iv |-> %d`, n, g.primary[f], g.chunk(x[0]), ver(x[3]), x[1], x[2])
		}
	}
	g.recovering[n] = nil
	own := g.ownOpen[n]
	for j := len(own) - 1; j >= 0; j-- {
		g.line(`k |-> "mayfail", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(own[j][0]), ver(own[j][1]))
	}
	g.memberOpen[n], g.ownOpen[n] = nil, nil
	for k := range g.landAt {
		if k[0] == uint64(n) {
			delete(g.landAt, k)
		}
	}
	g.line(`k |-> "restart", n |-> %d`, n)
}

// repliedLater reports whether node n, PG f's primary of iv, took its own disk's peering reply, or its disk answered a later primary, listing chunk c, or one whose decision listed c on that disk, for an interval after iv and after every interval n decided that some primary activated since: a recovery write of c in iv that lands now changes no list that decision reads, as a demoted primary's gated write lands only at the next decision its disk persists. A reply of an interval a later activated decision of n passed over is no longer read, nor is one of an interval not yet decided whose PG's next decision in rest is of a later interval: that decision reads a later reply, taken after the write landed.
func (g *gen) repliedLater(n int, f, c, iv uint64, rest []trace.Record) bool {
	own, ok := g.nodeDisk[n]
	if !ok {
		own = uint64(n)
	}
	after := iv
	for k := range g.decisions {
		if k[0] == uint64(n) && k[1] == f && k[2] <= g.activated[f] {
			after = max(after, k[2])
		}
	}
	for k, got := range g.replies {
		if k[1] != f || k[2] <= after || !g.decisions[k] && decidedNext(rest, f) > k[2] {
			continue
		}
		for d, x := range got {
			if g.disk(d) == own && (x.chunks[c][2] != 0 || g.listedBy[[4]uint64{f, k[2], d, c}]) {
				return true
			}
		}
	}
	return false
}

// decidedNext returns the interval of PG f's first decision in rest, or 0 for none.
func decidedNext(rest []trace.Record, f uint64) uint64 {
	for _, x := range rest {
		if x.Name == trace.OutDecide && x.Fields[0] == f {
			return x.Fields[1]
		}
	}
	return 0
}

func (g *gen) line(format string, args ...any) {
	g.lines = append(g.lines, "["+fmt.Sprintf(format, args...)+"]")
}

// lineAt writes a line at position at, before the lines already there, and keeps the marked positions on the lines they name.
func (g *gen) lineAt(at int, format string, args ...any) {
	if at == len(g.lines) {
		g.line(format, args...)
		return
	}
	g.lines = slices.Insert(g.lines, at, "["+fmt.Sprintf(format, args...)+"]")
	for k, p := range g.left {
		if p >= at {
			g.left[k] = p + 1
		}
	}
	for k, p := range g.formatAt {
		if p >= at {
			g.formatAt[k] = p + 1
		}
	}
	for k, p := range g.orderAt {
		if p >= at {
			g.orderAt[k] = p + 1
		}
	}
	for k, p := range g.landAt {
		if p > at {
			g.landAt[k] = p + 1
		}
	}
	for _, m := range []map[int]int{g.mergedAt, g.takenAt} {
		for k, p := range m {
			if p >= at {
				m[k] = p + 1
			}
		}
	}
}

// moveBack moves line from to just before line to, an earlier one, and keeps the marked positions on the lines they name.
func (g *gen) moveBack(from, to int) {
	l := g.lines[from]
	g.lines = slices.Delete(g.lines, from, from+1)
	type mark struct {
		m map[int]int
		k int
	}
	var named []mark
	for _, m := range []map[int]int{g.mergedAt, g.takenAt} {
		for k, p := range m {
			if p == from {
				named = append(named, mark{m, k})
			} else if p > from {
				m[k] = p - 1
			}
		}
	}
	for _, m := range []map[[2]uint64]int{g.left, g.orderAt} {
		for k, p := range m {
			if p > from {
				m[k] = p - 1
			}
		}
	}
	for k, p := range g.formatAt {
		if p > from {
			g.formatAt[k] = p - 1
		}
	}
	for k, p := range g.landAt {
		if p > from+1 {
			g.landAt[k] = p - 1
		}
	}
	g.lineAt(to, "%s", l[1:len(l)-1])
	for _, x := range named {
		x.m[x.k] = to
	}
}

// takeMerged moves the newest send of write w the spec's pin merged with an attempt already taken to just after that
// take, as an order or reanswer with no send since it takes another attempt the primary held beside that one.
func (g *gen) takeMerged(w int) {
	if m, ok := g.mergedAt[w]; ok && g.unanswered[w] == 0 && m < g.takenAt[w] {
		g.moveLine(m, g.takenAt[w])
		delete(g.mergedAt, w)
	}
}

// moveLine moves line from to just after line to, a later one, and keeps the marked positions on the lines they name.
func (g *gen) moveLine(from, to int) {
	l := g.lines[from]
	copy(g.lines[from:to], g.lines[from+1:to+1])
	g.lines[to] = l
	for _, m := range []map[int]int{g.mergedAt, g.takenAt} {
		for k, p := range m {
			if p > from && p <= to {
				m[k] = p - 1
			}
		}
	}
	for _, m := range []map[[2]uint64]int{g.left, g.orderAt} {
		for k, p := range m {
			if p > from && p <= to {
				m[k] = p - 1
			}
		}
	}
	for k, p := range g.formatAt {
		if p > from && p <= to {
			g.formatAt[k] = p - 1
		}
	}
	for k, p := range g.landAt {
		if p > from+1 && p <= to+1 {
			g.landAt[k] = p - 1
		}
	}
}

// leave marks that node n leaves PG f's activated interval at the next line.
func (g *gen) leave(n int, f uint64) {
	k := [2]uint64{uint64(n), f}
	if _, ok := g.left[k]; !ok {
		g.left[k] = len(g.lines)
	}
}

// recoveredAt is where a recovery write of chunk c in interval iv of PG f to node n read from spec disk s, logged under version rv, lands: the spec takes its read and its write as one step while its primary leads, so a record from after the primary, the target or the source left the interval goes before the first of those steps, across lines that keep what Recover reads. The store took the write in time, as maydeliver lets an open version land, but only one the primary ordered before that step moves; any other stays where the check refuses it. The target's store logged the versions of iv up to rv before the recovery, so their lines after that step are copied before it, and the originals stutter.
func (g *gen) recoveredAt(f, c, iv uint64, n int, s, rv uint64) int {
	at := len(g.lines)
	for _, x := range []int{g.primary[f], n} {
		if p, ok := g.left[[2]uint64{uint64(x), f}]; ok {
			at = min(at, p)
		}
	}
	if p, ok := g.formatAt[s]; ok {
		at = min(at, p)
	}
	if p, ok := g.orderAt[[2]uint64{c, iv}]; !ok || p >= at {
		return len(g.lines)
	}
	var landed []string
	for _, l := range g.lines[at:] {
		for _, kind := range []string{"logged", "maydeliver"} {
			v, ok := strings.CutPrefix(l, fmt.Sprintf(`[k |-> "%s", n |-> %d, c |-> %d, v |-> `, kind, n, g.chunk(c)))
			if x, err := strconv.ParseUint(strings.TrimSuffix(v, "]"), 10, 64); ok && err == nil && !slices.Contains(landed, l) && x/verBase == iv && x <= (rv>>32)*verBase+rv&(1<<32-1) {
				landed = append(landed, l)
			}
		}
	}
	for _, l := range landed {
		g.lineAt(at, "%s", strings.TrimSuffix(strings.TrimPrefix(l, "["), "]"))
		at++
	}
	return at
}

func marks(m uint64) string { return [...]string{"up", "down", "maint"}[m] }

func disks(ms []member) []uint64 {
	var d []uint64
	for _, m := range ms {
		d = append(d, m.disk)
	}
	return d
}

// classify appends the spec's table changes for one committed table: its folds, then its membership changes.
func (g *gen) classify(epoch uint64, changed []uint64, next map[uint64]*entry) {
	for _, f := range changed {
		old, e := g.entries[f], next[f]
		if e.activated > old.activated && e.activated > 1 {
			g.tables++
			g.line(`k |-> "fold", p |-> %d, iv |-> %d`, g.pg(f), e.activated)
		}
	}
	marked := map[[2]uint64]bool{}
	var rest []uint64
	for _, f := range changed {
		old, e := g.entries[f], next[f]
		if e.interval == old.interval {
			continue
		}
		d, to, ok := oneMark(old, e)
		if ok {
			marked[[2]uint64{d, to}] = true
			continue
		}
		rest = append(rest, f)
	}
	for _, f := range rest {
		old, e := g.entries[f], next[f]
		p := g.pg(f)
		a, b := disks(old.members), disks(e.members)
		asked := g.operatorAsk[f]
		delete(g.operatorAsk, f)
		la, lb := leavingOf(old), leavingOf(e)
		added, removed, newly, ended := minus(b, a), minus(a, b), minus(lb, la), minus(la, lb)
		kept := keptMarks(old, e)
		switch {
		case kept && len(added) == 0 && len(ended) == 1 && len(newly) == 1 && slices.Equal(b, append(minus(a, newly), newly...)):
			g.planned(fmt.Sprintf(`[k |-> "stay", p |-> %d, l |-> %d, out |-> %d]`, p, g.disk(ended[0]), g.disk(newly[0])))
		case kept && len(added) == 0 && len(ended) == 1 && len(newly) == 0 && len(removed) == 1 && slices.Contains(b, ended[0]) && slices.Equal(b, minus(a, removed)):
			g.planned(fmt.Sprintf(`[k |-> "stay", p |-> %d, l |-> %d, out |-> %d]`, p, g.disk(ended[0]), g.disk(removed[0])))
		case slices.Equal(a, b) && sameMembers(old, e) && e.minLive == old.minLive && !asked:
			g.tables++
			g.line(`k |-> "renew", p |-> %d`, p)
		case kept && len(added) == 1 && len(removed) == 0 && len(ended) == 0 && len(newly) == 1 &&
			(slices.Equal(b, append(slices.Clone(a), added...)) || slices.Equal(b, append(append(minus(a, newly), added...), newly...))):
			g.planned(fmt.Sprintf(`[k |-> "add", p |-> %d, old |-> %d, new |-> %d]`, p, g.disk(newly[0]), g.disk(added[0])))
		case kept && len(added) == 0 && len(newly) == 0 && len(removed) > 0 && slices.Equal(removed, ended) && slices.Equal(b, minus(a, removed)):
			if slices.ContainsFunc(removed, func(d uint64) bool { return !g.outOfPool[g.disk(d)] }) {
				g.planned(fmt.Sprintf(`[k |-> "remove", p |-> %d]`, p))
			} else {
				g.planned(fmt.Sprintf(`[k |-> "drop", p |-> %d]`, p))
			}
		case slices.Equal(la, lb) && len(a) == len(b) && differs(a, b) == 1:
			j := slices.IndexFunc(a, func(d uint64) bool { return !slices.Contains(b, d) })
			g.planned(fmt.Sprintf(`[k |-> "replace", p |-> %d, old |-> %d, new |-> %d]`, p, g.disk(a[j]), g.disk(b[j])))
		case len(a) >= 2 && len(a) == len(b) && a[0] == b[1] && a[1] == b[0] && slices.Equal(a[2:], b[2:]) && kept:
			g.planned(fmt.Sprintf(`[k |-> "rotate", p |-> %d]`, p))
		case len(la) == 0 && len(lb) == 0 && len(a) == len(b) && differs(sortedCopy(a), sortedCopy(b)) == 0 && marksChanged(old, e) <= 1:
			mem, ms := g.memberList(e.members)
			g.planned(fmt.Sprintf(`[k |-> "edit", p |-> %d, mem |-> %s, marks |-> %s]`, p, mem, ms))
		default:
			mem, ms := g.memberList(e.members)
			g.planned(fmt.Sprintf(`[k |-> "refill", p |-> %d, mem |-> %s, marks |-> %s, lv |-> %s]`, p, mem, ms, g.diskSet(lb)))
		}
	}
	for _, k := range sortedPairs(marked) {
		g.planned(fmt.Sprintf(`[k |-> "mark", d |-> %d, to |-> "%s"]`, g.disk(k[0]), marks(k[1])))
	}
	for _, f := range changed {
		g.entries[f] = next[f]
	}
	g.tableIv[epoch] = map[uint64]uint64{}
	for f, e := range g.entries {
		g.tableIv[epoch][f] = e.interval
	}
	g.epochs[epoch] = g.tables
	g.tableLine(changed)
}

func (g *gen) memberList(ms []member) (mem, ms2 string) {
	var d, k []string
	for _, m := range ms {
		d = append(d, strconv.FormatUint(g.disk(m.disk), 10))
		k = append(k, `"`+marks(m.mark)+`"`)
	}
	return "<<" + strings.Join(d, ", ") + ">>", "<<" + strings.Join(k, ", ") + ">>"
}

// tableLine has the check compare the entries of the code's table with the spec's after the changes classify named.
func (g *gen) tableLine(changed []uint64) {
	var rows []string
	for _, f := range changed {
		e := g.entries[f]
		mem, ms := g.memberList(e.members)
		var ps []string
		for _, x := range e.past {
			pm, pk := g.memberList(x.members)
			ps = append(ps, fmt.Sprintf("[iv |-> %d, ml |-> %d, mem |-> %s, marks |-> %s]", x.interval, x.minLive, pm, pk))
		}
		rows = append(rows, fmt.Sprintf("[p |-> %d, iv |-> %d, la |-> %d, ml |-> %d, lv |-> %s, mem |-> %s, marks |-> %s, past |-> <<%s>>]",
			g.pg(f), e.interval, e.activated, e.minLive, g.diskSet(leavingOf(e)), mem, ms, strings.Join(ps, ", ")))
	}
	g.line(`k |-> "table", rows |-> <<%s>>`, strings.Join(rows, ", "))
}

func (g *gen) planned(item string) {
	g.plan = append(g.plan, item)
	g.tables++
	g.line(`k |-> "plan"`)
}

func leavingOf(e *entry) []uint64 {
	var ds []uint64
	for _, m := range e.members {
		if m.leaving {
			ds = append(ds, m.disk)
		}
	}
	return ds
}

// diskSet writes disks as a spec set.
func (g *gen) diskSet(ds []uint64) string {
	var d []string
	for _, x := range ds {
		d = append(d, strconv.FormatUint(g.disk(x), 10))
	}
	return "{" + strings.Join(d, ", ") + "}"
}

// minus is a's disks that are not in b.
func minus(a, b []uint64) []uint64 {
	return slices.DeleteFunc(slices.Clone(a), func(d uint64) bool { return slices.Contains(b, d) })
}

func sortedCopy(a []uint64) []uint64 {
	a = slices.Clone(a)
	slices.Sort(a)
	return a
}

func differs(a, b []uint64) int {
	n := 0
	for i := range a {
		if a[i] != b[i] {
			n++
		}
	}
	return n
}

func sortedPairs(m map[[2]uint64]bool) [][2]uint64 {
	var out [][2]uint64
	for k := range m {
		out = append(out, k)
	}
	slices.SortFunc(out, func(a, b [2]uint64) int {
		if a[0] != b[0] {
			return int(a[0]) - int(b[0])
		}
		return int(a[1]) - int(b[1])
	})
	return out
}

func sameMembers(a, b *entry) bool { return slices.Equal(a.members, b.members) }

// keptMarks reports whether every disk in both a and b has one mark, and b's others are up.
func keptMarks(a, b *entry) bool {
	for _, m := range b.members {
		j := slices.IndexFunc(a.members, func(o member) bool { return o.disk == m.disk })
		if j >= 0 && a.members[j].mark != m.mark || j < 0 && m.mark != 0 {
			return false
		}
	}
	return true
}

// marksChanged counts the disks of a whose mark differs in b.
func marksChanged(a, b *entry) int {
	n := 0
	for _, m := range a.members {
		if j := slices.IndexFunc(b.members, func(o member) bool { return o.disk == m.disk }); j >= 0 && b.members[j].mark != m.mark {
			n++
		}
	}
	return n
}

// oneMark reports the one disk whose mark alone changes between a and b, no leaving flag changing.
func oneMark(a, b *entry) (disk, to uint64, ok bool) {
	if !slices.Equal(disks(a.members), disks(b.members)) {
		return 0, 0, false
	}
	n := 0
	for i := range a.members {
		if a.members[i].leaving != b.members[i].leaving {
			return 0, 0, false
		}
		if a.members[i].mark != b.members[i].mark {
			disk, to = a.members[i].disk, b.members[i].mark
			n++
		}
	}
	return disk, to, n == 1
}

// load reads the records from the first table on: that table is the spec's Init, which starts interval 1 activated, so the bootstrap peering of interval 1 has no line.
func (g *gen) load(path string) []trace.Record {
	f, err := os.Open(path)
	if err != nil {
		fatal("%v", err)
	}
	defer f.Close()
	var recs []trace.Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	started := false
	for sc.Scan() {
		r, err := trace.Parse(sc.Bytes())
		if err != nil {
			fatal("%v", err)
		}
		if !started && r.Name == trace.OutTable {
			started, g.nodes = true, int(r.Node)
		}
		if started {
			recs = append(recs, r)
		}
	}
	if err := sc.Err(); err != nil {
		fatal("%v", err)
	}
	return recs
}

func (g *gen) tableAt(recs []trace.Record, i int) (epoch uint64, changed []uint64, next map[uint64]*entry, end int) {
	epoch = recs[i].Fields[0]
	next = map[uint64]*entry{}
	var cur *entry
	var members *[]member
	for i++; i < len(recs); i++ {
		r := &recs[i]
		switch r.Name {
		case trace.OutEntry:
			cur = &entry{interval: r.Fields[1], activated: r.Fields[2], minLive: r.Fields[3]}
			next[r.Fields[0]] = cur
			changed = append(changed, r.Fields[0])
			members = &cur.members
		case trace.OutPast:
			cur.past = append(cur.past, past{interval: r.Fields[1], minLive: r.Fields[2]})
			members = &cur.past[len(cur.past)-1].members
		case trace.OutMember:
			*members = append(*members, member{r.Fields[1], r.Fields[2], r.Fields[3] != 0})
		default:
			return epoch, changed, next, i
		}
	}
	return epoch, changed, next, i
}

func (g *gen) scan(recs []trace.Record) {
	for i := range recs {
		r := &recs[i]
		if r.Name != trace.OutSend {
			continue
		}
		c := r.Fields[1]
		j := slices.Index(g.chunks, c)
		if j < 0 {
			g.chunks = append(g.chunks, c)
			g.chunkPG = append(g.chunkPG, g.pg(r.Fields[2]))
			g.blocks = append(g.blocks, 1)
			j = len(g.chunks) - 1
		}
		key := [2]uint64{r.Fields[4], r.Fields[0]}
		if g.wid[key] != 0 {
			continue
		}
		off, n := r.Fields[3]>>32, r.Fields[3]&(1<<32-1)
		blk := int(off/4096) + 1
		g.blocks[j] = max(g.blocks[j], blk)
		g.writes = append(g.writes, write{chunk: j + 1, block: blk, inc: int(r.Fields[4] & (1<<32 - 1)), whole: off%4096 == 0 && n%4096 == 0, identity: r.Fields[4], request: r.Fields[0]})
		g.wid[key] = len(g.writes)
		if _, ok := g.byID[r.Fields[0]]; ok {
			g.byID[r.Fields[0]] = 0
		} else {
			g.byID[r.Fields[0]] = len(g.writes)
		}
	}
}

// heard writes the refusals held for the primary's fail that match: the spec's refusal ends the version at the primary in one step, so its line waits for the primary to hear it, or for the member to move on or the primary to leave, past which MemberFail or GoneRefuses has no step.
func (g *gen) heard(match func(chunk uint64, x refusal) bool) {
	var cs []uint64
	for c := range g.refused {
		cs = append(cs, c)
	}
	slices.Sort(cs)
	for _, c := range cs {
		g.refused[c] = slices.DeleteFunc(g.refused[c], func(x refusal) bool {
			if !match(c, x) {
				return false
			}
			g.line("%s", x.line)
			return true
		})
	}
}

// heardPast writes the refusals of version v of chunk c by members that left its interval, as the primary fails v.
func (g *gen) heardPast(c, v uint64) {
	for _, l := range g.past[[2]uint64{c, v}] {
		g.line("%s", l)
	}
	delete(g.past, [2]uint64{c, v})
}

func (g *gen) run(recs []trace.Record) {
	epoch, changed, first, i := g.tableAt(recs, 0)
	g.pgs = slices.Clone(changed)
	for f, e := range first {
		g.entries[f], g.initial[f] = e, *e
	}
	g.tables = 1
	g.epochs[epoch] = 1
	g.scan(recs)
	for i < len(recs) {
		r := &recs[i]
		n := int(r.Node) + 1
		i++
		switch r.Name {
		case trace.OutTable:
			epoch, changed, next, end := g.tableAt(recs, i-1)
			g.classify(epoch, changed, next)
			i = end
		case trace.OutAdopt:
			for _, f := range g.pgs {
				if iv := g.tableIv[r.Fields[0]][f]; iv > g.activated[f] && g.activated[f] != 0 {
					g.leave(n, f)
				}
			}
			g.heard(func(_ uint64, x refusal) bool { return x.node == n })
			g.landOpen(n)
			e, ok := g.epochs[r.Fields[0]]
			if !ok {
				fatal("node %d adopts epoch %d, which no table named", n, r.Fields[0])
			}
			g.line(`k |-> "adopt", n |-> %d, e |-> %d`, n, e)
		case trace.OutSend:
			w := g.wid[[2]uint64{r.Fields[4], r.Fields[0]}]
			if g.unanswered[w] > 0 {
				g.mergedAt[w] = len(g.lines)
			}
			g.unanswered[w]++
			g.line(`k |-> "send", w |-> %d`, w)
		case trace.OutOrder:
			w := g.wid[[2]uint64{r.Fields[3], r.Fields[0]}]
			g.verWrite[[2]uint64{r.Fields[1], r.Fields[2]}] = w
			g.takeMerged(w)
			g.unanswered[w] = 0
			g.takenAt[w] = len(g.lines)
			g.line(`k |-> "order", n |-> %d, w |-> %d, c |-> %d, v |-> %s`, n, w, g.chunk(r.Fields[1]), ver(r.Fields[2]))
			cv := [2]uint64{r.Fields[1], r.Fields[2]}
			g.ownOpen[n] = append(g.ownOpen[n], cv)
		case trace.OutAccepted:
			g.memberOpen[n] = append(g.memberOpen[n], [2]uint64{r.Fields[0], r.Fields[1]})
		case trace.OutMemberFail:
			g.recovering[n] = slices.DeleteFunc(g.recovering[n], func(x [4]uint64) bool { return x[0] == r.Fields[0] })
			kind := "mfail"
			if r.Fields[2] != 0 {
				kind = "gone"
			} else if r.Fields[4] != 0 {
				kind = "unready"
			}
			if r.Fields[3] != 0 {
				kind += "recover"
			}
			g.refused[r.Fields[0]] = append(g.refused[r.Fields[0]], refusal{fmt.Sprintf(`k |-> "%s", n |-> %d, c |-> %d, v |-> %s`, kind, n, g.chunk(r.Fields[0]), ver(r.Fields[1])), n})
		case trace.OutPastRefused:
			k := [2]uint64{r.Fields[0], r.Fields[1]}
			g.past[k] = append(g.past[k], fmt.Sprintf(`k |-> "pastrefused", n |-> %d, c |-> %d, v |-> %s`, n, g.chunk(r.Fields[0]), ver(r.Fields[1])))
		case trace.OutFail:
			g.heard(func(c uint64, _ refusal) bool { return c == r.Fields[0] })
			// A primary that activated a later interval than the version's fails a recovery write logged under it, not its client.
			if f := g.chunkField(r.Fields[0]); g.primary[f] != n || g.activated[f] <= r.Fields[1]>>32 {
				g.heardPast(r.Fields[0], r.Fields[1])
			}
		case trace.OutLogged:
			kind := "logged"
			if slices.Contains(g.ownOpen[n], [2]uint64{r.Fields[0], r.Fields[1]}) {
				kind = "ownlogged"
			}
			g.ownOpen[n] = dropVersion(g.ownOpen[n], r.Fields[0], r.Fields[1])
			g.memberOpen[n] = dropVersion(g.memberOpen[n], r.Fields[0], r.Fields[1])
			// A version the member logs after a table that came while it was open landed before that table: its first maydeliver delivers it.
			if k := [3]uint64{uint64(n), r.Fields[0], r.Fields[1]}; g.landAt[k] != 0 {
				g.lines[g.landAt[k]-1] = strings.Replace(g.lines[g.landAt[k]-1], `"maydeliver"`, `"logged"`, 1)
				delete(g.landAt, k)
			}
			g.line(`k |-> "%s", n |-> %d, c |-> %d, v |-> %s`, kind, n, g.chunk(r.Fields[0]), ver(r.Fields[1]))
		case trace.OutRecoverOrder:
			s, at := g.source(r.Fields[1]), len(g.lines)
			// A source formatted since that answered the read before its format: the spec's Recover takes the read with the write.
			if p, ok := g.formatAt[s]; ok && g.served[[3]uint64{s, r.Fields[0], r.Fields[2]}] {
				at = p
			}
			g.lineAt(at, `k |-> "recoverorder", n |-> %d, c |-> %d, s |-> %d, iv |-> %d`, n, g.chunk(r.Fields[0]), s, r.Fields[2])
			g.orderAt[[2]uint64{r.Fields[0], r.Fields[2]}] = at
		case trace.OutRecovering:
			g.recovering[n] = append(g.recovering[n], [4]uint64{r.Fields[0], g.source(r.Fields[1]), r.Fields[2], r.Fields[3]})
		case trace.OutRecovered:
			g.recovering[n] = slices.DeleteFunc(g.recovering[n], func(x [4]uint64) bool { return x[0] == r.Fields[0] && x[2] == r.Fields[3] })
			f, s := g.chunkField(r.Fields[0]), g.source(r.Fields[2])
			at, kind := len(g.lines), "recovered"
			if g.decisions[[3]uint64{uint64(n), f, r.Fields[3]}] && g.repliedLater(n, f, r.Fields[0], r.Fields[3], recs[i:]) {
				kind = "recoveredlate"
			} else if r.Fields[3] == g.activated[f] {
				at = g.recoveredAt(f, r.Fields[0], r.Fields[3], n, s, r.Fields[1])
			}
			g.lineAt(at, `k |-> "%s", n |-> %d, l |-> %d, c |-> %d, v |-> %s, s |-> %d, iv |-> %d`, kind, n, g.primary[f], g.chunk(r.Fields[0]), ver(r.Fields[1]), s, r.Fields[3])
		case trace.OutReported:
			// No traced run splits a PG, so the line leaves out the report's split count.
			g.line(`k |-> "reported", n |-> %d, p |-> %d, iv |-> %d`, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.OutAsk:
			if n > g.nodes {
				g.operatorAsk[r.Fields[0]] = true
				break
			}
			kind := "ask"
			if g.restarted[[2]uint64{uint64(n), r.Fields[0]}] {
				kind = "restartask"
			}
			g.line(`k |-> "%s", n |-> %d, p |-> %d, iv |-> %d`, kind, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.OutRecoverRead:
			g.served[[3]uint64{g.specDisk(n), r.Fields[0], r.Fields[1]}] = true
		case trace.OutFormat:
			old, ok := g.nodeDisk[n]
			if !ok {
				old = uint64(n)
			}
			g.formatAt[old] = len(g.lines)
			g.outOfPool[old] = true
			g.fmts++
			g.diskIDs[r.Fields[0]] = uint64(g.nodes + g.fmts)
			g.nodeDisk[n] = g.diskIDs[r.Fields[0]]
			g.line(`k |-> "format", n |-> %d`, n)
		case trace.OutLost:
			g.outOfPool[g.disk(r.Fields[0])] = true
			g.line(`k |-> "lost", d |-> %d`, g.disk(r.Fields[0]))
		case trace.OutCommitted:
			g.line(`k |-> "committed", c |-> %d, v |-> %s, heard |-> %s`, g.chunk(r.Fields[0]), ver(r.Fields[1]), g.answered(g.verWrite[[2]uint64{r.Fields[0], r.Fields[1]}]))
		case trace.OutReanswer:
			w := g.verWrite[[2]uint64{r.Fields[0], r.Fields[1]}]
			if g.unanswered[w] == 0 && g.answer[w] != "" {
				g.line(`k |-> "reanswered", c |-> %d, v |-> %s`, g.chunk(r.Fields[0]), ver(r.Fields[1]))
				break
			}
			g.takeMerged(w)
			g.unanswered[w] = 0
			g.takenAt[w] = len(g.lines)
			g.line(`k |-> "reanswer", n |-> %d, w |-> %d, c |-> %d, v |-> %s, heard |-> %s`, n, w, g.chunk(r.Fields[0]), ver(r.Fields[1]), g.answered(w))
		case trace.OutHeard:
			if w := g.byID[r.Fields[0]]; w == 0 || g.answer[w] == "" {
				fatal("request %d heard with no answer line", r.Fields[0])
			} else {
				g.heardOK[g.answer[w]] = true
				g.sentBefore(w)
			}
		case trace.OutStale:
			w := g.wid[[2]uint64{r.Fields[2], r.Fields[0]}]
			kind := "stale"
			if g.unanswered[w] == 0 {
				kind = "staled"
			}
			g.unanswered[w] = 0
			g.takenAt[w] = len(g.lines)
			g.line(`k |-> "%s", n |-> %d, w |-> %d`, kind, n, w)
		case trace.OutTrim:
			d, f := g.specDisk(n), g.chunkField(r.Fields[0])
			key := [3]uint64{d, f, r.Fields[1]}
			for i, x := range g.writes {
				if x.identity == r.Fields[1] && g.pgs[g.chunkPG[x.chunk-1]-1] == f && x.request > g.watermark[key] && x.request < r.Fields[2] {
					g.line(`k |-> "trim", d |-> %d, w |-> %d`, d, i+1)
				}
			}
			g.watermark[key] = max(g.watermark[key], r.Fields[2]-1)
		case trace.OutResends:
			var es []string
			for i < len(recs) && recs[i].Name == trace.OutResendEntry && recs[i].Fields[0] == r.Fields[0] {
				e := &recs[i]
				es = append(es, fmt.Sprintf("[c |-> %d, v |-> %s, w |-> %d]", g.chunk(e.Fields[1]), ver(e.Fields[2]), g.wid[[2]uint64{e.Fields[3], e.Fields[4]}]))
				i++
			}
			g.line(`k |-> "resends", d |-> %d, e |-> {%s}`, g.disk(r.Fields[0]), strings.Join(es, ", "))
		case trace.OutReply:
			key := [3]uint64{uint64(n), r.Fields[0], r.Fields[1]}
			if g.replies[key] == nil {
				g.replies[key] = map[uint64]*reply{}
			}
			g.cur = &reply{decided: r.Fields[3], chunks: map[uint64][3]uint64{}}
			g.replies[key][r.Fields[2]] = g.cur
		case trace.OutReplyChunk:
			g.cur.chunks[r.Fields[1]] = [3]uint64{r.Fields[2], r.Fields[3], r.Fields[4]}
		case trace.OutDecide:
			kept := make([]string, len(g.chunks))
			for j := range kept {
				kept[j] = "0"
			}
			for ; i < len(recs) && recs[i].Name == trace.OutKept && recs[i].Node == r.Node; i++ {
				kept[g.chunk(recs[i].Fields[0])-1] = ver(recs[i].Fields[1])
			}
			var listed []string
			for ; i < len(recs) && recs[i].Name == trace.OutListed && recs[i].Node == r.Node; i++ {
				listed = append(listed, fmt.Sprintf("[d |-> %d, c |-> %d, v |-> %s]", g.disk(recs[i].Fields[0]), g.chunk(recs[i].Fields[1]), ver(recs[i].Fields[2])))
				g.listedBy[[4]uint64{r.Fields[0], r.Fields[1], recs[i].Fields[0], recs[i].Fields[1]}] = true
			}
			g.decide(n, r, kept, listed)
			g.persistedUnrecorded(n, r, recs[i:])
		case trace.OutPersisted:
			key := [3]uint64{r.Fields[2], r.Fields[0], r.Fields[1]}
			g.line(`k |-> "persisted", d |-> %d, p |-> %d, iv |-> %d, again |-> %s`, g.disk(r.Fields[2]), g.pg(r.Fields[0]), r.Fields[1], strings.ToUpper(strconv.FormatBool(g.lists[key])))
			g.lists[key] = true
		case trace.OutActivate:
			g.primary[r.Fields[0]] = n
			g.activated[r.Fields[0]] = r.Fields[1]
			delete(g.restarted, [2]uint64{uint64(n), r.Fields[0]})
			for k := range g.left {
				if k[1] == r.Fields[0] {
					delete(g.left, k)
				}
			}
			g.line(`k |-> "activate", n |-> %d, p |-> %d, iv |-> %d`, n, g.pg(r.Fields[0]), r.Fields[1])
		case trace.EvCrash:
			for _, f := range g.pgs {
				g.restarted[[2]uint64{uint64(n), f}] = true
				if g.primary[f] == n {
					g.leave(n, f)
				}
			}
			g.heard(func(c uint64, x refusal) bool { return x.node == n || g.primary[g.chunkField(c)] == n })
			g.crash(n)
		default:
			if !slices.Contains(belowProtocol, r.Name) {
				fatal("record %v at node %d has no mapping", r.Name, n)
			}
		}
	}
	g.heard(func(uint64, refusal) bool { return true })
}

// belowProtocol are the records with no spec action, the machine's steps below the protocol's and the other records of a run.
var belowProtocol = []trace.Name{
	trace.EvNetRecv, trace.EvPeerLost, trace.EvDiskDone, trace.EvTimer, trace.EvResumed, trace.EvRestart, trace.EvDrop, trace.EvPause, trace.EvResume,
	trace.ActNetSend, trace.ActDiskRead, trace.ActDiskWrite, trace.ActDiskFsync, trace.ActDiskTrim, trace.ActSetTimer, trace.ActEntry,
	trace.SysEpollWait, trace.SysRead, trace.SysWritev, trace.SysIOUringEnter, trace.SysTimerfdSettime, trace.SysEventfdWrite,
	trace.OutDurable, trace.OutFailed, trace.OutCheckpointDone,
}

// specDisk is the spec's disk of node n.
func (g *gen) specDisk(n int) uint64 {
	if d, ok := g.nodeDisk[n]; ok {
		return d
	}
	return uint64(n)
}

// sentBefore moves the sends of write w after its newest answer line, which its client just heard, to before that line: the
// client sent them before it heard, and the spec's client hears an answer in the step that gives it, after which it sends no more.
func (g *gen) sentBefore(w int) {
	at := slices.IndexFunc(g.lines, func(l string) bool { return strings.Contains(l, g.answer[w]) })
	send := fmt.Sprintf(`[k |-> "send", w |-> %d]`, w)
	for i := at + 1; i < len(g.lines); i++ {
		if g.lines[i] == send {
			g.moveBack(i, at)
			at++
		}
	}
}

// answered is the heard field of an answer line of write w: a token its client hearing it marks TRUE, FALSE once written.
func (g *gen) answered(w int) string {
	g.answer[w] = fmt.Sprintf("@heard%d@", len(g.lines))
	return g.answer[w]
}

// decide writes the peer line of decision r. Peer reads each reply from the state of the disk that sent it, and Format
// gives a node a new spec disk, so the line goes before the format of any disk that answered.
func (g *gen) decide(n int, r *trace.Record, kept, listed []string) {
	key := [3]uint64{uint64(n), r.Fields[0], r.Fields[1]}
	g.decisions[key] = true
	got := g.replies[key]
	var ds []uint64
	for d := range got {
		ds = append(ds, d)
	}
	slices.Sort(ds)
	var rep, j []string
	at := len(g.lines)
	for _, d := range ds {
		j = append(j, strconv.FormatUint(g.disk(d), 10))
		if p, ok := g.formatAt[g.disk(d)]; ok {
			at = min(at, p)
		}
		x := got[d]
		for _, h := range g.chunks {
			cv, ok := x.chunks[h]
			if !ok && g.chunkPG[g.chunk(h)-1] != g.pg(r.Fields[0]) {
				continue
			}
			list := "-1"
			if cv[2] != 0 {
				list = ver(cv[2] - 1)
			}
			rep = append(rep, fmt.Sprintf("[d |-> %d, c |-> %d, whole |-> %s, logged |-> %s, list |-> %s, decided |-> %d]", g.disk(d), g.chunk(h), ver(cv[0]), ver(cv[1]), list, x.decided))
		}
	}
	g.lineAt(at, `k |-> "peer", n |-> %d, p |-> %d, iv |-> %d, j |-> {%s}, kept |-> <<%s>>, listed |-> {%s}, rep |-> {%s}`,
		n, g.pg(r.Fields[0]), r.Fields[1], strings.Join(j, ", "), strings.Join(kept, ", "), strings.Join(listed, ", "), strings.Join(rep, ", "))
}

// persistedUnrecorded writes a persisted line for each member of decision r whose list became durable with no Persisted record, as when its node crashed before the store's answer: a later reply of its disk reports r's interval as its decision. It writes none for an interval activated later, since the primary activates only on answers it heard.
func (g *gen) persistedUnrecorded(n int, r *trace.Record, rest []trace.Record) {
	pg, iv := r.Fields[0], r.Fields[1]
	if slices.ContainsFunc(rest, func(x trace.Record) bool {
		return x.Name == trace.OutActivate && x.Fields[0] == pg && x.Fields[1] == iv
	}) {
		return
	}
	var ds []uint64
	for d := range g.replies[[3]uint64{uint64(n), pg, iv}] {
		ds = append(ds, d)
	}
	slices.Sort(ds)
	for _, d := range ds {
		recorded, durable := false, false
		for k := range rest {
			x := &rest[k]
			switch {
			case x.Name == trace.OutPersisted && x.Fields[0] == pg && x.Fields[1] == iv && x.Fields[2] == d:
				recorded = true
			case x.Name == trace.OutReply && x.Fields[0] == pg && x.Fields[2] == d && x.Fields[3] == iv:
				durable = true
			}
		}
		if durable && !recorded {
			key := [3]uint64{d, pg, iv}
			g.line(`k |-> "persisted", d |-> %d, p |-> %d, iv |-> %d, again |-> FALSE`, g.disk(d), g.pg(pg), iv)
			g.lists[key] = true
		}
	}
}

func (g *gen) write(w *bufio.Writer, module string) {
	fmt.Fprintf(w, "---- MODULE %s ----\n", module)
	fmt.Fprintln(w, "EXTENDS Integers")
	nodes := g.nodes
	fmt.Fprintf(w, "TNode == 1..%d\n", nodes)
	fmt.Fprintf(w, "TPG == 1..%d\n", len(g.pgs))
	fmt.Fprintf(w, "TChunk == 1..%d\n", max(len(g.chunks), 1))
	var a []string
	if len(g.chunks) == 0 {
		a = []string{"1"}
	}
	for _, p := range g.chunkPG {
		a = append(a, strconv.Itoa(p))
	}
	fmt.Fprintf(w, "TChunkPG == <<%s>>\n", strings.Join(a, ", "))
	a = nil
	if len(g.chunks) == 0 {
		a = []string{"1"}
	}
	for _, b := range g.blocks {
		a = append(a, strconv.Itoa(b))
	}
	fmt.Fprintf(w, "TBlocks == <<%s>>\n", strings.Join(a, ", "))
	var mem, ml []string
	for _, f := range g.pgs {
		e := g.initial[f]
		var d []string
		for _, m := range e.members {
			d = append(d, strconv.FormatUint(m.disk, 10))
		}
		mem = append(mem, "<<"+strings.Join(d, ", ")+">>")
		ml = append(ml, strconv.FormatUint(e.minLive, 10))
	}
	fmt.Fprintf(w, "TInitMem == <<%s>>\n", strings.Join(mem, ", "))
	fmt.Fprintf(w, "TMinLive == <<%s>>\n", strings.Join(ml, ", "))
	fmt.Fprintf(w, "TPlan == <<%s>>\n", strings.Join(g.plan, ",\n  "))
	var ws []string
	for _, x := range g.writes {
		ws = append(ws, fmt.Sprintf("[c |-> %d, blk |-> %d, whole |-> %s, inc |-> %d, re |-> 0, after |-> 0]", x.chunk, x.block, strings.ToUpper(strconv.FormatBool(x.whole)), x.inc))
	}
	fmt.Fprintf(w, "TWrites == <<%s>>\n", strings.Join(ws, ",\n  "))
	fmt.Fprintf(w, "TTables == %d\n", g.tables)
	fmt.Fprintf(w, "TMaxFormat == %d\n", max(g.fmts, 1))
	lines := strings.Join(g.lines, ",\n  ")
	lines = heardToken.ReplaceAllStringFunc(lines, func(t string) string { return strings.ToUpper(strconv.FormatBool(g.heardOK[t])) })
	fmt.Fprintf(w, "Trace == <<\n  %s\n>>\n", lines)
	fmt.Fprintln(w, "====")
}

var heardToken = regexp.MustCompile(`@heard[0-9]+@`)

func newGen() *gen {
	return &gen{entries: map[uint64]*entry{}, initial: map[uint64]entry{}, wid: map[[2]uint64]int{}, byID: map[uint64]int{}, verWrite: map[[2]uint64]int{}, answer: map[int]string{}, unanswered: map[int]int{}, mergedAt: map[int]int{}, takenAt: map[int]int{}, heardOK: map[string]bool{}, watermark: map[[3]uint64]uint64{}, epochs: map[uint64]int{}, replies: map[[3]uint64]map[uint64]*reply{}, decisions: map[[3]uint64]bool{},
		lists: map[[3]uint64]bool{}, operatorAsk: map[uint64]bool{}, restarted: map[[2]uint64]bool{}, diskIDs: map[uint64]uint64{}, primary: map[uint64]int{}, ownOpen: map[int][][2]uint64{}, memberOpen: map[int][][2]uint64{},
		activated: map[uint64]uint64{}, tableIv: map[uint64]map[uint64]uint64{}, left: map[[2]uint64]int{}, formatAt: map[uint64]int{}, outOfPool: map[uint64]bool{}, nodeDisk: map[int]uint64{}, recovering: map[int][][4]uint64{}, orderAt: map[[2]uint64]int{}, served: map[[3]uint64]bool{}, landAt: map[[3]uint64]int{}, refused: map[uint64][]refusal{}, past: map[[2]uint64][]string{}, listedBy: map[[4]uint64]bool{}}
}

func main() {
	module := flag.String("module", "trace_data", "module name")
	flag.Parse()
	if flag.NArg() != 1 {
		fatal("usage: tracedata [-module name] run.trace")
	}
	g := newGen()
	recs := g.load(flag.Arg(0))
	if len(recs) == 0 {
		fatal("no table in the trace")
	}
	g.run(recs)
	if !slices.ContainsFunc(g.lines, func(l string) bool {
		return !strings.HasPrefix(l, `[k |-> "table"`) && !strings.HasPrefix(l, `[k |-> "plan"`) && !strings.HasPrefix(l, `[k |-> "fold"`) && !strings.HasPrefix(l, `[k |-> "renew"`)
	}) {
		fatal("the trace has no protocol step")
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	g.write(w, *module)
}
