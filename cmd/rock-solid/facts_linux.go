package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/ops"
)

// partWait is how long a facts request waits for every loop's part: a loop is off the data path
// between events, so one that has not answered by then is stuck.
const partWait = 5 * time.Second

// errNoPG refuses a PG no loop's table holds.
var errNoPG = errors.New("no such PG in the node's tables")

// facts answers GET /facts and /facts/pg/<pool>.<pg> on the admin port from the process's own
// state: each loop copies its part at its next event through its mail, and nothing asks another
// node. One answer is built at a time, at most once a second per path; a loop that does not answer
// in time is named in the answer, and gets no new call until it runs the one it has.
type facts struct {
	node   uint32
	start  time.Time
	run    uint64
	flags  []ops.Flag
	hosts  []ops.Host
	mounts []ops.Mount
	// paths are each loop's device path, "" for a loop with none.
	paths []string
	mails []*mail
	// fill copies loop i's part into f, pg its part of a PG's facts and whether its table holds the
	// PG; each runs on loop i.
	fill func(i int, ctx *core.Context, f *ops.Facts)
	pg   func(i int, p *ops.PGFacts) bool
	wait time.Duration

	// parts[i] is written only by loop i's call, and read only once that call answered.
	parts []ops.Facts
	// late[i] is set while loop i holds a call it has not run.
	late []atomic.Bool

	mu sync.Mutex
	// flight is closed when the answer being built is done, nil while none is.
	flight chan struct{}
	cache  map[factsKey]ops.Cache
	ln     net.Listener
	srv    *http.Server
}

// factsKey is a path's answer: /facts, or a PG's.
type factsKey struct {
	pg        bool
	pool, idx uint32
}

// listen listens on addr for the admin port; an empty addr leaves the port off.
func (s *facts) listen(addr string) error {
	s.start, s.run, s.wait = time.Now(), rand.Uint64(), partWait
	s.parts, s.late, s.cache = make([]ops.Facts, len(s.mails)), make([]atomic.Bool, len(s.mails)), map[factsKey]ops.Cache{}
	if addr == "" {
		return nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("-admin: %w", err)
	}
	s.ln, s.srv = ln, &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	return nil
}

// serve answers until close.
func (s *facts) serve() {
	if s.srv == nil {
		return
	}
	go func() {
		if err := s.srv.Serve(s.ln); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("admin: %v", err)
		}
	}()
}

func (s *facts) close() {
	if s.srv != nil {
		_ = s.srv.Close()
	}
}

func (s *facts) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	var k factsKey
	build := s.facts
	switch rest, isPG := strings.CutPrefix(r.URL.Path, "/facts/pg/"); {
	case r.URL.Path == "/facts":
	case isPG:
		pool, pg, ok := strings.Cut(rest, ".")
		p, err1 := strconv.ParseUint(pool, 10, 32)
		g, err2 := strconv.ParseUint(pg, 10, 32)
		if !ok || err1 != nil || err2 != nil {
			http.Error(w, "not /facts/pg/<pool>.<pg>", http.StatusNotFound)
			return
		}
		k = factsKey{pg: true, pool: uint32(p), idx: uint32(g)}
		build = func() ([]byte, error) { return s.pgFacts(k.pool, k.idx) }
	default:
		http.NotFound(w, r)
		return
	}
	body, err := s.answer(r.Context().Done(), k, build)
	switch {
	case errors.Is(err, errNoPG):
		http.Error(w, err.Error(), http.StatusNotFound)
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

func (s *facts) now() core.Tick { return core.TickFromDuration(time.Since(s.start)) }

// answer is k's answer built within the last second, or the one build makes now once no other
// build runs; it gives up when gone closes.
func (s *facts) answer(gone <-chan struct{}, k factsKey, build func() ([]byte, error)) ([]byte, error) {
	for {
		s.mu.Lock()
		c := s.cache[k]
		if b := c.Fresh(s.now()); b != nil {
			s.mu.Unlock()
			if len(b) == 0 {
				return nil, errNoPG
			}
			return b, nil
		}
		if fl := s.flight; fl != nil {
			s.mu.Unlock()
			select {
			case <-fl:
				continue
			case <-gone:
				return nil, errors.New("the request ended")
			}
		}
		s.flight = make(chan struct{})
		s.mu.Unlock()
		return s.buildOnce(k, build)
	}
}

// buildOnce runs build as the one build in flight and keeps its answer for a second; a PG no table
// holds is kept as an empty answer, so asking for it again is rate-limited like any other path.
func (s *facts) buildOnce(k factsKey, build func() ([]byte, error)) (b []byte, err error) {
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		now := s.now()
		for k, c := range s.cache {
			if c.Fresh(now) == nil {
				delete(s.cache, k)
			}
		}
		if keep := b; err == nil || errors.Is(err, errNoPG) {
			if keep == nil {
				keep = []byte{}
			}
			c := ops.Cache{}
			c.Keep(now, keep)
			s.cache[k] = c
		}
		close(s.flight)
		s.flight = nil
	}()
	return build()
}

// gather has each loop without a late call run f(i, ctx) at its next event, waits for them, and
// returns the loops that did not answer in time.
func (s *facts) gather(f func(i int, ctx *core.Context)) []uint32 {
	done := make(chan int, len(s.mails))
	answered := make([]bool, len(s.mails))
	posted := 0
	for i, m := range s.mails {
		if s.late[i].Load() {
			continue
		}
		s.late[i].Store(true)
		posted++
		m.post(func(ctx *core.Context) {
			f(i, ctx)
			s.late[i].Store(false)
			done <- i
		})
	}
	timeout := time.NewTimer(s.wait)
	defer timeout.Stop()
wait:
	for ; posted > 0; posted-- {
		select {
		case i := <-done:
			answered[i] = true
		case <-timeout.C:
			break wait
		}
	}
	var late []uint32
	for i, ok := range answered {
		if !ok {
			late = append(late, uint32(i))
		}
	}
	return late
}

func (s *facts) facts() ([]byte, error) {
	late := s.gather(func(i int, ctx *core.Context) {
		s.parts[i].Reset()
		s.fill(i, ctx, &s.parts[i])
	})
	f := ops.Facts{Schema: ops.FactsV1, Node: ops.Node{ID: s.node}, Mounts: s.mounts, Unanswered: late}
	if !slices.Contains(late, 0) {
		p := &s.parts[0]
		f.Node, f.Map, f.Session = p.Node, p.Map, p.Session
	}
	f.Node.Run, f.Node.UptimeUs, f.Node.Wall = s.run, s.now(), time.Now().UTC().Format(time.RFC3339Nano)
	f.Node.Flags, f.Node.Hosts = s.flags, s.hosts
	for i := range s.parts {
		if !slices.Contains(late, uint32(i)) {
			f.Add(&s.parts[i])
		}
	}
	for i := range f.Disks {
		if k := &f.Disks[i]; int(k.Loop) < len(s.paths) {
			k.Path = s.paths[k.Loop]
		}
	}
	return json.Marshal(&f)
}

// pgFacts is PG pool.pg's answer, refused when every loop answered and none's table holds the PG.
func (s *facts) pgFacts(pool, pg uint32) ([]byte, error) {
	ps := make([]ops.PGFacts, len(s.mails))
	known := make([]bool, len(s.mails))
	for i := range ps {
		ps[i] = ops.PGFacts{Pool: pool, PG: pg}
	}
	late := s.gather(func(i int, _ *core.Context) { known[i] = s.pg(i, &ps[i]) })
	p := ops.PGFacts{Schema: ops.PGFactsV1, Node: s.node, Pool: pool, PG: pg, Unanswered: late}
	held := false
	for i := range ps {
		if !slices.Contains(late, uint32(i)) {
			held = held || known[i]
			p.Members = append(p.Members, ps[i].Members...)
			p.Unfound = append(p.Unfound, ps[i].Unfound...)
		}
	}
	if !held && len(late) == 0 {
		return nil, errNoPG
	}
	return json.Marshal(&p)
}
