package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/ops"
)

// partWait is how long a facts request waits for every loop's part: a loop is off the data path
// between events, so one that has not answered by then is stuck.
const partWait = 5 * time.Second

// facts answers GET /facts and /facts/pg/<pool>.<pg> on the admin port from the process's own
// state: each loop copies its part at its next event through its mail, and nothing asks another
// node. Requests take turns, and an answer is built at most once a second per path.
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
	// fill copies loop i's part into f, pg its part of a PG's facts; each runs on loop i.
	fill func(i int, ctx *core.Context, f *ops.Facts)
	pg   func(i int, p *ops.PGFacts)

	mu    sync.Mutex
	parts []ops.Facts
	cache map[string]ops.Cache
	ln    net.Listener
	srv   *http.Server
}

// listen listens on addr for the admin port; an empty addr leaves the port off.
func (s *facts) listen(addr string) error {
	s.start, s.run, s.parts, s.cache = time.Now(), rand.Uint64(), make([]ops.Facts, len(s.mails)), map[string]ops.Cache{}
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
	var build func() ([]byte, error)
	switch rest, isPG := strings.CutPrefix(r.URL.Path, "/facts/pg/"); {
	case r.URL.Path == "/facts":
		build = s.facts
	case isPG:
		pool, pg, ok := strings.Cut(rest, ".")
		p, err1 := strconv.ParseUint(pool, 10, 32)
		g, err2 := strconv.ParseUint(pg, 10, 32)
		if !ok || err1 != nil || err2 != nil {
			http.Error(w, "not /facts/pg/<pool>.<pg>", http.StatusNotFound)
			return
		}
		build = func() ([]byte, error) { return s.pgFacts(uint32(p), uint32(g)) }
	default:
		http.NotFound(w, r)
		return
	}
	body, err := s.answer(r.URL.Path, build)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// answer is the answer to path built within the last second, or one build makes now.
func (s *facts) answer(path string, build func() ([]byte, error)) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := core.TickFromDuration(time.Since(s.start))
	c := s.cache[path]
	if b := c.Fresh(now); b != nil {
		return b, nil
	}
	b, err := build()
	if err == nil {
		c.Keep(now, b)
		s.cache[path] = c
	}
	return b, err
}

// gather has each loop run f(i, ctx) at its next event, and waits for all of them.
func (s *facts) gather(f func(i int, ctx *core.Context)) error {
	done := make(chan struct{}, len(s.mails))
	for i, m := range s.mails {
		m.post(func(ctx *core.Context) {
			f(i, ctx)
			done <- struct{}{}
		})
	}
	timeout := time.After(partWait)
	for range s.mails {
		select {
		case <-done:
		case <-timeout:
			// A late loop still writes its part: the next request fills new ones.
			s.parts = make([]ops.Facts, len(s.mails))
			return fmt.Errorf("a loop did not answer within %v", partWait)
		}
	}
	return nil
}

func (s *facts) facts() ([]byte, error) {
	err := s.gather(func(i int, ctx *core.Context) {
		s.parts[i].Reset()
		s.fill(i, ctx, &s.parts[i])
	})
	if err != nil {
		return nil, err
	}
	p := &s.parts[0]
	f := ops.Facts{Schema: ops.FactsV1, Node: p.Node, Map: p.Map, Session: p.Session, Mounts: s.mounts}
	f.Node.Run, f.Node.UptimeUs, f.Node.Wall = s.run, core.TickFromDuration(time.Since(s.start)), time.Now().UTC().Format(time.RFC3339Nano)
	f.Node.Flags, f.Node.Hosts = s.flags, s.hosts
	for i := range s.parts {
		f.Add(&s.parts[i])
	}
	for i := range f.Disks {
		if k := &f.Disks[i]; int(k.Loop) < len(s.paths) {
			k.Path = s.paths[k.Loop]
		}
	}
	return json.Marshal(&f)
}

func (s *facts) pgFacts(pool, pg uint32) ([]byte, error) {
	ps := make([]ops.PGFacts, len(s.mails))
	for i := range ps {
		ps[i] = ops.PGFacts{Pool: pool, PG: pg}
	}
	if err := s.gather(func(i int, _ *core.Context) { s.pg(i, &ps[i]) }); err != nil {
		return nil, err
	}
	p := ops.PGFacts{Schema: ops.PGFactsV1, Node: s.node, Pool: pool, PG: pg}
	for i := range ps {
		p.Members = append(p.Members, ps[i].Members...)
		p.Unfound = append(p.Unfound, ps[i].Unfound...)
	}
	return json.Marshal(&p)
}
