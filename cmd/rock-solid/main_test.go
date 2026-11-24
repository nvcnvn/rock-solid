package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

func TestParseNodeRefusesABadConfiguration(t *testing.T) {
	ok := "-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a"
	if cfg, err := parseNode(strings.Fields(ok)); err != nil || cfg.MetaOnly {
		t.Fatalf("%q: %v, role meta %v", ok, err, cfg.MetaOnly)
	}
	if cfg, err := parseNode(strings.Fields(ok + " -role meta")); err != nil || !cfg.machine().MetaOnly {
		t.Fatalf("-role meta: %v, the node machine's role meta %v", err, cfg.machine().MetaOnly)
	}
	for _, args := range []string{
		"-node 4294967298 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2147483648 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 2147483648=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -peers 2147483648=127.0.0.1:7200 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -pool 4294967297 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 1 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -peers 1=127.0.0.1:7200 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -peers 3=127.0.0.1:7200,3=127.0.0.1:7300 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100",
		"-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:65535 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:0 -device /dev/a",
		"-node 2 -listen 127.0.0.1:7100 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -device /dev/a",
		"-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a -role data",
	} {
		if _, err := parseNode(strings.Fields(args)); err == nil {
			t.Errorf("%q taken", args)
		}
	}
}

// A voter's node ID past 32 bits, or in the upper half the command line draws its own from, is
// refused.
func TestParseCtlRefusesANodeOutOfRange(t *testing.T) {
	if _, err := parseCtl(strings.Fields("-node 1 -voters 1=127.0.0.1:7000 -disk d")); err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{
		"-node 4294967297 -voters 1=127.0.0.1:7000 -disk d",
		"-node 2147483648 -voters 2147483648=127.0.0.1:7000 -disk d",
		"-node 1 -voters 1=127.0.0.1:7000,2147483649=127.0.0.1:7001 -disk d",
	} {
		if _, err := parseCtl(strings.Fields(args)); err == nil {
			t.Errorf("%q taken", args)
		}
	}
}

func TestParseAdminTakesEachCommand(t *testing.T) {
	for args, want := range map[string]meta.Admin{
		"disk lost 5 -ctl 1=127.0.0.1:7000":                      {Op: meta.AdminLost, Disk: 5},
		"-ctl 1=127.0.0.1:7000 disk remove 6":                    {Op: meta.AdminRemove, Disk: 6},
		"disk drain 7 -ctl 1=127.0.0.1:7000":                     {Op: meta.AdminDrain, Disk: 7},
		"-ctl 1=127.0.0.1:7000 disk undrain 8":                   {Op: meta.AdminUndrain, Disk: 8},
		"disk failed -ctl 1=127.0.0.1:7000,2=127.0.0.1:7001":     {Op: meta.AdminFailed},
		"pool 2 -ctl 1=127.0.0.1:7000 -preset rep2 -pg-count 64": {Op: meta.AdminPool, Pool: 2, Spec: chunkstore.PoolSpec{Replicas: new(2), PGCount: new(uint32(64))}},
		"pool 3 -ctl 1=127.0.0.1:7000 -overcommit=false -failure-domain disk": {Op: meta.AdminPool, Pool: 3,
			Spec: chunkstore.PoolSpec{Overcommit: new(false), FailureDomain: "disk"}},
		"pg give-up 2 0 -ctl 1=127.0.0.1:7000": {Op: meta.AdminGiveUp, Pool: 2},
		"pg give-up 2 7 -ctl 1=127.0.0.1:7000": {Op: meta.AdminGiveUp, Pool: 2, PG: 7},
	} {
		a, cfg, err := parseAdmin(strings.Fields(args))
		if err != nil || a.Op != want.Op || a.Disk != want.Disk || a.Pool != want.Pool || a.PG != want.PG || !reflect.DeepEqual(a.Spec, want.Spec) || cfg.Group != 1 || len(cfg.Ctls) == 0 {
			t.Errorf("%q parsed as %+v, %+v, %v", args, a, cfg, err)
		}
	}
	for _, args := range []string{
		"disk lost -ctl 1=127.0.0.1:7000",
		"disk lost 0 -ctl 1=127.0.0.1:7000",
		"disk lost 5",
		"disk lost 5 -ctl 1=127.0.0.1:7000 -replicas 2",
		"disk drop 5 -ctl 1=127.0.0.1:7000",
		"disk drain 0 -ctl 1=127.0.0.1:7000",
		"disk undrain 5 -ctl 1=127.0.0.1:7000 -replicas 2",
		"pool -ctl 1=127.0.0.1:7000",
		"pool 4294967296 -ctl 1=127.0.0.1:7000",
		"pool 1 -ctl 1=127.0.0.1:7000 -preset rep9",
		"pool 1 -ctl 1=127.0.0.1:7000 -write-limit 300",
		"pool 1 -ctl 1=127.0.0.1:7000 -replicas -1",
		"pg give-up 2 -ctl 1=127.0.0.1:7000",
		"pg give-up 0 1 -ctl 1=127.0.0.1:7000",
		"pg give-up 2 x -ctl 1=127.0.0.1:7000",
		"pg give-up 2 4294967296 -ctl 1=127.0.0.1:7000",
		"pg give-up 2 1 -ctl 1=127.0.0.1:7000 -replicas 2",
	} {
		if _, _, err := parseAdmin(strings.Fields(args)); err == nil {
			t.Errorf("%q taken", args)
		}
	}
}

// A give-up prints the interval it gave up and each chunk it dropped, or that it dropped none, and how many more stay unfound.
func TestAGiveUpPrintsWhatItDropped(t *testing.T) {
	a := meta.Admin{Op: meta.AdminGiveUp, Pool: 1, PG: 3}
	two := []core.ChunkID{{Volume: 1, Index: 4}, {Volume: 1, Inode: 2, Index: 9}}
	for g, want := range map[*meta.AdminAnswer]string{
		{GiveUp: chunkstore.GiveUp{Interval: 5, All: true}, Unfound: 3}:                    "PG 1.3 interval 5 given up, every chunk kept as its members hold it\n",
		{GiveUp: chunkstore.GiveUp{Interval: 6, Drop: true, Chunks: two}, Unfound: 2}:      "PG 1.3 interval 6 given up, 2 chunks dropped\nchunk 1/0/4 dropped\nchunk 1/2/9 dropped\n",
		{GiveUp: chunkstore.GiveUp{Interval: 6, Drop: true, Chunks: two[:1]}, Unfound: 70}: "PG 1.3 interval 6 given up, 1 chunks dropped\nchunk 1/0/4 dropped\n69 more chunks stay unfound: give up again\n",
	} {
		var out bytes.Buffer
		printDone(&out, &a, g)
		if out.String() != want {
			t.Errorf("%+v printed %q, want %q", *g, out.String(), want)
		}
	}
}

// GOMEMLIMIT follows the budget less every slab mapped, at start and as page slabs grow; a budget
// the start's slabs fill is refused, and none leaves GOMEMLIMIT alone.
func TestTheHeapLimitIsTheBudgetLessTheMappedSlabs(t *testing.T) {
	var logged bytes.Buffer
	defer log.SetOutput(log.Writer())
	log.SetOutput(&logged)
	var limits []int64
	h := newHeapLimit(256)
	h.set = func(n int64) int64 { limits = append(limits, n); return 0 }
	h.add(64 << 20)
	if err := h.check(64 << 20); err != nil {
		t.Fatal(err)
	}
	h.add(64 << 20)
	h.add(64 << 20)
	if logged.Len() != 0 {
		t.Fatalf("logged %q with the heap at its minimum", logged.String())
	}
	h.add(64 << 20)
	h.add(64 << 20)
	if want := []int64{192 << 20, 128 << 20, 64 << 20, 64 << 20, 64 << 20}; !reflect.DeepEqual(limits, want) {
		t.Fatalf("limits %v, want %v", limits, want)
	}
	if n := strings.Count(logged.String(), "\n"); n != 1 {
		t.Fatalf("logged %d lines, want 1, as the slabs passed the minimum heap: %q", n, logged.String())
	}
	for _, c := range []struct {
		budget, pages int64
		ok            bool
	}{{192, 64 << 20, true}, {191, 64 << 20, false}, {192, 128 << 20, false}} {
		b := newHeapLimit(c.budget)
		b.set = func(int64) int64 { return 0 }
		b.add(64 << 20)
		if err := b.check(c.pages); (err == nil) != c.ok {
			t.Fatalf("-memory %d MiB with a 64 MiB pool and %d MiB of page slabs: %v", c.budget, c.pages>>20, err)
		}
	}
	none := newHeapLimit(0)
	none.set = func(int64) int64 { t.Fatal("no budget set a limit"); return 0 }
	none.add(1 << 30)
	if none.check(1<<30) != nil {
		t.Fatal("no budget was refused")
	}
	if _, err := parseNode(strings.Fields("-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a -memory -1")); err == nil {
		t.Fatal("a negative -memory was taken")
	}
}

// A deleted control-plane voter exits with its own status, so a service manager can leave it down;
// every other failure exits 1.
func TestADeletedVoterExitsWithItsOwnStatus(t *testing.T) {
	if got := exitStatus(fmt.Errorf("ctl: %w", meta.ErrDeleted)); got != exitDeleted || exitDeleted == 1 || exitDeleted == 2 {
		t.Fatalf("a deleted voter exits %d, exitDeleted %d: want a status of its own", got, exitDeleted)
	}
	if got := exitStatus(errors.New("disk failed")); got != 1 {
		t.Fatalf("a failed voter exits %d, want 1", got)
	}
}

// A -max-replay-bytes above a quarter of the disk is refused: the reserve keeps room for a
// checkpoint of up to twice it, and the records it bounds take up to twice it again.
func TestTheMaxReplayBytesFitAQuarterOfTheDisk(t *testing.T) {
	if err := replayFits(256<<20, 1<<30); err != nil {
		t.Fatal(err)
	}
	if err := replayFits(256<<20+1, 1<<30); err == nil {
		t.Fatal("a max_replay_bytes above a quarter of the disk was taken")
	}
}
