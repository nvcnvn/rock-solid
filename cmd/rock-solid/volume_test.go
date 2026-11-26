package main

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// A membership that does not yet say where a create goes is one to read again: no pool known, as
// just after a control-plane failover, no shard map, or a data group with no voter; a pool the
// grants do not hold is refused.
func TestACreateWaitsForAMembershipThatPlacesIt(t *testing.T) {
	f := meta.FS{Volume: 1, Req: &meta.Request{ID: 7}}
	if err := f.CreateVolume(&meta.Volume{Pool: 1, Size: 1}, &meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2}); err != nil {
		t.Fatal(err)
	}
	groups := func(voters ...meta.Replica) []meta.Group {
		return []meta.Group{{ID: 1}, {ID: 2, Start: []byte{meta.SpaceCtl + 1}, Voters: voters}}
	}
	pool1 := []meta.PoolGrant{{Pool: 1}}
	for _, c := range []struct {
		name string
		m    meta.Membership
		want string
	}{
		{"no pool known", meta.Membership{Plans: groups(meta.Replica{ID: 1})}, "not yet"},
		{"no such pool", meta.Membership{Grants: []meta.PoolGrant{{Pool: 2}}, Plans: groups(meta.Replica{ID: 1})}, "pool 1 does not exist"},
		{"no map", meta.Membership{Grants: pool1}, "not yet"},
		{"no voter", meta.Membership{Grants: pool1, Plans: groups()}, "not yet"},
		{"placed", meta.Membership{Grants: pool1, Plans: groups(meta.Replica{ID: 1})}, ""},
	} {
		g, err := target(&c.m, 1, &f.Txn)
		switch {
		case c.want == "" && (err != nil || g.ID != 2):
			t.Errorf("%s: %+v, %v", c.name, g, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want) || errors.Is(err, errNotYet) != (c.want == "not yet")):
			t.Errorf("%s: %+v, %v", c.name, g, err)
		}
	}
}

// A create sent and never answered may have made its volume, which the error says; one never sent
// did not.
func TestAnUnansweredCreateMayHaveMadeItsVolume(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			defer func() { _ = c.Close() }()
		}
	}()
	m := meta.Membership{Disks: []meta.DiskAddr{{Disk: 1, At: core.NodeID{Node: 2}}}}
	g := meta.Group{ID: 2, Voters: []meta.Replica{{ID: 1, Node: 2, Disk: 1}}}
	txn := meta.Txn{Ops: []meta.Op{meta.Put([]byte{meta.SpaceVolume, 1}, []byte{1})}}
	silent := []voter{{Node: 2, Addr: netip.MustParseAddrPort(l.Addr().String())}}
	if _, err := sendTxn(silent, &m, &g, &txn, time.Now().Add(time.Second)); !errors.Is(err, errUnanswered) {
		t.Fatalf("a create sent to a silent leader got %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sendTxn(silent, &m, &g, &txn, time.Now().Add(time.Second)); err == nil || errors.Is(err, errUnanswered) {
		t.Fatalf("a create never sent got %v", err)
	}
}
