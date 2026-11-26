package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/rock-solid/internal/core"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

// volume runs `rock-solid volume create` with args against the voter at ctlPort and the node at
// nodePort, and returns its exit code and what it printed.
func volume(ctlPort, nodePort int, args ...string) (int, string) {
	var out bytes.Buffer
	args = append([]string{"volume", "create", "-ctl", fmt.Sprintf("1=127.0.0.1:%d", ctlPort), "-group", fmt.Sprint(ctlGroup),
		"-peers", fmt.Sprintf("2=127.0.0.1:%d", nodePort), "-timeout", "30s"}, args...)
	code := runVolume(args, &out, &out)
	return code, out.String()
}

// Volumes made by the command line over TCP on one storage node whose device runs on io_uring:
// the leader adds the first data group once the node reports and the node hosts its voter; a
// create takes ID 1; the Txn of a create sent twice, as after a lost answer, is answered both
// times and makes one volume, so the next create takes ID 3; and after the node and the control
// plane restart, the map keeps its data group, volume 1's record reads back and the next create
// takes ID 4.
func TestVolumesCreatedOverTCPAreKeptAcrossARestart(t *testing.T) {
	dir := testDir(t)
	base := freeBase(t, 3)
	pc, stop := runCtlOn(t, dir, base, true)
	dev := filepath.Join(dir, "vol-dev")
	device(t, dev, false)
	args := fmt.Sprintf("-node 2 -ctl 1=127.0.0.1:%d -group %d -listen 127.0.0.1:%d -device %s", base, ctlGroup, base+1, dev)
	var out output
	cmd := startNode(t, args, &out)
	hosted := func(p *ctl) bool {
		return within(60*time.Second, func() bool {
			return onCtl(t, p, func(_ *core.Context, c *meta.Control) bool { return hosts(c, 2, 1, 1) })
		})
	}
	if !hosted(pc) {
		t.Fatalf("the node never hosted the first data group's voter; the node said:\n%s", out.String())
	}
	if code, said := admin(pc, "pool", "1", "-replicas", "1", "-min-live", "1", "-failure-domain", "disk", "-pg-count", "8"); code != 0 {
		t.Fatalf("the pool command exited %d: %s", code, said)
	}
	if code, said := volume(base, base+1, "-pool", "1", "-size", "1073741824"); code != 0 || !strings.Contains(said, "volume 1 created") {
		t.Fatalf("the first create exited %d: %s\nthe node said:\n%s", code, said, out.String())
	}
	if code, said := volume(base, base+1, "-pool", "9", "-size", "1"); code != 1 || !strings.Contains(said, "pool 9 does not exist") {
		t.Fatalf("a create in no pool exited %d: %s", code, said)
	}

	cfg, err := parseVolume([]string{"volume", "create", "-ctl", fmt.Sprintf("1=127.0.0.1:%d", base), "-group", fmt.Sprint(ctlGroup),
		"-peers", fmt.Sprintf("2=127.0.0.1:%d", base+1), "-pool", "1", "-size", "1"})
	if err != nil {
		t.Fatal(err)
	}
	var m meta.Membership
	if _, err := sendAdmin(cfg.adminConfig, &meta.Admin{Op: meta.AdminMembership}, &m); err != nil {
		t.Fatal(err)
	}
	g := &m.Plans[1]
	f := meta.FS{Volume: 2, Req: &meta.Request{ID: 5}}
	if err := f.CreateVolume(&cfg.Volume, &meta.Attr{Mode: meta.ModeDir | 0o755, Nlink: 2}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if r, err := sendTxn(cfg.Peers, &m, g, &f.Txn, time.Now().Add(30*time.Second)); err != nil || r.FailedCheck != -1 {
			t.Fatalf("send %d of one create got %+v, %v", i, r, err)
		}
	}
	if code, said := volume(base, base+1, "-pool", "1", "-size", "1"); code != 0 || !strings.Contains(said, "volume 3 created") {
		t.Fatalf("the create after a resent one exited %d: %s", code, said)
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	stop()
	var again output
	startNode(t, args, &again)
	pc, _ = runCtlOn(t, dir, base, false)
	if !hosted(pc) {
		t.Fatalf("after the restart the node never hosted the first data group's voter; the node said:\n%s", again.String())
	}
	if n := onCtl(t, pc, func(_ *core.Context, c *meta.Control) int { return len(c.ShardMap().Groups) }); n != 2 {
		t.Fatalf("after the restart the map has %d groups", n)
	}
	rec := meta.AppendVolume(nil, &meta.Volume{Pool: 1, Size: 1 << 30})
	key, _ := meta.AppendKey(nil, &meta.Key{Space: meta.SpaceVolume, Volume: 1, Tag: meta.VolumeRecord})
	if r, err := sendTxn(cfg.Peers, &m, g, &meta.Txn{Ops: []meta.Op{meta.Check(key, rec, false)}}, time.Now().Add(60*time.Second)); err != nil || r.FailedCheck != -1 {
		t.Fatalf("volume 1's record after the restart: %+v, %v", r, err)
	}
	if code, said := volume(base, base+1, "-pool", "1", "-size", "1"); code != 0 || !strings.Contains(said, "volume 4 created") {
		t.Fatalf("the create after the restart exited %d: %s\nthe node said:\n%s", code, said, again.String())
	}
}
