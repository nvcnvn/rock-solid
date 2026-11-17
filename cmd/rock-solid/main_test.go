package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/nvcnvn/rock-solid/internal/chunkstore"
	"github.com/nvcnvn/rock-solid/internal/meta"
)

func TestParseNodeRefusesABadConfiguration(t *testing.T) {
	ok := "-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a"
	if _, err := parseNode(strings.Fields(ok)); err != nil {
		t.Fatalf("%q: %v", ok, err)
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
		"disk failed -ctl 1=127.0.0.1:7000,2=127.0.0.1:7001":     {Op: meta.AdminFailed},
		"pool 2 -ctl 1=127.0.0.1:7000 -preset rep2 -pg-count 64": {Op: meta.AdminPool, Pool: 2, Spec: chunkstore.PoolSpec{Replicas: new(2), PGCount: new(uint32(64))}},
		"pool 3 -ctl 1=127.0.0.1:7000 -overcommit=false -failure-domain disk": {Op: meta.AdminPool, Pool: 3,
			Spec: chunkstore.PoolSpec{Overcommit: new(false), FailureDomain: "disk"}},
	} {
		a, cfg, err := parseAdmin(strings.Fields(args))
		if err != nil || a.Op != want.Op || a.Disk != want.Disk || a.Pool != want.Pool || !reflect.DeepEqual(a.Spec, want.Spec) || cfg.Group != 1 || len(cfg.Ctls) == 0 {
			t.Errorf("%q parsed as %+v, %+v, %v", args, a, cfg, err)
		}
	}
	for _, args := range []string{
		"disk lost -ctl 1=127.0.0.1:7000",
		"disk lost 0 -ctl 1=127.0.0.1:7000",
		"disk lost 5",
		"disk lost 5 -ctl 1=127.0.0.1:7000 -replicas 2",
		"disk drop 5 -ctl 1=127.0.0.1:7000",
		"pool -ctl 1=127.0.0.1:7000",
		"pool 4294967296 -ctl 1=127.0.0.1:7000",
		"pool 1 -ctl 1=127.0.0.1:7000 -preset rep9",
		"pool 1 -ctl 1=127.0.0.1:7000 -write-limit 300",
		"pool 1 -ctl 1=127.0.0.1:7000 -replicas -1",
	} {
		if _, _, err := parseAdmin(strings.Fields(args)); err == nil {
			t.Errorf("%q taken", args)
		}
	}
}
