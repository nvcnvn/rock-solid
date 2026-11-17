package main

import (
	"strings"
	"testing"
)

func TestParseNodeRefusesABadConfiguration(t *testing.T) {
	ok := "-node 2 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a"
	if _, err := parseNode(strings.Fields(ok)); err != nil {
		t.Fatalf("%q: %v", ok, err)
	}
	for _, args := range []string{
		"-node 4294967298 -ctl 1=127.0.0.1:7000 -listen 127.0.0.1:7100 -device /dev/a",
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

func TestParseCtlRefusesANodePast32Bits(t *testing.T) {
	if _, err := parseCtl(strings.Fields("-node 4294967297 -voters 1=127.0.0.1:7000 -disk d")); err == nil {
		t.Fatal("-node 4294967297 taken as node 1")
	}
}
