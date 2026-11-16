//go:build !linux

package main

import "errors"

type ctl struct{}

func newCtl(ctlConfig) (*ctl, error) {
	return nil, errors.New("rock-solid ctl runs on Linux only: it needs epoll and io_uring")
}

func (*ctl) Run() error { return nil }

func (*ctl) Stop() {}
