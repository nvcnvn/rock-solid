//go:build !linux

package main

import "errors"

type storageNode struct{}

func newNode(nodeConfig) (*storageNode, error) {
	return nil, errors.New("rock-solid node runs on Linux only: it needs epoll and io_uring")
}

func (*storageNode) Run() error { return nil }

func (*storageNode) Stop() {}
