#!/bin/sh
# usage: sudo ./teardown-root.sh
rdma link delete rxe0 2>/dev/null || true
modprobe -r rdma_rxe 2>/dev/null || true
