#!/bin/sh
# One-time root setup for the RDMA half of the spike: a Soft-RoCE (rdma_rxe) device on an
# ordinary NIC. No hugepages, no driver unbinding. Undo with ./teardown-root.sh.
# usage: sudo ./setup-root.sh [netdev]   (default: first UP non-virtual interface with an IPv4 address)
set -e
NETDEV=${1:-$(ip -4 -o addr show scope global | awk '$2 !~ /^(docker|br-|veth|virbr)/ {print $2; exit}')}
modprobe rdma_rxe
modprobe ib_uverbs
rdma dev show rxe0 >/dev/null 2>&1 || rdma link add rxe0 type rxe netdev "$NETDEV"
# Ubuntu without the rdma-core package leaves uverbs root-only; open it so the spike runs as a user.
chmod 666 /dev/infiniband/uverbs* /dev/infiniband/rdma_cm 2>/dev/null || true
rdma link show rxe0/1
ls -l /dev/infiniband
