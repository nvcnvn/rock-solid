# rock-solid

A distributed storage system in Go for homelab Kubernetes clusters, with ReadWriteMany shared filesystems first. The design is in [docs/design.md](docs/design.md); [ADR 0009](docs/decisions/0009-write-in-go.md) records why it is written in Go and the rules its code follows.

## Acknowledgments

The Raft shard is [etcd-raft](https://github.com/etcd-io/raft) (Copyright The etcd Authors, Apache License 2.0), used as a Go module with one patch that seeds its election timeouts. An earlier port of it to Rust, file by file with its tests, is kept as the [Raft port spike](spikes/raft-port/README.md).
