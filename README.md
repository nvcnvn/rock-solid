# rock-solid

## Acknowledgments

The Raft implementation began as a learning exercise. We read [etcd-raft](https://github.com/etcd-io/raft) (Go, Copyright The etcd Authors, Apache License 2.0) and ported it to Rust file by file, together with its tests; see the [Raft port spike](spikes/raft-port/README.md) and [ADR 0007](docs/decisions/0007-port-etcd-raft.md). Each ported file keeps the etcd copyright line and says what changed. We plan to rewrite it later as idiomatic, optimized Rust; until then it follows upstream releases.
