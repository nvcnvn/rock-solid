# rock-solid

A distributed storage system in Go for homelab Kubernetes clusters, with ReadWriteMany shared filesystems first. It is in early development: nothing runs yet.

- [docs/design.md](docs/design.md): the design, from goals to on-disk format, with the results of every spike.
- [docs/decisions/](docs/decisions/): architecture decision records; [ADR 0009](docs/decisions/0009-write-in-go.md) records why it is written in Go and the rules its code follows.
- [docs/build-checklist.md](docs/build-checklist.md) and [docs/test-scenarios.md](docs/test-scenarios.md): the pieces of the first five phases, in build order, and the scenarios their tests cover.
- [spikes/](spikes/) and [studies/](studies/): the experiments and outside findings the design rests on.

`scripts/lint.sh` runs every check; it needs only Go, which `go.mod` pins.

## Acknowledgments

The Raft shard is [etcd-raft](https://github.com/etcd-io/raft) (Copyright The etcd Authors, Apache License 2.0), used as a Go module with one patch that seeds its election timeouts. An earlier port of it to Rust, file by file with its tests, is kept as the [Raft port spike](spikes/raft-port/README.md).
