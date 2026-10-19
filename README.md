# rock-solid

A distributed storage system in Go for homelab Kubernetes clusters, with ReadWriteMany shared filesystems first. It is in early development: nothing runs yet.

- [docs/design.md](docs/design.md): the whole design, from goals to on-disk format, every decision with the alternatives weighed, the [rules for Go code](docs/design.md#runtime-in-go), and the results of every spike.
- [docs/glossary.md](docs/glossary.md): the storage and distributed-systems terms the design uses, explained for readers new to storage.
- [docs/build-checklist.md](docs/build-checklist.md) and [docs/test-scenarios.md](docs/test-scenarios.md): the pieces of the first five phases, in build order, and the scenarios their tests cover.
- [blogs/](blogs/): twelve deep-dive posts that walk through the design topic by topic, each idea first in plain words and then in full detail, for engineers new to storage internals.
- [spikes/](spikes/) and [studies/](studies/): the experiments and outside findings the design rests on.

`scripts/lint.sh` runs every check; it needs only Go, which `go.mod` pins.

## Acknowledgments

The Raft shard will be [etcd-raft](https://github.com/etcd-io/raft) (Copyright The etcd Authors, Apache License 2.0), used as a Go module through a pinned fork of v3.6.0 with two changes: seeded election timeouts and v3.7.0's ReadIndex retry fix ([Raft shard](docs/design.md#metadata-layer-metastore)).
