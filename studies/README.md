# Studies

Findings about other projects, written up for review before deciding what to do with them (report upstream, work around, or drop). Each study states what was found, how to reproduce it from a clean checkout, and what is still unverified.

| Study | Project | Status |
| --- | --- | --- |
| [etcd-raft-async-snapshot-apply](etcd-raft-async-snapshot-apply/README.md) | etcd-raft v3.7.0 and `main` | Reproduced, not reported |
| [session-expiry](session-expiry/README.md) | CephFS, NFSv4.1, Chubby, ZooKeeper, etcd, HDFS, Kubernetes, Lustre | Read from source; proposal adopted in the design |
| [sub-block-writes](sub-block-writes/README.md) | Ceph BlueStore and erasure-coded pools, CephFS, Vitastor, JuiceFS, SeaweedFS, OpenZFS, ext4, XFS, Btrfs, F2FS, Lustre, Linux FUSE, ublk and krbd, QEMU, KubeVirt | Read from source plus a model spike; proposal adopted in the design |
| [capacity-and-enospc](capacity-and-enospc/README.md) | Ceph (`MAX AVAIL`, full flags, CephFS, RBD), Vitastor, Longhorn, CockroachDB, MinIO, Kubernetes CSI capacity, ext4, XFS, Btrfs, Linux NFS client, `errseq_t` and FUSE, JuiceFS, dm-thin and LVM thin, ublk and NVMe | Read from source plus a model spike; a root-only block spike written, not run; proposal awaiting review |
