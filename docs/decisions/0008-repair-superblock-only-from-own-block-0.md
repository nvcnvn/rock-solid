# 0008. Restore block 0 from the superblock copy only while block 0 is this format's

Status: Accepted (2026-10-17)

## Context
The disk log keeps its superblock twice, in the first and the last block of segment 0 ([On-disk format](../design.md#on-disk-format)), so one bad block cannot make a disk unreadable. Recovery must decide when to trust the copy and when to write it back over block 0.

A block 0 that does not decode is not always damage. A disk can be reused without the copy being cleared: a partition table (`parted`, `sgdisk`), a new filesystem (`mkfs`), an LVM or BlueStore label, or a partial wipe with `wipefs` or `dd` all write at the start of the disk, and the copy sits up to 64 MiB in. Device names such as `/dev/nvme1n1` can also change across reboots, so a node can be pointed at another disk that once held a log. Writing the copy back over such a block 0 destroys the new owner's data.

Other systems:

- ext4, XFS and Btrfs mount from the primary superblock only. Their copies are used by offline tools an operator runs (`e2fsck -b`, `xfs_repair`, `btrfs rescue super-recover`, which asks before it writes).
- The Linux kernel reads the backup GPT header only when a valid protective MBR in block 0 still marks the disk as GPT, and never writes it ([`block/partitions/efi.c`](https://android.googlesource.com/kernel/common/+/e6cfc1098ef0/block/partitions/efi.c)).
- ZFS rewrites its four labels automatically. [FreeBSD bug 204622](https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=204622) reports an import and export that rewrote ZFS labels on a disk reformatted with UFS, breaking the UFS.
- Ceph BlueStore keeps label copies at 0, 1, 10, 100 and 1000 GiB, refuses to start an OSD whose copies disagree, and repairs them only through `ceph-bluestore-tool repair` ([docs](https://docs.ceph.com/en/latest/man/8/ceph-bluestore-tool/)). Stale copies left by `sgdisk --zap-all` and `wipefs` have kept reused disks from starting ([IBM note](https://www.ibm.com/support/pages/node/7242545)).
- mdadm's 0.90 and 1.0 superblocks sit near the end of the device. The [kernel RAID wiki](https://raid.wiki.kernel.org/index.php/RAID_superblock_formats) calls that dangerous with auto-detection, and stale ones have been [found by `grub-probe`](https://lists.alioth.debian.org/pipermail/pkg-grub-devel/2014-September/014284.html).

The failures the copy is for are a block 0 the drive cannot read (NVMe reports uncorrectable media as an error, not as flipped bits) and a block 0 written torn or rotted in place, which keeps this format's magic and fails its checksum.

## Decision
What block 0 holds decides:

| Block 0 | Recovery |
| --- | --- |
| A valid superblock | Uses it, and rewrites the copy if the copy differs |
| Unreadable, or this format's magic and version with a bad checksum or inconsistent contents | Uses the copy if it is valid and sits where its own geometry puts it, and rewrites block 0 |
| Zeros, another magic or another format version | Writes nothing. Reports `CopyOnly` when the copy is valid, else `NotFormatted` |

Restoring the copy over a block 0 of the last row is an operator's decision, through a repair command once node tooling exists.

## Consequences
- A bad or rotted block 0 is repaired at start with no operator, like a damaged copy.
- A disk that was wiped, repartitioned or given to another system is never written by recovery, whatever survives at the end of segment 0.
- A bit flip inside the magic or version of block 0 needs the operator's repair, as a reused disk does.
- The copy is found from the disk's size when block 0 is unreadable or damaged, so a disk keeps the size it was formatted with.
