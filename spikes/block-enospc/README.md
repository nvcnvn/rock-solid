# Block ENOSPC spike

**Question.** When a thin-provisioned block device runs out of space, what does a filesystem on it do if the device fails each write that needs new space with `BLK_STS_NOSPC`, and is anything lost? The answer sets what rock-solid's [block frontend](../../docs/design.md#block-frontend) does when a pool is full ([capacity and ENOSPC study](../../studies/capacity-and-enospc/README.md)).

## Setup

- Linux 6.18.44 (Firecracker guest `6.18.44-fc-v139`), root, e2fsprogs 1.47.0. This kernel has no device-mapper, so [run-loop.sh](run-loop.sh) uses a stand-in for dm-thin's `error_if_no_space`: a 1 GiB loop device whose backing file lives on a 192 MiB tmpfs. A write to a block never written needs a new tmpfs page and fails once the tmpfs is full; the loop driver completes it with `BLK_STS_NOSPC` ("critical space allocation"), which is what a ublk server returning `-ENOSPC` gives ([`ublk_drv.c`](https://github.com/torvalds/linux/blob/v6.18/drivers/block/ublk_drv.c#L1159), `errno_to_blk_status`). An overwrite of a block written earlier reuses its page and succeeds, as a rock-solid overwrite of a live, unshared block passes `write_limit`. A discard punches the file and frees pages.
- ext4 made with `lazy_itable_init=0,lazy_journal_init=0,nodiscard`, default `errors=continue`; a 16 MiB file written first; then a 512 MiB file written and `fsync`ed, the old file overwritten, the big file removed, 200 files created, a new 4 MiB file written, and `e2fsck -fn` after unmount. T1D mounts with `-o discard`.
- Stand-in limits: the loop device fails at once and never holds a write, and it cannot show a hold or a timeout. XFS was not tested: `mkfs.xfs` is not installed and nothing was installed. [run-root.sh](run-root.sh) runs the same checks on dm-thin, for ext4 and XFS, failing at once and holding; it needs a kernel with device-mapper and has not run.

```sh
cd spikes/block-enospc
sudo ./run-loop.sh > results-loop.txt                          # T1L, about 10 s
sudo ID=T1D MOUNT_OPTS=discard ./run-loop.sh >> results-loop.txt
sudo ./run-root.sh | tee results-root.txt                      # T1 to T6, dm-thin; not run yet
```

## Results

Raw output: [results-loop.txt](results-loop.txt).

| Test | Result |
| --- | --- |
| T1L: ext4, no discard; the 512 MiB write | `fsync` fails with **ENOSPC**, not EIO; the mount stays `rw`; the kernel logs "failed to convert unwritten extents to written extents -- potential data loss! (inode 13, error -5)" |
| T1L: overwrite of the old file | Succeeds |
| T1L: 4 MiB write after removing the big file | **Fails**: the space ext4 freed never reached the device |
| T1L: `e2fsck -fn` after unmount | **Exit 4**: block bitmap differences in groups 3 and 5, wrong free counts for both groups and in total, bitmap checksums that do not match; the journal still needed recovery after unmount |
| T1D: ext4 `-o discard`, the same steps | ENOSPC at `fsync`, `rw`, overwrites work, and **the write after `rm` succeeds**; `e2fsck` still exits 4 with group 3's bitmap wrong and its checksum mismatched (no pending journal this time) |

**Pending, [run-root.sh](run-root.sh) on dm-thin.** Expected outcomes, revised where T1L contradicted the first ones (which had the filesystem consistent after a refused write):

| Test | Setup | Expected |
| --- | --- | --- |
| T1 | ext4, `error_if_no_space` | ENOSPC at `fsync`, `rw`, overwrites work, **`e2fsck` finds damage**, as T1L |
| T2 | XFS, same | ENOSPC at `fsync`; metadata writeback retries ENOSPC forever by default (`xfs_error_meta_init`), so no damage while mounted; what `xfs_repair -n` finds after unmount, when `fail_at_unmount` fails those retries, is recorded, not predicted |
| T3, T4 | ext4, XFS, writes held, pool grown after 20 s | The writer waits about 20 s, then succeeds; the check after unmount is clean |
| T5 | ext4, held for `no_space_timeout`, never grown | ENOSPC after about 60 s, then damage as T1 |
| T6 | XFS, same | As T2, after about 60 s |

## Conclusions

1. **Failing a write that needs new space corrupts ext4's metadata**, even though the application sees the right error. The writes that failed included the first block bitmap of block groups never used before (groups 3 and 5), which live in blocks the device had never stored; ext4 kept running with `errors=continue`, and the lost bitmaps left free counts and checksums wrong on disk (T1L, T1D). No delay before the failure changes that: a hold that ends in failure, as dm-thin's `no_space_timeout` does, only postpones the damage when the pool does not grow. So rock-solid's block frontend holds writes that need new space for as long as the pool is full, and never fails them unless an operator asks.
2. **ENOSPC, not EIO, reaches `fsync`**, and overwrites of old blocks keep working, so letting overwrites past `write_limit` is right: the journal and every block the filesystem already wrote keep working while new allocation waits.
3. **Without discard, deleting files frees nothing in the pool** (T1L): a full pool stays full after the application cleans up. With `-o discard` the space came back at once (T1D). The CSI driver mounts the filesystems it creates on block volumes with `discard`.
4. Pending, on dm-thin: whether XFS behaves the same, and how ext4 and XFS ride out a hold that ends when the pool grows (run-root.sh T2 to T6).
