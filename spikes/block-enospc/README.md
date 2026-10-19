# Block ENOSPC spike (not run yet)

**Question.** When a thin-provisioned block device runs out of space, what do ext4 and XFS on it do if the device fails the write with `BLK_STS_NOSPC`, and if it holds the write until space appears or a timeout? The answer sets what rock-solid's [block frontend](../../docs/design.md#block-frontend) does when a pool is full ([capacity and ENOSPC study](../../studies/capacity-and-enospc/README.md)).

**Status: needs root, not run.** It creates loop devices and device-mapper targets, so it was written as a script for later and has no results yet. The expectations below come from reading Linux v6.18 (cited in the study), not from a run.

## Setup

- dm-thin stands in for a full rock-solid pool behind ublk: a 1 GiB thin volume on a 192 MiB pool, so the pool runs out first. With `error_if_no_space` dm-thin fails a write that needs a new block with `BLK_STS_NOSPC` ([`dm-thin.c` `should_error_unserviceable_bio`](https://github.com/torvalds/linux/blob/v6.18/drivers/md/dm-thin.c#L1592)), which is what a ublk server returning `-ENOSPC` gives ([`ublk_drv.c`](https://github.com/torvalds/linux/blob/v6.18/drivers/block/ublk_drv.c#L1159), `errno_to_blk_status`). Without it, dm-thin holds such writes for `no_space_timeout` (60 s by default), then fails them with `BLK_STS_NOSPC`, and keeps serving writes to blocks already provisioned and not shared, which is what the study proposes for overwrites.
- Stand-in limits: dm-thin holds bios in its own list, while a ublk server that holds requests keeps their tags, so a hold in ublk can exhaust the queue depth, which this spike cannot show. The script never changes `no_space_timeout`, a module-wide setting.

```sh
cd spikes/block-enospc
sudo ./run-root.sh | tee results-root.txt     # all tests, about 4 minutes; or: sudo ./run-root.sh T1 T5
```

## Tests and expected outcomes

| Test | Setup | Expected |
| --- | --- | --- |
| T1 | ext4, `error_if_no_space`; write 512 MiB, `fsync`; overwrite a 16 MiB file written before; create 200 files | `fsync` fails with ENOSPC, not EIO (`mapping_set_error` keeps ENOSPC); the overwrite succeeds; the mount stays `rw` unless a new journal or metadata block was refused, which would abort the journal |
| T2 | XFS, same | `fsync` fails with ENOSPC; the overwrite succeeds; metadata writeback retries ENOSPC forever by default (`xfs_error_meta_init`), so no shutdown unless a log write fails |
| T3 | ext4, queue; the pool grows after 20 s | The writer waits about 20 s and then succeeds; no error in `dmesg` |
| T4 | XFS, same | Same as T3 |
| T5 | ext4, queue, never grown | ENOSPC after about `no_space_timeout`; overwrites still work; the mount stays `rw` |
| T6 | XFS, same | Same as T5 |

A test that contradicts its row changes the study's proposal for the block frontend: if T1 or T2 shut the filesystem down even though overwrites pass, failing at once is not an option; if T3 or T4 report errors, holding gains nothing.
