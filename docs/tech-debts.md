# Tech debts

## Version pin: Rust toolchain 1.99.1
- **Where:** `rust-toolchain.toml`, `rust-version` in `Cargo.toml`
- **Why:** each stable release adds clippy lints, and `scripts/lint.sh` treats warnings as errors, so an unpinned toolchain would break the build without any code change.
- **Remove when:** never removed, only bumped. Bump both together when a newer stable is needed, and fix the new lints in the same change.

## Workaround: records are copied twice on the write path
- **Where:** `crates/core/src/log/commit.rs`, `GroupCommit::assemble`
- **Why:** a round builds its record in a heap image, then copies the image into one pool buffer, so every written block is copied twice. `Action::DiskWrite` takes a single `IoBuf`; writing the producers' buffers in place needs a vectored write action that every driver supports.
- **Remove when:** a driver needs vectored writes (SPDK, phase 9), or the phase 5 durable-write benchmark shows the copies in its CPU profile. Then add a vectored `DiskWrite` and gather the producers' buffers directly.

## Workaround: recovery reads one block range at a time
- **Where:** `crates/core/src/log/recovery.rs`, `Recovery`
- **Why:** one read in flight keeps recovery a simple stage machine. Startup reads every segment header, 4,096 to 262,144 of them, and the unused tail of each open segment, one after another.
- **Remove when:** phase 5 measures startup on a real disk. If reading the segment headers takes more than a second, issue them as independent reads with many in flight.

## Workaround: a checkpoint's manifest must fit in one record
- **Where:** `crates/core/src/log/commit.rs`, `GroupCommit::begin_checkpoint` returns `SubmitError::TooLarge`
- **Why:** the manifest is a single blob. With the default 1 MiB records it lists about 58,000 snapshot pieces of up to 1 MiB each, about 56 GiB of snapshot, far above stage 1's memory budget. A smaller `max_record_bytes` lowers the limit faster than in proportion, since both the piece count and the piece size shrink.
- **Remove when:** an index needs a larger snapshot, at the latest with the LSM tree's stage 2. Then let a manifest span several records as an atomic group, applied only when complete, as RocksDB groups manifest edits.

## Workaround: the disk's roots are single copies
- **Where:** `crates/core/src/log/segment.rs` (one superblock), `crates/core/src/log/recovery.rs` (a segment whose header does not decode counts as free)
- **Why:** the second superblock copy, quarantined segments and the segment usage table are not built yet. Until then a bad block 0 makes the disk unreadable, and bit rot in a full segment's header lets an index rebuild skip its records silently and group commit reuse the segment. A normal start still reports the missing records as corruption.
- **Remove when:** the build checklist's **Superblock copy**, **Quarantined segments** and **Segment usage table** items are reviewed.
