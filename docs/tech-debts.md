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
- **Where:** `crates/core/src/log/commit.rs`, `GroupCommit::checkpoint_piece` returns `SubmitError::TooLarge`
- **Why:** the manifest is a single blob. With the default 1 MiB records it lists about 58,000 snapshot pieces of up to 1 MiB each, about 56 GiB of snapshot, far above stage 1's memory budget. A smaller `max_record_bytes` lowers the limit faster than in proportion, since both the piece count and the piece size shrink.
- **Remove when:** an index needs a larger snapshot, at the latest with the LSM tree's stage 2. Then let a manifest span several records as an atomic group, applied only when complete, as RocksDB groups manifest edits.

## Workaround: a segment whose header reads as zeros counts as free
- **Where:** `crates/core/src/log/recovery.rs`, `Recovery::on_segment_header`
- **Why:** the segment usage table is not built yet, and reading every zeroed segment in full to check for records would read the whole disk at each start. A fault that zeroes the header of a segment holding records, such as a misdirected write of zeros, lets an index rebuild skip those records and group commit reuse the segment. A normal start still reports the missing records as corruption.
- **Remove when:** the build checklist's **Segment usage table** item is reviewed; a segment is then free only because the table says so.

## Workaround: the phase 1 code is still Rust
- **Where:** `crates/`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `clippy.toml`, `clippy-io.toml`, `deny.toml`, `rustfmt.toml`, `scripts/lint.sh`, the Rust section of `CLAUDE.md`, `.claude/skills/sans-io`
- **Why:** [ADR 0009](decisions/0009-write-in-go.md) moved the project to Go after the disk log, group commit, recovery, the stage 1 LSM tree and the chunk index were built in Rust. Until their Go packages pass the same review, the Rust code is the only working implementation and the Rust tooling checks it. The workarounds above that name `crates/core` are in this code.
- **Remove when:** the build checklist's **Port to Go** items are reviewed. Then delete `crates/` and the Rust tooling, give `scripts/lint.sh`, `CLAUDE.md` and the sans-IO skill their Go rules, and carry each workaround above into the Go package that keeps it or delete it.
