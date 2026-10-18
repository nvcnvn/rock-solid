---
name: sans-io
description: Conventions for the sans-IO core - state machines in crates/core, meta and sim that take events and return actions and never do I/O. Use when writing, changing or reviewing any state machine (ChunkStore replica, placement, Raft shard, sessions, leases, inode owner, cache modes), the Event/Action types, the simulator, or when adding a new crate and choosing whether it is sans-IO or I/O.
---

# Sans-IO core

Design source: the "Modularity" and "Testing strategy" sections of [docs/design.md](../../../docs/design.md). The goal: one seed replays a whole cluster run exactly. Anything nondeterministic in a state machine breaks that.

## Shape

```rust
impl ChunkReplica {
    pub fn on_event(&mut self, ctx: &mut Context<'_, impl BufPool>, ev: Event) -> Vec<Action> { ... }
}
```

- **Input:** `ctx: &mut Context<'_, impl BufPool>` (`ctx.now: Tick` where 1 tick = 1 µs, `ctx.pool: &mut BufPool`) and an `Event` only (`NetRecv`, `PeerLost`, `DiskDone(IoId, Result<(), DiskError>)`, `Timer`). The state machine never reads a clock, so time comes in as `ctx.now`.
- **Output:** `Action`s (`NetSend`, `DiskRead(IoId, Lba, IoBuf)`, `DiskWrite(IoId, Lba, IoBuf)`, `DiskFsync`, `DiskTrim`, `SetTimer`). The machine doesn't wait; it records what it's waiting for under an `IoId` or `TimerId`, and the matching event resumes it.
- **No async, no threads, no locks.** `on_event` is synchronous and `&mut self`. The driver owns concurrency.
- **Randomness** comes from a seeded RNG passed in (or owned and seeded at construction). Never use an OS RNG.
- **Ordered containers:** `BTreeMap` and `BTreeSet`. Iteration order shapes the order of emitted actions, so `HashMap`'s random seed breaks replay.
- **Errors:** return them as data in actions or events. Reserve panics for broken invariants, never for peer or disk input: a corrupt message is an event to handle.

## Driver contract (from the bypass-drivers spike)

These rules hold from the first default driver, so SPDK and RDMA fit later without changes to the core:

1. **Buffers come from the driver's pool; the core frees them.** Data travels as an `IoBuf` handle from the driver's pool, never as a `Vec<u8>` the core allocated. Control messages of 256 bytes or less go inline. A buffer in an action stays with the driver until `DiskDone`, or for `NetSend` until the peer's reply or `PeerLost(peer)`; a timeout doesn't free it. `PeerRequests` in `core::peer` tracks this.
2. **Every driver exposes a readiness fd and defaults to event mode.** That's a driver concern, but the core must never assume busy polling.
3. **Connections fail as a unit:** a single `PeerLost(NodeId)` fails every outstanding request to that peer. The core keeps its own deadlines with `SetTimer`; it never trusts transport timeouts. `SetTimer` re-arms an armed `TimerId` and there is no cancel, so a stale `Timer` can arrive: check the deadline against `ctx.now` first.

## Enforcement

`scripts/lint.sh` runs everything below and must pass before a commit:

- The root [clippy.toml](../../../clippy.toml) bans `Instant`, `SystemTime`, `HashMap`/`HashSet`, `std::fs`, `std::net`, threads, `Mutex`, env vars and `process::exit` in every crate by default.
- The script fails if a sans-IO crate depends on `tokio`, `mio`, `libc`, `nix`, `rustix`, `io-uring`, `socket2`, `async-std`, `smol` or `getrandom`, even indirectly. For an RNG use `rand_chacha` or `rand_pcg` with `default-features = false`.
- **Sans-IO by default.** An I/O crate (`drivers-*`, `frontend-*`, `daemon`, `csi`) opts out with `ln -s ../../clippy-io.toml crates/<name>/clippy.toml`. Never add that symlink to `core`, `meta` or `sim`. Never silence a ban with `#[allow(clippy::disallowed_*)]`; move the I/O into a driver instead.

## Placing new logic

- Protocol logic (who may cache, when to recall, what's fenced) goes in `core` or `meta`, even when only one frontend uses it today. A frontend crate only translates between FUSE, ublk or S3 calls and events/actions.
- Each new crate is `crates/<dir>/` with package name `rock-<dir>` (`core` is a reserved crate name), `[lints] workspace = true`, and the workspace `version`, `edition`, `license`, `rust-version`.

## Testing a state machine

- Write unit tests by feeding events and asserting on the returned actions. No mocks are needed.
- Every new state machine registers its invariants with the simulator's checker: acked writes are readable, replica counts hold, there's at most one lock holder, no stale read in WriteShared, no write from a fenced session is applied, epochs never go backwards. Add the invariant in the same change as the feature.
- A bug found by a seed gets that seed as a regression test before the fix.
- For a new protocol (epochs, leases, recalls, cross-shard commit), model it in `stateright` first (roadmap phase 2), then write the state machine to match the model.
