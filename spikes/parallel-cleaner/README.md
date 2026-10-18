# Parallel cleaner spike

**Question.** Can the disk log's cleaner run on worker threads, so that its copy work (liveness check, CRC32C verification, copying blocks) leaves the thread that owns the disk's index and serves writes? How much does that save the owner per write, what does it cost in total CPU, and must the workers then update a shared index, or can they hand their moves back to the owner, keeping the index single-writer as [thread-per-core](../../docs/design.md#metadata-layer-metastore) needs? The [cleaner spike](../cleaner-wa/README.md) estimated 2.8 / 7.5 / 20 µs of cleaner CPU per user 4K write at 80 / 90 / 95% full (C7), all of it on the writing thread today.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker), 4 vCPUs of an Intel Xeon at 2.1 GHz, 15 GiB. Rust 1.99.1, no dependencies. [src/main.rs](src/main.rs) keeps a 1 GiB disk in memory: 4,096 segments of 64 blocks of 4K, the same segment count as the cleaner spike. It has a hot stream of user writes and cold streams of cleaner copies, a chunk index from logical block to physical block, a reverse map, a CRC32C per block (SSE4.2) and a live count per segment. One *owner* thread writes uniform random 4K overwrites: it copies the user's 4K into the open hot segment, computes its CRC32C and updates the index. The cleaner is greedy, keeps 8 segments free, and copies each live block of a victim to a cold segment after verifying its CRC32C. It runs in one of three modes:

- **inline:** on the owner thread, 4 victims per pass, whenever free segments drop below 8. This is today's single-threaded shape.
- **owner:** on 1 to 3 worker threads. A worker sends the victim's moves (logical block, old location, new location) to the owner over a channel. The owner applies each move only if the index still points at the old location, then frees the victim. Until then the target segment cannot be picked as a victim.
- **shared:** on worker threads that update an atomic index themselves with compare-and-swap, and free the victim.

A run fills the disk, warms up for 4 disk capacities of writes, then measures 2. The owner's *work* is the time spent in its write calls, minus time stalled waiting for a free segment. Worker CPU comes from `CLOCK_THREAD_CPUTIME_ID`. P3 paces the owner at 50,000 writes/s, about 200 MB/s of 4K writes. That load is enough to need cleaning at 95% full, and every mode keeps up with it. [run.py](run.py) drives the tests and checks expected outcomes written before the first run. The CRC32C is a single-stream SSE4.2 loop, about 0.6 to 0.8 µs per block, 4× slower than a three-stream one ([Go comparison spike](../go-cpu-per-op/README.md), C0); `crc=0` skips it, which only that spike uses.

```
cargo build --release
python3 run.py              # P0, P0b, P1, P2, P3; about 8 minutes -> results.txt
```

Raw output: [results.txt](results.txt).

## Results

| Test | Result |
| --- | --- |
| P0: inline, unthrottled, write amplification against the greedy model | 2.62 / 4.89 / 9.11 at 80 / 90 / 95% full, against 2.69 / 5.18 / 10.17 predicted. FAIL at 95% (10.4% low): see P0b. Owner work per write **3.1 / 5.6 / 10.2 µs**; max rate 320k / 178k / 97k writes/s; **p99 186 / 242 / 514 µs**, the writes that run a pass |
| P0b: blocks per segment at 95% full, 4,096 segments | 16 blocks 6.72, 64 blocks 9.11, 256 blocks 10.14; 64 blocks with 12× warm-up 9.10. The gap is segment size, not warm-up: with few blocks per segment, live counts vary more between segments and greedy finds emptier victims. The model holds for large segments, as the cleaner spike found at 1,024 blocks. A real disk with 64 MiB segments therefore copies about 13% more than this spike does (10.2 against 9.1 copies per write at 95%) |
| P1: owner mode, unthrottled, 95% full | 1 / 2 / 3 workers: 108k / 183k / 249k writes/s, **1.11 / 1.88 / 2.56× inline**. The cleaner sets the rate: the owner works 1.2 to 1.4 µs per write and stalls the rest of the time. Worker CPU 9.2 to 11.5 µs per user write |
| P2: shared mode, unthrottled, 95% full | 104k / 165k / 221k writes/s, 1.07 / 1.70 / 2.28× inline; owner work 1.2 µs per write |
| P3: paced at 50k writes/s, owner work per write | 90% full: inline 5.86 µs, **owner mode 1.56 µs**, shared 1.39 µs. 95% full: inline 10.96 µs, **owner mode 1.78 µs**, shared 1.20 µs |
| P3: paced, write latency on the owner | p99 at 90 / 95% full: inline 247 / 537 µs, **owner mode 6.2 / 6.3 µs**, shared 11.6 / 1.9 µs. p50 rises from 0.74 to 0.77 µs inline to 1.13 to 1.29 µs with workers running. Max is 1.1 to 33 ms in every mode, including inline: likely vCPU scheduling on the VM (not verified) |
| P3: paced, total CPU per write (owner work + workers) | One worker: owner mode **1.30× inline at 90% (FAIL, want < 1.25×)**, 1.12× at 95%; shared 1.26× and 1.07×. Two workers where one keeps up: 1.66× and 1.42× in owner mode |
| P4: cost of applying moves on the owner | Owner mode minus shared mode, one worker: 0.16 µs per write at 90%, 0.59 µs at 95%, or **42 to 72 ns per move** (3.9 and 8.1 moves per write). The cleaner spike's in-memory index update was 122 ns |
| Wasted copies (overwritten while being moved) | At most 0.0003 per user write in every mode under uniform overwrites |

## Conclusions

1. **Take the cleaner's copy work off the thread that owns the index.** At 90 to 95% full, the owner's work per write drops from 5.9 to 11 µs to 1.6 to 1.8 µs, and its p99 from 250 to 540 µs to about 6 µs (P3). Max write rate grows with workers, 1.9× with two and 2.6× with three at 95% full (P1).
2. **The owner applies the moves; workers never write the index.** Applying costs the owner 42 to 72 ns per move, 0.16 to 0.59 µs per write (P4). In exchange the index stays single-writer, which thread-per-core and an LSM memtable both need, and the cleaner's result is an ordinary event to the owner's state machine. A move is applied only if the index still points at the old location, and the victim is freed only after its moves are applied. A target segment is not a victim candidate until its moves are applied. Apply a victim's batch in bounded slices if p99 matters: at 95% full a batch is about 60 moves.
3. **Moving the work costs total CPU: 7 to 30% with one worker, more with idle extra workers.** The owner's own write gets about 0.4 µs slower when a worker runs (p50 0.75 to 1.15 µs), and the workers spend more per copy than inline cleaning did (P3). Two workers where one keeps up cost 42 to 66% more than inline. So run one worker per disk and add more only when the owner stalls for free segments. On this 2.1 GHz Xeon, one worker cleans for about 100k user writes/s at 95% full (P1).
4. **The shape does not depend on the language.** These are threads, a channel and a conditional apply. In Go it is a goroutine worker pool and a channel to the owner goroutine, with the same costs; a pointer-free index costs the garbage collector nothing to scan.

Not covered: disk I/O (the cleaner reads its victims from memory here, not from the drive), cost-benefit cleaning, record headers, Raft entries and sorted runs in the log, skewed overwrites (where wasted copies should rise), and more than 4 cores. Owner mode used atomics for its index too; a real single-writer index needs none, so its owner cost may be lower.
