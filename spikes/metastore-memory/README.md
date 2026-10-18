# MetaStore memory spike (bytes per file in the stage-1 memtable)

**Question.** How many bytes of RAM does one file cost in a MetaStore shard held only in memtables (the stage-1 `MemTree` in [crates/core/src/lsm.rs](https://github.com/nvcnvn/rock-solid/blob/200714b/crates/core/src/lsm.rs)), so we know at what file count per node the LSM tree's stage 2 (sorted runs, compaction) becomes necessary?

**Setup.** macOS 27.0 (26A428), Apple M4 Pro (14 cores), 64 GiB RAM, rustc 1.99.1, libc 0.2.177. The harness is one Rust binary ([src/main.rs](src/main.rs)) in its own cargo workspace. It measures the real `rock_core::lsm::MemTree` through a path dependency on `crates/core` at `f3bfd04` plus the uncommitted stage-1 `lsm.rs` and `log/` in the working tree. Each F1 to F3 point runs in its own process; F4 to F8 share one process holding a 10M-file tree. `crates/core` has since been deleted with the rest of the Rust code ([Runtime in Go](../../docs/design.md#runtime-in-go)); the Rust harness builds at commit `200714b`.

File model ([key layout](../../docs/design.md#metadata-layer-metastore) encoding: a one-byte tag per `word/`, fixed-width big-endian integers, only the last component variable, no terminator):

| Key | Bytes | Value | Bytes |
| --- | --- | --- | --- |
| `fs/{volume u32}/{inode u64}/a` | 14 | attributes (below) | 92 |
| `fs/{volume}/{dir}/d/{name}` | 14 + name | inode u64, type u8 | 9 |
| `fs/{volume}/{inode}/x/{name}` | 14 + name | xattr value | 24 to 57 |
| `fs/{volume}/{slot·0}/n`, one per directory | 14 | next sequence u64, bytes used u64 | 16 |

- **Attribute record, 92 B:** mode, uid, gid, nlink, flags (5 × u32); size and `alloc_end` (2 × u64, as in the design's key table); atime, mtime, ctime and btime (4 × i64 seconds + u32 ns; btime because SMB and `statx` report birth time); a u64 change counter for NFSv4 change attributes and cache coherence. No generation number, because the key layout never reuses inode IDs, and no `rdev`.
- **Inode IDs:** directory `j` gets slot `j` and inode `(j << 40) | 1`; its files take sequence numbers 2, 3, … in that slot. The root is inode 1. Directories have attributes, a directory entry in their parent, and the slot counter.
- **Tree shape:** each directory holds 20 to 50 files (uniform, mean 35) and up to 4 subdirectories (parent of `j` is `(j-1)/4`). Directory 1 holds 996 files and 4 subdirectories, the 1,000-entry listing of F7. That gives 0.0286 directories and 2.09 tree entries per file.
- **Names:** lengths drawn from the histogram of 372,021 names under `/usr` and `/Applications` on this machine ([name-lengths.txt](name-lengths.txt): mean 20.1 B, p50 17, p99 53; lengths only, no names kept); bytes random from a 64-character alphabet, unique within a directory. F3 also uses fixed 8, 24 and 64 B.
- **Xattrs** (F1 only), on files and directories: `user.DOSATTRIB` 24 B, then `com.apple.FinderInfo` 32 B and `com.apple.quarantine` 57 B.
- **Insert order ("create order"):** directories breadth first, as a `cp -r` would create them; within a directory, files in random name order. So attribute keys mostly append at the right end of the tree and directory entries land at random inside their directory's range. F1 also measures a fully shuffled order and the order `restore` produces (sorted).
- **Keys and values** are passed to `MemTree::insert` as exact-capacity `Vec`s. A caller that builds them with spare capacity pays more.

Measurement:

- **Live heap:** a counting global allocator records requested bytes (`req`) and the bytes macOS really allocates, `malloc_size(ptr)` (`act`), and their peaks. Every figure is the heap growth caused by building the tree, divided by the number of regular files (directories are included in the cost, not in the count).
- **macOS 27's default malloc rounds like jemalloc:** 16 B steps up to 128 B, then 4 size classes per doubling ([results-size-classes.txt](results-size-classes.txt)), so `act` equals the `jem` model for every small allocation. Linux glibc rounds differently (8-byte header, 16-byte alignment, 32 B minimum), so the harness also sums a **modelled** glibc chunk size (`glibc`) and jemalloc class (`jem`) per allocation. These two are arithmetic, not measurements. For the multi-GB buffers of F4's packed forms, the `jem` model overstates (huge allocations are only touched up to their length).
- **Process memory:** `task_info(TASK_VM_INFO)` resident, compressed and `phys_footprint`. The host was not idle (load about 3.5, 4.8 GB of swap in use by other programs), and at 30M files macOS compressed part of the tree, so resident fell while `phys_footprint` stayed at the heap figure (F2).
- **Timings** are single-threaded wall time and include the counting allocator, which adds about 10 to 20 ns per allocation and free (printed at the top of [results-10m.txt](results-10m.txt)).

Stand-ins and limits:

- **Hardware differs from the KV spike's bar.** redb's 7.2 µs get and 196 µs listing were measured on a loaded Intel laptop through redb's page cache. Here everything is in the heap of an M4 Pro, so F7 compares an in-memory tree against an engine, not two engines on one machine.
- **Random name bytes** understate prefix compression of names (real names share prefixes like `lib` and suffixes like `.h`). The fixed key prefix (tag, volume, inode, tag), which gives most of F4's compression, is realistic.
- **No concurrent applies** during checkpoint. A real checkpoint is written while applies continue, so it also needs a frozen copy of the tree or copy-on-write; F5 measures only the serialization itself.
- **F4's packed and prefix-compressed forms are immutable.** An insert into a sorted array is O(n), so using them means a small mutable memtable merged into them: that is stage 2 held in RAM, not a drop-in `Tree`.

Expected outcomes, written before the first run and printed by the harness as `[OK ]`/`[FAIL]` checks:

| Test | Expected |
| --- | --- |
| F1 attributes + directory entry | 300 to 400 B per file (`act`) |
| F1 +1 xattr, +3 xattrs | Not agreed with the user; my guess: each xattr adds 100 to 160 B |
| F2 1M, 10M, 30M files | Flat: within 10% |
| F3 names of 8, 24, 64 B and sampled | Linear in name length (R² ≥ 0.95) |
| F4 cheaper representations | The best is up to 2× smaller: 0.5× to 1× the `MemTree` |
| F5 peak live heap during `checkpoint` at 10M files | Close to 2× the tree (1.8× to 2.2×) |
| F6 restore at 10M files | Faster than 1M entries/s |
| F7 get and 1,000-entry listing at 10M files | Get p50 below 7.2 µs, listing p50 below 196 µs |
| F8 projection | None: derived from F1 to F5 |

```
./run.sh                 # every step below, a few minutes; peaks near 12 GB of heap (F2 at 30M)
./run.sh f1 big          # selected steps: names classes f1 f2 f3 big
```

| File | Contents |
| --- | --- |
| [name-lengths.txt](name-lengths.txt) | Name-length histogram of `/usr` and `/Applications` (input to every test) |
| [results-size-classes.txt](results-size-classes.txt) | macOS malloc size classes ([size-classes.c](size-classes.c)) |
| [results-f1.txt](results-f1.txt) | F1 |
| [results-f2.txt](results-f2.txt) | F2 |
| [results-f3.txt](results-f3.txt) | F3 |
| [results-10m.txt](results-10m.txt) | Node sizes, F7, F5, F6, F4, F8 on one 10M-file tree |
| [results-f1-compact.txt](results-f1-compact.txt), [results-10m-compact.txt](results-10m-compact.txt) | F1 and the 10M-file run again, after conclusions 3 and 5 were applied |

## Results

All figures are `act` bytes per regular file (macOS, measured) unless marked glibc (modelled).

| Test | Result |
| --- | --- |
| F1 attributes + directory entry, 1M files | **373 B/file** [OK]. glibc model 411 B. Logical key + value bytes are only 154 B; requested heap 326 B. 4.48 allocations per file |
| F1 where the 373 B go | **BTreeMap nodes: 95 B per entry, 198 B per file, 53% of the total.** A `BTreeMap<Vec<u8>, Vec<u8>>` leaf holds 11 entries of 2 × 24 B inline: 544 B requested, rounded to the 640 B class (internal nodes 640 B). At full fill that is 58 B per entry; 95 B means leaves are about 60% full. The rest is the key buffer (16 B for an attribute key, 48 B on average for a directory entry key) and value buffer (96 B attributes, 16 B directory entry) |
| F1 +1 xattr (`user.DOSATTRIB`, 24 B) | 548 B/file: **+175 B** [FAIL, guessed 100 to 160]. Per xattr: 32 B key buffer, 32 B value buffer and about 105 B of node per inode, on 1.03 inodes per file |
| F1 +3 xattrs | 955 B/file: +194 B per xattr [FAIL, same guess]. With SMB's per-entry xattrs, a file costs 1.5× to 2.6× the plain figure |
| F1 insert order | Create order 373 B, shuffled 353 B, **restored by `checkpoint` + `restore` 397 B (+6.6%)**: node bytes per entry 95 / 85 / 107 |
| F2 1M, 10M, 30M files | 372.8 / 372.9 / 372.9 B [OK, flat to 0.1%]. glibc model 411.1 at every scale. `phys_footprint` growth 380 B/file (1.02× the heap) at every scale. At 30M, macOS held 181 B/file in its compressor, so resident growth fell to 199 B/file |
| F3 names 8 / 24 / 64 B / sampled (20.1 B mean) | 363.5 / 380.0 / 412.9 / 372.8 B [OK]: 358 + 0.87 B per name byte, R² 0.997 (requested heap: exactly 1.03 B per byte, R² 1.000; glibc model 1.19 B per byte). Steps of 16 B come from size-class rounding of the key buffer |
| F4 `BTreeMap<Vec,Vec>` bulk-built from sorted input (`FromIterator`) | 296 B (0.79×). get p50 1.00 µs, listing 2.6 µs |
| F4 `BTreeMap<Box<[u8]>, Box<[u8]>>`, create order | 299 B (0.80×): 16 B per slot instead of 24, leaf 384 B. get p50 1.00 µs p99 3.50 µs, listing 3.0 µs |
| F4 `BTreeSet` of one allocation per entry (`klen u16 \| key \| value`), create order | **235 B (0.63×)** [OK]: 2.40 allocations per file, leaf 192 B. glibc model 263 B. get p50 1.04 µs p99 1.75 µs, listing 3.2 µs. Drop-in mutable replacement |
| F4 the same, bulk-built from sorted input | 209 B (0.56×), until inserts split its full nodes |
| F4 packed arena (entries in one buffer + sorted `u32` offsets), immutable | 179 B (0.48×). get p50 1.92 µs (binary search, 25 steps over 20.9M offsets), listing 2.4 µs |
| F4 prefix-compressed blocks of 16 with restart offsets (LevelDB block layout), immutable | **135.5 B (0.36×)** [FAIL: better than the 2× expected]. get p50 1.38 µs p99 4.04 µs, listing 8.6 µs (decoding). Smaller than the 154 B of logical data, because the 13-byte inode prefix repeats in consecutive keys |
| F5 peak live heap during `checkpoint`, 10M files | **1.745× the tree** [FAIL, expected 1.8 to 2.2]: 278 B/file of pieces on top of 373. Only 167 B/file is serialized data: **the pieces carry 67% spare capacity** (cause below). With exact-capacity pieces the peak would be 1.45× |
| F5 peak during `restore` | Pieces + new tree = (278 + 397) / 373 = **1.81×** the tree at a restart. Installing a snapshot into a live replica (old tree still held) peaked at **2.81×** |
| F6 checkpoint and restore, 10M files (20.9M entries) | `max_piece` 1,044,480 B (default `max_record_bytes` 1 MiB, computed with `Layout::of` as `commit.rs` does); 1,596 pieces, **167 B/file serialized** (80 B per entry). Checkpoint 0.47 s (23 ns per entry, 3.5 GB/s). **Restore 3.80 s = 5.49M entries/s** [OK]: a restart pays about 0.38 s per million files before replay |
| F7 point get, 10M files, 200k random keys | **p50 0.88 µs, p99 1.46 µs** [OK, redb 7.2 µs] (attributes 0.83, directory entries 0.96). The restored tree: p50 1.12 µs, p99 5.96 µs |
| F7 1,000-entry listing (prefix scan) | **p50 3.3 µs, p99 4.1 µs** [OK, redb 196 µs]. readdirplus (listing + 1,000 attribute gets): p50 84 µs, p99 104 µs |
| F8 projection | Table below |

**F8: files per node by MetaStore RAM budget** (bytes per file at 10M files, attributes + directory entry, no xattrs). A node holds one full replica of every shard it is in, and a follower's tree is the same size as the leader's. "Replicas" is the number of file replicas that fit in the budget. "Distinct" is the cluster capacity per node with 3 replicas per shard (replicas ÷ 3): N nodes hold N × distinct files.

| Representation | B/file | 4 GiB replicas / distinct | 16 GiB | 64 GiB |
| --- | --- | --- | --- | --- |
| `MemTree`, macOS malloc (measured) | 373 | 11.5M / 3.8M | 46.1M / 15.4M | 184M / 61M |
| `MemTree`, glibc model | 411 | 10.4M / 3.5M | 41.8M / 13.9M | 167M / 56M |
| `MemTree` with the checkpoint peak (1.75×) | 651 | 6.6M / 2.2M | 26.4M / 8.8M | 106M / 35M |
| One allocation per entry, `BTreeSet` (best mutable) | 235 | 18.3M / 6.1M | 73.1M / 24.4M | 292M / 97M |
| One allocation per entry, glibc model | 263 | 16.3M / 5.4M | 65.3M / 21.8M | 261M / 87M |
| Prefix-compressed blocks (best, immutable) | 135 | 31.7M / 10.6M | 127M / 42M | 507M / 169M |

Not counted: Raft entries applied but not yet checkpointed (up to `max_replay_bytes`, 64 MiB per disk log), replay records of client requests, the open-file registry and caches, allocator fragmentation beyond `phys_footprint`'s 2%, and xattrs (one `user.DOSATTRIB` per inode adds 175 B per file to the `MemTree`, ×1.47). The checkpoint row is the only one that includes a transient peak.

**Causes of the surprises:**

- **Spare capacity in checkpoint pieces.** `MemTree::checkpoint` starts each piece with `Vec::new()` and grows it by `extend_from_slice` ([lsm.rs](https://github.com/nvcnvn/rock-solid/blob/200714b/crates/core/src/lsm.rs), `checkpoint`). `RawVec::grow_amortized` grows to `max(2 × cap, needed)` (library/alloc/src/raw_vec/mod.rs:523 in Rust 1.99.1), so whenever an append needs more than double, the capacity leaves the power-of-two sequence: a piece of 1,044,435 B ended with 1,212,416 B of capacity and the next with 1,835,008 B. Pieces averaged 1.667× their length. `Vec::with_capacity(max_piece)` removes it.
- **Restored trees are bigger than built ones.** `restore` inserts entries in sorted order one at a time. When a leaf is full and the insert lands at its right end, `splitpoint` keeps 6 of 11 entries on the left (library/alloc/src/collections/btree/node.rs:914-922), and sorted inserts never touch that left leaf again, so restored leaves stay about 55% full: 107 B of node per entry against 95 in create order. `BTreeMap::from_iter` instead calls `bulk_build_from_sorted_iter` (map.rs:2544-2553), which fills nodes completely: 296 B against 397 B per file.
- **Node overhead dominates.** Rust's B-tree has a fixed capacity of 11 entries per node (`B = 6`, node.rs:43), and `Vec<u8>` keys and values are 24 B each inline, so a leaf is 544 B and rounds to a 640 B class. Shrinking the inline slot (`Box<[u8]>`, 16 B) or merging key and value into one allocation (one 16 B slot, 192 B leaf, one malloc instead of two) cuts the per-entry cost without changing the tree.

## Conclusions

1. **A file costs 373 B in the stage-1 `MemTree` (411 B with glibc), flat from 1M to 30M files and growing 0.9 to 1.2 B per name byte** (F1 to F3). Use 400 B per file for budgets on Linux, plus about 175 B per xattr stored as its own key (F1).
2. **Stage 1 holds about 2.9M file replicas per GiB of MetaStore RAM (2.6M with glibc).** A node with a 16 GiB budget holds 42M to 46M file replicas: 14M to 15M distinct files per node with 3 replicas (F8). Stage 2 becomes necessary when a node's MetaStore must hold more than that, or sooner when SMB xattrs are on (548 B per file with one xattr per inode, 31M replicas in 16 GiB). Below that, a memtable-only shard is faster than the redb bar: get p50 0.88 µs, 1,000-entry listing 3.3 µs (F7).
3. **Checkpoint must stream: do not return every piece at once.** As written, `checkpoint` holds 278 B per file of pieces on top of the 373 B tree (1.75×), and `restore` holds all pieces next to the new tree (1.81×; 2.81× when replacing a live tree). Build pieces with `Vec::with_capacity(max_piece)` (167 B per file of data, peak 1.45×), and hand them to the log one at a time so the peak is one piece. Because applies continue while a real checkpoint is written, streaming also needs a frozen view: a copy-on-write tree, or a frozen memtable plus a fresh one, which is stage 2's flush (F5).
4. **Restart costs about 0.38 s per million files** to restore (5.5M entries per second, 2.09 entries per file), plus replay; at 46M file replicas per node that is 17 s before replay if every shard restores one after another (F6). It is CPU-bound and parallel per shard.
5. **Before stage 2, `MemTree` can get 1.6× smaller with no design change:** store each entry as one allocation in a `BTreeSet` keyed by the key part (235 B per file, same get and listing speed), and restore with `from_iter`/bulk build instead of repeated `insert` (the sorted inserts leave leaves 55% full). Both are changes inside `MemTree` behind the `Tree` trait (F4).
6. **Stage 2's sorted runs, if kept in RAM, are 2.8× smaller than `MemTree`**: prefix-compressed blocks of 16 entries take 135 B per file, less than the 154 B of raw key and value, and still read in 1.4 µs and list 1,000 entries in 8.6 µs (F4). So stage 2 can raise the file count per node 2.8× before any run has to leave memory, and the same layout is the natural on-log format for those runs.

## Rerun after applying conclusions 3 and 5

`MemTree` now stores each entry as one allocation, restores by bulk build and streams snapshots from a frozen view (`Tree::begin_snapshot`, `Tree::next_piece`). The harness builds against that API, so `./run.sh` reproduces the rerun; the results above came from `f3bfd04`. The streamed F5 hands out every piece and drops each at once, as the log does after a piece is durable.

| Test | Result |
| --- | --- |
| F1 attributes + directory entry, 1M files | **235 B/file** (glibc model 263 B), as F4 predicted. 2.39 allocations per file |
| F1 +1 / +3 xattrs | +101 B, then +117 B per xattr (glibc model +103, +124 B) |
| F1 restored by snapshot + bulk-built `restore` | 209 B/file (was 397 B) |
| F5 streamed snapshot, 10M files | **Peak 1.0004× the tree**: 1,048,720 B above it, one piece. Pieces hold exactly their length. Holding every piece at once is 1.71×, 1 + serialized / tree |
| F6 restore, 10M files | **19 to 25M entries/s** (was 5.5M), 0.08 to 0.11 s per million files |
| F7 get and 1,000-entry listing | Get p50 0.96 to 1.00 µs, listing p50 3.4 to 3.5 µs, over three runs on a host at load 3.1 to 3.6 |
