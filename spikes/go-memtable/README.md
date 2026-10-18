# Go memtable spike

**Question.** If the MetaStore were written in Go, what would its stage-1 memtable (the whole shard state in memory, [On-disk format](../../docs/design.md#on-disk-format)) cost the garbage collector at 10 to 20 million files per node, and which layout avoids that cost without giving up the memory per file the Rust `MemTree` gets? The candidates are a B-tree of `[]byte` entries (the Rust `MemTree`'s shape), the same B-tree over handles into byte slabs, and a copy-on-write B+-tree of 4 KiB pages in pointer-free slabs.

**Setup.** Linux 6.18.44 in a cloud VM (Firecracker), 4 vCPUs of an Intel Xeon at 2.1 GHz, 15 GiB. Go 1.27.0 (Green Tea on by default; a second binary is built with `GOEXPERIMENT=nogreenteagc`), Rust 1.99.1, no dependencies outside the standard libraries and `crates/core`. `crates/core` has since been deleted with the rest of the Rust code ([Runtime in Go](../../docs/design.md#runtime-in-go)); the Rust harness builds at commit `200714b`.

- **Data.** [gen.go](gen.go) writes the [MetaStore memory spike](../metastore-memory/README.md)'s file model to a file: [key layout](../../docs/design.md#metadata-layer-metastore) keys, a 92 B attribute record per inode, a directory entry per name with lengths drawn from that spike's histogram, a slot counter per directory, 20 to 50 files per directory and a 1,000-entry directory. No xattrs. 154 B of key and value per file, 2.09 entries per file. Every variant and the Rust baseline load the same file in the same order.
- **Variants.** [btree.go](btree.go) is a B-tree of degree 32, as `github.com/google/btree` builds it. *slices* ([variants.go](variants.go)) stores each entry as one `[]byte`, `key_len u16 | key | value`, as the Rust `MemTree`'s `Entry` does: one pointer per entry. *arena* stores `uint64` handles into 64 MiB byte slabs, so only the B-tree's nodes are pointers. An overwrite appends a new entry and leaves a hole, which this spike does not reclaim. *pages* ([pages.go](pages.go)) is a B+-tree of 4 KiB pages in 64 MiB slabs of `[16384][4096]byte`: slot offsets and entries inside each page, children by page number, and copy-on-write of the pages a write touches while a snapshot is frozen. A leaf splits in half, except an append past its last key, which keeps the leaf full.
- **Arena variant.** [goarena.go](goarena.go), built only with `GOEXPERIMENT=arenas`, allocates variant 1's entries, nodes and item arrays from one Go arena; the [tuned Go spike](../go-tuned/README.md) measures it.
- **Rust baseline.** [rust/](rust/) loads the same file into the real `rock_core::lsm::MemTree` and measures memory with glibc's `mallinfo2`.
- **Measurements** ([main.go](main.go)): memory per file from `runtime/metrics` heap objects and from RSS (M1). Three forced `runtime.GC()` cycles with the tree live: wall time, GC CPU and the longest pause (M2). Ten seconds of request load (M3): two goroutines doing lookups while a third allocates short-lived 256 B to 4 KiB objects at a target of 256 MB/s. The memory limit is set 512 MiB above the tree, as a daemon would set `GOMEMLIMIT`. Speed of a million random gets, a 1,000-entry scan and a million same-length overwrites (M4). For pages, 100,000 updates while a snapshot is frozen (M6). A rebuild in key order, as loading a snapshot does (M7).

```
./run.sh        # about 25 minutes; generates 10M- and 20M-file data (about 5 GB) under work/, deletes it after
```

Raw output: [results.txt](results.txt).

Expected outcomes, written before the run: slices costs the collector a full mark of every entry each cycle; arena and pages cost it about nothing; pages take about 230 B per file at the 69% fill of a B-tree under random inserts.

## Results

At 10 million files (20.9 million entries):

| Test | Rust `MemTree` | Go slices | Go arena | Go pages |
| --- | --- | --- | --- | --- |
| M1 bytes per file, create order | 264 (malloc) | 256 heap, 268 RSS | 199 heap, 192 RSS | 276 heap, 270 RSS; **pages 63% full** |
| M7 bytes per file, rebuilt in key order | 237 | 279 | 207 | **175**; pages 98% full |
| M2 forced GC: wall, GC CPU | — | **247 to 266 ms, 558 to 636 ms** | 19 to 24 ms, 23 to 33 ms | **0.4 to 1.0 ms, 0.2 to 2.5 ms** |
| M3 under request garbage: GC CPU | — | **0.93 cores, 23% of process CPU**; garbage reached 161 of 256 MB/s | 0.02 cores, 0.5% | 0.001 cores, 0.0% |
| M3 longest GC pause | — | 0.33 ms | 0.23 ms | 0.26 ms |
| M3 get p99.9 under load | — | 29 µs | 30 µs | 21 µs |
| M4 get p50 / p99 | 2.29 / 3.80 µs | 2.64 / 4.50 µs | 2.99 / 5.87 µs | **1.66 / 5.29 µs** (1.43 / 2.36 µs under M3) |
| M4 1,000-entry scan p50 | 4.2 µs | 8.3 µs | 17.2 µs | 10.1 µs |
| M4 same-length overwrite | 3.17 µs | 4.24 µs | 4.20 µs, leaving 78 MB of holes per million | 2.59 µs, in place |

| Test | Result |
| --- | --- |
| M2, M3 at 20 million files, slices | Forced GC 537 to 549 ms wall, 1,254 to 1,268 ms CPU: twice the 10M figures. Under request garbage, **GC took 1.35 cores, 35% of process CPU, and the allocating goroutine reached only 10 of 256 MB/s**: GC assists charge marking work to whichever goroutine allocates. Longest pause 0.66 ms |
| M2, M3 at 20 million files, pages | Forced GC 0.3 to 0.9 ms; 0.0% of CPU under request garbage. get p50 1.76 µs |
| Green Tea against `nogreenteagc`, slices | Forced-GC CPU 13.6% lower at 10M (601 against 696 ms, mean of three) and 6.9% lower at 20M (1,261 against 1,355 ms); wall time the same. Under request garbage 23.3% against 23.6% and 35.2% against 38.1% of process CPU |
| M6 copy-on-write snapshot, pages, 10M files | 100,000 random updates while frozen copied 94,208 pages: **+22.5% memory (368 MiB)** until the snapshot ends, then freed. An update costs 4.46 µs while frozen, against 2.59 µs in place. The frozen view still held all 20.9M entries |
| M1 build time | Rust 8.13 s; Go slices 8.15 s, arena 9.44 s, pages 7.72 s for 20.9M inserts. The first run after generating each data set took 1.4 to 2.3× longer, likely while the kernel wrote the new file back (not verified), so its build time is not comparable |
| M7 rebuild time | Not comparable: the Rust baseline bulk-builds from snapshot pieces (16.1 s including writing them), the Go variants re-insert one entry at a time beside the original tree (25.9 to 46.7 s) |

## Conclusions

1. **In Go, the memtable's pointer layout decides its cost, not the collector's version.** A tree with one pointer per entry, which is the Rust `MemTree`'s shape, costs about 0.6 CPU-seconds per collection at 10 million files and 1.3 at 20 million. Under request garbage near the memory limit, collection takes 23 to 35% of the process's CPU and throttles every goroutine that allocates (M2, M3). Pauses stay under 1 ms: the cost is CPU and allocation stalls, not stop-the-world pauses. Green Tea cuts the CPU by 7 to 14% and changes nothing else.
2. **A Go memtable must keep its entries out of the collector's view: 4 KiB pages in pointer-free slabs.** The page tree costs the collector under 1 ms per cycle at any size and nothing under load. Its median get is the fastest measured, Rust included (1.4 to 1.8 µs against 2.3 µs), though its p99 is not (5.3 against 3.8 µs at 10M files, idle). It takes about the bytes per file of the Rust `MemTree` in create order (270 against 264 B) and 26% less after a restore (175 against 237 B).
3. **The page tree needs fuller pages before it ships.** Inserts in create order leave pages 63% full, not the 69% expected, because each directory's entries arrive as ascending runs inside the tree. Splitting at the insertion point made it worse (58%). Redistributing into a neighbour before splitting, as a B*-tree does, or rebuilding pages in the background are the candidates. A restore fills them to 98%.
4. **Copy-on-write snapshots cost memory in proportion to the pages written while frozen**: +22.5% for 100,000 updates on 10 million files. That replaces the Rust `MemTree`'s side sets of replaced and added entries, and needs a bound on snapshot duration or on the pages it may copy.
5. **Byte slabs with handles are the cheapest in memory before any overwrite (192 B per file), but need their own allocator.** Every overwrite leaves a hole (78 MB per million updates), and the B-tree's nodes still cost the collector 20 ms per cycle.

Not covered: deletes and page merges, xattrs and entries larger than a page, prefix compression inside pages, concurrent readers during writes, and `syscall.Mmap` for the slabs instead of the Go heap.
