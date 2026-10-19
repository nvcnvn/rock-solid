# Log per core spike

**Question.** With an event loop per core, should each disk's log belong to one loop that collects the records of shards running on other loops through its inbox, or should each loop append to its own log on every disk? The first keeps one flush per disk per round but adds two inbox hops per round for every other loop; the second keeps loops independent but multiplies flushes, and the [NVMe flush spike](../nvme-flush/README.md) (N7) saw a consumer drive take two concurrent flush streams at unchanged latency and lose latency at four or more. What does an inbox hop cost in Go, and does one owner per disk keep up with several producer loops?

**Setup.** Written against commit `676095bf56fd`. [main.go](main.go) is a Go harness with no dependencies; [run.sh](run.sh) runs it and [summarize.py](summarize.py) takes medians and prints the checks.

- *Per loop* (`-mode perloop -k K`): K goroutines, each a loop with 8 items of 4 KiB outstanding, each group-committing its 8 items as one `O_DIRECT` `pwrite` into its own region of a 1 GiB file, then `fdatasync`. This is one log per loop on one disk.
- *Owner* (`-mode owner -k K`): K producer loops, each with 8 items outstanding, push them through a single-producer ring into one owner loop and write the owner's eventfd only when the ring was empty. The owner sleeps in `epoll_wait` on that eventfd, drains every ring, writes everything queued as one record (up to 1 MiB) and flushes once, then pushes each completion back through the producer's ring and eventfd. This is the inbox design of [Runtime in Go](../../docs/design.md#runtime-in-go) with one log per disk.
- *Hop* (`-mode hop -batch B`): two loops ping-pong B items through the same rings and eventfds; a round trip is two hops.

The file is `fallocate`d by writing it in full once, so `fdatasync` never commits the ext4 journal. Device flushes come from `/sys/block/vda/stat` (field 16), CPU per item from `getrusage(RUSAGE_SELF)` over the whole process. `GOMAXPROCS` is set above the number of loops, as the design requires. Each configuration ran 3 times for 3 s (hops 2 s), interleaved.

**Stand-in.** A cloud VM (Firecracker, Linux 6.18.44, 4 vCPUs of a 2.1 GHz Xeon, Go 1.27.0). Its disk is a virtio block device with **one hardware queue** and a volatile write cache, where a flush costs about 60 µs and the device is bandwidth-bound ([stream overlap spike](../stream-overlap/README.md)). A consumer NVMe drive has one hardware queue per CPU and is flush-bound at about 1 ms per commit. **The machine was shared with six other agents running benchmarks** (load average 4.7 on 4 vCPUs at the end), so latencies are noisy; CPU per item and flushes per record are the reliable numbers. The VM cannot reproduce N7: it says nothing about how many flush streams an NVMe drive overlaps.

```
DEV=vda ./run.sh vm      # about 90 s; writes work/log.dat (1 GiB) and deletes it -> results-vm.txt
DEV=nvme0n1 FILE=/mnt/nvme/log.dat ./run.sh nvme   # FILE on a filesystem of that drive
```

Raw output: [results-vm.txt](results-vm.txt). One pilot run preceded the checks; the expected outcomes were then written into `summarize.py` before the recorded run.

## Results (VM stand-in, medians of 3)

| Test | Result |
| --- | --- |
| H1: hop, 1 item, both loops asleep between hops | **13.6 µs of CPU per hop** (27.2 µs per round trip, 2.06 context switches), round trip p50 27 µs, p99 68 µs. Nearly all of it is the wakeup of a loop blocked in `epoll_wait` |
| H1b: hop, 8 / 64 items per hop | 0.73 / **0.07 µs of CPU per item per hop**: a hop costs per wakeup, not per item |
| L1: one log per loop, 1 / 2 / 4 / 8 loops | 21,900 / 25,700 / 32,900 / 39,800 items/s; p50 331 / 507 / 914 / 1,521 µs; 1.00 / 1.00 / 0.69 / 0.36 device flushes per record (with one hardware queue the kernel folds flushes from 4 loops up); CPU 8.2 to 10.7 µs per item |
| C1: 2 loops each with its own log against 1 (N7 on NVMe: about 2×) | **1.17× the items/s (FAIL, expected ≥ 1.6)**: this device serializes its flushes; it cannot stand in for N7 |
| L2: one owner, 1 / 2 / 4 / 8 producer loops | 19,300 / 22,700 / 35,700 / 50,400 items/s; p50 346 / 575 / 778 / 1,162 µs; **1.00 device flush per record** (C2 OK); items per record 8 / 9.1 / 17.3 / 39.3; 2.0 / 2.3 / 4.4 / 9.9 eventfd writes per record |
| C3: owner against one log per loop, items/s | 1.08× at 4 loops, **1.27× at 8** (OK): the owner packs more items per flush as producers grow, where separate logs keep 8 per flush |
| C4: owner against one log per loop, CPU per item | 1.15 / 1.20 / 0.93 / 1.00× at 1 / 2 / 4 / 8 loops (OK, ≤ 1.25): the hops cost up to 20% with one or two producers and nothing measurable with more, where larger records amortize them |
| Latency, owner against one log per loop | p50 5 to 13% higher at 1 and 2 loops (one hop each way), 15 to 24% lower at 4 and 8 |

## Conclusions

1. **An inbox hop costs a wakeup, about 13.6 µs of CPU on this VM, and almost nothing per item once batched** (0.07 µs per item at 64 items). Handing a round's records to the disk's owner therefore costs two wakeups per producer loop per round, not per operation: at 1,000 rounds per second, about 2.7% of a core per producer loop in the worst case where both loops would otherwise sleep. A producer loop that submits its own log's write and flush through io_uring pays one completion wakeup per round anyway.
2. **One owner per disk keeps one flush per round and grows its records with the producers**, so at 4 and 8 producer loops it moved more items than a log per loop with the same CPU per item, and with one flush per record where separate logs on an NVMe drive would issue one flush per loop per round, past the drive's two-stream knee (N7) once a disk has more than two loops.
3. The VM's virtio disk serializes flushes and folds them across loops, so it cannot show the NVMe drive's flush concurrency; `DEV=nvme0n1 ./run.sh nvme` on the drive of the NVMe flush spike would.
