---
name: spike
description: Run a throwaway experiment that answers one design question against real kernels, libraries or hardware (FUSE, SPDK, RDMA, Samba, io_uring, eBPF...) before the design commits to it. Use when the user asks to spike, prototype, measure or check whether something works, or when a design open question can only be settled by measurement. Covers layout under spikes/, building without root, recording results and feeding conclusions back into docs/design.md.
---

# Spike

A spike answers **one question** that shapes the design. The spike's code is thrown away; what lasts is its findings, which go back into the design. Examples: [spikes/fuse-coherence](../../../spikes/fuse-coherence/), [spikes/bypass-drivers](../../../spikes/bypass-drivers/).

## 1. Frame the question first

Before writing code, agree on with the user:

- **Question:** one sentence that ends in "?". It names the design decision it unblocks, for example "Can SPDK and RDMA be added later behind the `Action`/`Event` interface without changing the core?".
- **Tests:** numbered IDs grouped by letter (T1, T2, T5b; S1–S6; R0–R6). Every test has an expected outcome written down *before* it runs. Plan tests that try to break the hypothesis, not only ones that confirm it.
- **Stand-ins:** what substitutes for the real system (two FUSE mounts for two nodes, Soft-RoCE for a real NIC, a file-backed bdev for NVMe), and which conclusions that stand-in can't support.

## 2. Layout

```
spikes/<name>/
  README.md       Question, Setup, run commands, results tables, Conclusions
  fetch.sh        shallow clones at pinned tags, plus a user-local sysroot from .debs
  env.sh          source me: PATH, include, lib and pkg-config paths into vendor/
  build.sh        builds into bin/ (or next to the source)
  *.c / run.py    harness: C for the system under test, Python to drive scenarios
  results*.txt    raw output, first line: kernel, library versions, date
  setup-root.sh   only if root is unavoidable; with teardown-root.sh to undo it
  .gitignore      vendor/ bin/ work/
```

- **No root, no `apt install`.** `apt-get download` the `.deb`s and unpack them with `dpkg -x` into `vendor/sysroot`, then point `env.sh` at it (see `bypass-drivers/fetch.sh`). Pin every clone to a tag with `--depth 1`. If root is truly needed, put it in `setup-root.sh` and **ask the user to run it**; never run sudo yourself.
- Use C against the C library directly; it keeps bindings out of the question. Go is worth it when the question is about Go (cgo crossings, the scheduler, the collector); give the spike its own `go.mod`, so it stays out of the main module and `scripts/lint.sh`.
- The harness prints `[OK ]` or `[STALE]`/`[FAIL]` with `got=` and `want=` for each check, plus timings in µs, so the results file reads on its own.
- Stop processes and unmount (`fusermount3 -u`) on exit, even on failure.

## 3. Run and verify

- Save the full output to `results*.txt`. Never edit results by hand; rerun instead.
- A surprising result counts only after you find its cause in the source: cite the file and line (for example `fs/fuse/file.c`: `ff->open_flags`, `rxe_comp.c:772`). Link the kernel source at the exact tag.
- Measure cost as well as correctness: latency p50, and **CPU per operation** (a design goal). Busy-poll and event modes cost differently; report both.
- Note where a stand-in limits a conclusion (Soft-RoCE is slower than TCP, so it can't settle RDMA performance).

## 4. Write up

`README.md`, in this order:

1. **Question.** Bold label, one paragraph.
2. **Setup.** Kernel, library versions, hardware, stand-ins, then a code block of the commands to reproduce.
3. **Results tables:** `| Test | Result |`, one row per test ID. Bold the result that changes the design.
4. **Conclusions:** numbered rules the design must follow, each with the measurement behind it.

Then fold it into [docs/design.md](../../../docs/design.md) with the `design-doc` skill:

- Add or rewrite `## Appendix: <name> spike` with the same Question / Setup / Test table / Conclusions, linking `../spikes/<name>/`.
- Apply each conclusion where it belongs (architecture, driver contract, testing strategy, roadmap). Tick off or rewrite any open question it answers, and add the new ones it raises.
- If a conclusion settles a design choice, record it in the section that owns it and in "Alternatives considered" (see the `design-doc` skill).

Commit the spike and the design changes together: `Add <name> spike`.
