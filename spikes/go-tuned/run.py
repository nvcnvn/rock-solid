#!/usr/bin/env python3
"""Tuned Go against Rust: design (T1), compiler options (T2) and arenas (T3). Builds every binary,
collects the PGO profiles, runs the tests and writes raw lines, tables and checks to results.txt.

python3 run.py [t1] [t2] [t3]
"""
import datetime
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CPU_SPIKE = os.path.join(HERE, "..", "go-cpu-per-op")
MEM_SPIKE = os.path.join(HERE, "..", "go-memtable")
CLEANER_SPIKE = os.path.join(HERE, "..", "parallel-cleaner")
BIN = os.path.join(HERE, "bin")
WORK = os.path.join(HERE, "work")
GO_ENV = {**os.environ, "GOTOOLCHAIN": os.environ.get("GOTOOLCHAIN", "go1.27.0")}
FAILS = []


def sh(args, cwd=HERE, env=None):
    subprocess.run(args, cwd=cwd, check=True, env=env or GO_ENV)


def go_build(out, pkg, cwd, goamd64="v1", pgo="off", gcflags=None, experiment=None):
    env = {**GO_ENV, "GOAMD64": goamd64}
    if experiment:
        env["GOEXPERIMENT"] = experiment
    args = ["go", "build", "-o", out, f"-pgo={pgo}"]
    if gcflags:
        args.append(f"-gcflags=all={gcflags}")
    sh(args + [pkg], cwd=cwd, env=env)


def cargo_build(cwd, target_dir, rustflags=None):
    env = dict(GO_ENV)
    if rustflags:
        env["RUSTFLAGS"] = rustflags
    sh(["cargo", "build", "--release", "--quiet", "--target-dir", target_dir], cwd=cwd, env=env)


def run(args, env=None):
    out = subprocess.run(args, check=True, capture_output=True, text=True, env=env or GO_ENV).stdout
    line = out.strip().splitlines()[-1]
    print(line, flush=True)
    return {k: float(v) for k, v in (p.split("=") for p in line.split("|")[1].split())}


def avg(runs):
    return {k: sum(r[k] for r in runs) / len(runs) for k in runs[0]}


def check(tid, what, ok, got, want):
    print(f"[{'OK ' if ok else 'FAIL'}] {tid} {what} got={got} want={want}", flush=True)
    if not ok:
        FAILS.append(tid)


def table(title, cols, rows):
    print(f"\n| {title} | " + " | ".join(cols) + " |")
    print("|---" * (len(cols) + 1) + "|")
    for name, vals in rows:
        print(f"| {name} | " + " | ".join(vals) + " |")
    print(flush=True)


RUST_REPL = os.path.join(CPU_SPIKE, "replicate-rust", "target", "release", "replicate-rust")
RUST_REPL_V3 = os.path.join(WORK, "rust-v3-replicate", "release", "replicate-rust")
RUST_CLEANER = os.path.join(CLEANER_SPIKE, "target", "release", "parallel-cleaner")
RUST_CLEANER_V3 = os.path.join(WORK, "rust-v3-cleaner", "release", "parallel-cleaner")


def build():
    os.makedirs(BIN, exist_ok=True)
    os.makedirs(WORK, exist_ok=True)
    go_build(os.path.join(BIN, "replicate-go"), "./replicate-go", CPU_SPIKE)
    go_build(os.path.join(BIN, "replicate-go2"), "./replicate-go2", HERE)
    go_build(os.path.join(BIN, "replicate-go3"), "./replicate-go3", HERE)
    go_build(os.path.join(BIN, "cleaner-go"), "./cleaner-go", CPU_SPIKE)
    cargo_build(os.path.join(CPU_SPIKE, "replicate-rust"), os.path.join(CPU_SPIKE, "replicate-rust", "target"))
    cargo_build(os.path.join(CPU_SPIKE, "replicate-rust"), os.path.join(WORK, "rust-v3-replicate"),
                "-C target-cpu=x86-64-v3")
    cargo_build(CLEANER_SPIKE, os.path.join(CLEANER_SPIKE, "target"))
    cargo_build(CLEANER_SPIKE, os.path.join(WORK, "rust-v3-cleaner"), "-C target-cpu=x86-64-v3")


CLEANER_ARGS = ["mode=inline", "u=0.95", "crc=0"]


def t1():
    print("=== T1: replicated 4K write, three Go designs against Rust (all batching writes)", flush=True)
    configs = [("rust, epoll loop per node", [RUST_REPL], ["coalesce=1"]),
               ("go v1, goroutine per connection", [os.path.join(BIN, "replicate-go")], ["coalesce=1"]),
               ("go v2, atomics, no handoff", [os.path.join(BIN, "replicate-go2")], []),
               ("go v3, epoll loop per node", [os.path.join(BIN, "replicate-go3")], ["lock=0"])]
    res = {}
    for conns in (1, 16):
        for _ in range(2):
            for name, binary, extra in configs:
                res.setdefault((name, conns), []).append(run(binary + [f"conns={conns}", "secs=10"] + extra))
    rows = []
    for conns in (1, 16):
        base = avg(res[(configs[0][0], conns)])
        for name, _, _ in configs:
            a = avg(res[(name, conns)])
            res[(name, conns)] = a
            rows.append((f"{name}, {conns} conn",
                         [f"{a['ops_per_s']:.0f}", f"{a['cpu_us_per_op']:.1f}",
                          f"{a['cpu_us_per_op'] / base['cpu_us_per_op']:.2f}", f"{a['cores']:.2f}",
                          f"{a['reads_per_op']:.2f}", f"{a['writes_per_op']:.2f}", f"{a['lat_p50_us']:.1f}",
                          f"{a['lat_p99_us']:.1f}"]))
    table("design, connections", ["ops/s", "CPU us/op", "x Rust", "cores", "socket reads/op", "socket writes/op",
                                  "p50 us", "p99 us"], rows)
    v2 = res[("go v2, atomics, no handoff", 16)]
    check("T1", "go v2 CPU per write at 16 connections", 30 <= v2["cpu_us_per_op"] <= 34,
          f"{v2['cpu_us_per_op']:.1f}us", "30-34us (estimate before the run)")
    check("T1", "go v2 socket writes per operation at 16 connections", v2["writes_per_op"] <= 3.2,
          f"{v2['writes_per_op']:.2f}", "<=3.2 (estimate before the run)")


def t2():
    print("=== T2: compiler options; PGO profiles come from a 10 s run of each default build", flush=True)
    repl_prof = os.path.join(WORK, "replicate-go3.pprof")
    clean_prof = os.path.join(WORK, "cleaner-go.pprof")
    run([os.path.join(BIN, "replicate-go3"), "conns=16", "secs=10", "lock=0"], env={**GO_ENV, "CPUPROFILE": repl_prof})
    run([os.path.join(BIN, "cleaner-go")] + CLEANER_ARGS, env={**GO_ENV, "CPUPROFILE": clean_prof})
    builds = [("default (GOAMD64=v1)", {}), ("GOAMD64=v3", {"goamd64": "v3"}),
              ("PGO", {"pgo": "PROFILE"}), ("PGO + GOAMD64=v3", {"pgo": "PROFILE", "goamd64": "v3"}),
              ("no bounds checks (-B, measurement only)", {"gcflags": "-B"})]
    binaries = {}
    for i, (name, opts) in enumerate(builds):
        for tool, pkg, cwd, prof in (("replicate-go3", "./replicate-go3", HERE, repl_prof),
                                     ("cleaner-go", "./cleaner-go", CPU_SPIKE, clean_prof)):
            out = os.path.join(WORK, f"{tool}-{i}")
            o = dict(opts)
            if o.get("pgo") == "PROFILE":
                o["pgo"] = prof
            go_build(out, pkg, cwd, **o)
            binaries[(name, tool)] = out
    res = {}
    for _ in range(2):
        for name, _ in builds:
            res.setdefault(("go", name, "repl"), []).append(
                run([binaries[(name, "replicate-go3")], "conns=16", "secs=10", "lock=0"]))
            res.setdefault(("go", name, "clean"), []).append(run([binaries[(name, "cleaner-go")]] + CLEANER_ARGS))
        for name, repl, clean in (("default (x86-64-v2 for replicate, v1 for cleaner)", RUST_REPL, RUST_CLEANER),
                                  ("target-cpu=x86-64-v3", RUST_REPL_V3, RUST_CLEANER_V3)):
            res.setdefault(("rust", name, "repl"), []).append(run([repl, "conns=16", "secs=10", "coalesce=1"]))
            res.setdefault(("rust", name, "clean"), []).append(run([clean] + CLEANER_ARGS))
    rows = []
    base = {}
    for lang, names in (("go", [n for n, _ in builds]),
                        ("rust", ["default (x86-64-v2 for replicate, v1 for cleaner)", "target-cpu=x86-64-v3"])):
        for name in names:
            r, c = avg(res[(lang, name, "repl")]), avg(res[(lang, name, "clean")])
            base.setdefault(lang, (r, c))
            br, bc = base[lang]
            rows.append((f"{lang}: {name}", [f"{r['cpu_us_per_op']:.1f}", f"{r['cpu_us_per_op'] / br['cpu_us_per_op']:.3f}",
                                             f"{r['ops_per_s']:.0f}", f"{c['owner_work_ns']:.0f}",
                                             f"{c['owner_work_ns'] / bc['owner_work_ns']:.3f}"]))
            res[(lang, name)] = (r, c)
    table("language: build", ["replicate CPU us/op (16 conn)", "x default", "ops/s", "cleaner owner ns/write (inline, 95%)",
                              "x default"], rows)
    d, p = res[("go", "default (GOAMD64=v1)")], res[("go", "PGO")]
    gain = 1 - p[1]["owner_work_ns"] / d[1]["owner_work_ns"]
    check("T2", "PGO gain on the cleaner harness", 0.02 <= gain <= 0.07, f"{gain:.1%}", "2-7% (estimate)")
    gain = 1 - p[0]["cpu_us_per_op"] / d[0]["cpu_us_per_op"]
    check("T2", "PGO gain on the replicated write", 0.02 <= gain <= 0.07, f"{gain:.1%}", "2-7% (estimate)")
    b = res[("go", "no bounds checks (-B, measurement only)")]
    gain = 1 - b[1]["owner_work_ns"] / d[1]["owner_work_ns"]
    check("T2", "bounds checks' share of the cleaner harness", 0.01 <= gain <= 0.05, f"{gain:.1%}", "1-5% (estimate)")


def t3():
    print("=== T3: memtable at 10M files: []byte B-tree on the Go heap, the same in a Go arena, 4K pages", flush=True)
    exe = os.path.join(BIN, "go-memtable-arenas")
    go_build(exe, ".", MEM_SPIKE, experiment="arenas")
    data, probes = os.path.join(WORK, "d10.bin"), os.path.join(WORK, "p10.bin")
    names = os.path.join(MEM_SPIKE, "..", "metastore-memory", "name-lengths.txt")
    out = subprocess.run([exe, "gen", "files=10000000", f"names={names}", f"data={data}", f"probes={probes}"],
                         check=True, capture_output=True, text=True, env=GO_ENV).stdout
    print(out.strip(), flush=True)
    sh(["sync"])
    for v in ("slices", "slices-goarena", "pages"):
        out = subprocess.run([exe, "run", f"variant={v}", f"data={data}", f"probes={probes}", "restore=0"],
                             check=True, capture_output=True, text=True, env=GO_ENV).stdout
        print(out.strip(), flush=True)
    os.remove(data)
    os.remove(probes)


def main():
    want = set(sys.argv[1:]) or {"t1", "t2", "t3"}
    build()
    sys.stdout = Tee(os.path.join(HERE, "results.txt"))
    gov = subprocess.run(["go", "version"], capture_output=True, text=True, env=GO_ENV).stdout.split()[2]
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    cpu = next((ln.split(":", 1)[1].strip() for ln in open("/proc/cpuinfo") if ln.startswith("model name")), "?")
    flags = next((ln for ln in open("/proc/cpuinfo") if ln.startswith("flags")), "")
    print(f"# {platform.release()}, {cpu}, {os.cpu_count()} CPUs, avx2={'avx2' in flags.split()}, "
          f"avx512f={'avx512f' in flags.split()}, {gov}, {rustc}, {datetime.datetime.now().isoformat(timespec='seconds')}")
    for name, fn in (("t1", t1), ("t2", t2), ("t3", t3)):
        if name in want:
            fn()
    print(f"\n# checks failed: {', '.join(FAILS) or 'none'}")


class Tee:
    def __init__(self, path):
        self.f = open(path, "w")

    def write(self, s):
        self.f.write(s)
        sys.__stdout__.write(s)

    def flush(self):
        self.f.flush()
        sys.__stdout__.flush()


if __name__ == "__main__":
    main()
