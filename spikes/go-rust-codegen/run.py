#!/usr/bin/env python3
"""Where Go's extra CPU per replicated write goes. Builds everything, then dumps the generated
code of the frame parse loop and Go's bounds checks and inlining decisions (A), runs the
microbenchmarks (M), the replicate variants (V), their syscall and scheduler counters (P1), perf
profiles split by classify.py (P2) and Go's own pprof profile, and writes results.txt.

python3 run.py [asm] [micro] [replicate] [counters] [profile] [pprof]

Every timed command runs under `flock $BENCH_LOCK` so it never overlaps another benchmark on the
host. perf comes from vendor/ (fetch.sh).
"""
import datetime
import os
import platform
import re
import statistics
import subprocess
import sys

import classify

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "bin")
WORK = os.path.join(HERE, "work")
PERF_DIR = os.path.join(HERE, "vendor", "sysroot", "usr", "lib", "linux-tools-6.8.0-147")
SYSROOT_LIBS = [os.path.join(HERE, "vendor", "sysroot", p, "x86_64-linux-gnu") for p in ("usr/lib", "lib")]
ENV = {**os.environ, "GOTOOLCHAIN": os.environ.get("GOTOOLCHAIN", "go1.27.0"),
       "PATH": PERF_DIR + ":" + os.environ["PATH"],
       "LD_LIBRARY_PATH": ":".join(SYSROOT_LIBS + [os.environ.get("LD_LIBRARY_PATH", "")])}
GCCGO = os.path.join(HERE, "vendor", "gccgo")
GCCGO_ENV = {"LD_LIBRARY_PATH": os.path.join(GCCGO, "usr", "lib", "x86_64-linux-gnu")}
LOCK = os.environ.get("BENCH_LOCK", os.path.join(WORK, "bench.lock"))
MICRO_CPU = "3"
MICRO_ROUNDS = int(os.environ.get("MICRO_ROUNDS", "3"))
REPL_ROUNDS = int(os.environ.get("REPL_ROUNDS", "5"))
REPL_SECS = os.environ.get("REPL_SECS", "5")
OUT = open(os.path.join(HERE, "results.txt"), "a" if len(sys.argv) > 1 else "w")


def say(line=""):
    print(line, flush=True)
    OUT.write(line + "\n")
    OUT.flush()


def sh(args, cwd=HERE):
    subprocess.run(args, cwd=cwd, check=True, env=ENV)


def timed(args, env=None):
    out = subprocess.run(["flock", LOCK] + args, check=True, capture_output=True, text=True,
                         env={**ENV, **(env or {})})
    return out.stdout, out.stderr


def kv(line):
    return {k: float(v) for k, v in (p.split("=") for p in line.split("|")[1].split())}


def build():
    os.makedirs(BIN, exist_ok=True)
    os.makedirs(WORK, exist_ok=True)
    for name, src in (("replicate-rust", os.path.join(HERE, "..", "go-cpu-per-op", "replicate-rust")),
                      ("replicate-rust-kept", os.path.join(HERE, "replicate-rust-kept")),
                      ("micro-rust", os.path.join(HERE, "micro-rust"))):
        target = os.path.join(WORK, name)
        sh(["cargo", "build", "--release", "--quiet", "--target-dir", target], cwd=src)
        sh(["cp", os.path.join(target, "release", name), os.path.join(BIN, name)])
    sh(["go", "build", "-o", os.path.join(BIN, "replicate-go3"), "./replicate-go3"],
       cwd=os.path.join(HERE, "..", "go-tuned"))
    for name in ("replicate-go4", "micro-go"):
        sh(["go", "build", "-o", os.path.join(BIN, name), "./" + name])
    lib = os.path.join(GCCGO, "usr", "lib")
    subprocess.run([os.path.join(GCCGO, "usr", "bin", "gccgo-14"), "-B", os.path.join(lib, "gcc", "x86_64-linux-gnu", "14"),
                    "-B", os.path.join(GCCGO, "usr", "libexec", "gcc", "x86_64-linux-gnu", "14"),
                    "-L", os.path.join(lib, "x86_64-linux-gnu"), "-L", os.path.join(lib, "gcc", "x86_64-linux-gnu", "14"),
                    "-O2", "-march=x86-64-v2", "micro-go/main.go", "-o", os.path.join(BIN, "micro-gccgo")],
                   cwd=HERE, check=True, env={**ENV, **GCCGO_ENV})


def micro():
    say("=== M: microbenchmarks, ns per operation, median of 7 repetitions per round, "
        f"{MICRO_ROUNDS} rounds alternating, each pinned to CPU {MICRO_CPU} with taskset")
    res = {}
    for _ in range(MICRO_ROUNDS):
        for lang in ("go", "gccgo", "rust"):
            out, _ = timed(["taskset", "-c", MICRO_CPU, os.path.join(BIN, f"micro-{lang}")],
                           GCCGO_ENV if lang == "gccgo" else None)
            if lang == "gccgo":
                out = out.replace("lang=go ", "lang=gccgo ")
            for line in out.splitlines():
                say(line)
                m = re.match(r"BENCH lang=(\w+) name=(\S+) ns_per_op=([\d.]+)", line)
                res.setdefault(m.group(2), {}).setdefault(m.group(1), []).append(float(m.group(3)))
    say("\n| Benchmark | Go (gc) ns | gccgo ns | Rust ns | gc − Rust |\n|---|---|---|---|---|")
    for name, by in res.items():
        med = {lang: statistics.median(v) for lang, v in by.items()}
        cell = lambda lang: f"{med[lang]:.1f}" if lang in med else ""
        diff = f"{med['go'] - med['rust']:+.1f}" if "go" in med and "rust" in med else ""
        say(f"| {name} | {cell('go')} | {cell('gccgo')} | {cell('rust')} | {diff} |")
    say()


VARIANTS = [
    ("rust", ["replicate-rust"], {}),
    ("rust-kept", ["replicate-rust-kept", "touch=0"], {}),
    ("rust-kept touch", ["replicate-rust-kept", "touch=1"], {}),
    ("go3", ["replicate-go3", "lock=0"], {}),
    ("go4 touch", ["replicate-go4", "lock=0", "raw=0", "touch=1"], {}),
    ("go4 touch P8", ["replicate-go4", "lock=0", "raw=0", "touch=1"], {"GOMAXPROCS": "8"}),
    ("go4 raw touch", ["replicate-go4", "lock=0", "raw=1", "touch=1"], {}),
    ("go4 raw touch P8", ["replicate-go4", "lock=0", "raw=1", "touch=1"], {"GOMAXPROCS": "8"}),
]


def repl_cmd(args):
    return [os.path.join(BIN, args[0]), "conns=16", "secs=" + REPL_SECS] + args[1:]


def replicate():
    say(f"=== V: replicated 4K write, 16 connections, {REPL_SECS} s per run, {REPL_ROUNDS} rounds, "
        "variants interleaved")
    res = {}
    for _ in range(REPL_ROUNDS):
        for name, args, env in VARIANTS:
            out, _ = timed(repl_cmd(args), env)
            line = out.strip().splitlines()[-1]
            say(f"[{name}] {line}")
            res.setdefault(name, []).append(kv(line)["cpu_us_per_op"])
    say("\n| Variant | CPU µs per write, median | min to max | vs rust-kept touch |\n|---|---|---|---|")
    base = statistics.median(res["rust-kept touch"])
    for name, _, _ in VARIANTS:
        m = statistics.median(res[name])
        say(f"| {name} | {m:.2f} | {min(res[name]):.2f} to {max(res[name]):.2f} | {m - base:+.2f} |")
    say()


EVENTS = ["task-clock", "context-switches", "cpu-migrations", "page-faults", "raw_syscalls:sys_enter"] + [
    "syscalls:sys_enter_" + s for s in
    ("read", "recvfrom", "write", "sendto", "epoll_wait", "epoll_ctl", "futex", "nanosleep", "sched_yield",
     "tgkill")]


def counters():
    say("=== P1: syscalls and scheduler events per write (perf stat over the whole process)")
    for name, args, env in VARIANTS:
        out, err = timed(["perf", "stat", "-x", ",", "-e", ",".join(EVENTS), "--"] + repl_cmd(args), env)
        line = out.strip().splitlines()[-1]
        r = kv(line)
        ops = r["ops_per_s"] * r["secs"]
        say(f"[{name}] {line}")
        vals = []
        for row in err.splitlines():
            f = row.split(",")
            if len(f) > 2 and f[2] in EVENTS:
                ev = f[2].replace("syscalls:sys_enter_", "").replace("raw_syscalls:sys_enter", "all syscalls")
                v = float(f[0]) / ops * (1e3 if ev == "task-clock" else 1)
                vals.append(f"{ev}={v:.3f}" if ev != "task-clock" else f"task_clock_us={v:.2f}")
        say(f"COUNTERS {name} | " + " ".join(vals))
    say()


PROFILED = ["rust", "rust-kept touch", "go3", "go4 touch", "go4 raw touch P8"]


def profile():
    say("=== P2: perf record -g -e cpu-clock -F 4000, from the start of the measured window (-D) to exit, "
        "split by classify.py, µs per write, mean of 2 runs")
    table = {}
    for _ in range(2):
        for name, args, env in VARIANTS:
            if name not in PROFILED:
                continue
            data = os.path.join(WORK, name.replace(" ", "-") + ".perf")
            delay = "2300" if "touch=1" in args else "300"
            out, _ = timed(["perf", "record", "-q", "-g", "-e", "cpu-clock", "-F", "4000", "-D", delay,
                            "-o", data, "--"] + repl_cmd(args), env)
            line = out.strip().splitlines()[-1]
            r = kv(line)
            ops = r["ops_per_s"] * r["secs"]
            totals = classify.parse(data, ENV)
            say(f"[{name}] {line}")
            for k, v in sorted(totals.items()):
                table.setdefault(k, {}).setdefault(name, []).append(v / 1000 / ops)
            table.setdefault("total", {}).setdefault(name, []).append(sum(totals.values()) / 1000 / ops)
    say("\n| Bucket | " + " | ".join(PROFILED) + " |\n|---" * 1 + "|---" * len(PROFILED) + "|")
    order = sorted((k for k in table if k != "total"), key=lambda k: (not k.startswith("kernel"), k))
    for k in order + ["total"]:
        cells = [f"{statistics.mean(table[k][n]):.2f}" if n in table[k] else "0.00" for n in PROFILED]
        say(f"| {k} | " + " | ".join(cells) + " |")
    say()


def pprof():
    say("=== Go pprof of go3 (CPUPROFILE), top 30 by flat time")
    prof = os.path.join(WORK, "go3.pprof")
    out, _ = timed(repl_cmd(["replicate-go3", "lock=0"]), {"CPUPROFILE": prof})
    say("[go3] " + out.strip().splitlines()[-1])
    top = subprocess.run(["go", "tool", "pprof", "-top", "-nodecount=30", os.path.join(BIN, "replicate-go3"), prof],
                         capture_output=True, text=True, env=ENV, check=True).stdout
    for line in top.splitlines():
        say(line)
    say()


def objdump_go(binary, func):
    out = subprocess.run(["go", "tool", "objdump", "-s", f"^{re.escape(func)}$", binary], capture_output=True,
                         text=True, env=ENV, check=True).stdout
    return [l.split("\t", 3)[-1].strip() for l in out.splitlines() if l.startswith("  ")]


def objdump_rust(binary, func):
    out = subprocess.run(["objdump", "-d", "--no-show-raw-insn", "-C", binary], capture_output=True, text=True,
                         check=True).stdout
    body = out.split(f"<{func}>:\n", 1)[1].split("\n\n", 1)[0]
    return [l.split("\t", 1)[-1].strip() for l in body.splitlines() if not re.match(r"\s*[0-9a-f]+:\s*int3", l)]


def asm():
    os.makedirs(os.path.join(HERE, "asm"), exist_ok=True)
    say("=== A: generated code of the frame parse loop (micro-go main.parse, micro-rust parse)")
    for name, lines in (("go main.parse", objdump_go(os.path.join(BIN, "micro-go"), "main.parse")),
                        ("go main.parseBCE", objdump_go(os.path.join(BIN, "micro-go"), "main.parseBCE")),
                        ("go main.parseInPlace", objdump_go(os.path.join(BIN, "micro-go"), "main.parseInPlace")),
                        ("rust micro_rust::parse", objdump_rust(os.path.join(BIN, "micro-rust"), "micro_rust::parse"))):
        path = os.path.join(HERE, "asm", name.split()[1].replace("::", "-").replace(".", "-") + ".s")
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        checks = sum(1 for l in lines if re.search(r"panicBounds|panicIndex|panicSlice|slice_index_fail|"
                                                    r"panic_bounds_check", l))
        say(f"ASM {name}: {len(lines)} instructions, {checks} bounds-check panic sites, "
            f"{sum(1 for l in lines if 'X15' in l and 'MOVUPS X15' in l)} stack zeroing stores -> asm/{os.path.basename(path)}")
    bce = subprocess.run(["go", "build", "-gcflags=-d=ssa/check_bce/debug=1", "-o", os.devnull, "./replicate-go3"],
                         cwd=os.path.join(HERE, "..", "go-tuned"), capture_output=True, text=True, env=ENV).stderr
    own = [l for l in bce.splitlines() if l.startswith("replicate-go3/")]
    say(f"BCE replicate-go3/main.go: {len(own)} bounds checks left by the prove pass")
    for l in own:
        say("  " + l)
    inl = subprocess.run(["go", "build", "-gcflags=-m=2", "-o", os.devnull, "./replicate-go3"],
                         cwd=os.path.join(HERE, "..", "go-tuned"), capture_output=True, text=True, env=ENV).stderr
    for l in inl.splitlines():
        if l.startswith("replicate-go3/") and ("cannot inline" in l or re.search(r"can inline (header|\(\*ring\))", l)):
            say("  " + l[:160])
    say()


def main():
    stages = sys.argv[1:] or ["asm", "micro", "replicate", "counters", "profile", "pprof"]
    build()
    go = subprocess.run(["go", "version"], capture_output=True, text=True, env=ENV).stdout.split()[2]
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    perf = subprocess.run(["perf", "--version"], capture_output=True, text=True, env=ENV).stdout.strip()
    cpu = next(l.split(":")[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    say(f"# {platform.release()}, {cpu}, {os.cpu_count()} CPUs, {go}, {rustc}, {perf}, "
        f"{datetime.datetime.now().isoformat(timespec='seconds')}, stages: {' '.join(stages)}")
    for s in stages:
        globals()[s]()


if __name__ == "__main__":
    main()
