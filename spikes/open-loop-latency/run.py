#!/usr/bin/env python3
"""Open-loop latency sweep of the replicated 4K write servers. Builds the servers and the load
generator, runs every (rate, server) point twice with the servers interleaved, each point under the
shared benchmark lock, and prints raw lines, a summary table and checks.

Servers: rust and go read each readable connection until it would block (drain=1, as the original
harnesses); rust-d0 and go-d0 make one read per connection per loop pass (drain=0).

python3 run.py | tee results.txt    # full sweep
python3 run.py point SERVER RATE    # one point (k writes/s), printed
"""
import datetime
import os
import platform
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "bin")
LOCK = os.environ.get(
    "BENCH_LOCK",
    "/tmp/claude-0/-home-user-rock-solid/f623274f-f6de-5737-97ab-d96b3a8c7e82/scratchpad/bench.lock",
)
ENV = {**os.environ, "GOTOOLCHAIN": os.environ.get("GOTOOLCHAIN", "go1.27.0")}
RATES = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120, 140]
LANGS = ["rust", "go", "rust-d0", "go-d0"]
REPS = 2
CONNS = 16
SECS = 10
NET_KEYS = ["RetransSegs", "TCPLossProbes", "TCPToZeroWindowAdv", "TCPWinProbe", "PruneCalled",
            "TCPBacklogDrop", "TCPRcvQDrop"]
MAX_FOREIGN_CORES = 0.3
FAILS = []


def build():
    os.makedirs(BIN, exist_ok=True)
    rust = os.path.join(HERE, "server-rust")
    subprocess.run(["cargo", "build", "--release", "--quiet"], cwd=rust, check=True, env=ENV)
    subprocess.run(["cp", os.path.join(rust, "target", "release", "server-rust"), BIN], check=True)
    subprocess.run(["go", "build", "-o", os.path.join(BIN, "server-go"), "./server-go"],
                   cwd=HERE, check=True, env=ENV)
    for prog in ["loadgen", "vcpu-gaps"]:
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-o", os.path.join(BIN, prog),
                        os.path.join(HERE, "loadgen", f"{prog}.c"), "-lm"], check=True)


def netstat():
    d = {}
    for path in ["/proc/net/netstat", "/proc/net/snmp"]:
        lines = open(path).read().splitlines()
        for names, values in zip(lines[::2], lines[1::2]):
            d.update(zip(names.split()[1:], map(int, values.split()[1:])))
    return d


def point(lang, rate_k, seed=1):
    """Starts a server, drives it at rate_k thousand writes/s and returns the raw lines."""
    net0 = netstat()
    binary, drain = lang.split("-")[0], 0 if lang.endswith("-d0") else 1
    srv = subprocess.Popen([os.path.join(BIN, f"server-{binary}"), f"conns={CONNS}", f"drain={drain}"],
                           stdout=subprocess.PIPE, text=True, env=ENV)
    port = int(srv.stdout.readline().split()[1])
    load = subprocess.run([os.path.join(BIN, "loadgen"), f"port={port}", f"pid={srv.pid}",
                           f"rate={rate_k * 1000}", f"secs={SECS}", "warm=1", f"conns={CONNS}",
                           f"seed={seed}", f"label={lang}"], capture_output=True, text=True)
    out, _ = srv.communicate(timeout=30)
    if load.returncode or srv.returncode:
        raise RuntimeError(f"{lang} {rate_k}k: loadgen {load.returncode} {load.stderr} server {srv.returncode}")
    net1 = netstat()
    net = " ".join(f"{k}={net1.get(k, 0) - net0.get(k, 0)}" for k in NET_KEYS)
    return load.stdout.strip(), out.strip().splitlines()[-1], f"NET lang={lang} | {net}"


def parse(line):
    return {k: float(v) for k, v in (p.split("=") for p in line.split("|")[1].split())}


def check(tid, what, ok, got, want):
    print(f"[{'OK ' if ok else 'FAIL'}] {tid} {what} got={got} want={want}", flush=True)
    if not ok:
        FAILS.append(tid)


def locked_point(lang, rate_k, seed):
    """Runs one point; reruns it while other processes used more than MAX_FOREIGN_CORES."""
    for _ in range(4):
        out = subprocess.run(["flock", LOCK, sys.executable, os.path.abspath(__file__), "point", lang,
                              str(rate_k), str(seed)], capture_output=True, text=True, check=True).stdout
        lines = out.strip().splitlines()[-3:]
        for line in lines:
            print(line, flush=True)
        load, server, net = map(parse, lines)
        foreign = load["machine_cores"] - load["srv_cores"] - load["cli_cores"]
        if foreign <= MAX_FOREIGN_CORES:
            return load, server, net
        print(f"[RETRY] {lang} {rate_k}k: other processes used {foreign:.2f} cores", flush=True)
    raise RuntimeError(f"{lang} {rate_k}k: machine never quiet")


def knee(rows, key):
    """Lowest offered rate whose `key` latency is more than twice that at the lowest rate."""
    for r in RATES:
        if rows[r][key] > 2 * rows[RATES[0]][key]:
            return r
    return None


def saturation(rows):
    """Highest offered rate served in full: achieved within 2% of offered and nothing dropped."""
    best = None
    for r in RATES:
        if rows[r]["achieved"] >= 0.98 * rows[r]["offered"] and rows[r]["dropped"] == 0:
            best = r
        else:
            break
    return best


def sweep():
    build()
    rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
    gov = subprocess.run(["go", "version"], capture_output=True, text=True, env=ENV).stdout.strip()
    cpu = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    print(f"# kernel {platform.release()}, {rustc}, {gov}, {os.cpu_count()} vCPUs of {cpu}, "
          f"{datetime.datetime.now().isoformat(timespec='seconds')}", flush=True)
    print(f"# conns={CONNS} secs={SECS} warm=1 arrivals=poisson cap=16384 reps={REPS}", flush=True)
    gaps = subprocess.run(["flock", LOCK, os.path.join(BIN, "vcpu-gaps"), "10"], capture_output=True,
                          text=True, check=True).stdout.strip()
    print(gaps, flush=True)
    runs = {(lang, r): [] for lang in LANGS for r in RATES}
    for rep in range(REPS):
        for i, r in enumerate(RATES):
            order = LANGS[(rep + i) % len(LANGS):] + LANGS[:(rep + i) % len(LANGS)]
            for lang in order:
                load, server, net = locked_point(lang, r, seed=rep + 1)
                load["srv_total_us_per_op"] = server["cpu_us_per_op"]
                load["retrans"] = net["RetransSegs"]
                load["max_wbuf_mb"] = server["max_wbuf"] / 1e6
                runs[(lang, r)].append(load)
    rows = {lang: {} for lang in LANGS}
    for (lang, r), rs in runs.items():
        rows[lang][r] = {k: sum(x[k] for x in rs) / len(rs) for k in rs[0]}
        rows[lang][r]["p99_lo"] = min(x["p99_us"] for x in rs)
        rows[lang][r]["p99_hi"] = max(x["p99_us"] for x in rs)
    print("\n## Mean of reps per offered rate (k writes/s); latency in us; cores = server CPU / wall")
    print("| offered | lang | achieved | dropped | p50 | p99 (range) | p99.9 | max | srv cores | srv us/op "
          "| cli cores | retrans | max wbuf MB |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for r in RATES:
        for lang in LANGS:
            x = rows[lang][r]
            print(f"| {r}k | {lang} | {x['achieved']:.0f} | {x['dropped']:.0f} | {x['p50_us']:.0f} | "
                  f"{x['p99_us']:.0f} ({x['p99_lo']:.0f}-{x['p99_hi']:.0f}) | {x['p999_us']:.0f} | "
                  f"{x['max_us']:.0f} | {x['srv_cores']:.2f} | {x['srv_us_per_op']:.1f} | {x['cli_cores']:.2f} | "
                  f"{x['retrans']:.0f} | {x['max_wbuf_mb']:.1f} |")
    knees = {lang: knee(rows[lang], "p99_us") for lang in LANGS}
    p50_knees = {lang: knee(rows[lang], "p50_us") for lang in LANGS}
    sats = {lang: saturation(rows[lang]) for lang in LANGS}
    print()
    for lang in LANGS:
        print(f"{lang}: p50/p99 at {RATES[0]}k {rows[lang][RATES[0]]['p50_us']:.0f}/"
              f"{rows[lang][RATES[0]]['p99_us']:.0f} us, p50 knee (>2x) at {p50_knees[lang]}k, "
              f"p99 knee (>2x) at {knees[lang]}k, full service up to {sats[lang]}k, max achieved "
              f"{max(rows[lang][r]['achieved'] for r in RATES):.0f}/s")
    lo = rows["go"][RATES[0]]["p50_us"] - rows["rust"][RATES[0]]["p50_us"]
    check("E1", "Go p50 minus Rust p50 at lowest rate, us", 0 <= lo <= 15, f"{lo:.1f}", "0..15")
    for tid, what, v, a, b in [("E2", "Rust p99 knee / Go p99 knee", knees, "rust", "go"),
                               ("E3", "Rust / Go full-service rate", sats, "rust", "go"),
                               ("E6", "drain=0 Rust p99 knee / Go p99 knee", knees, "rust-d0", "go-d0"),
                               ("E6", "drain=0 Rust / Go full-service rate", sats, "rust-d0", "go-d0")]:
        ratio = v[a] / v[b] if v[a] and v[b] else None
        check(tid, what, ratio is not None and 1.1 <= ratio <= 1.35,
              f"{ratio:.2f}" if ratio else f"{v[a]}/{v[b]}", "1.1..1.35")
    us = {lang: rows[lang][30]["srv_us_per_op"] for lang in LANGS}
    check("E4", "Go / Rust server us per write at 30k", 1.1 <= us["go"] / us["rust"] <= 1.4,
          f"{us['go']:.1f}/{us['rust']:.1f}={us['go'] / us['rust']:.2f}", "1.1..1.4")
    for lang in [x for x in LANGS if sats[x]]:
        hi, lo = rows[lang][sats[lang]]["srv_us_per_op"], rows[lang][RATES[0]]["srv_us_per_op"]
        check("E5", f"{lang} server us per write at {sats[lang]}k below {RATES[0]}k", hi < lo,
              f"{hi:.1f}<{lo:.1f}", "falls with load")
    print(f"\nFAILS: {FAILS or 'none'}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "point":
        seed = int(sys.argv[4]) if len(sys.argv) > 4 else 1
        for line in point(sys.argv[2], float(sys.argv[3]), seed):
            print(line, flush=True)
        return
    sweep()


if __name__ == "__main__":
    main()
