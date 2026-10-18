#!/usr/bin/env python3
"""Drive the GOMAXPROCS-under-quota spike. Needs root for the cgroup v1 cpu controller.

T0  default GOMAXPROCS for a range of CFS quotas
T1  default GOMAXPROCS follows a quota change while running; an explicit one does not
M*  loops and GOMAXPROCS under 2 and 3 CPU quotas and with no quota, at two loads
"""
import os
import platform
import re
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "bin")
CG = "/sys/fs/cgroup/cpu/gomaxprocs-spike"
GEN_CG = "/sys/fs/cgroup/cpu/gomaxprocs-spike-gen"
PERIOD = 100000
WORK_US = 20
OP_US = 25  # work plus the loop's own cost per message, measured without a quota
SECS = float(os.environ.get("SECS", "6"))
REPS = int(os.environ.get("REPS", "3"))


def write(path, value):
    with open(path, "w") as f:
        f.write(str(value))


def set_quota(cpus):
    # The cgroup gets the maximum weight, so the other benchmarks sharing the VM cannot take the
    # CPUs its quota allows, as on a node whose pods' requests fit its cores.
    write(f"{CG}/cpu.shares", 262144)
    write(f"{CG}/cpu.cfs_period_us", PERIOD)
    write(f"{CG}/cpu.cfs_quota_us", -1 if cpus is None else int(cpus * PERIOD))


def cpustat():
    out = {}
    with open(f"{CG}/cpu.stat") as f:
        for line in f:
            k, v = line.split()
            out[k] = int(v)
    return out


def watch(env_extra, changes):
    """Run server -watch inside the cgroup; apply (t, quota) changes; return its output."""
    env = dict(os.environ)
    env.pop("GOMAXPROCS", None)
    env.update(env_extra)
    p = subprocess.Popen(
        ["sh", "-c", f"echo $$ > {CG}/cgroup.procs && exec {BIN}/server -watch"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, env=env)
    t0 = time.time()
    for t, q in changes:
        time.sleep(max(0, t - (time.time() - t0)))
        set_quota(q)
    time.sleep(1.5)  # the runtime rereads the limit at most once per second
    p.stdin.close()
    out = p.stdout.read()
    p.wait()
    return out


def main():
    for d in (CG, GEN_CG):
        if not os.path.isdir(d):
            os.mkdir(d)
    write(f"{GEN_CG}/cpu.shares", 262144)
    go = subprocess.run(["go", "version"], capture_output=True, text=True).stdout.strip()
    print(f"# kernel={platform.release()} {go} nproc={os.cpu_count()} date={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
          f"cgroup=v1 shares=262144, gen in its own cgroup with shares=262144 period_us={PERIOD} work_us={WORK_US} secs={SECS} reps={REPS} (shared 4-vCPU VM)")
    sys.stdout.flush()
    iters = int(float(subprocess.run([f"{BIN}/server", "-calibrate"], capture_output=True, text=True).stdout) * WORK_US)
    print(f"# spin iterations for {WORK_US} us: {iters}")

    print("## T0 default GOMAXPROCS by quota (want ceil(quota), at least 2, at most nproc)")
    for q in [0.5, 1, 1.5, 2, 2.01, 2.5, 3, 3.5, 6, None]:
        set_quota(q)
        out = watch({}, [])
        got = re.search(r"gomaxprocs=(\d+)", out).group(1)
        print(f"T0 quota_cpus={q} gomaxprocs={got}")
    print("## T1 quota 3 -> 1.5 at t=1s -> none at t=2.5s; default vs GOMAXPROCS=3")
    for name, env in [("default", {}), ("explicit3", {"GOMAXPROCS": "3"})]:
        set_quota(3)
        out = watch(env, [(1.0, 1.5), (2.5, None)])
        seq = []
        for m in re.finditer(r"t=([\d.]+) gomaxprocs=(\d+)", out):
            if not seq or seq[-1][1] != m.group(2):
                seq.append((m.group(1), m.group(2)))
        print(f"T1 {name} changes=" + " ".join(f"{t}s:{g}" for t, g in seq))
    sys.stdout.flush()

    if os.environ.get("ONLY") == "T":
        return
    print("## M matrix: loops x GOMAXPROCS under quota; load = fraction of the quota offered to the loops")
    plan = []
    for limit in [2, 3, None]:
        n = limit or os.cpu_count()
        loads = [0.4, 0.65] if limit else [0.4]
        for load in loads:
            for loops, procs in [(n, None), (n, n + 1), (n - 1, n - 1), (n - 1, None)]:
                plan.append((limit, load, loops, procs))
    for rep in range(REPS):
        for limit, load, loops, procs in plan:
            n = limit or os.cpu_count()
            rate = int(load * n * 1e6 / OP_US)
            set_quota(limit)
            env = dict(os.environ)
            env.pop("GOMAXPROCS", None)
            env["GEN_CGROUP"] = GEN_CG
            if procs:
                env["GOMAXPROCS"] = str(procs)
            s0 = cpustat()
            r = subprocess.run([f"{BIN}/gen", CG, str(rate), str(SECS), str(loops), f"{BIN}/server",
                                "-loops", str(loops), "-iters", str(iters)],
                               capture_output=True, text=True, env=env, timeout=SECS * 4 + 30)
            s1 = cpustat()
            res = re.search(r"RESULT (.*)", r.stdout)
            gen = re.search(r"GEN (.*)", r.stdout)
            if not res or not gen:
                print(f"[FAIL] limit={limit} loops={loops} procs={procs}: {r.stdout} {r.stderr}")
                continue
            periods = s1["nr_periods"] - s0["nr_periods"]
            thr = s1["nr_throttled"] - s0["nr_throttled"]
            thr_ms = (s1["throttled_time"] - s0["throttled_time"]) / 1e6
            drops = re.search(r"drops=(\d+)", gen.group(1)).group(1)
            print(f"M rep={rep} limit={limit} load={load} rate={rate} procs_set={procs or 'default'} "
                  f"periods={periods} throttled={thr} throttled_pct={100 * thr / max(periods, 1):.1f} throttled_ms={thr_ms:.0f} drops={drops} {res.group(1)}")
            sys.stdout.flush()
    cleanup()


def cleanup():
    if os.path.isdir(CG):
        set_quota(None)
        os.rmdir(CG)
    if os.path.isdir(GEN_CG):
        os.rmdir(GEN_CG)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))
    try:
        main()
    finally:
        try:
            cleanup()
        except OSError as e:
            print(f"# cleanup: {e}", file=sys.stderr)
