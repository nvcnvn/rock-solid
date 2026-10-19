# Drives the handoff scenarios as root. Each scenario prints its expected outcome before it runs,
# then one [OK ]/[FAIL] line per check with got= and want=. Clients run as separate processes so a
# hung syscall cannot hang the harness. On exit every connection is aborted through fusectl,
# everything under work/ is unmounted, and every process started here is killed.
#   python3 run.py [H1 H2 H3 H4 ...]
import json, os, re, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "bin", "hofs")
WORK = os.path.join(HERE, "work")
FUSECTL = "/sys/fs/fuse/connections"
procs = []
fails = 0


def log(s):
    print(s, flush=True)


def check(name, ok, got, want):
    global fails
    if not ok:
        fails += 1
    log(f"  [{'OK ' if ok else 'FAIL'}] {name}: got={got} want={want}")


def expect(s):
    log(f"  expected: {s}")


class Proc:
    def __init__(self, args, name):
        self.name = name
        self.lines = []
        self.p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True, start_new_session=True)
        procs.append(self)
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            self.lines.append(line.rstrip())

    def send(self, s):
        self.p.stdin.write(s + "\n")
        self.p.stdin.flush()

    def wait_for(self, pat, timeout=5.0, start=0):
        end = time.monotonic() + timeout
        while True:
            for l in list(self.lines[start:]):
                if re.search(pat, l):
                    return l
            if time.monotonic() >= end:
                return None
            time.sleep(0.005)

    def count(self, pat):
        return sum(1 for l in self.lines if re.search(pat, l))

    def ev_time(self, pat):
        l = self.wait_for(pat, 0)
        return int(l.split()[1]) if l else None

    def ping(self):
        n = len(self.lines)
        self.send("ping")
        l = self.wait_for("PONG", 3, n)
        return dict(kv.split("=") for kv in l.split()[3:]) if l else {}

    def kill9(self):
        os.kill(self.p.pid, signal.SIGKILL)
        self.p.wait()

    def dump(self):
        for l in self.lines:
            log(f"    {self.name}| {l}")


def sh(cmd, check_rc=True):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check_rc and r.returncode:
        raise RuntimeError(f"{cmd}: {r.stdout}{r.stderr}")
    return r


def conn_id(mnt):
    for l in open("/proc/self/mountinfo"):
        f = l.split()
        if f[4] == mnt:
            return int(f[2].split(":")[1])
    return None


def waiting(cid):
    try:
        return int(open(f"{FUSECTL}/{cid}/waiting").read())
    except OSError:
        return None


def client(args, name):
    """A syscall in its own process; result read later with result()."""
    code = ("import os,sys,time,json\nt=time.monotonic()\n"
            "try:\n  r=" + args + "\n  out={'ok':True}\n"
            "except OSError as e:\n  out={'ok':False,'errno':e.errno,'err':e.strerror}\n"
            "out['t0']=t;out['t1']=time.monotonic();print(json.dumps(out),flush=True)\n")
    return Proc(["python3", "-c", code], name)


def client_on(cpu, args, name, prefix=()):
    code = ("import os,sys,time,json\nt=time.monotonic()\n"
            "try:\n  r=" + args + "\n  out={'ok':True}\n"
            "except OSError as e:\n  out={'ok':False,'errno':e.errno,'err':e.strerror}\n"
            "out['t0']=t;out['t1']=time.monotonic();print(json.dumps(out),flush=True)\n")
    return Proc(list(prefix) + ["taskset", "-c", str(cpu), "python3", "-c", code], name)


def result(c, timeout=0.0):
    l = c.wait_for(r"^\{", timeout)
    return json.loads(l) if l else None


def workers(d, n, stop, prefix=()):
    return Proc(list(prefix) + ["python3", os.path.join(HERE, "worker.py"), d, str(n), stop], "io")


def worker_results(w, stop, timeout=10):
    open(stop, "w").close()
    l = w.wait_for(r"^\[", timeout)
    return json.loads(l) if l else None


def fresh(name, nfiles=4, mode=0o644):
    back = os.path.join(WORK, name, "back")
    mnt = os.path.join(WORK, name, "mnt")
    os.makedirs(back, exist_ok=True)
    os.makedirs(mnt, exist_ok=True)
    os.chmod(back, 0o777)
    for k in range(nfiles):
        p = os.path.join(back, f"f{k}")
        with open(p, "wb") as f:
            f.write(b"\0" * 65536)
        os.chmod(p, mode)
    open(os.path.join(back, "hang1"), "w").close()
    stop = os.path.join(WORK, name, "stop")
    if os.path.exists(stop):
        os.unlink(stop)
    return back, mnt, stop


def holder(sock, prefix=()):
    h = Proc(list(prefix) + [BIN, "hold", sock], "holder")
    assert h.wait_for("HOLDER_LISTENING"), "holder did not start"
    return h


def mount_daemon(mnt, back, sock, extra=(), prefix=()):
    d = Proc(list(prefix) + [BIN, "mount", mnt, back, "--holder", sock] + list(extra), "A")
    assert d.wait_for("PUT_TO_HOLDER"), d.dump() or "mount failed"
    return d


def successor(sock, back, extra=(), prefix=(), name="B"):
    return Proc(list(prefix) + [BIN, "get", sock, back] + list(extra), name)


def summarize_io(res):
    ops = sum(r["ops"] for r in res)
    nerr = sum(r["nerr"] for r in res)
    mx = max(r["max"] for r in res)
    errs = [e for r in res for e in r["errors"]]
    return ops, nerr, mx, errs


# ---------------------------------------------------------------------------------------------


def H1():
    log("\n=== H1: holder process keeps the fd, daemon SIGKILLed with I/O in flight, cold successor resends")
    expect("every run: >=1 request read by A and unanswered at the kill (fusectl waiting > 0 after the kill), "
           "B receives them with bit 63 set, the hung LOOKUP and all 4 I/O workers finish with 0 errors and "
           "verified data; the worst op stalls for about the restart time (a few ms)")
    for run in range(5):
        back, mnt, stop = fresh("h1")
        sock = os.path.join(WORK, "h1", "h.sock")
        h = holder(sock)
        a = mount_daemon(mnt, back, sock, ["--batch", "16", "--delay-us", "2000", "--hang"])
        cid = conn_id(mnt)
        w = workers(mnt, 4, stop)
        hang = client(f"os.stat('{mnt}/hang1')", "hang")
        time.sleep(0.8)
        assert a.wait_for("HUNG", 2)
        w_before = waiting(cid)
        tk = time.monotonic()
        a.kill9()
        w_after = waiting(cid)
        b = successor(sock, back, ["--resend", "--batch", "16", "--delay-us", "2000"])
        b.wait_for("SERVING")
        t_serving = b.ev_time("SERVING")
        time.sleep(0.8)
        hr = result(hang, 3)
        res = worker_results(w, stop)
        ops, nerr, mx, errs = summarize_io(res)
        resent = b.count("GOT_RESENT")
        worst = max(res, key=lambda r: r["max"])
        log(f"  run {run}: waiting before kill={w_before} after kill={w_after} resent={resent} "
            f"(I/O {resent - 1}) ops={ops} worst op started {1000 * (worst['max_at'] - tk):.1f} ms after the kill, "
            f"errors={nerr} max_op_us={mx:.0f} kill->B serving={(t_serving - tk * 1e6) / 1000 if t_serving else None:.2f} ms")
        check(f"H1.{run} requests in flight at kill", (w_after or 0) > 0, w_after, ">0")
        check(f"H1.{run} B got the resent LOOKUP", b.count("GOT_RESENT.*op=1 ") == 1, b.count("GOT_RESENT.*op=1 "), 1)
        check(f"H1.{run} hung LOOKUP succeeds after resend", hr is not None and hr["ok"], hr, "ok")
        check(f"H1.{run} I/O workers: no errors, data verified", nerr == 0 and ops > 0, f"{nerr} {errs[:3]}", "0")
        check(f"H1.{run} INITs in B", b.count("INIT") == 0, b.count("INIT"), 0)
        cleanup_mount(mnt)
        for p in (b, h):
            p.kill9()


def H2():
    log("\n=== H2: allow_other; I/O as uid 65534 (nobody) across a SIGKILL handoff")
    expect("control without allow_other: nobody's stat fails with EACCES (the kernel admits only the mounting "
           "uid: fuse_allow_current_process). With allow_other: nobody reads and writes; across the handoff "
           "0 errors; the handed-over fd carries allow_other since it is a superblock option")
    nob = ["setpriv", "--reuid=65534", "--regid=65534", "--clear-groups"]
    back, mnt, stop = fresh("h2", mode=0o666)
    sock = os.path.join(WORK, "h2", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock)
    c = client_on(0, f"os.stat('{mnt}/f0')", "ctl", nob)
    r0 = result(c, 3)
    check("H2.ctl nobody without allow_other", r0 and not r0["ok"] and r0["errno"] == 13, r0, "EACCES")
    cleanup_mount(mnt)
    a.kill9()
    h.kill9()
    back, mnt, stop = fresh("h2", mode=0o666)
    h = holder(sock)
    a = mount_daemon(mnt, back, sock, ["--allow-other", "--batch", "16", "--delay-us", "2000"])
    w = workers(mnt, 4, stop, nob)
    time.sleep(0.8)
    a.kill9()
    b = successor(sock, back, ["--resend", "--batch", "16", "--delay-us", "2000"])
    b.wait_for("SERVING")
    time.sleep(0.8)
    res = worker_results(w, stop)
    ops, nerr, mx, errs = summarize_io(res)
    log(f"  uids={sorted(set(r['uid'] for r in res))} ops={ops} errors={nerr} max_op_us={mx:.0f} resent={b.count('GOT_RESENT')}")
    check("H2 workers ran as uid 65534", all(r["uid"] == 65534 for r in res), [r["uid"] for r in res], 65534)
    check("H2 nobody I/O across handoff: no errors", nerr == 0 and ops > 0, f"{nerr} {errs[:3]}", "0")
    check("H2 B serves after handoff (allow_other kept)", "allow_other" in open("/proc/self/mountinfo").read()
          .split(mnt)[1].split("\n")[0], "allow_other in mountinfo", "present")
    cleanup_mount(mnt)
    b.kill9()
    h.kill9()


def H3():
    log("\n=== H3: mount namespaces: daemon (Bidirectional), holder (plugin) and app (HostToContainer) in "
        "different mount namespaces; the daemon's namespace dies; successor in a namespace that cannot see the mount")
    expect("a mount made in a namespace whose kubelet dir is shared with the host propagates to the host and "
           "into the app's slave namespace; it outlives the namespace it was made in; the successor needs only "
           "the fd, not the mount; the app sees 0 errors. Control: a mount made in a private-propagation "
           "namespace is invisible to the host and app")
    base = os.path.join(WORK, "h3")
    K = os.path.join(base, "kubelet")
    back, _, stop = fresh("h3")
    for d in ("staging/vol", "staging/vol2", "pods/p1/vol", "plugins"):
        os.makedirs(os.path.join(K, d), exist_ok=True)
    sh(f"mount --bind {K} {K} && mount --make-shared {K}")
    pod = Proc(["unshare", "-m", "--propagation", "unchanged", "sh", "-c",
                f"mount --make-rslave {K} && echo READY && exec sleep 600"], "pod")
    assert pod.wait_for("READY")
    podns = ["nsenter", "-t", str(pod.p.pid), "-m"]
    sock = os.path.join(K, "plugins", "h.sock")
    h = holder(sock, ["unshare", "-m", "--propagation", "unchanged"])
    plugns = ["nsenter", "-t", str(h.p.pid), "-m"]
    stg = os.path.join(K, "staging", "vol")
    tgt = os.path.join(K, "pods", "p1", "vol")
    a = mount_daemon(stg, back, sock, ["--batch", "16", "--delay-us", "2000"],
                     ["unshare", "-m", "--propagation", "unchanged"])
    sh(" ".join(plugns) + f" mount --bind {stg} {tgt}")
    def has_mount(pid, path):
        return any(l.split()[4] == path and "fuse.hofs" in l for l in open(f"/proc/{pid}/mountinfo"))
    check("H3 mount visible in host ns", has_mount("self", stg) and has_mount("self", tgt), "staging+target", "both")
    check("H3 mount propagated into app (slave) ns", has_mount(pod.p.pid, tgt), has_mount(pod.p.pid, tgt), True)
    w = workers(tgt, 4, stop, podns)
    time.sleep(0.8)
    a_ns = os.readlink(f"/proc/{a.p.pid}/ns/mnt")
    a.kill9()
    time.sleep(0.1)
    alive = [p for p in os.listdir("/proc") if p.isdigit() and _ns(p) == a_ns]
    check("H3 daemon's mount ns has no processes left", alive == [], alive, [])
    check("H3 mount survives its ns in host and app ns", has_mount("self", tgt) and has_mount(pod.p.pid, tgt),
          "present", "present")
    b = successor(sock, back, ["--resend", "--batch", "16", "--delay-us", "2000"],
                  ["unshare", "-m", "--propagation", "private", "sh", "-c",
                   f"umount -l {K} && exec \"$0\" \"$@\""])
    b.wait_for("SERVING")
    check("H3 successor ns cannot see the mount", not has_mount(b.p.pid, tgt) and not has_mount(b.p.pid, stg),
          has_mount(b.p.pid, tgt), False)
    time.sleep(0.8)
    res = worker_results(w, stop)
    ops, nerr, mx, errs = summarize_io(res)
    log(f"  ops={ops} errors={nerr} max_op_us={mx:.0f} resent={b.count('GOT_RESENT')}")
    check("H3 app I/O across handoff: no errors", nerr == 0 and ops > 0, f"{nerr} {errs[:3]}", "0")
    h2 = holder(os.path.join(base, "h2.sock"))
    vol2 = os.path.join(K, "staging", "vol2")
    a2 = mount_daemon(vol2, back, os.path.join(base, "h2.sock"), (), ["unshare", "-m"])
    check("H3.ctl control mount exists in its own ns", has_mount(a2.p.pid, vol2), has_mount(a2.p.pid, vol2), True)
    check("H3.ctl private-propagation mount invisible to host", not has_mount("self", vol2),
          has_mount("self", vol2), False)
    a2.kill9()
    h2.kill9()
    sh(f"umount {tgt}; umount {stg}", False)
    check("H3 unmount from host propagates to app ns", not has_mount(pod.p.pid, tgt), has_mount(pod.p.pid, tgt), False)
    for p in (b, h, pod):
        p.kill9()
    sh(f"umount -l {K}", False)


def _ns(pid):
    try:
        return os.readlink(f"/proc/{pid}/ns/mnt")
    except OSError:
        return None


def H4():
    log("\n=== H4: FUSE over io_uring (FUSE_OVER_IO_URING, 4 queues x 2 entries, one thread)")
    U0()
    U1()
    U2()
    U3s()


def U3s():
    for run in range(int(os.environ.get("U3_RUNS", "10"))):
        U3(run)


def U0():
    log("\n--- U0: requests arrive on the ring, not on read(/dev/fuse)")
    expect("I/O works; every request after INIT comes through the ring (read_reqs=0)")
    back, mnt, stop = fresh("u0")
    sock = os.path.join(WORK, "u0", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock, ["--uring"])
    w = workers(mnt, 4, stop)
    time.sleep(0.5)
    res = worker_results(w, stop)
    ops, nerr, mx, errs = summarize_io(res)
    pg = a.ping()
    log(f"  ops={ops} errors={nerr} p50_us={sorted(r['p50'] for r in res)} daemon={pg}")
    check("U0 ring serves I/O", nerr == 0 and int(pg.get("uring_reqs", 0)) > 0 and int(pg.get("read_reqs", 1)) == 0,
          pg, "uring_reqs>0 read_reqs=0")
    cleanup_mount(mnt)
    a.kill9()
    h.kill9()


def U1():
    log("\n--- U1: daemon SIGKILLed idle; a request made in the gap; successor tries read(/dev/fuse), then re-registers")
    expect("(a) a successor that only reads /dev/fuse never sees the gap request: once the ring is ready, "
           "fiq->ops sends every request to a ring queue; (b) after re-registering fresh entries the gap request "
           "is still blocked: REGISTER does not dispatch requests already queued (fuse_uring_do_register); "
           "(c) one later request from the same CPU is served and its commit dispatches the queued one")
    back, mnt, stop = fresh("u1")
    sock = os.path.join(WORK, "u1", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock, ["--uring"])
    os.stat(f"{mnt}/f0")
    a.kill9()
    gap = client_on(1, f"os.stat('{mnt}/f1')", "gap")
    time.sleep(0.3)
    b = successor(sock, back, ["--uring", "--no-register", "--resend"])
    b.wait_for("SERVING")
    time.sleep(1.0)
    r = result(gap)
    pg = b.ping()
    check("U1a gap request not served via read(/dev/fuse)", r is None and int(pg.get("read_reqs", 0)) == 0,
          f"{r} {pg}", "blocked, read_reqs=0")
    b.send("register")
    b.wait_for("URING_REGISTERED")
    time.sleep(1.0)
    r = result(gap)
    check("U1b gap request still blocked after re-register", r is None, r, "blocked")
    other = client_on(2, f"os.stat('{mnt}/f2')", "cpu2")
    ro = result(other, 2)
    check("U1 new request on another CPU served", ro is not None and ro["ok"], ro, "ok")
    k = client_on(1, f"os.stat('{mnt}/f3')", "kick")
    rk = result(k, 2)
    r = result(gap, 2)
    check("U1c kick on the gap request's CPU releases it", rk and rk["ok"] and r and r["ok"],
          f"kick={rk} gap={r}", "both ok")
    t0 = time.monotonic()
    rc = sh(f"umount {mnt}", False).returncode
    check("U1 unmount with dead ring entries", rc == 0, f"rc={rc} {1000 * (time.monotonic() - t0):.1f} ms", "rc=0")
    for p in (b, h):
        p.kill9()


def U2():
    log("\n--- U2: daemon SIGKILLed with a request fetched over the ring; successor re-registers and resends")
    expect("the fetched request stays blocked: FUSE_NOTIFY_RESEND only requeues the processing lists of "
           "/dev/fuse devices (fuse_resend walks fc->devices), not the ring queues' lists")
    back, mnt, stop = fresh("u2")
    sock = os.path.join(WORK, "u2", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock, ["--uring", "--hang"])
    cid = conn_id(mnt)
    hang = client_on(1, f"os.stat('{mnt}/hang1')", "hang")
    assert a.wait_for("HUNG", 2)
    a.kill9()
    b = successor(sock, back, ["--uring", "--resend", "--kick", mnt])
    b.wait_for("SERVING")
    time.sleep(2.0)
    r = result(hang)
    check("U2 fetched request after resend + re-register + kick", r is None, r, "still blocked")
    log(f"  fusectl waiting={waiting(cid)} resent seen by B={b.count('GOT_RESENT')}")
    sh(f"umount -l {mnt}", False)
    if cid is not None:
        open(f"{FUSECTL}/{cid}/abort", "w").write("1")
    r = result(hang, 3)
    log(f"  after umount -l and fusectl abort: {r}")
    for p in (b, h):
        p.kill9()


def U3(run):
    log(f"\n--- U3.{run}: ring buffers in a memfd held by the holder; successor maps it at the same address and adopts")
    expect("the successor answers the request the dead daemon had fetched (the kernel copies replies from the "
           "user address it registered, which now maps the same memory), re-registers the rest at the same "
           "buffers and kicks each CPU's queue: the hung LOOKUP, the gap requests and all I/O workers finish "
           "with 0 errors")
    back, mnt, stop = fresh("u3")
    sock = os.path.join(WORK, "u3", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock, ["--uring", "--shm", "--hang", "--delay-us", "500"])
    w = workers(mnt, 4, stop)
    hang = client_on(run % 4, f"os.stat('{mnt}/hang1')", "hang")
    time.sleep(0.6)
    assert a.wait_for("HUNG", 2)
    tk = time.monotonic()
    a.kill9()
    gaps = [client_on(c, f"os.stat('{mnt}/f{c}')", f"gap{c}") for c in range(4)]
    time.sleep(0.1)
    b = successor(sock, back, ["--uring", "--shm", "--resend", "--kick", mnt, "--delay-us", "500"])
    b.wait_for("SERVING")
    t_serving = b.ev_time("SERVING")
    time.sleep(0.8)
    hr = result(hang, 2)
    gr = [result(g, 2) for g in gaps]
    res = worker_results(w, stop)
    if res is None:
        check(f"U3.{run} I/O workers finished", False, "hung", "finished")
        a.dump()
        b.dump()
        cleanup()
        return
    ops, nerr, mx, errs = summarize_io(res)
    ad = b.wait_for("URING_ADOPTED", 0)
    log(f"  {ad.split(' ', 2)[2] if ad else None} ops={ops} errors={nerr} max_op_us={mx:.0f} "
        f"kill->B serving={(t_serving - tk * 1e6) / 1000 if t_serving else 0:.2f} ms")
    check(f"U3.{run} fetched LOOKUP answered by successor", hr is not None and hr["ok"], hr, "ok")
    check(f"U3.{run} gap requests on every CPU served", all(g and g["ok"] for g in gr), gr, "all ok")
    check(f"U3.{run} I/O workers: no errors", nerr == 0 and ops > 0, f"{nerr} {errs[:3]}", "0")
    if nerr:
        log(f"  kill at {tk:.6f}")
        a.dump()
        b.dump()
    t0 = time.monotonic()
    rc = sh(f"umount {mnt}", False).returncode
    check(f"U3.{run} unmount", rc == 0, f"rc={rc} {1000 * (time.monotonic() - t0):.1f} ms", "rc=0")
    for p in (b, h):
        p.kill9()


def G1():
    log("\n=== G1: go-fuse v2.12.0 NewServer on an initialized fd through the /dev/fd/N mountpoint")
    expect("NewServer blocks reading the INIT it always expects first (server.go handleInit); when a "
           "request arrives it is handled as the first request with no kernel settings, and doInit never "
           "runs, so the stat either fails or NewServer reports an init error")
    back, mnt, stop = fresh("g1")
    sock = os.path.join(WORK, "g1", "h.sock")
    h = holder(sock)
    a = mount_daemon(mnt, back, sock)
    cid = conn_id(mnt)
    a.kill9()
    g = Proc([os.path.join(HERE, "bin", "gofuse"), "adopt", sock], "gofuse")
    blocked = g.wait_for("NEWSERVER_BLOCKED_2S", 4)
    check("G1 NewServer blocks waiting for INIT", blocked is not None, blocked, "blocked")
    c = client(f"os.stat('{mnt}')", "stat")
    r = result(c, 3)
    time.sleep(0.5)
    ret = g.wait_for("NEWSERVER_RETURNED", 0)
    log(f"  stat: {r}; {ret}")
    g.dump()
    ks = g.wait_for("KERNEL_SETTINGS", 0)
    check("G1 first request after adoption fails, or NewServer reports an init error",
          r is None or not r["ok"] or "err=<nil>" not in (ret or ""), f"stat={r} {ret}", "failure")
    check("G1 adopted server knows the kernel's INIT settings", ks is not None and "major=7" in ks, ks,
          "major=7 minor=45")
    open(f"{FUSECTL}/{cid}/abort", "w").write("1")
    c.kill9()
    cleanup_mount(mnt)
    for p in (g, h):
        p.kill9()


def G2():
    log("\n=== G2: go-fuse ProtocolServer (transport-free): replayed INIT, 64-bit unique echo, allocations")
    expect("a replayed INIT sets its kernel settings; a resent unique (bit 63) is echoed whole; "
           "HandleRequest allocates per request (requestAlloc and its cancel channel)")
    g = Proc([os.path.join(HERE, "bin", "gofuse"), "codec"], "gofuse")
    l = g.wait_for("GETATTR_ALLOCS", 30)
    g.dump()
    echo = g.wait_for("full_echo=true", 0)
    check("G2 full 64-bit unique echoed", echo is not None, echo is not None, True)
    allocs = float(re.search(r"allocs_per_op=([\d.]+)", l).group(1)) if l else None
    check("G2 allocations per GETATTR", allocs is not None and allocs > 0, allocs, ">0 (violates zero-alloc rule)")


def cleanup_mount(mnt):
    if sh(f"umount {mnt}", False).returncode:
        sh(f"umount -l {mnt}", False)


def cleanup():
    for p in procs:
        try:
            os.killpg(p.p.pid, signal.SIGKILL)
        except OSError:
            pass
    for l in reversed(open("/proc/self/mountinfo").read().splitlines()):
        f = l.split()
        if f[4].startswith(WORK):
            if "fuse.hofs" in l and os.path.exists(f"{FUSECTL}/{f[2].split(':')[1]}/abort"):
                open(f"{FUSECTL}/{f[2].split(':')[1]}/abort", "w").write("1")
            sh(f"umount -l {f[4]}", False)


def main():
    if os.getuid() != 0:
        sys.exit("run as root")
    if not os.listdir(FUSECTL):
        sh(f"mount -t fusectl none {FUSECTL}", False)
    uring = open("/sys/module/fuse/parameters/enable_uring").read().strip()
    gofuse = ", go-fuse v2.12.0, " + sh("go version").stdout.strip()
    log(f"# {os.uname().release} {os.uname().machine}, fuse.enable_uring={uring}, "
        f"possible CPUs {open('/sys/devices/system/cpu/possible').read().strip()}, "
        f"{sh('unshare --version').stdout.strip()}, python {sys.version.split()[0]}, "
        f"{time.strftime('%Y-%m-%d %H:%M:%S %Z')}{gofuse}")
    os.makedirs(WORK, exist_ok=True)
    sh(f"pkill -9 -f '^{HERE}/bin/'", False)
    cleanup()
    tests = sys.argv[1:] or ["H1", "H2", "H3", "H4", "G1", "G2"]
    try:
        for t in tests:
            globals()[t]()
    finally:
        cleanup()
    log(f"\n{'ALL CHECKS PASSED' if fails == 0 else f'{fails} CHECK(S) FAILED'}")


if __name__ == "__main__":
    main()
