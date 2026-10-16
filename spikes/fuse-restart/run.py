#!/usr/bin/env python3
"""FUSE daemon restart spike driver. Each scenario mounts bin/rawfs (or bin/llfs) on work/<id>/mnt over
work/<id>/back, hands the /dev/fuse fd to a successor in some way, and checks what a client sees.

usage: python3 run.py [test-id ...]      (no ids: run all)
       python3 run.py --client           (internal: the client process the scenarios drive)
"""
import atexit, errno, json, os, select, shutil, signal, statistics, subprocess, sys, threading, time

S = os.path.dirname(os.path.abspath(__file__))
W = os.path.join(S, "work")
BIN = os.path.join(S, "bin")
RAWFS = os.path.join(BIN, "rawfs")
UNIT_PREFIX = "rawfs-spike-"

procs = []
mounts = []
units = []


def now_us():
    return time.monotonic_ns() // 1000


# ---------------------------------------------------------------- client process

def client_main():
    handles = {}
    for line in sys.stdin:
        c = json.loads(line)
        op = c["op"]
        t0 = now_us()
        r = {}
        try:
            if op == "stat":
                os.stat(c["path"])
            elif op == "open":
                fd = os.open(c["path"], os.O_RDWR)
                handles[fd] = True
                r["h"] = fd
            elif op == "pread":
                r["data"] = os.pread(c["h"], c.get("n", 64), c.get("off", 0)).decode()
            elif op == "pwrite":
                r["n"] = os.pwrite(c["h"], c["data"].encode(), c.get("off", 0))
            elif op == "mkdir":
                os.mkdir(c["path"])
            elif op == "listdir":
                r["names"] = sorted(os.listdir(c["path"]))
            elif op == "loop":
                lat, errs, slow = [], {}, []
                end = time.monotonic() + c["secs"]
                while time.monotonic() < end:
                    a = now_us()
                    try:
                        os.stat(c["path"])
                    except OSError as e:
                        n = errno.errorcode.get(e.errno, str(e.errno))
                        errs[n] = errs.get(n, 0) + 1
                    d = now_us() - a
                    if d > 200:
                        slow.append((a, d))
                    lat.append(d)
                    time.sleep(0.0001)
                lat.sort()
                r.update(n=len(lat), errs=errs, p50=lat[len(lat) // 2], max=lat[-1], slow=slow)
            r["ok"] = True
        except OSError as e:
            r["err"] = errno.errorcode.get(e.errno, str(e.errno))
        r["t0"], r["t1"] = t0, now_us()
        r["us"] = r["t1"] - t0
        print(json.dumps(r), flush=True)


class Client:
    def __init__(self):
        self.p = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--client"], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, text=True, bufsize=1)
        procs.append(self.p)

    def send(self, **c):
        self.p.stdin.write(json.dumps(c) + "\n")
        self.p.stdin.flush()

    def result(self, timeout):
        r, _, _ = select.select([self.p.stdout], [], [], timeout)
        if not r:
            return None
        return json.loads(self.p.stdout.readline())

    def call(self, timeout=5, **c):
        self.send(**c)
        return self.result(timeout)

    def kill(self):
        self.p.kill()
        self.p.wait()


def outcome(r):
    if r is None:
        return "hang"
    return "ok" if r.get("ok") else r["err"]


# ---------------------------------------------------------------- daemons

class Daemon:
    """A process that prints 'EV <us> <what>' lines; commands go to its stdin."""

    def __init__(self, args, env=None, logfile=None):
        self.events = []
        self.cv = threading.Condition()
        self.logfile = logfile
        if logfile is None:
            self.p = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True, bufsize=1, env={**os.environ, **(env or {})})
            procs.append(self.p)
            threading.Thread(target=self._pump, daemon=True).start()
        else:
            self.p = None
            threading.Thread(target=self._tail, daemon=True).start()

    def _add(self, line):
        parts = line.rstrip("\n").split(" ", 2)
        if len(parts) == 3 and parts[0] == "EV":
            ev = (int(parts[1]), parts[2])
        else:
            ev = (now_us(), "OUT " + line.rstrip("\n"))
        with self.cv:
            self.events.append(ev)
            self.cv.notify_all()

    def _pump(self):
        for line in self.p.stdout:
            self._add(line)

    def _tail(self):
        pos = 0
        while True:
            try:
                with open(self.logfile) as f:
                    f.seek(pos)
                    for line in f:
                        self._add(line)
                    pos = f.tell()
            except FileNotFoundError:
                pass
            time.sleep(0.002)

    def wait_for(self, what, timeout=5, after=0):
        end = time.monotonic() + timeout
        with self.cv:
            while True:
                for i, (t, e) in enumerate(self.events):
                    if i >= after and what in e:
                        return t, e
                left = end - time.monotonic()
                if left <= 0:
                    return None
                self.cv.wait(left)

    def find(self, what):
        with self.cv:
            return [(t, e) for t, e in self.events if what in e]

    def cmd(self, line):
        self.p.stdin.write(line + "\n")
        self.p.stdin.flush()

    def exited(self, timeout=3):
        try:
            return self.p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None


def rawfs(*args, env=None):
    return Daemon([RAWFS, *args], env=env)


# ---------------------------------------------------------------- mounts and checks

def setup(tid):
    d = os.path.join(W, tid)
    if os.path.exists(d):
        cleanup_mount(os.path.join(d, "mnt"))
        shutil.rmtree(d, ignore_errors=True)
    os.makedirs(os.path.join(d, "mnt"))
    os.makedirs(os.path.join(d, "back"))
    for name, data in (("f", "hello-from-backing\n"), ("hang1", "x\n")):
        with open(os.path.join(d, "back", name), "w") as f:
            f.write(data)
    mounts.append(os.path.join(d, "mnt"))
    return d, os.path.join(d, "mnt"), os.path.join(d, "back")


def mountinfo(mnt):
    with open("/proc/self/mountinfo") as f:
        for line in f:
            p = line.split()
            if p[4] == mnt:
                return p
    return None


def conn_dir(mnt):
    p = mountinfo(mnt)
    if not p:
        return None
    return "/sys/fs/fuse/connections/" + p[2].split(":")[1]


def waiting(mnt):
    d = conn_dir(mnt)
    try:
        with open(d + "/waiting") as f:
            return int(f.read())
    except (OSError, TypeError):
        return None


def cleanup_mount(mnt):
    d = conn_dir(mnt)
    if d:
        try:
            with open(d + "/abort", "w") as f:
                f.write("1")
        except OSError:
            pass
    while mountinfo(mnt):
        if subprocess.run(["fusermount3", "-u", "-z", mnt], capture_output=True).returncode:
            break


def umount_retry(mnt):
    """fusermount3 -u, retried: a desktop session's mount monitor can hold a fresh mount busy for a moment."""
    for _ in range(40):
        rc = subprocess.run(["fusermount3", "-u", mnt], capture_output=True).returncode
        if rc == 0:
            return 0
        time.sleep(0.05)
    return rc


def cleanup_all():
    for u in units:
        subprocess.run(["systemctl", "--user", "kill", "-s", "KILL", u], capture_output=True)
        subprocess.run(["systemctl", "--user", "stop", u], capture_output=True)
        subprocess.run(["systemctl", "--user", "reset-failed", u], capture_output=True)
    for m in mounts:
        cleanup_mount(m)
    for p in procs:
        if p.poll() is None:
            p.kill()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
    for m in mounts:
        cleanup_mount(m)
    procs.clear()
    mounts.clear()
    units.clear()


atexit.register(cleanup_all)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

results = {}


def check(tid, label, got, want):
    ok = got == want
    print(f"  [{'OK ' if ok else 'FAIL'}] {label}: got={got!r} want={want!r}", flush=True)
    results.setdefault(tid, []).append(ok)
    return ok


def note(text):
    print(f"  {text}", flush=True)


def scenario(tid, title, expect):
    print(f"\n=== {tid} {title}\n  expect: {expect}", flush=True)


def resent_unique(u):
    return hex(int(u, 16) | (1 << 63))


def field(ev, key):
    for tok in ev[1].split():
        if tok.startswith(key + "="):
            return tok.split("=", 1)[1]
    return None


# ---------------------------------------------------------------- scenarios

def t_f1():
    tid = "F1"
    scenario(tid, "handoff over SCM_RIGHTS to a freshly exec'd daemon, A exits, B serves without INIT",
             "mount stays, open handle reads and writes through B, B sees no INIT and rebuilds nodeids from backing")
    d, mnt, back = setup("f1")
    a = rawfs("mount", mnt, back)
    check(tid, "A INIT", a.wait_for("INIT") is not None, True)
    c = Client()
    h = c.call(op="open", path=mnt + "/f")["h"]
    check(tid, "read via A", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back)
    b.wait_for("LISTENING")
    t0 = now_us()
    a.cmd("handoff " + sock)
    check(tid, "A exit code", a.exited(), 0)
    tb = b.wait_for("TAKEOVER")
    note(f"handoff A HANDOFF_START -> B TAKEOVER: {tb[0] - a.find('HANDOFF_START')[0][0]} us "
         f"(harness command -> B ready {tb[0] - t0} us)")
    check(tid, "mount still in mountinfo", mountinfo(mnt) is not None, True)
    check(tid, "pread on handle opened under A", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    check(tid, "pwrite on handle opened under A", c.call(op="pwrite", h=h, data="WRITTEN-BY-B")["n"], 12)
    with open(back + "/f") as f:
        check(tid, "backing has B's write", f.read(12), "WRITTEN-BY-B")
    check(tid, "stat", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
    check(tid, "listdir", c.call(op="listdir", path=mnt)["names"], ["f", "hang1"])
    c2 = Client()
    h2 = c2.call(op="open", path=mnt + "/f")
    check(tid, "fresh open after handoff", "h" in h2, True)
    b.cmd("ping")
    pong = b.wait_for("PONG")
    check(tid, "INITs seen by B", field(pong, "inits"), "0")
    note(f"B: {pong[1]}; B rebuilt nodeids: {len(b.find('RESCAN'))} RESCAN(s)")
    cleanup_all()


def t_f2_f3():
    scenario("F2", "request A read but never answered, A hands off and exits, B does not resend",
             "caller stays blocked (request sits on the processing list of the fud B now owns)")
    d, mnt, back = setup("f2")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    c, ch = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    hung = a.wait_for("HUNG")
    note(f"A: {hung[1]}")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back)
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    b.wait_for("TAKEOVER")
    r = ch.result(2.0)
    check("F2", "caller 2 s after A exited", outcome(r), "hang")
    check("F2", "fusectl waiting", waiting(mnt), 1)
    check("F2", "other requests still served by B", outcome(c.call(op="stat", path=mnt + "/f")), "ok")

    scenario("F3", "B sends FUSE_NOTIFY_RESEND after takeover",
             "the hung LOOKUP is requeued with FUSE_UNIQUE_RESEND (bit 63) set, B answers it, caller succeeds")
    t0 = now_us()
    b.cmd("resend")
    r = ch.result(2.0)
    check("F3", "caller after RESEND", outcome(r), "ok")
    got = b.wait_for("GOT_RESENT")
    check("F3", "resent unique = A's unique | 1<<63", field(got, "unique") if got else None,
          resent_unique(field(hung, "unique")))
    if r:
        note(f"RESEND sent -> caller returns: {r['t1'] - t0} us; caller total blocked {r['us']} us")
    init = a.find("INIT")[0]
    note(f"A INIT: {init[1]}")
    check("F3", "kernel offered FUSE_HAS_RESEND", field(init, "has_resend"), "1")
    check("F3", "A's INIT reply carried FUSE_HAS_RESEND (bit 39)", bool(int(field(init, "reply_flags"), 16) & (1 << 39)),
          False)
    check("F3", "fusectl waiting", waiting(mnt), 0)
    cleanup_all()

    scenario("F2b", "same as F2 but A did not negotiate FUSE_PARALLEL_DIROPS",
             "the hung LOOKUP holds the parent directory's fuse_inode mutex, so every other lookup in that "
             "directory blocks too until B resends")
    d, mnt, back = setup("f2b")
    a = rawfs("mount", mnt, back, "--hang", env={"RAWFS_NO_PARALLEL_DIROPS": "1"})
    a.wait_for("INIT")
    c, ch = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back)
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    b.wait_for("TAKEOVER")
    c.send(op="stat", path=mnt + "/f")
    check("F2b", "stat of another name in the same directory", outcome(c.result(1.0)), "hang")
    b.cmd("resend")
    check("F2b", "both callers after RESEND", [outcome(ch.result(2.0)), outcome(c.result(2.0))], ["ok", "ok"])
    cleanup_all()

    scenario("F3c", "B answers a resent request with the unique masked back to A's value (bit 63 cleared)",
             "the reply is rejected with ENOENT (fuse_request_find compares the full unique) and the caller stays "
             "blocked")
    d, mnt, back = setup("f3c")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    ch = Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back, "--resend", env={"RAWFS_STRIP_RESEND": "1"})
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    fail = b.wait_for("REPLY_FAIL")
    check("F3c", "reply write errno", field(fail, "errno") if fail else None, str(errno.ENOENT))
    check("F3c", "caller", outcome(ch.result(1.0)), "hang")
    cleanup_all()

    scenario("F3b", "RESEND of a non-idempotent request (MKDIR) that A already applied",
             "B re-executes it and the caller gets EEXIST for its own mkdir")
    d, mnt, back = setup("f3b")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    ch = Client()
    ch.send(op="mkdir", path=mnt + "/mkhangdir")
    note(f"A: {a.wait_for('HUNG')[1]}")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back, "--resend")
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    check("F3b", "caller mkdir result", outcome(ch.result(2.0)), "EEXIST")
    note(f"B: {b.wait_for('MKDIR_ERR')[1]}")
    cleanup_all()


def t_f4():
    scenario("F4a", "daemon closes its only /dev/fuse fd (exit, no handoff)",
             "connection aborted; next request ENOTCONN, mount entry stays until unmounted")
    d, mnt, back = setup("f4a")
    a = rawfs("mount", mnt, back)
    a.wait_for("INIT")
    c = Client()
    check("F4a", "stat before", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
    a.cmd("exit")
    a.exited()
    check("F4a", "stat after last fd closed", outcome(c.call(op="stat", path=mnt + "/f")), "ENOTCONN")
    check("F4a", "mount entry still present", mountinfo(mnt) is not None, True)
    cleanup_all()

    scenario("F4b", "daemon killed (SIGKILL) with a request read but unanswered, nobody else holds the fd",
             "hung caller gets ECONNABORTED (fuse_dev_end_requests), later callers ENOTCONN")
    d, mnt, back = setup("f4b")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    c, ch = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    a.p.kill()
    a.exited()
    check("F4b", "hung caller", outcome(ch.result(2.0)), "ECONNABORTED")
    check("F4b", "next caller", outcome(c.call(op="stat", path=mnt + "/f")), "ENOTCONN")
    cleanup_all()

    scenario("F4c", "A reads through a FUSE_DEV_IOC_CLONE fd, hands off only the original fd, exits",
             "mount survives (B holds a fud), but the request read on the clone gets ECONNABORTED "
             "when the clone's last reference closes")
    d, mnt, back = setup("f4c")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    a.cmd("clone")
    a.wait_for("CLONED")
    c, ch = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back)
    b.wait_for("LISTENING")
    a.cmd("handoff-first " + sock)
    a.exited()
    check("F4c", "caller whose request was read on the dropped clone", outcome(ch.result(2.0)), "ECONNABORTED")
    check("F4c", "other requests via B", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
    cleanup_all()

    scenario("F4d", "same, but A hands off both the original and the clone fd; B resends",
             "caller succeeds")
    d, mnt, back = setup("f4d")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    a.cmd("clone")
    a.wait_for("CLONED")
    ch = Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back, "--resend")
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    check("F4d", "B received fds", field(b.wait_for("TAKEOVER"), "nfds"), "2")
    check("F4d", "caller", outcome(ch.result(2.0)), "ok")
    cleanup_all()


def t_f5_f6():
    scenario("F5a", "fd-holder process: A puts its fd in a holder, A crashes (SIGKILL) with a request unanswered, "
             "a cold-started B gets the fd from the holder and resends",
             "mount survives the crash; hung caller and callers during the gap succeed once B serves")
    d, mnt, back = setup("f5a")
    hsock = d + "/holder.sock"
    hold = rawfs("hold", hsock)
    hold.wait_for("HOLDER_LISTENING")
    a = rawfs("mount", mnt, back, "--hang")
    a.wait_for("INIT")
    a.cmd("put " + hsock)
    check("F5a", "holder got fds", hold.wait_for("HOLDER_PUT") is not None, True)
    ch, cg = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    t_kill = now_us()
    a.p.kill()
    a.exited()
    cg.send(op="stat", path=mnt + "/f")

    scenario("F6a", "request issued while no daemon serves (holder only), B started 500 ms later",
             "it blocks for the gap and then succeeds; no error")
    time.sleep(0.5)
    check("F6a", "gap caller still blocked before B starts", cg.result(0) is None, True)
    w = waiting(mnt)
    check("F6a", "fusectl waiting >= 2 (hung + gap caller)", w is not None and w >= 2, True)
    note(f"fusectl waiting={w} (above 2: desktop mount monitors such as gvfs-udisks2-volume-monitor probe new mounts)")
    t_b = now_us()
    b = rawfs("get", hsock, back, "--resend")
    r = cg.result(3.0)
    check("F6a", "gap caller", outcome(r), "ok")
    if r:
        note(f"gap caller blocked {r['us']} us; B exec -> caller returns {r['t1'] - t_b} us")
    check("F5a", "hung caller (read by crashed A)", outcome(ch.result(3.0)), "ok")
    tb = b.wait_for("TAKEOVER")
    note(f"crash -> B TAKEOVER: {tb[0] - t_kill} us (includes the deliberate 500 ms)")
    note(f"holder: {[e for _, e in hold.find('HOLDER')]}")
    check("F5a", "fusermount3 -u", umount_retry(mnt), 0)
    check("F5a", "holder notices unmount (POLLERR) and drops fds", hold.wait_for("HOLDER_CONN_DEAD", 3) is not None,
          True)
    cleanup_all()

    scenario("F5b", "re-exec in place: A execve()s the new binary, fd inherited (no CLOEXEC), same PID",
             "mount and open handle keep working, no INIT")
    d, mnt, back = setup("f5b")
    a = rawfs("mount", mnt, back)
    a.wait_for("INIT")
    c = Client()
    h = c.call(op="open", path=mnt + "/f")["h"]
    a.cmd("reexec")
    tb = a.wait_for("TAKEOVER")
    check("F5b", "same pid after exec", field(a.find("START mode=adopt")[0], "pid"), str(a.p.pid))
    check("F5b", "pread on old handle", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    a.cmd("ping")
    check("F5b", "INITs after exec", field(a.wait_for("PONG"), "inits"), "0")
    note(f"EXEC_START -> TAKEOVER: {tb[0] - a.find('EXEC_START')[0][0]} us")
    cleanup_all()

    t_f5c()
    t_f6_gap()


def systemd_unit(tid, mnt, back, log):
    unit = UNIT_PREFIX + tid.lower() + "-" + str(os.getpid())
    units.append(unit)
    r = subprocess.run(["systemd-run", "--user", "--unit", unit, "-p", "Type=notify", "-p", "NotifyAccess=main",
                        "-p", "FileDescriptorStoreMax=8", "-p", "Restart=always", "-p", "RestartSec=0",
                        "-p", f"StandardOutput=append:{log}", "-p", f"StandardError=append:{log}",
                        RAWFS, "mount", mnt, back, "--fdstore"], capture_output=True, text=True)
    return unit, r


def sd_show(unit, prop):
    return subprocess.run(["systemctl", "--user", "show", "-p", prop, "--value", unit], capture_output=True,
                          text=True).stdout.strip()


def t_f5c():
    scenario("F5c", "systemd --user fdstore: the daemon stores its fuse fd + a memfd with INIT state "
             "(FDSTORE=1), then is restarted / killed",
             "systemd hands the fds back as LISTEN_FDS; mount and open handle survive both; "
             "'systemctl stop' flushes the store and aborts the connection")
    d, mnt, back = setup("f5c")
    log = d + "/unit.log"
    unit, r = systemd_unit("F5c", mnt, back, log)
    if not check("F5c", "systemd-run --user", r.returncode, 0):
        note(r.stderr.strip())
        return
    lt = Daemon(None, logfile=log)
    check("F5c", "INIT under systemd", lt.wait_for("INIT") is not None, True)
    lt.wait_for("SD_NOTIFY READY=1")
    check("F5c", "fds in store", sd_show(unit, "NFileDescriptorStore"), "2")
    c = Client()
    h = c.call(op="open", path=mnt + "/f")["h"]
    pid0 = sd_show(unit, "MainPID")
    n = len(lt.events)
    rr = subprocess.run(["systemctl", "--user", "restart", unit], capture_output=True, text=True)
    check("F5c", "systemctl restart", rr.returncode, 0)
    check("F5c", "adopted after restart", lt.wait_for("ADOPTED_FROM_FDSTORE", after=n) is not None, True)
    check("F5c", "new MainPID", sd_show(unit, "MainPID") != pid0, True)
    check("F5c", "pread on old handle after restart", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    n = len(lt.events)
    pid1 = sd_show(unit, "MainPID")
    os.kill(int(pid1), signal.SIGKILL)
    check("F5c", "adopted after SIGKILL + Restart=always", lt.wait_for("ADOPTED_FROM_FDSTORE", 5, after=n) is not None,
          True)
    check("F5c", "pread on old handle after crash", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    check("F5c", "INITs across both restarts", len(lt.find("INIT kernel")), 1)
    note(f"LISTEN_FDS seen: {lt.find('LISTEN_FDS')[-1][1]}")
    subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True)
    check("F5c", "after 'systemctl stop'", outcome(c.call(op="stat", path=mnt + "/f")), "ENOTCONN")
    cleanup_all()


def gap_run(kind, rep):
    """One handoff of the given kind under a stat loop.

    Returns the stall (longest stat that overlaps the handoff, from the handoff command to the successor being
    ready), the loop's p50 and max, its errors and op count, and the daemon-side gap from its own events.
    """
    d, mnt, back = setup(f"gap-{kind}-{rep}")
    c = Client()
    gap = None
    if kind.startswith("systemd"):
        log = d + "/unit.log"
        unit, _ = systemd_unit(f"gap{rep}", mnt, back, log)
        lt = Daemon(None, logfile=log)
        lt.wait_for("SD_NOTIFY READY=1")
        c.send(op="loop", path=mnt + "/f", secs=1.2)
        time.sleep(0.4)
        n = len(lt.events)
        t_h = now_us()
        if kind == "systemd-restart":
            subprocess.run(["systemctl", "--user", "restart", unit], capture_output=True)
        else:
            os.kill(int(sd_show(unit, "MainPID")), signal.SIGKILL)
        t_ready = lt.wait_for("SD_NOTIFY READY=1", after=n)[0]
        gap = t_ready - t_h
    else:
        a = rawfs("mount", mnt, back)
        a.wait_for("INIT")
        if kind == "scm-hot":
            sock = d + "/handoff.sock"
            b = rawfs("takeover", sock, back)
            b.wait_for("LISTENING")
        elif kind == "holder-cold":
            hsock = d + "/holder.sock"
            hold = rawfs("hold", hsock)
            hold.wait_for("HOLDER_LISTENING")
            a.cmd("put " + hsock)
            hold.wait_for("HOLDER_PUT")
        c.send(op="loop", path=mnt + "/f", secs=1.2)
        time.sleep(0.4)
        t_h = now_us()
        if kind == "none":
            t_ready = t_h
        elif kind == "scm-hot":
            a.cmd("handoff " + sock)
            t_ready = b.wait_for("TAKEOVER")[0]
            gap = t_ready - a.wait_for("HANDOFF_START")[0]
        elif kind == "reexec":
            a.cmd("reexec")
            t_ready = a.wait_for("TAKEOVER")[0]
            gap = t_ready - a.wait_for("EXEC_START")[0]
        elif kind == "holder-cold":
            a.p.kill()
            a.exited()
            b = rawfs("get", hsock, back, "--resend")
            t_ready = b.wait_for("TAKEOVER")[0]
            gap = t_ready - t_h
    r = c.result(5)
    cleanup_all()
    stall = max([dur for st, dur in r["slow"] if st < t_ready and st + dur > t_h] or [0])
    return dict(stall=stall, p50=r["p50"], max=r["max"], errs=r["errs"], n=r["n"], gap=gap)


def t_f6_gap():
    scenario("F6b", "client stat loop (1.2 s, ~100 us apart) across each handoff kind, 5 reps each; "
             "'none' is the control with no handoff",
             "no errors in any run; the stall of the stat overlapping the handoff is about the daemon-side gap: "
             "sub-ms for a hot standby over SCM_RIGHTS, a few ms for exec-based ones, more for systemd")
    for kind in ("none", "scm-hot", "reexec", "holder-cold", "systemd-restart", "systemd-sigkill"):
        runs = [gap_run(kind, i) for i in range(5)]
        errs = {}
        for x in runs:
            for k, v in x["errs"].items():
                errs[k] = errs.get(k, 0) + v
        check("F6b", f"{kind}: errors during the loop", errs, {})
        stalls = sorted(x["stall"] for x in runs)
        gaps = sorted(x["gap"] for x in runs if x["gap"] is not None)
        note(f"{kind}: stat p50 {statistics.median(x['p50'] for x in runs):.0f} us, loop max (us) "
             f"{sorted(x['max'] for x in runs)}, stall at handoff (us) {stalls} -> median "
             f"{statistics.median(stalls):.0f} us" + (f"; daemon-side gap (us) {gaps} -> median "
                                                      f"{statistics.median(gaps):.0f} us" if gaps else ""))


def t_f7():
    scenario("F7", "cost of one libfuse daemon per mount (10 mounts) vs one daemon with 10 sessions",
             "a few MB RSS and 1 thread per single-threaded daemon (mostly shared libc pages, small PSS); "
             "loop_mt adds worker threads under load")
    llfs = os.path.join(BIN, "llfs-default")

    def status(pid):
        out = {}
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                k, v = line.split(":", 1)
                if k in ("VmRSS", "RssAnon", "Threads"):
                    out[k] = int(v.split()[0])
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    out["Pss"] = int(line.split()[1])
        return out

    def load(mnts):
        cs = [Client() for _ in range(8)]
        for i, cl in enumerate(cs):
            cl.send(op="loop", path=mnts[i % 2] + "/f", secs=1.0)
        for cl in cs:
            cl.result(5)
            cl.kill()

    def summarize(label, pids):
        st = [status(p) for p in pids]
        tot = {k: sum(s[k] for s in st) for k in ("VmRSS", "RssAnon", "Pss", "Threads")}
        note(f"{label}: processes={len(pids)} RSS={tot['VmRSS']} kB PSS={tot['Pss']} kB "
             f"RssAnon={tot['RssAnon']} kB threads={tot['Threads']}")
        return tot

    for mode in ("single", "mt"):
        ds, mnts = [], []
        for i in range(10):
            d, mnt, back = setup(f"f7-{mode}-{i}")
            args = [llfs, "libmount", mnt, back] + (["--mt"] if mode == "mt" else [])
            ds.append(Daemon(args))
            mnts.append(mnt)
        for x in ds:
            x.wait_for("MOUNTED")
        c = Client()
        check("F7", f"10 x llfs {mode}: stat on every mount", [outcome(c.call(op="stat", path=m + "/f")) for m in mnts],
              ["ok"] * 10)
        summarize(f"10 x llfs ({mode}-threaded), idle", [x.p.pid for x in ds])
        load(mnts)
        summarize(f"10 x llfs ({mode}-threaded), after 8 concurrent stat loops on 2 of the mounts", [x.p.pid for x in ds])
        cleanup_all()

    d, _, back = setup("f7-multi")
    multi_mnts = []
    for i in range(10):
        m = os.path.join(d, f"m{i}")
        os.makedirs(m)
        multi_mnts.append(m)
        mounts.append(m)
    x = Daemon([llfs, "multi", "10", os.path.join(d, "m"), back])
    x.wait_for("MOUNTED")
    c = Client()
    check("F7", "1 x llfs multi 10: stat on every mount",
          [outcome(c.call(op="stat", path=m + "/f")) for m in multi_mnts], ["ok"] * 10)
    summarize("1 x llfs with 10 sessions (one thread each), idle", [x.p.pid])
    cleanup_all()


def t_f8():
    scenario("F8", "daemon negotiated request_timeout=15 s (FUSE_REQUEST_TIMEOUT, 6.14+); a request A read is "
             "never answered after handoff (B does not resend)",
             "kernel aborts the whole connection after 15-30 s although B holds the fd: hung caller ECONNABORTED, "
             "then ENOTCONN")
    d, mnt, back = setup("f8")
    a = rawfs("mount", mnt, back, "--hang", env={"RAWFS_TIMEOUT": "15"})
    note(f"A: {a.wait_for('INIT')[1]}")
    c, ch = Client(), Client()
    ch.send(op="stat", path=mnt + "/hang1")
    a.wait_for("HUNG")
    sock = d + "/handoff.sock"
    b = rawfs("takeover", sock, back)
    b.wait_for("LISTENING")
    a.cmd("handoff " + sock)
    a.exited()
    r = ch.result(40)
    check("F8", "hung caller", outcome(r), "ECONNABORTED")
    if r:
        note(f"hung caller blocked {r['us'] / 1e6:.1f} s")
    check("F8", "next caller", outcome(c.call(op="stat", path=mnt + "/f")), "ENOTCONN")
    ev = b.wait_for("DEV_READ_ERR", 2)
    note(f"B: {ev[1] if ev else 'no read error'}")
    cleanup_all()


def alive(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1][0] != "Z"
    except OSError:
        return False


def t_f9():
    scenario("F9a", "control: mounted with -o auto_unmount, A killed with no handoff",
             "fusermount3 wakes on the closed socket, but its probe open(mnt) runs with euid 0 on a mount without "
             "allow_other, gets EACCES instead of ENOTCONN (should_auto_unmount), and leaves the dead mount")
    d, mnt, back = setup("f9a")
    a = rawfs("mount", mnt, back, "--auto-unmount")
    fm = int(field(a.wait_for("FUSERMOUNT"), "pid"))
    a.wait_for("INIT")
    check("F9a", "fusermount3 watchdog alive while A runs", alive(fm), True)
    a.p.kill()
    a.exited()
    time.sleep(0.5)
    check("F9a", "fusermount3 exited", alive(fm), False)
    check("F9a", "mount present 500 ms after A killed", mountinfo(mnt) is not None, True)
    cleanup_all()

    for variant, cmdname, passed in (("b", "handoff-first", False), ("c", "handoff", True)):
        scenario(f"F9{variant}", f"-o auto_unmount; A hands off "
                 f"{'the fuse fd and the fusermount3 socket' if passed else 'only the fuse fd'} and exits",
                 "mount survives A's exit (the probe open(mnt) would succeed through B anyway); "
                 + ("fusermount3 keeps watching, now tied to B" if passed else
                    "fusermount3 sees the socket close and exits, so nothing watches B any more"))
        d, mnt, back = setup(f"f9{variant}")
        a = rawfs("mount", mnt, back, "--auto-unmount")
        fm = int(field(a.wait_for("FUSERMOUNT"), "pid"))
        a.wait_for("INIT")
        sock = d + "/handoff.sock"
        b = rawfs("takeover", sock, back)
        b.wait_for("LISTENING")
        a.cmd(f"{cmdname} {sock}")
        a.exited()
        note(f"B: {b.wait_for('TAKEOVER')[1]}")
        time.sleep(0.3)
        c = Client()
        check(f"F9{variant}", "mount present 300 ms after A exit", mountinfo(mnt) is not None, True)
        check(f"F9{variant}", "stat via B", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
        check(f"F9{variant}", "fusermount3 watchdog alive", alive(fm), passed)
        if passed:
            b.p.kill()
            b.exited()
            time.sleep(0.3)
            check(f"F9{variant}", "fusermount3 exits when B dies", alive(fm), False)
        cleanup_all()


def t_l():
    llfs = os.path.join(BIN, "llfs")
    scenario("L1", "libfuse 3.18.3 session started on an already-INITed fd via the '/dev/fd/N' mountpoint",
             "libfuse waits for an INIT that never comes and answers every request with EIO")
    d, mnt, back = setup("l1")
    a = rawfs("mount", mnt, back)
    a.wait_for("INIT")
    a.cmd(f"exec {llfs} devfd {back}")
    note(f"llfs: {a.wait_for('DEVFD_MOUNT')[1]}")
    c = Client()
    check("L1", "stat via libfuse on /dev/fd/N", outcome(c.call(op="stat", path=mnt + "/f")), "EIO")
    cleanup_all()

    scenario("L2", "libfuse 3.18.3 (built -Denable-custom-io=true): llfs re-execs itself, the new image uses "
             "fuse_session_custom_io and feeds libfuse a synthetic INIT copied from the original, swallowing the reply",
             "session works without a kernel INIT; replayed INIT reply equals the original; open handle survives")
    d, mnt, back = setup("l2")
    x = Daemon([llfs, "mount", mnt, back])
    check("L2", "custom io accepted", field(x.wait_for("CUSTOM_IO"), "ret"), "0")
    x.wait_for("MOUNTED")
    c = Client()
    h = c.call(op="open", path=mnt + "/f")["h"]
    check("L2", "read before", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    x.wait_for("INIT_REPLY_RECORDED")
    check("L2", "trigger lookup", outcome(c.call(op="stat", path=mnt + "/handoff_reexec")), "ENOENT")
    rep = x.wait_for("INIT_REPLY_SWALLOWED")
    check("L2", "replayed INIT reply == original", field(rep, "matches_original") if rep else None, "1")
    check("L2", "same pid", field(x.find("START mode=adopt")[0], "pid"), str(x.p.pid))
    check("L2", "read on old handle after re-exec", c.call(op="pread", h=h)["data"], "hello-from-backing\n")
    check("L2", "stat after re-exec", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
    note(f"EXEC_START -> TAKEOVER: {x.wait_for('TAKEOVER')[0] - x.find('EXEC_START')[0][0]} us")
    cleanup_all()

    scenario("L3", "default libfuse 3.18.3 build: fuse_session_custom_io",
             "-ENOTSUP (-95): custom io is compiled out unless -Denable-custom-io=true")
    d, mnt, back = setup("l3")
    x = Daemon([os.path.join(BIN, "llfs-default"), "mount", mnt, back])
    check("L3", "fuse_session_custom_io ret", field(x.wait_for("CUSTOM_IO"), "ret"), "-95")
    cleanup_all()

    scenario("L4", "a different implementation takes over: rawfs INITs, then execs llfs adopt (libfuse computes "
             "its own INIT reply)",
             "libfuse's reply differs from what the kernel holds (it is swallowed, the kernel keeps rawfs's); "
             "requests still work here because both sides only use basic ops")
    d, mnt, back = setup("l4")
    a = rawfs("mount", mnt, back)
    a.wait_for("INIT")
    a.cmd(f"exec {llfs} adopt {back}")
    rep = a.wait_for("INIT_REPLY_SWALLOWED")
    note(f"llfs: {rep[1] if rep else None}")
    check("L4", "replayed INIT reply == original", field(rep, "matches_original") if rep else None, "0")
    c = Client()
    check("L4", "stat", outcome(c.call(op="stat", path=mnt + "/f")), "ok")
    cleanup_all()


TESTS = {"F1": t_f1, "F2": t_f2_f3, "F4": t_f4, "F5": t_f5_f6, "F7": t_f7, "F8": t_f8, "F9": t_f9, "L": t_l}


def header():
    def v(cmd):
        return subprocess.run(cmd, capture_output=True, text=True).stdout.strip().splitlines()[0]
    lf = open(os.path.join(S, "vendor/libfuse/meson.build")).read().split("version:", 1)[1].split("'")[1]
    print(f"# kernel {os.uname().release}, libfuse {lf} (vendor build), {v(['fusermount3', '-V'])}, "
          f"{v(['systemctl', '--user', '--version'])}, Python {sys.version.split()[0]}, "
          f"{time.strftime('%Y-%m-%d')}", flush=True)


def main():
    if sys.argv[1:2] == ["--client"]:
        client_main()
        return
    os.makedirs(W, exist_ok=True)
    header()
    for tid in sys.argv[1:] or TESTS:
        try:
            TESTS[tid]()
        except Exception as e:  # keep going so one broken scenario does not hide the others
            import traceback
            traceback.print_exc(file=sys.stdout)
            check(tid, "scenario raised", repr(e), None)
        finally:
            cleanup_all()
    print("\n=== summary")
    for tid, oks in results.items():
        print(f"  {tid}: {sum(oks)}/{len(oks)} checks OK")


if __name__ == "__main__":
    main()
