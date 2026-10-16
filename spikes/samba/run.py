"""Samba spike: how smbd handles caching (leases, oplocks), share modes, byte-range locks and change notify,
on one gateway, on two gateways over one directory (no CTDB), and on top of a FUSE filesystem.

  . ./env.sh && python3 run.py [L] [G] [F] [--fast]

L: one gateway on a local directory: Samba's own state machine.
G: two gateways (two smbd with separate state, as on two nodes without CTDB) on one directory.
F: gateways on passfs FUSE mounts: two mounts of one backing dir stand in for two nodes of our filesystem.
--fast skips the tests that wait for timeouts (~1.5 min).
"""
import fcntl, os, shutil, signal, struct, sys, threading, time

from smbspike import (Client, Gateway, Mount, W, attempt, status_name)
from smbprotocol.change_notify import CompletionFilter, FileSystemWatcher
from smbprotocol.file_info import FileInformationClass

FAST = "--fast" in sys.argv
PARTS = [a for a in sys.argv[1:] if not a.startswith("-")] or ["L", "G", "F"]
NOISE = (".Trash", "autorun.inf", ".xdg-volume-info", ".hidden", "/.git")


def scenario(t):
    print(f"\n=== {t}", flush=True)


def say(*a):
    print("  ", *a, flush=True)


def brk(c, settle=0.3):
    b = c.take_breaks(settle)
    return ", ".join(f"{e['kind']} {e.get('cur', '')}->{e['new']}" + (" (ack required)" if e["ack_required"] else "")
                     for e in b) or "no break"


def fresh(d):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    return d


def timed(fn, *a, **kw):
    t = time.monotonic()
    r = attempt(fn, *a, **kw)
    return r, (time.monotonic() - t) * 1e3


def in_thread(fn, *a, **kw):
    out = {}

    def run():
        out["r"], out["ms"] = timed(fn, *a, **kw)
    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th, out


def getlk(path, start, length):
    """F_OFD_GETLK from a non-SMB process: does the kernel see Samba's byte-range lock?"""
    fd = os.open(path, os.O_RDWR)
    try:
        lk = struct.pack("hhqqi4x", fcntl.F_WRLCK, os.SEEK_SET, start, length, 0)
        r = fcntl.fcntl(fd, 36, lk)  # F_OFD_GETLK
        typ, _, s, l, pid = struct.unpack("hhqqi4x", r)
        return "unlocked" if typ == fcntl.F_UNLCK else f"{'write' if typ == fcntl.F_WRLCK else 'read'} lock " \
            f"[{s}, +{l}) pid={pid}"
    finally:
        os.close(fd)


# ======================================================================= L: one gateway

def part_L():
    d = fresh(os.path.join(W, "local"))
    g = Gateway("l", 4461, d)
    gk = Gateway("lk", 4462, d, share={"kernel oplocks": "yes"})
    try:
        c1, c2 = Client(4461, "c1"), Client(4461, "c2")

        scenario("L1 lease grant and break between two clients of one gateway")
        h1 = c1.open("f", lease="RWH")
        h1.write(b"AAAA")
        say(f"c1 open asks RWH, granted {h1.granted}")
        h1b = c1.open("f", lease="RWH", lease_key=h1.lease_key)
        say(f"c1 second open, same lease key: granted {h1b.granted}; c1 sees {brk(c1)}")
        h1b.close()
        h2 = c2.open("f", lease="RWH")
        say(f"c2 open asks RWH: granted {h2.granted}, open took {h2.open_ms:.2f} ms (includes c1's ack); "
            f"c1 sees {brk(c1)}")
        h2.write(b"BBBB")
        say(f"c2 writes: c1 sees {brk(c1)}; c2 sees {brk(c2)}")
        h2.close()
        h1.close()
        h3 = c2.open("f", lease="RWH")
        say(f"after c1 closed, c2 reopens: granted {h3.granted}")
        h3.close()

        scenario("L2 a read-only second opener, then a writer: what each break costs the opener")
        h1 = c1.open("g", lease="RWH")
        h1.write(b"AAAA")
        h2 = c2.open("g", access="r", lease="RH")
        say(f"c2 read-only open with lease RH: granted {h2.granted}, {h2.open_ms:.2f} ms; c1 sees {brk(c1)}")
        h3 = Client(4461, "c3").open("g", access="rw")
        r, ms = timed(h3.write, b"CCCC")
        say(f"c3 (no lease) writes: {r[0]} in {ms:.2f} ms; c1 sees {brk(c1)}; c2 sees {brk(c2)}")
        for h in (h1, h2, h3):
            h.close()

        if not FAST:
            scenario("L3 the lease holder never acks (a hung or partitioned client)")
            cz = Client(4461, "cz", ack="never")
            hz = cz.open("u", lease="RWH")
            hz.write(b"ZZZZ")
            th, out = in_thread(c2.open, "u")
            th.join(120)
            r = out["r"]
            say(f"c2 open: {r[0]} after {out['ms'] / 1e3:.1f} s; cz saw {brk(cz, 0)}")
            if r[1]:
                r2, ms = timed(r[1].write, b"WWWW")
                say(f"c2 write: {r2[0]} in {ms / 1e3:.1f} s; cz saw {brk(cz, 0)}")
                r[1].close()
            cz.close()
            cz = Client(4461, "cz2", ack="never")
            hz = cz.open("u2", lease="RH")
            h2 = c2.open("u2")
            r, ms = timed(h2.write, b"WWWW")
            say(f"holder of RH (no W) never acks; c2 (no lease) writes: {r[0]} in {ms / 1e3:.2f} s; "
                f"cz2 saw {brk(cz, 0)}")
            h2.close()
            cz.close()

        scenario("L4 share modes (deny modes) are Samba state, not kernel state")
        h1 = c1.open("s", access="rw", share="r")  # allow others to read only: deny write
        h1.write(b"AAAA")
        say(f"c1 opens with share=READ (deny write, deny delete)")
        say(f"c2 open for write: {attempt(c2.open, 's', access='w')[0]}")
        r = attempt(c2.open, "s", access="r", share="rw")
        say(f"c2 open for read:  {r[0]}")
        r[1] and r[1].close()
        say(f"c2 open for delete: {attempt(c2.open, 's', access='d')[0]}")
        st = g.tool("smbstatus", "-s", g.conf, "-L").splitlines()
        say("smbstatus -L:", next((l.split()[2] for l in st if l.rstrip().endswith(" s") or "  s  " in l), "?"))
        with open(os.path.join(d, "s"), "r+b") as f:
            f.write(b"LLLL")
        say(f"local process opens O_RDWR and writes: allowed; c1 reads {h1.read(0, 4)!r}")
        h1.close()

        scenario("L5 byte-range locks: mandatory for SMB clients, mirrored as OFD locks for local processes")
        h1, h2 = c1.open("k"), c2.open("k")
        h1.write(b"x" * 300)
        h1.lock(0, 100)
        say("c1 exclusive lock [0, 100)")
        say(f"c2 lock [50, 60):  {attempt(h2.lock, 50, 10)[0]}")
        say(f"c2 write at 60:    {attempt(h2.write, b'y', 60)[0]}")
        say(f"c2 read at 60:     {attempt(h2.read, 60, 1)[0]}")
        say(f"c2 write at 200:   {attempt(h2.write, b'y', 200)[0]}")
        say(f"local F_OFD_GETLK [0, 100): {getlk(os.path.join(d, 'k'), 0, 100)}")
        with open(os.path.join(d, "k"), "r+b") as f:
            f.seek(10)
            f.write(b"L")
        say(f"local write at 10: allowed (POSIX locks are advisory); c1 reads {h1.read(10, 1)!r}")
        th, out = in_thread(h2.lock, 0, 10, True, True)  # blocking lock
        time.sleep(0.5)
        t0 = time.monotonic()
        h1.unlock(0, 100)
        th.join(10)
        say(f"c2 blocking lock [0, 10) waiting; c1 unlocks: c2 {out['r'][0]} "
            f"{(time.monotonic() - t0) * 1e3:.1f} ms after the unlock")
        h1.close(), h2.close()

        scenario("L6 a local (non-SMB) writer vs a client holding a lease")
        h1 = c1.open("n", lease="RWH")
        h1.write(b"AAAA")
        h1.read(0, 4)
        with open(os.path.join(d, "n"), "r+b") as f:
            f.write(b"LLLL")
        say(f"kernel oplocks = no (default): local write while c1 holds RWH: c1 sees {brk(c1)}")
        h1.close()
        k1, k2 = Client(4462, "k1"), Client(4462, "k2")
        h = k1.open("n2", lease="RWH")
        say(f"kernel oplocks = yes: c1 asks lease RWH: granted {h.granted}")
        h.close()
        h = k1.open("n2", oplock="batch")
        say(f"kernel oplocks = yes: c1 asks batch oplock: granted {h.granted}")
        t = time.monotonic()
        fd = os.open(os.path.join(d, "n2"), os.O_RDWR)
        ms = (time.monotonic() - t) * 1e3
        os.write(fd, b"LLLL")
        os.close(fd)
        say(f"local open(O_RDWR) took {ms:.2f} ms (kernel lease break); c1 sees {brk(k1)}")
        h.close()
        if not FAST:
            kz = Client(4462, "kz", ack="never")
            h = kz.open("n4", oplock="batch")
            h.write(b"AAAA")
            t = time.monotonic()
            fd = os.open(os.path.join(d, "n4"), os.O_RDWR)
            ms = (time.monotonic() - t) * 1e3
            os.close(fd)
            say(f"kernel oplocks = yes, batch holder never acks: local open(O_RDWR) took {ms / 1e3:.1f} s; "
                f"holder saw {brk(kz, 0)}")
            kz.close()
        h = k1.open("n3", oplock="batch")
        h2 = k2.open("n3")
        say(f"kernel oplocks = yes, SMB second opener: granted c1 {h.granted}; c1 sees {brk(k1)}")
        h.close(), h2.close()
        k1.close(), k2.close()

        scenario("L7 change notify on a local directory")
        os.makedirs(os.path.join(d, "dir"))
        cn = c1.open("dir", directory=True)
        w = FileSystemWatcher(cn.o)
        w.start(CompletionFilter.FILE_NOTIFY_CHANGE_FILE_NAME)
        time.sleep(0.2)
        t = time.monotonic()
        open(os.path.join(d, "dir", "new"), "w").close()
        ok = w.response_event.wait(3)
        say(f"local process creates dir/new: c1 notified={ok}" + (f" after {(time.monotonic() - t) * 1e3:.1f} ms, "
            f"{[(a['file_name'].get_value(), a['action'].get_value()) for a in w.result]}" if ok else ""))
        cn.close()
        c1.close(), c2.close()
    finally:
        g.stop(), gk.stop()


# ======================================================================= G: two gateways, no CTDB

def part_G():
    d = fresh(os.path.join(W, "shared"))
    ga, gb = Gateway("a", 4471, d), Gateway("b", 4472, d)
    gka = Gateway("ka", 4473, d, share={"kernel oplocks": "yes"})
    gkb = Gateway("kb", 4474, d, share={"kernel oplocks": "yes"})
    try:
        a, b = Client(4471, "a"), Client(4472, "b")

        scenario("G1 lease on gateway A, writer on gateway B")
        ha = a.open("f", lease="RWH")
        ha.write(b"AAAA")
        hb = b.open("f", lease="RWH")
        say(f"a granted {ha.granted}; b (other gateway) granted {hb.granted}; a sees {brk(a)}")
        hb.write(b"BBBB")
        say(f"b writes: a sees {brk(a)}. a's cached data is now stale, and a was told nothing")
        ha.close(), hb.close()

        scenario("G2 share mode on gateway A, opener on gateway B")
        ha = a.open("s", access="rw", share="r")
        say(f"a opens deny-write; b opens for write: {attempt(b.open, 's', access='w')[0]}")
        ha.close()

        scenario("G3 byte-range lock on gateway A, contender on gateway B (posix locking = yes)")
        ha, hb = a.open("k"), b.open("k")
        ha.write(b"x" * 200)
        ha.lock(0, 100)
        say(f"a exclusive lock [0, 100); b lock [50, 60): {attempt(hb.lock, 50, 10)[0]}")
        say(f"b write at 60: {attempt(hb.write, b'y', 60)[0]}")
        say(f"b read at 60:  {attempt(hb.read, 60, 1)[0]}")
        ha.close(), hb.close()

        scenario("G4 both gateways with kernel oplocks = yes (one kernel, so kernel leases are shared)")
        ka, kb = Client(4473, "ka"), Client(4474, "kb")
        ha = ka.open("o", oplock="batch")
        ha.write(b"AAAA")
        hb = kb.open("o")
        say(f"a granted {ha.granted}; b opens via the other gateway in {hb.open_ms:.2f} ms; a sees {brk(ka)}")
        hb.write(b"BBBB")
        ha.close(), hb.close()
        ka.close(), kb.close()
        a.close(), b.close()
    finally:
        for g in (ga, gb, gka, gkb):
            g.stop()


# ======================================================================= F: on top of FUSE

def fuse_ops(m, fn, only="/"):
    m.stats()
    fn()
    time.sleep(0.05)
    counts, lines = m.stats(only)
    lines = [l for l in lines if not any(n in l for n in NOISE)]
    counts = {}
    for l in lines:
        counts[l.split()[0]] = counts.get(l.split()[0], 0) + 1
    return counts, lines


def fmt(counts):
    return f"{sum(counts.values()):3d} FUSE requests: " + " ".join(f"{k}={v}" for k, v in sorted(counts.items()))


def part_F():
    back = fresh(os.path.join(W, "backing"))
    os.makedirs(os.path.join(back, "d100"))
    for i in range(100):
        open(os.path.join(back, "d100", f"f{i:03d}"), "w").close()
    with open(os.path.join(back, "r"), "wb") as f:
        f.write(b"R" * 4096)

    TUNED = {"case sensitive": "yes", "ea support": "no", "store dos attributes": "no"}
    for k, (ttl, conf, label) in enumerate([("1", {}, "defaults, attr/entry timeout 1 s"),
                                             ("0", {}, "defaults, attr/entry timeout 0 s"),
                                             ("1", TUNED, "timeout 1 s, " + ", ".join(f"{a} = {b}" for a, b in TUNED.items()))]):
        m = Mount("A", back, f"ttl={ttl}")
        g = Gateway("fa", 4481, m.mnt, share=conf)
        try:
            c = Client(4481, "c")
            time.sleep(1.5)  # let the desktop's mount probing settle
            scenario(f"F1 FUSE requests per SMB operation (Samba on passfs; {label})")
            n = [0]

            def create():
                n[0] += 1
                h = c.open(f"new{k}_{n[0]}", disp="create")
                h.write(b"N" * 4096)
                h.close()

            def readf():
                h = c.open("r", access="r")
                h.read(0, 4096)
                h.close()

            def stat():
                c.open("r", access="").close()

            def listdir():
                h = c.open("d100", directory=True)
                h.o.query_directory("*", FileInformationClass.FILE_ID_BOTH_DIRECTORY_INFORMATION)
                h.close()

            create(), readf(), stat(), listdir()  # warm Samba's caches
            time.sleep(float(ttl) + 0.1)
            for op, fn in [("create + write 4K + close", create), ("open + read 4K + close", readf),
                           ("open + close (stat)", stat), ("list a 100-entry directory", listdir)]:
                counts, lines = fuse_ops(m, fn)
                say(f"{op:28s} {fmt(counts)}")
                if op.startswith("create") and k == 0:
                    seq = " | ".join(" ".join(l.split()[:3]) for l in lines)
                    say(f"  sequence: {seq}")
                time.sleep(float(ttl) + 0.1)
            for op, fn in [("open + read 4K + close", readf), ("create + write 4K + close", create)]:
                t = time.monotonic()
                for _ in range(200):
                    fn()
                say(f"latency {op:28s} {(time.monotonic() - t) / 200 * 1e3:.2f} ms")
            c.close()
        finally:
            g.stop()
            m.stop()

    big = os.path.join(back, "big")
    os.makedirs(big)
    for i in range(10000):
        open(os.path.join(big, f"e{i:05d}"), "w").close()
    for k, conf in enumerate([{}, {"case sensitive": "yes"}]):
        m = Mount("A", back)
        g = Gateway("fa", 4481, m.mnt, share=conf)
        try:
            c = Client(4481, "c")
            scenario(f"F1b creating new names in a 10,000-entry directory, case sensitive = {conf.get('case sensitive', 'auto (default)')}")
            c.open(f"big/w{k}", disp="create").close()
            counts, _ = fuse_ops(m, lambda: c.open(f"big/x{k}", disp="create").close())
            say(f"one create: {fmt(counts)}")
            t = time.monotonic()
            for i in range(50):
                c.open(f"big/y{k}_{i}", disp="create").close()
            say(f"latency create + close: {(time.monotonic() - t) / 50 * 1e3:.2f} ms")
            t = time.monotonic()
            for i in range(50):
                attempt(c.open, f"big/missing{i}", disp="open")
            say(f"latency open of a missing name (OBJECT_NAME_NOT_FOUND): {(time.monotonic() - t) / 50 * 1e3:.2f} ms")
            c.close()
        finally:
            g.stop(), m.stop()

    d = fresh(os.path.join(W, "local-lat"))
    shutil.copy(os.path.join(back, "r"), d)
    g = Gateway("fl", 4482, d)
    try:
        c = Client(4482, "c")
        scenario("F1c baseline: the same SMB operations on a local ext4 share (client-side Python overhead included)")
        for label in ("open + read 4K + close", "create + write 4K + close"):
            t = time.monotonic()
            for i in range(200):
                if label.startswith("open"):
                    h = c.open("r", access="r"); h.read(0, 4096); h.close()
                else:
                    h = c.open(f"n{i}", disp="create"); h.write(b"N" * 4096); h.close()
            say(f"latency {label:28s} {(time.monotonic() - t) / 200 * 1e3:.2f} ms")
        c.close()
    finally:
        g.stop()

    scenario("F2 xattrs: what Samba stores in them, and a filesystem without them")
    m = Mount("A", back)
    g = Gateway("fa", 4481, m.mnt)
    try:
        c = Client(4481, "c")
        _, lines = fuse_ops(m, lambda: (c.open("xa", disp="create").close()))
        names = sorted({l.split()[2] for l in lines if "xattr" in l.split()[0] and len(l.split()) > 2})
        say(f"xattrs touched on create: {names}")
        c.close()
    finally:
        g.stop(), m.stop()
    m = Mount("A", back, "noxattr")
    g = Gateway("fa", 4481, m.mnt)
    try:
        c = Client(4481, "c")
        r = attempt(lambda: (c.open("xb", disp="create").close()))
        say(f"noxattr mount: create {r[0]}")
        c.close()
    finally:
        g.stop(), m.stop()

    for locks in ("", "locks"):
        A, B = Mount("A", back, *([locks] if locks else [])), Mount("B", back, *([locks] if locks else []))
        ga = Gateway("fa", 4481, A.mnt, share={"kernel oplocks": "yes"})
        gb = Gateway("fb", 4482, B.mnt, share={"kernel oplocks": "yes"})
        try:
            a, b = Client(4481, "a"), Client(4482, "b")
            if not locks:
                scenario("F3 kernel leases on FUSE: node A (gateway A on mount A), node B (gateway B on mount B)")
                ha = a.open("o", oplock="batch")
                ha.write(b"AAAA")
                hb = b.open("o")
                say(f"a granted {ha.granted}; b opens via mount B in {hb.open_ms:.2f} ms; a sees {brk(a)}")
                hb.write(b"BBBB")
                say(f"b writes: a sees {brk(a)}")
                ha.close(), hb.close()
                hits = []
                signal.signal(signal.SIGIO, lambda *_: hits.append(time.monotonic()))
                fd = os.open(os.path.join(A.mnt, "o"), os.O_RDONLY)
                fcntl.fcntl(fd, fcntl.F_SETLEASE, fcntl.F_RDLCK)
                w = os.open(os.path.join(B.mnt, "o"), os.O_WRONLY)
                os.write(w, b"X"), os.close(w)
                time.sleep(0.2)
                say(f"local read lease on mount A; open for write on mount B: lease break signal={bool(hits)}")
                th = threading.Thread(target=lambda: os.close(os.open(os.path.join(A.mnt, "o"), os.O_WRONLY)))
                th.start()
                time.sleep(0.2)
                say(f"open for write on mount A: lease break signal={bool(hits)}")
                fcntl.fcntl(fd, fcntl.F_SETLEASE, fcntl.F_UNLCK)
                th.join(), os.close(fd)

                scenario("F4 change notify across nodes")
                os.makedirs(os.path.join(back, "nd"), exist_ok=True)
                cn = a.open("nd", directory=True)
                w = FileSystemWatcher(cn.o)
                w.start(CompletionFilter.FILE_NOTIFY_CHANGE_FILE_NAME)
                time.sleep(0.2)
                open(os.path.join(B.mnt, "nd", "fromB"), "w").close()
                say(f"file created via mount B: a notified={w.response_event.wait(2)}")
                open(os.path.join(A.mnt, "nd", "fromA"), "w").close()
                say(f"file created via mount A: a notified={w.response_event.wait(2)}")
                cn.close()

                scenario("F5 file identity: Samba keys locking.tdb and brlock.tdb by (st_dev, st_ino)")
                sa, sb = os.stat(os.path.join(A.mnt, "o")), os.stat(os.path.join(B.mnt, "o"))
                say(f"mount A: dev={sa.st_dev:#x} ino={sa.st_ino}; mount B: dev={sb.st_dev:#x} ino={sb.st_ino}")
            scenario(f"F6 byte-range locks across nodes, passfs {'forwards locks to the backing file' if locks else 'leaves locks to the kernel (local per mount)'}")
            ha, hb = a.open("k"), b.open("k")
            ha.write(b"x" * 200)
            ha.lock(0, 100)
            say(f"a exclusive lock [0, 100); b lock [50, 60) via the other node: {attempt(hb.lock, 50, 10)[0]}")
            say(f"local F_OFD_GETLK on mount B: {getlk(os.path.join(B.mnt, 'k'), 0, 100)}")
            ha.close(), hb.close()
            a.close(), b.close()
        finally:
            ga.stop(), gb.stop()
            A.stop(), B.stop()


if __name__ == "__main__":
    print(f"# kernel {os.uname().release}, Samba {os.popen('smbd --version').read().split()[1]}, "
          f"{time.strftime('%Y-%m-%d')}")
    os.makedirs(W, exist_ok=True)
    for p in PARTS:
        {"L": part_L, "G": part_G, "F": part_F}[p]()
