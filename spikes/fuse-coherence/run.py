"""Two-mount FUSE coherence spike driver. Node A and node B mount the same backing file."""
import mmap, os, subprocess, sys, time

S = os.path.dirname(os.path.abspath(__file__))
W = os.path.join(S, "work")
BIN = os.path.join(S, "spikefs")


class Node:
    def __init__(self, name, *flags):
        self.name = name
        self.mnt = os.path.join(W, "mnt_" + name)
        os.makedirs(self.mnt, exist_ok=True)
        self.ctl, self.ack = os.path.join(W, name + ".ctl"), os.path.join(W, name + ".ack")
        for p in (self.ctl, self.ack):
            if not os.path.exists(p):
                os.mkfifo(p)
        self.p = subprocess.Popen([BIN, self.mnt, os.path.join(W, "backing"), self.ctl, self.ack,
                                   os.path.join(W, name + ".log"), *flags])
        for _ in range(100):
            if os.path.exists(self.path):
                break
            time.sleep(0.05)
        self.path  # noqa

    @property
    def path(self):
        return os.path.join(self.mnt, "f")

    def cmd(self, c):
        with open(self.ctl, "w") as f:
            f.write(c + "\n")
        with open(self.ack) as f:
            return f.readline().strip()

    def stop(self):
        subprocess.run(["fusermount3", "-u", self.mnt])
        self.p.wait(timeout=5)


def reset_backing():
    with open(os.path.join(W, "backing"), "wb") as f:
        f.write(b"A" * 65536)
    with open(os.path.join(W, "backing.ver"), "wb") as f:
        f.write(b"\0" * 8)


def rd(fd, n=4):
    return os.pread(fd, n, 0)


def check(label, got, want):
    ok = got == want
    print(f"  [{'OK ' if ok else 'STALE' if got != want else ''}] {label}: got={got!r} want={want!r}")
    return ok


def scenario(title):
    print(f"\n=== {title}")


def main():
    os.makedirs(W, exist_ok=True)
    reset_backing()
    flagsA = sys.argv[1:]  # unused
    A = Node("A")
    B = Node("B")
    try:
        a = os.open(A.path, os.O_RDWR)

        scenario("T1 baseline: B cached handle, A writes, no invalidation")
        b1 = os.open(B.path, os.O_RDWR)
        check("B first read", rd(b1), b"AAAA")
        os.pwrite(a, b"BBBB", 0)
        check("B read after A write", rd(b1), b"BBBB")

        scenario("T2 notify_inval_inode on B, same open handle")
        print("  ", B.cmd("inval"))
        check("B read after inval", rd(b1), b"BBBB")

        scenario("T3 one-shot recall is not enough: B re-caches, A writes again")
        check("B read (repopulates cache)", rd(b1), b"BBBB")
        os.pwrite(a, b"CCCC", 0)
        check("B read via old handle", rd(b1), b"CCCC")

        scenario("T4 switch new opens to direct_io; old cached handle still open")
        print("  ", B.cmd("direct 1"))
        b2 = os.open(B.path, os.O_RDWR)
        os.pwrite(a, b"DDDD", 0)
        check("B read via new direct handle", rd(b2), b"DDDD")
        check("B read via old cached handle", rd(b1), b"DDDD")
        os.pwrite(b2, b"EEEE", 0)
        check("B old handle after B's own direct write", rd(b1), b"EEEE")
        os.close(b2)
        print("  ", B.cmd("direct 0"))

        scenario("T5 auto_inval_data + attr_ttl=0: per-read revalidation of old cached handle")
        print("  ", B.cmd("ttl 0"), B.cmd("inval"))
        rd(b1)
        print("  ", B.cmd("zero"))
        os.pwrite(a, b"FFFF", 0)
        check("B read via old cached handle", rd(b1), b"FFFF")
        check("B read again, no remote write", rd(b1), b"FFFF")
        print("   B daemon:", B.cmd("stats"), "(for 2 reads)")

        scenario("T5b same, but via mmap of the old cached handle")
        m = mmap.mmap(b1, 4096, mmap.MAP_SHARED, mmap.PROT_READ)
        check("B mmap read", m[:4], b"FFFF")
        os.pwrite(a, b"GGGG", 0)
        check("B mmap read after A write", m[:4], b"GGGG")
        print("  ", B.cmd("inval"))
        check("B mmap read after inval", m[:4], b"GGGG")
        m.close()
        print("  ", B.cmd("ttl 3600"))

        scenario("T5c churned change counter: old cached handle, A writes with no version bump visible to B")
        print("  ", B.cmd("ttl 0"), B.cmd("churn 1"), B.cmd("inval"))
        rd(b1)
        with open(os.path.join(W, "backing"), "r+b") as f:  # write behind B's back, version untouched
            f.write(b"QQQQ")
        check("B read via old cached handle", rd(b1), b"QQQQ")
        print("  ", B.cmd("churn 0"), B.cmd("ttl 3600"), B.cmd("inval"))

        scenario("T6 close-to-open: new open without keep_cache drops stale pages")
        rd(b1)
        os.pwrite(a, b"HHHH", 0)
        print("  ", B.cmd("keep 0"))
        b3 = os.open(B.path, os.O_RDONLY)
        check("B read via new handle (no keep_cache)", rd(b3), b"HHHH")
        os.close(b3)
        print("  ", B.cmd("keep 1"))
        os.close(b1)

        scenario("T7 latency: 20000 x 4K pread on B, by mode")
        for label, setup in [("cached (keep_cache)", ["direct 0", "ttl 3600"]),
                             ("cached + per-read revalidate (ttl 0)", ["direct 0", "ttl 0"]),
                             ("cached + ttl 0 + churned change counter", ["direct 0", "ttl 0", "churn 1"]),
                             ("direct_io", ["churn 0", "direct 1", "ttl 3600"])]:
            for c in setup:
                B.cmd(c)
            B.cmd("inval_attr")
            fd = os.open(B.path, os.O_RDONLY)
            rd(fd, 4096)
            B.cmd("zero")
            t = time.perf_counter()
            for i in range(20000):
                os.pread(fd, 4096, (i % 16) * 4096)
            dt = (time.perf_counter() - t) / 20000 * 1e6
            print(f"   {label:40s} {dt:7.2f} us/read   daemon: {B.cmd('stats')}")
            os.close(fd)
        B.cmd("direct 0"); B.cmd("ttl 3600")

        scenario("T8 cost of notify_inval_inode (clean pages, 64K cached)")
        fd = os.open(B.path, os.O_RDONLY)
        for _ in range(3):
            os.pread(fd, 65536, 0)
            print("  ", B.cmd("inval"))
        os.close(fd)
        os.close(a)
    finally:
        B.stop()
        A.stop()

    scenario("T9 writeback_cache mount on B (mount-wide FUSE_WRITEBACK_CACHE)")
    reset_backing()
    A = Node("A")
    B = Node("B", "wb")
    try:
        a = os.open(A.path, os.O_RDWR)
        b = os.open(B.path, os.O_RDWR)
        rd(b)
        B.cmd("zero")
        os.pwrite(b, b"WWWW", 0)
        print("   B daemon after B write:", B.cmd("stats"))
        check("A sees B's write", rd(a), b"WWWW")
        print("  ", B.cmd("inval"))
        with open(os.path.join(W, "backing"), "rb") as f:
            check("backing store after B inval (launder)", f.read(4), b"WWWW")
        B.cmd("ttl 0")
        B.cmd("inval")
        rd(b)
        os.pwrite(a, b"XXXX", 0)
        check("B (wb, autoinval, ttl 0) read after A write", rd(b), b"XXXX")
        os.close(a); os.close(b)
    finally:
        B.stop()
        A.stop()


if __name__ == "__main__":
    main()
