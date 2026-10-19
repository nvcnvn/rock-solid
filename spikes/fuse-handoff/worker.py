# I/O load for the handoff spike: N processes, each writes a 4 KiB block stamped with
# (worker, seq) to its own file through the mount, reads it back and checks it, until a stop
# file appears. Prints one JSON line: ops, errors (errno and time), latency p50/p99/max in µs.
import json, os, struct, sys, time
from multiprocessing import Process, Queue

def run(k, d, stop, q):
    path = os.path.join(d, f"f{k}")
    lat, errs, ops, maxat, curmax = [], [], 0, 0.0, -1.0
    fd = None
    while not os.path.exists(stop):
        t0 = time.monotonic()
        try:
            if fd is None:
                fd = os.open(path, os.O_RDWR)
            seq = ops
            buf = struct.pack("<QQ", k, seq) * 256
            off = (seq % 16) * 4096
            os.pwrite(fd, buf, off)
            got = os.pread(fd, 4096, off)
            if got != buf:
                errs.append(["CORRUPT", t0])
        except OSError as e:
            errs.append([e.errno, os.strerror(e.errno), t0])
            fd = None
            time.sleep(0.01)
        dt = (time.monotonic() - t0) * 1e6
        if dt > curmax:
            curmax, maxat = dt, t0
        lat.append(dt)
        ops += 1
    lat.sort()
    q.put({"worker": k, "uid": os.getuid(), "ops": ops, "errors": errs[:10], "nerr": len(errs),
           "p50": lat[len(lat) // 2] if lat else 0, "p99": lat[int(len(lat) * 0.99)] if lat else 0,
           "max": lat[-1] if lat else 0, "max_at": maxat})

if __name__ == "__main__":
    d, n, stop = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    q = Queue()
    ps = [Process(target=run, args=(k, d, stop, q)) for k in range(n)]
    for p in ps:
        p.start()
    res = [q.get() for _ in ps]
    for p in ps:
        p.join()
    print(json.dumps(sorted(res, key=lambda r: r["worker"])), flush=True)
