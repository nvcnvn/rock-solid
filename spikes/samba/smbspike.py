"""Helpers for the Samba spike: unprivileged smbd gateways, an SMB2/3 client that sees lease breaks,
and passfs FUSE mounts. Needs env.sh sourced (sysroot paths, PYTHONPATH for smbprotocol)."""
import os, queue, signal, socket, struct, subprocess, threading, time, uuid
from collections import OrderedDict

from smbprotocol.connection import Connection, Dialects
from smbprotocol.exceptions import SMBResponseException
from smbprotocol.header import Commands
from smbprotocol.open import (CreateDisposition, CreateOptions, FileAttributes, FilePipePrinterAccessMask,
                              DirectoryAccessMask, ImpersonationLevel, LockFlags, Open, RequestedOplockLevel,
                              ShareAccess, SMB2LockElement)
from smbprotocol.create_contexts import (CreateContextName, SMB2CreateContextRequest, SMB2CreateRequestLeaseV2)
from smbprotocol.session import Session
from smbprotocol.structure import BytesField, IntField, Structure
from smbprotocol.tree import TreeConnect

S = os.path.dirname(os.path.abspath(__file__))
W = os.path.join(S, "work")
USER, PASS = os.environ.get("USER", "spike"), "pw"
NT = {0xC0000043: "SHARING_VIOLATION", 0xC0000054: "FILE_LOCK_CONFLICT", 0xC0000055: "LOCK_NOT_GRANTED",
      0xC0000034: "OBJECT_NAME_NOT_FOUND", 0xC0000022: "ACCESS_DENIED", 0xC00000BB: "NOT_SUPPORTED",
      0xC0000035: "OBJECT_NAME_COLLISION", 0xC0000120: "CANCELLED"}


def status_name(e):
    return NT.get(e.status, hex(e.status)) if isinstance(e, SMBResponseException) else repr(e)


# ---------------------------------------------------------------- gateways

def listening(port):
    with open("/proc/net/tcp") as f:
        return any(l.split()[1].endswith(":%04X" % port) and l.split()[3] == "0A" for l in f.readlines()[1:])


class Gateway:
    """One smbd with its own state directory (so its own locking.tdb, brlock.tdb): one 'node'."""

    def __init__(self, name, port, path, share=None, glob=None):
        self.name, self.port, self.path = name, port, path
        self.dir = os.path.join(W, "gw-" + name)
        st = os.path.join(self.dir, "st")
        for d in ("lock", "state", "cache", "private", "pid", "ncalrpc", "binddns"):
            os.makedirs(os.path.join(st, d), exist_ok=True)
        g = OrderedDict([
            ("netbios name", "GW" + name.upper()), ("workgroup", "SPIKE"), ("server role", "standalone server"),
            ("smb ports", str(port)), ("interfaces", "127.0.0.1"), ("bind interfaces only", "yes"),
            ("lock directory", st + "/lock"), ("state directory", st + "/state"), ("cache directory", st + "/cache"),
            ("private dir", st + "/private"), ("pid directory", st + "/pid"), ("ncalrpc dir", st + "/ncalrpc"),
            ("binddns dir", st + "/binddns"), ("passdb backend", f"tdbsam:{st}/private/passdb.tdb"),
            ("load printers", "no"), ("printcap name", "/dev/null"), ("disable spoolss", "yes"),
            ("server min protocol", "SMB2_10"), ("log level", "1"), ("log file", self.dir + "/log.smbd"),
        ])
        g.update(glob or {})
        sh = OrderedDict([("path", path), ("read only", "no")])
        sh.update(share or {})
        self.conf = os.path.join(self.dir, "smb.conf")
        with open(self.conf, "w") as f:
            f.write("[global]\n" + "".join(f"{k} = {v}\n" for k, v in g.items()))
            f.write("[s]\n" + "".join(f"{k} = {v}\n" for k, v in sh.items()))
        pw, gr = os.path.join(self.dir, "passwd"), os.path.join(self.dir, "group")
        with open(pw, "w") as f:
            f.write(f"{USER}:x:{os.getuid()}:{os.getgid()}:spike:{os.environ['HOME']}:/bin/false\n"
                    "nobody:x:65534:65534::/:/bin/false\n")
        with open(gr, "w") as f:
            f.write(f"{USER}:x:{os.getgid()}:\nnogroup:x:65534:\n")
        self.env = dict(os.environ, UID_WRAPPER="1", UID_WRAPPER_ROOT="1", NSS_WRAPPER_PASSWD=pw,
                        NSS_WRAPPER_GROUP=gr, LD_PRELOAD=os.environ["SPIKE_PRELOAD"])
        subprocess.run(["pdbedit", "-s", self.conf, "-a", "-u", USER, "-t"], input=f"{PASS}\n{PASS}\n",
                       env=self.env, text=True, capture_output=True, check=True)
        # Own session: smbd signals its whole process group when it exits. stdin is a pipe we hold open:
        # in --foreground mode smbd exits on stdin EOF, so it also dies with the harness.
        self.p = subprocess.Popen(["smbd", "-s", self.conf, "--foreground", "--no-process-group"], env=self.env,
                                  stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                  start_new_session=True)
        for _ in range(200):
            if listening(port):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError(f"smbd {name} did not start; see {self.dir}/log.smbd")

    def tool(self, *args):
        """Run a Samba tool against this gateway's config and state (smbstatus, tdbdump...)."""
        return subprocess.run(list(args), env=self.env, text=True, capture_output=True).stdout

    def stop(self):
        os.killpg(self.p.pid, signal.SIGTERM)
        try:
            self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(self.p.pid, signal.SIGKILL)
            self.p.wait()


# ---------------------------------------------------------------- client

class _LeaseBreakAck(Structure):
    COMMAND = Commands.SMB2_OPLOCK_BREAK  # lease break ack shares the command code, told apart by size 36

    def __init__(self):
        self.fields = OrderedDict([
            ("structure_size", IntField(size=2, default=36)), ("reserved", IntField(size=2)),
            ("flags", IntField(size=4)), ("lease_key", BytesField(size=16)), ("lease_state", IntField(size=4)),
            ("lease_duration", IntField(size=8))])
        super().__init__()


class _OplockBreakAck(Structure):
    COMMAND = Commands.SMB2_OPLOCK_BREAK

    def __init__(self):
        self.fields = OrderedDict([
            ("structure_size", IntField(size=2, default=24)), ("oplock_level", IntField(size=1)),
            ("reserved", IntField(size=1)), ("reserved2", IntField(size=4)), ("file_id", BytesField(size=16))])
        super().__init__()


class _Reserved:
    def get_value(self):
        return 0


class _BreakSink:
    """Stands in for the pending request smbprotocol expects for message id 0xFFFF...: server-initiated breaks."""

    def __init__(self, client):
        self.client, self.session_id, self.async_id = client, None, None
        self.response_event, self.response_event_lock = threading.Event(), threading.Lock()
        self.message = {"reserved": _Reserved()}

    @property
    def response(self):
        return None

    @response.setter
    def response(self, header):
        self.client._on_break(header["data"].get_value())


class _Requests(dict):
    def __init__(self, sink):
        super().__init__()
        self.sink = sink

    def __getitem__(self, k):
        return self.sink if k == 0xFFFFFFFFFFFFFFFF else super().__getitem__(k)


LS = {"R": 1, "H": 2, "W": 4}


def ls_str(v):
    return "".join(c for c in "RWH" if v & LS[c]) or "none"


def ls_val(s):
    return sum(LS[c] for c in (s or "") if c in LS)


class Client:
    """One SMB client machine. ack: 'auto' acks lease/oplock breaks at once, 'never' leaves them unacked."""

    def __init__(self, port, name, ack="auto", dialect=Dialects.SMB_3_1_1):
        self.name, self.ack = name, ack
        self.breaks = []
        self.cv = threading.Condition()
        self.acks = queue.Queue()
        self.conn = Connection(uuid.uuid4(), "127.0.0.1", port)
        self.conn.outstanding_requests = _Requests(_BreakSink(self))
        self.conn.connect(dialect)
        self.sess = Session(self.conn, USER, PASS, require_encryption=False)
        self.sess.connect()
        self.tree = TreeConnect(self.sess, rf"\\127.0.0.1\s")
        self.tree.connect()
        threading.Thread(target=self._acker, daemon=True).start()

    def _on_break(self, body):  # runs on smbprotocol's receive thread: must not send from here
        t = time.monotonic()
        size = struct.unpack_from("<H", body)[0]
        if size == 44:
            _, flags, key, cur, new, reason = struct.unpack_from("<HxxI16sIII", body)
            ev = dict(kind="lease", t=t, key=key, cur=ls_str(cur), new=ls_str(new), new_v=new,
                      ack_required=bool(flags & 1))
        else:
            lvl = body[2]
            ev = dict(kind="oplock", t=t, file_id=body[8:24], new={0: "none", 1: "II"}.get(lvl, hex(lvl)), lvl=lvl,
                      ack_required=True)
        with self.cv:
            self.breaks.append(ev)
            self.cv.notify_all()
        if self.ack == "auto" and ev["ack_required"]:
            self.acks.put(ev)

    def _acker(self):
        while True:
            ev = self.acks.get()
            try:
                self.ack_break(ev)
            except Exception as e:  # handle may already be gone
                ev["ack_err"] = repr(e)

    def ack_break(self, ev):
        if ev["kind"] == "lease":
            m = _LeaseBreakAck()
            m["lease_key"], m["lease_state"] = ev["key"], ev["new_v"]
        else:
            m = _OplockBreakAck()
            m["file_id"], m["oplock_level"] = ev["file_id"], ev["lvl"]
        req = self.conn.send(m, self.sess.session_id, self.tree.tree_connect_id)
        self.conn.receive(req)
        ev["acked"] = time.monotonic()

    def wait_breaks(self, n=1, timeout=2.0):
        end = time.monotonic() + timeout
        with self.cv:
            while len(self.breaks) < n and time.monotonic() < end:
                self.cv.wait(end - time.monotonic())
            return list(self.breaks)

    def take_breaks(self, settle=0.3):
        time.sleep(settle)
        with self.cv:
            b, self.breaks = self.breaks, []
        return b

    def open(self, path, access="rw", share="rwd", lease=None, oplock=None, disp="open_if", lease_key=None,
             directory=False):
        return Handle(self, path, access, share, lease, oplock, disp, lease_key, directory)

    def close(self):
        try:
            self.conn.disconnect(True)
        except Exception:
            pass


class Handle:
    def __init__(self, c, path, access, share, lease, oplock, disp, lease_key, directory):
        self.c, self.path = c, path
        self.o = Open(c.tree, path)
        A = FilePipePrinterAccessMask
        if directory:
            am = DirectoryAccessMask.FILE_LIST_DIRECTORY | DirectoryAccessMask.FILE_READ_ATTRIBUTES | \
                DirectoryAccessMask.SYNCHRONIZE
        else:
            am = A.FILE_READ_ATTRIBUTES | A.SYNCHRONIZE
            if "r" in access:
                am |= A.FILE_READ_DATA
            if "w" in access:
                am |= A.FILE_WRITE_DATA | A.FILE_WRITE_ATTRIBUTES
            if "d" in access:
                am |= A.DELETE
        sh = (ShareAccess.FILE_SHARE_READ if "r" in share else 0) | \
             (ShareAccess.FILE_SHARE_WRITE if "w" in share else 0) | \
             (ShareAccess.FILE_SHARE_DELETE if "d" in share else 0)
        d = {"open": CreateDisposition.FILE_OPEN, "open_if": CreateDisposition.FILE_OPEN_IF,
             "create": CreateDisposition.FILE_CREATE, "overwrite_if": CreateDisposition.FILE_OVERWRITE_IF}[disp]
        ctx, lvl = [], RequestedOplockLevel.SMB2_OPLOCK_LEVEL_NONE
        self.lease_key = lease_key or os.urandom(16)
        if lease is not None:
            lr = SMB2CreateRequestLeaseV2()
            lr["lease_key"], lr["lease_state"] = self.lease_key, ls_val(lease)
            lr["parent_lease_key"], lr["epoch"] = b"\0" * 16, b"\0\0"
            cc = SMB2CreateContextRequest()
            cc["buffer_name"], cc["buffer_data"] = CreateContextName.SMB2_CREATE_REQUEST_LEASE_V2, lr
            ctx.append(cc)
            lvl = RequestedOplockLevel.SMB2_OPLOCK_LEVEL_LEASE
        elif oplock:
            lvl = {"batch": RequestedOplockLevel.SMB2_OPLOCK_LEVEL_BATCH,
                   "excl": RequestedOplockLevel.SMB2_OPLOCK_LEVEL_EXCLUSIVE,
                   "II": RequestedOplockLevel.SMB2_OPLOCK_LEVEL_II}[oplock]
        opts = CreateOptions.FILE_DIRECTORY_FILE if directory else CreateOptions.FILE_NON_DIRECTORY_FILE
        t = time.monotonic()
        resp = self.o.create(ImpersonationLevel.Impersonation, am, FileAttributes.FILE_ATTRIBUTE_NORMAL if not directory
                             else FileAttributes.FILE_ATTRIBUTE_DIRECTORY, sh, d, opts, create_contexts=ctx or None,
                             oplock_level=lvl)
        self.open_ms = (time.monotonic() - t) * 1e3
        self.granted = "none"
        lvl = self.o.oplock_level
        if lvl == RequestedOplockLevel.SMB2_OPLOCK_LEVEL_LEASE:
            for r in resp or []:
                if hasattr(r, "fields") and "lease_state" in r.fields:
                    self.granted = ls_str(r["lease_state"].get_value())
        else:
            self.granted = {0: "none", 1: "oplock II", 8: "oplock excl", 9: "oplock batch"}.get(lvl, hex(lvl))

    def write(self, data, off=0):
        return self.o.write(data, off)

    def read(self, off=0, n=4):
        return self.o.read(off, n)

    def _lk(self, off, n, flags, wait=False):
        e = SMB2LockElement()
        e["offset"], e["length"], e["flags"] = off, n, flags
        return self.o.lock([e], wait=wait)

    def lock(self, off, n, excl=True, wait=False):
        f = LockFlags.SMB2_LOCKFLAG_EXCLUSIVE_LOCK if excl else LockFlags.SMB2_LOCKFLAG_SHARED_LOCK
        if not wait:
            f |= LockFlags.SMB2_LOCKFLAG_FAIL_IMMEDIATELY
        return self._lk(off, n, f, wait)

    def unlock(self, off, n):
        return self._lk(off, n, LockFlags.SMB2_LOCKFLAG_UNLOCK)

    def close(self):
        self.o.close()


def attempt(fn, *a, **kw):
    """Run fn and return ('OK', result) or (NT status name, None)."""
    try:
        return "OK", fn(*a, **kw)
    except SMBResponseException as e:
        return status_name(e), None


# ---------------------------------------------------------------- FUSE mounts

class Mount:
    """A passfs mount of BACKING: one 'node' of our cluster filesystem."""

    def __init__(self, name, backing, *opts):
        self.mnt = os.path.join(W, "mnt-" + name)
        os.makedirs(self.mnt, exist_ok=True)
        subprocess.run(["fusermount3", "-u", "-q", self.mnt], stderr=subprocess.DEVNULL)
        self.p = subprocess.Popen([os.path.join(S, "passfs"), backing, self.mnt, *opts],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            if os.path.ismount(self.mnt):
                break
            time.sleep(0.05)
        self.stats()

    def stats(self, only=None):
        """Per-op counts and op sequence since the last call, filtered to paths under `only`."""
        with open(os.path.join(self.mnt, ".stats")) as f:
            txt = f.read()
        lines = txt.split("\n", 1)[1].splitlines()
        if only is not None:
            lines = [l for l in lines if len(l.split()) > 1 and l.split()[1].startswith(only)]
        counts = {}
        for l in lines:
            counts[l.split()[0]] = counts.get(l.split()[0], 0) + 1
        return counts, lines

    def stop(self):
        subprocess.run(["fusermount3", "-u", self.mnt])
        self.p.wait(timeout=5)
