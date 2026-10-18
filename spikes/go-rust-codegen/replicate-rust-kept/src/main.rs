//! go-cpu-per-op/replicate-rust with its rings kept: the rings are never read, so LLVM deletes the
//! replicas' copies into them as dead stores. `black_box` on each ring after its loop makes the
//! stores observable, so every node copies each 4K write into its log as replicate-go3 does.
//! touch=1 writes every page of the rings before the measured window, so first-touch page faults
//! stay out of it.
//!
//! Usage: replicate-rust-kept conns=N secs=S [coalesce=1] [touch=1]
//!
//! With coalesce=0 every frame is written as soon as it is produced, one write per frame, as
//! replicate-go does; by default a loop pass writes each connection's frames together.

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

static COALESCE: AtomicBool = AtomicBool::new(true);
static TOUCH: AtomicBool = AtomicBool::new(true);
static READS: AtomicU64 = AtomicU64::new(0);
static WRITES: AtomicU64 = AtomicU64::new(0);
use std::thread;
use std::time::{Duration, Instant};

const HDR: usize = 24;
const PAYLOAD: usize = 4096;
const KIND_WRITE: u8 = 1;
const KIND_REPL: u8 = 2;
const KIND_ACK: u8 = 3;
const RING: usize = 64 << 20;
const EPOLLIN: u32 = 1;
const EPOLLOUT: u32 = 4;
const EPOLL_CTL_ADD: i32 = 1;
const EPOLL_CTL_MOD: i32 = 3;

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct EpollEvent {
    events: u32,
    data: u64,
}

unsafe extern "C" {
    fn epoll_create1(flags: i32) -> i32;
    fn epoll_ctl(epfd: i32, op: i32, fd: i32, ev: *mut EpollEvent) -> i32;
    fn epoll_wait(epfd: i32, evs: *mut EpollEvent, max: i32, timeout: i32) -> i32;
    fn getrusage(who: i32, ru: *mut [i64; 18]) -> i32;
}

fn cpu() -> Duration {
    let mut ru = [0i64; 18];
    // SAFETY: ru is large enough for struct rusage on x86_64 Linux.
    unsafe { getrusage(0, &mut ru) };
    Duration::from_micros((ru[0] * 1_000_000 + ru[1] + ru[2] * 1_000_000 + ru[3]) as u64)
}

fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Socket reads and writes the program has made, including those that would block.
fn io_calls() -> (u64, u64) {
    (READS.load(Relaxed), WRITES.load(Relaxed))
}

/// A peer that went away while shutting down, after the measured window.
fn closed(e: &std::io::Error) -> bool {
    matches!(e.kind(), ErrorKind::ConnectionReset | ErrorKind::BrokenPipe)
}

fn header(out: &mut Vec<u8>, kind: u8, len: usize, id: u64, crc: u32) {
    out.extend_from_slice(&[kind, 0, 0, 0]);
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&[0; 4]);
}

struct Ring {
    mem: Vec<u8>,
    off: usize,
}

impl Ring {
    fn new() -> Ring {
        Ring {
            mem: vec![u8::from(TOUCH.load(Relaxed)); RING],
            off: 0,
        }
    }
    fn store(&mut self, p: &[u8], crc: u32) {
        assert_eq!(crc32c(p), crc, "crc");
        if self.off + p.len() > RING {
            self.off = 0;
        }
        self.mem[self.off..self.off + p.len()].copy_from_slice(p);
        self.off += p.len();
    }
}

struct Conn {
    s: TcpStream,
    rbuf: Vec<u8>,
    rlen: usize,
    wbuf: Vec<u8>,
    wpos: usize,
    armed: bool,
}

struct Frame {
    kind: u8,
    id: u64,
    crc: u32,
    start: usize,
    len: usize,
}

struct Loop {
    ep: i32,
    conns: Vec<Conn>,
}

impl Loop {
    fn new() -> Loop {
        // SAFETY: plain syscall.
        Loop {
            ep: unsafe { epoll_create1(0) },
            conns: Vec::new(),
        }
    }

    fn add(&mut self, s: TcpStream) -> usize {
        s.set_nonblocking(true).unwrap();
        s.set_nodelay(true).unwrap();
        let tok = self.conns.len();
        let mut ev = EpollEvent {
            events: EPOLLIN,
            data: tok as u64,
        };
        // SAFETY: ev is valid for the call and the fd is open.
        assert_eq!(
            unsafe { epoll_ctl(self.ep, EPOLL_CTL_ADD, s.as_raw_fd(), &mut ev) },
            0
        );
        self.conns.push(Conn {
            s,
            rbuf: vec![0; 256 << 10],
            rlen: 0,
            wbuf: Vec::new(),
            wpos: 0,
            armed: false,
        });
        tok
    }

    fn arm(&mut self, tok: usize, out: bool) {
        let c = &mut self.conns[tok];
        if c.armed == out {
            return;
        }
        c.armed = out;
        let mut ev = EpollEvent {
            events: EPOLLIN | if out { EPOLLOUT } else { 0 },
            data: tok as u64,
        };
        // SAFETY: as in add.
        assert_eq!(
            unsafe { epoll_ctl(self.ep, EPOLL_CTL_MOD, c.s.as_raw_fd(), &mut ev) },
            0
        );
    }

    fn flush(&mut self, tok: usize) {
        let c = &mut self.conns[tok];
        while c.wpos < c.wbuf.len() {
            WRITES.fetch_add(1, Relaxed);
            match c.s.write(&c.wbuf[c.wpos..]) {
                Ok(n) => c.wpos += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if closed(&e) => {
                    c.wpos = c.wbuf.len();
                    break;
                }
                Err(e) => panic!("{e}"),
            }
        }
        let done = c.wpos == c.wbuf.len();
        if done {
            c.wbuf.clear();
            c.wpos = 0;
        }
        self.arm(tok, !done);
    }

    /// Reads what is available and returns the complete frames, leaving their bytes in rbuf.
    fn read(&mut self, tok: usize, frames: &mut Vec<Frame>) {
        let c = &mut self.conns[tok];
        loop {
            if c.rlen == c.rbuf.len() {
                c.rbuf.resize(c.rbuf.len() * 2, 0);
            }
            READS.fetch_add(1, Relaxed);
            match c.s.read(&mut c.rbuf[c.rlen..]) {
                Ok(0) => return,
                Ok(n) => c.rlen += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if closed(&e) => return,
                Err(e) => panic!("{e}"),
            }
        }
        let mut pos = 0;
        while c.rlen - pos >= HDR {
            let h = &c.rbuf[pos..pos + HDR];
            let len = u32::from_le_bytes(h[4..8].try_into().unwrap()) as usize;
            if c.rlen - pos < HDR + len {
                break;
            }
            frames.push(Frame {
                kind: h[0],
                id: u64::from_le_bytes(h[8..16].try_into().unwrap()),
                crc: u32::from_le_bytes(h[16..20].try_into().unwrap()),
                start: pos + HDR,
                len,
            });
            pos += HDR + len;
        }
    }

    /// Drops the bytes of the frames `read` returned.
    fn consume(&mut self, tok: usize, frames: &[Frame]) {
        let Some(last) = frames.last() else { return };
        let c = &mut self.conns[tok];
        let end = last.start + last.len;
        c.rbuf.copy_within(end..c.rlen, 0);
        c.rlen -= end;
    }

    fn run(&mut self, stop: &AtomicBool, mut on: impl FnMut(&mut Loop, usize, &[Frame])) {
        let mut evs = [EpollEvent::default(); 64];
        let mut frames = Vec::new();
        let mut dirty = Vec::new();
        while !stop.load(Relaxed) {
            // SAFETY: evs is valid for 64 events.
            let n = unsafe { epoll_wait(self.ep, evs.as_mut_ptr(), 64, 10) };
            for ev in &evs[..n.max(0) as usize] {
                let tok = ev.data as usize;
                if ev.events & EPOLLOUT != 0 {
                    dirty.push(tok);
                }
                if ev.events & EPOLLIN != 0 {
                    frames.clear();
                    self.read(tok, &mut frames);
                    if COALESCE.load(Relaxed) {
                        on(self, tok, &frames);
                    } else {
                        for f in &frames {
                            on(self, tok, std::slice::from_ref(f));
                            for t in 0..self.conns.len() {
                                if !self.conns[t].wbuf.is_empty() {
                                    self.flush(t);
                                }
                            }
                        }
                    }
                    self.consume(tok, &frames);
                }
            }
            dirty.extend((0..self.conns.len()).filter(|&t| !self.conns[t].wbuf.is_empty()));
            dirty.sort_unstable();
            dirty.dedup();
            for t in dirty.drain(..) {
                self.flush(t);
            }
        }
    }
}

fn pair(ln: &TcpListener) -> (TcpStream, TcpStream) {
    let a = TcpStream::connect(ln.local_addr().unwrap()).unwrap();
    let (b, _) = ln.accept().unwrap();
    (a, b)
}

fn replica(s: TcpStream, stop: Arc<AtomicBool>) {
    let mut lp = Loop::new();
    lp.add(s);
    let mut ring = Ring::new();
    lp.run(&stop, |lp, tok, frames| {
        let c = &mut lp.conns[tok];
        for f in frames {
            ring.store(&c.rbuf[f.start..f.start + f.len], f.crc);
            header(&mut c.wbuf, KIND_ACK, 0, f.id, 0);
        }
    });
    std::hint::black_box(&ring.mem);
}

fn primary(replicas: Vec<TcpStream>, clients: Vec<TcpStream>, stop: Arc<AtomicBool>) {
    let mut lp = Loop::new();
    for r in replicas {
        lp.add(r);
    }
    for c in clients {
        lp.add(c);
    }
    let mut ring = Ring::new();
    let mut pending: HashMap<u64, (usize, u64, u8)> = HashMap::new();
    let mut next = 0u64;
    lp.run(&stop, |lp, tok, frames| {
        for f in frames {
            if tok < 2 {
                let e = pending.get_mut(&f.id).expect("pending");
                e.2 += 1;
                if e.2 == 2 {
                    let (ct, cid, _) = pending.remove(&f.id).unwrap();
                    header(&mut lp.conns[ct].wbuf, KIND_ACK, 0, cid, 0);
                }
                continue;
            }
            assert_eq!(f.kind, KIND_WRITE);
            next += 1;
            let rbuf = std::mem::take(&mut lp.conns[tok].rbuf);
            let p = &rbuf[f.start..f.start + f.len];
            ring.store(p, f.crc);
            pending.insert(next, (tok, f.id, 0));
            for r in 0..2 {
                let w = &mut lp.conns[r].wbuf;
                header(w, KIND_REPL, p.len(), next, f.crc);
                w.extend_from_slice(p);
            }
            lp.conns[tok].rbuf = rbuf;
        }
    });
    std::hint::black_box(&ring.mem);
}

fn main() {
    assert!(std::arch::is_x86_feature_detected!("sse4.2"));
    let mut conns = 1;
    let mut secs = 10;
    for a in std::env::args().skip(1) {
        let (k, v) = a.split_once('=').unwrap();
        match k {
            "conns" => conns = v.parse().unwrap(),
            "secs" => secs = v.parse().unwrap(),
            "coalesce" => COALESCE.store(v == "1", Relaxed),
            "touch" => TOUCH.store(v == "1", Relaxed),
            _ => panic!("unknown {k}"),
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut to_replicas = Vec::new();
    let mut handles = Vec::new();
    for _ in 0..2 {
        let (a, b) = pair(&ln);
        to_replicas.push(a);
        let st = stop.clone();
        handles.push(thread::spawn(move || replica(b, st)));
    }
    let mut client_ends = Vec::new();
    let mut server_ends = Vec::new();
    for _ in 0..conns {
        let (a, b) = pair(&ln);
        client_ends.push(a);
        server_ends.push(b);
    }
    let st = stop.clone();
    handles.push(thread::spawn(move || primary(to_replicas, server_ends, st)));

    let data: Vec<u8> = (0..PAYLOAD).map(|i| (i * 7) as u8).collect();
    let crc = crc32c(&data);
    let mut lp = Loop::new();
    for c in client_ends {
        lp.add(c);
    }
    let mut sent = vec![Instant::now(); conns];
    let mut lat: Vec<u64> = Vec::with_capacity(1 << 24);
    let mut ops = 0u64;
    let send = |lp: &mut Loop, tok: usize, id: u64| {
        let w = &mut lp.conns[tok].wbuf;
        header(w, KIND_WRITE, PAYLOAD, id, crc);
        w.extend_from_slice(&data);
    };
    thread::sleep(Duration::from_millis(if TOUCH.load(Relaxed) {
        2000
    } else {
        200
    }));
    for t in 0..conns {
        send(&mut lp, t, 0);
        lp.flush(t);
    }
    let (c0, t0, io0) = (cpu(), Instant::now(), io_calls());
    let end = t0 + Duration::from_secs(secs);
    let client_stop = AtomicBool::new(false);
    lp.run(&client_stop, |lp, tok, frames| {
        for f in frames {
            let now = Instant::now();
            lat.push((now - sent[tok]).as_nanos() as u64);
            ops += 1;
            if now >= end {
                client_stop.store(true, Relaxed);
                continue;
            }
            sent[tok] = now;
            send(lp, tok, f.id + 1);
        }
    });
    let (wall, used, io1) = (t0.elapsed().as_secs_f64(), cpu() - c0, io_calls());
    stop.store(true, Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    lat.sort_unstable();
    let p = |q: f64| lat[((lat.len() as f64 * q) as usize).min(lat.len() - 1)] as f64 / 1000.0;
    let n = ops as f64;
    println!(
        "RUN lang=rust-kept conns={conns} coalesce={} touch={} | ops_per_s={:.0} cpu_us_per_op={:.2} cores={:.2} \
         reads_per_op={:.2} writes_per_op={:.2} lat_p50_us={:.1} \
         lat_p99_us={:.1} lat_p999_us={:.1} secs={wall:.1}",
        u8::from(COALESCE.load(Relaxed)),
        u8::from(TOUCH.load(Relaxed)),
        n / wall,
        used.as_micros() as f64 / n,
        used.as_secs_f64() / wall,
        (io1.0 - io0.0) as f64 / n,
        (io1.1 - io0.1) as f64 / n,
        p(0.5),
        p(0.99),
        p(0.999)
    );
}
