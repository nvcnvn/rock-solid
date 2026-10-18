//! Microbenchmarks of the pieces of replicate-rust's write path, each the same operation as in
//! micro-go: syscalls through std and libc, epoll_wait, Instant::now, frame parsing, header
//! encoding, CRC32C, the 4 KiB copies, the map of pending requests, and indexed loops. The parsing, header
//! and ring code is copied from replicate-rust.
//!
//! Usage: micro-rust [filter]   (runs the benchmarks whose name contains filter)

use std::collections::HashMap;
use std::ffi::c_void;
use std::hint::black_box;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

const HDR: usize = 24;
const PAYLOAD: usize = 4096;
const KIND_REPL: u8 = 2;
const KIND_ACK: u8 = 3;
const REPS: usize = 7;
const EPOLLIN: u32 = 1;
const EPOLL_CTL_ADD: i32 = 1;

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
    fn getppid() -> i32;
    fn read(fd: i32, buf: *mut c_void, n: usize) -> isize;
    fn write(fd: i32, buf: *const c_void, n: usize) -> isize;
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
    fn store(&mut self, p: &[u8]) {
        if self.off + p.len() > self.mem.len() {
            self.off = 0;
        }
        self.mem[self.off..self.off + p.len()].copy_from_slice(p);
        self.off += p.len();
    }
}

#[derive(Default)]
struct Conn {
    rbuf: Vec<u8>,
    rlen: usize,
    wbuf: Vec<u8>,
}

#[expect(
    dead_code,
    reason = "measures building frames as replicate-rust does; black_box keeps the stores"
)]
struct Frame {
    kind: u8,
    id: u64,
    crc: u32,
    start: usize,
    len: usize,
}

#[inline(never)]
fn parse(c: &Conn, frames: &mut Vec<Frame>) {
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

fn fill(frames: usize, size: usize) -> Conn {
    let mut b = Vec::new();
    for i in 0..frames {
        header(&mut b, KIND_REPL, size, i as u64, 0);
        b.resize(b.len() + size, 0);
    }
    Conn {
        rlen: b.len(),
        rbuf: b,
        wbuf: Vec::new(),
    }
}

fn tcp_pair() -> (TcpStream, TcpStream) {
    let ln = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = TcpStream::connect(ln.local_addr().unwrap()).unwrap();
    let (b, _) = ln.accept().unwrap();
    for s in [&a, &b] {
        s.set_nonblocking(true).unwrap();
        s.set_nodelay(true).unwrap();
    }
    (a, b)
}

type Run = Box<dyn FnMut(usize)>;

fn write_read_std(mut a: TcpStream, mut b: TcpStream, size: usize) -> Run {
    let msg = vec![0u8; size];
    let mut buf = vec![0u8; 256 << 10];
    Box::new(move |n| {
        for _ in 0..n {
            let w = a.write(&msg).unwrap();
            let mut got = 0;
            while got < w {
                match b.read(&mut buf) {
                    Ok(r) => got += r,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {}
                    Err(e) => panic!("{e}"),
                }
            }
        }
    })
}

fn write_read_libc(a: TcpStream, b: TcpStream, size: usize) -> Run {
    let msg = vec![0u8; size];
    let mut buf = vec![0u8; 256 << 10];
    Box::new(move |n| {
        for _ in 0..n {
            // SAFETY: msg and buf are valid for their lengths.
            let w = unsafe { write(a.as_raw_fd(), msg.as_ptr().cast(), msg.len()) };
            assert!(w > 0);
            let mut got = 0;
            while got < w {
                // SAFETY: as above.
                let r = unsafe { read(b.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                if r > 0 {
                    got += r;
                }
            }
        }
    })
}

fn epoll_bench(ep: i32, want: i32) -> Run {
    let mut evs = [EpollEvent::default(); 64];
    Box::new(move |n| {
        for _ in 0..n {
            // SAFETY: evs is valid for 64 events.
            let got = unsafe { epoll_wait(ep, evs.as_mut_ptr(), 64, 0) };
            assert_eq!(got, want);
        }
    })
}

#[derive(Clone, Copy)]
struct Pend {
    tok: usize,
    cid: u64,
    acks: u8,
}

fn map_cycle<S: std::hash::BuildHasher + Default + 'static>() -> Run {
    let mut pending: HashMap<u64, Pend, S> = HashMap::default();
    let mut next = 0u64;
    while next < 16 {
        next += 1;
        pending.insert(
            next,
            Pend {
                tok: 2,
                cid: next,
                acks: 0,
            },
        );
    }
    Box::new(move |n| {
        let mut sink = 0;
        for _ in 0..n {
            next += 1;
            pending.insert(
                next,
                Pend {
                    tok: 2,
                    cid: next,
                    acks: 0,
                },
            );
            let id = next - 16;
            for _ in 0..2 {
                let e = pending.get_mut(&id).expect("pending");
                e.acks += 1;
                if e.acks == 2 {
                    let p = pending.remove(&id).unwrap();
                    sink += p.cid + p.tok as u64;
                }
            }
        }
        black_box(sink);
    })
}

fn benches() -> Vec<(String, usize, Run)> {
    let mut bs: Vec<(String, usize, Run)> = Vec::new();
    let mut add = |name: &str, iters: usize, run: Run| bs.push((name.to_string(), iters, run));

    add(
        "getppid_libc",
        2_000_000,
        Box::new(|n| {
            for _ in 0..n {
                // SAFETY: plain syscall.
                black_box(unsafe { getppid() });
            }
        }),
    );

    let (ea, eb) = tcp_pair();
    let fa = ea.as_raw_fd();
    {
        let mut s = ea.try_clone().unwrap();
        let mut buf = vec![0u8; 256 << 10];
        add(
            "read_eagain_std",
            1_000_000,
            Box::new(move |n| {
                for _ in 0..n {
                    let e = s.read(&mut buf).unwrap_err();
                    assert_eq!(e.kind(), ErrorKind::WouldBlock);
                }
            }),
        );
    }
    {
        let s = ea.try_clone().unwrap();
        let mut buf = vec![0u8; 256 << 10];
        add(
            "read_eagain_libc",
            1_000_000,
            Box::new(move |n| {
                for _ in 0..n {
                    // SAFETY: buf is valid for its length.
                    let r = unsafe { read(s.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
                    assert_eq!(r, -1);
                    assert_eq!(
                        std::io::Error::last_os_error().kind(),
                        ErrorKind::WouldBlock
                    );
                }
            }),
        );
    }
    for size in [HDR, HDR + PAYLOAD] {
        add(
            &format!("write_read_{size}_std"),
            200_000,
            write_read_std(ea.try_clone().unwrap(), eb.try_clone().unwrap(), size),
        );
        add(
            &format!("write_read_{size}_libc"),
            200_000,
            write_read_libc(ea.try_clone().unwrap(), eb.try_clone().unwrap(), size),
        );
    }

    // SAFETY: plain syscalls; ev is valid for the call.
    let ep = unsafe {
        let ep = epoll_create1(0);
        let mut ev = EpollEvent {
            events: EPOLLIN,
            data: 0,
        };
        assert_eq!(epoll_ctl(ep, EPOLL_CTL_ADD, fa, &mut ev), 0);
        ep
    };
    add("epoll_wait0_empty_libc", 1_000_000, epoll_bench(ep, 0));
    {
        let mut inner = epoll_bench(ep, 1);
        let mut a = ea.try_clone().unwrap();
        let mut b = eb.try_clone().unwrap();
        let mut buf = vec![0u8; 64];
        add(
            "epoll_wait0_ready_libc",
            1_000_000,
            Box::new(move |n| {
                b.write_all(&[1]).unwrap();
                std::thread::sleep(Duration::from_millis(1));
                inner(n);
                while a.read(&mut buf).is_ok() {}
            }),
        );
    }

    add(
        "clock_now",
        5_000_000,
        Box::new(|n| {
            for _ in 0..n {
                black_box(Instant::now());
            }
        }),
    );

    for (name, count, size, iters) in [
        ("parse_repl16", 16, PAYLOAD, 2_000_000),
        ("parse_ack64", 64, 0, 500_000),
    ] {
        let c = fill(count, size);
        let mut frames = Vec::with_capacity(64);
        add(
            name,
            iters,
            Box::new(move |n| {
                for _ in 0..n {
                    frames.clear();
                    parse(black_box(&c), &mut frames);
                    black_box(&frames);
                }
            }),
        );
    }
    {
        let mut c = Conn::default();
        add(
            "header16",
            1_000_000,
            Box::new(move |n| {
                for i in 0..n {
                    c.wbuf.clear();
                    for j in 0..16 {
                        header(&mut c.wbuf, KIND_ACK, 0, (i + j) as u64, 0);
                    }
                    black_box(&c.wbuf);
                }
            }),
        );
    }
    let data = vec![0u8; PAYLOAD];
    {
        let mut c = Conn::default();
        let data = data.clone();
        add(
            "append_frame_4k",
            2_000_000,
            Box::new(move |n| {
                for i in 0..n {
                    c.wbuf.clear();
                    header(&mut c.wbuf, KIND_REPL, PAYLOAD, i as u64, 7);
                    c.wbuf.extend_from_slice(black_box(&data));
                    black_box(&c.wbuf);
                }
            }),
        );
    }
    {
        let mut data = data.clone();
        add(
            "crc32c_4k",
            1_000_000,
            Box::new(move |n| {
                for i in 0..n {
                    data[0] = i as u8;
                    black_box(crc32c::crc32c(&data));
                }
            }),
        );
    }
    for size in [64 << 10, 64 << 20] {
        let mut rg = Ring {
            mem: vec![1u8; size],
            off: 0,
        };
        let data = data.clone();
        add(
            &format!("ring_store_{}k", size >> 10),
            1_000_000,
            Box::new(move |n| {
                for _ in 0..n {
                    rg.store(black_box(&data));
                }
                black_box(&rg.mem);
            }),
        );
    }

    add(
        "map_cycle_sip",
        5_000_000,
        map_cycle::<std::hash::RandomState>(),
    );
    add(
        "map_cycle_fx",
        5_000_000,
        map_cycle::<rustc_hash::FxBuildHasher>(),
    );

    let bytes: Vec<u8> = (0..PAYLOAD).map(|i| (i * 7) as u8).collect();
    {
        let bytes = bytes.clone();
        add(
            "sum_bytes_4k",
            500_000,
            Box::new(move |n| {
                for _ in 0..n {
                    let b = black_box(&bytes);
                    let mut s = 0u64;
                    #[expect(clippy::needless_range_loop, reason = "measures an indexed loop")]
                    for i in 0..b.len() {
                        s += u64::from(b[i]);
                    }
                    black_box(s);
                }
            }),
        );
    }
    let idx: Vec<u32> = (0..1024u64)
        .map(|i| ((i * 2_654_435_761) % PAYLOAD as u64) as u32)
        .collect();
    add(
        "gather_1k",
        500_000,
        Box::new(move |n| {
            for _ in 0..n {
                let b = black_box(&bytes);
                let mut s = 0u64;
                for &j in black_box(&idx) {
                    s += u64::from(b[j as usize]);
                }
                black_box(s);
            }
        }),
    );
    bs
}

fn main() {
    let filter = std::env::args().nth(1).unwrap_or_default();
    for (name, iters, mut run) in benches() {
        if !name.contains(&filter) {
            continue;
        }
        run(iters / 10);
        let mut ns: Vec<f64> = (0..REPS)
            .map(|_| {
                let t = Instant::now();
                run(iters);
                t.elapsed().as_nanos() as f64 / iters as f64
            })
            .collect();
        ns.sort_by(f64::total_cmp);
        println!(
            "BENCH lang=rust name={name} ns_per_op={:.1} min={:.1} max={:.1}",
            ns[REPS / 2],
            ns[0],
            ns[REPS - 1]
        );
    }
}
