//! Parallel cleaner spike: an in-memory per-disk log with a hot stream of user 4K overwrites and a
//! greedy segment cleaner, run inline on the writing thread or on worker threads.
//!
//! Usage: parallel-cleaner key=value ...   (see `Cfg::parse` for keys)
//!
//! Modes:
//!   inline  the owner thread cleans when free segments run low (today's single-threaded shape)
//!   owner   workers pick victims, check liveness, verify CRC32C and copy; they send the moves to the
//!           owner thread, which alone updates the index and frees the victim (thread-per-core shape)
//!   shared  workers do the same and update a shared atomic index themselves with compare-and-swap

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, MutexGuard};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

const BLOCK: usize = 4096;
const INVALID: u32 = u32::MAX;
const VICTIMS_PER_PASS: usize = 4;

const FREE: u8 = 0;
const OPEN: u8 = 1;
const SEALED: u8 = 2;
const CLEANING: u8 = 3;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    Inline,
    Owner,
    Shared,
}

struct Cfg {
    mode: Mode,
    workers: usize,
    u: f64,
    segs: u32,
    seg_blocks: u32,
    free_low: usize,
    reserve: usize,
    warm: f64,
    measure: f64,
    rate: f64,
    seed: u64,
    tag: String,
}

impl Cfg {
    fn parse(args: &[String]) -> Cfg {
        let mut kv: HashMap<String, String> = HashMap::new();
        for a in args {
            let (k, v) = a.split_once('=').unwrap_or_else(|| panic!("bad arg {a}"));
            kv.insert(k.to_string(), v.to_string());
        }
        let known = [
            "mode",
            "workers",
            "u",
            "segs",
            "seg_blocks",
            "free_low",
            "reserve",
            "warm",
            "measure",
            "rate",
            "seed",
            "tag",
        ];
        for k in kv.keys() {
            assert!(known.contains(&k.as_str()), "unknown key {k}");
        }
        let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
        let mode = match get("mode", "inline").as_str() {
            "inline" => Mode::Inline,
            "owner" => Mode::Owner,
            "shared" => Mode::Shared,
            m => panic!("mode {m}"),
        };
        let workers = if mode == Mode::Inline {
            0
        } else {
            get("workers", "1").parse().unwrap()
        };
        Cfg {
            mode,
            workers,
            u: get("u", "0.9").parse().unwrap(),
            segs: get("segs", "4096").parse().unwrap(),
            seg_blocks: get("seg_blocks", "64").parse().unwrap(),
            free_low: get("free_low", "8").parse().unwrap(),
            reserve: get("reserve", &(2 + workers).to_string()).parse().unwrap(),
            warm: get("warm", "4").parse().unwrap(),
            measure: get("measure", "2").parse().unwrap(),
            rate: get("rate", "0").parse().unwrap(),
            seed: get("seed", "1").parse().unwrap(),
            tag: get("tag", "-"),
        }
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        ((self.next() as u128 * n as u128) >> 64) as u64
    }
}

fn crc32c(data: &[u8]) -> u32 {
    use std::arch::x86_64::_mm_crc32_u64;
    let mut c: u64 = 0xFFFF_FFFF;
    for w in data.as_chunks::<8>().0 {
        let v = u64::from_le_bytes(*w);
        // SAFETY: main() checks for SSE4.2 before any thread starts.
        c = unsafe { _mm_crc32_u64(c, v) };
    }
    !(c as u32)
}

#[repr(C)]
struct Timespec {
    sec: i64,
    nsec: i64,
}

unsafe extern "C" {
    fn clock_gettime(clock: i32, ts: *mut Timespec) -> i32;
}

fn thread_cpu_ns() -> u64 {
    const CLOCK_THREAD_CPUTIME_ID: i32 = 3;
    let mut ts = Timespec { sec: 0, nsec: 0 };
    // SAFETY: ts is a valid out-pointer for the call.
    unsafe { clock_gettime(CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    ts.sec as u64 * 1_000_000_000 + ts.nsec as u64
}

/// The disk's bytes. Each block is written only by the thread that holds its segment open, and read
/// only while its segment is sealed or being cleaned, so no two threads touch a block at once.
struct Disk {
    ptr: *mut u8,
    _mem: Vec<u8>,
}

unsafe impl Sync for Disk {}

impl Disk {
    #[allow(
        clippy::mut_from_ref,
        reason = "segment ownership rules out aliasing; see the type's doc"
    )]
    fn block(&self, p: u32) -> &mut [u8] {
        // SAFETY: p is below the block count, and see the type's doc for aliasing.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(p as usize * BLOCK), BLOCK) }
    }
}

struct Batch {
    victim: u32,
    moves: Vec<(u32, u32, u32)>,
}

struct Log {
    cfg: Cfg,
    disk: Disk,
    crc: Vec<AtomicU32>,
    rev: Vec<AtomicU32>,
    index: Vec<AtomicU32>,
    live: Vec<AtomicU32>,
    state: Vec<AtomicU8>,
    pending: Vec<AtomicU32>,
    free: Mutex<Vec<u32>>,
    free_n: AtomicUsize,
    picker: Mutex<()>,
    stop: AtomicBool,
    measuring: AtomicBool,
    copies: AtomicU64,
    wasted: AtomicU64,
    victims: AtomicU64,
    worker_cpu: AtomicU64,
}

fn atomics(n: usize, v: u32) -> Vec<AtomicU32> {
    (0..n).map(|_| AtomicU32::new(v)).collect()
}

impl Log {
    fn new(cfg: Cfg, lbas: usize) -> Log {
        let blocks = cfg.segs as usize * cfg.seg_blocks as usize;
        let mut mem = vec![0u8; blocks * BLOCK];
        let ptr = mem.as_mut_ptr();
        let segs = cfg.segs as usize;
        Log {
            disk: Disk { ptr, _mem: mem },
            crc: atomics(blocks, 0),
            rev: atomics(blocks, INVALID),
            index: atomics(lbas, INVALID),
            live: atomics(segs, 0),
            state: (0..segs).map(|_| AtomicU8::new(FREE)).collect(),
            pending: atomics(segs, 0),
            free: Mutex::new((0..cfg.segs).rev().collect()),
            free_n: AtomicUsize::new(segs),
            picker: Mutex::new(()),
            stop: AtomicBool::new(false),
            measuring: AtomicBool::new(false),
            copies: AtomicU64::new(0),
            wasted: AtomicU64::new(0),
            victims: AtomicU64::new(0),
            worker_cpu: AtomicU64::new(0),
            cfg,
        }
    }

    fn seg(&self, p: u32) -> usize {
        (p / self.cfg.seg_blocks) as usize
    }

    fn lock_free(&self) -> MutexGuard<'_, Vec<u32>> {
        self.free.lock().unwrap()
    }

    fn take_free(&self, keep: usize) -> Option<u32> {
        let mut f = self.lock_free();
        if f.len() <= keep {
            return None;
        }
        let s = f.pop().unwrap();
        self.free_n.store(f.len(), Release);
        self.state[s as usize].store(OPEN, Release);
        Some(s)
    }

    fn release(&self, v: u32) {
        self.live[v as usize].store(0, Relaxed);
        self.state[v as usize].store(FREE, Release);
        let mut f = self.lock_free();
        f.push(v);
        self.free_n.store(f.len(), Release);
    }

    fn seal(&self, s: u32) {
        self.state[s as usize].store(SEALED, Release);
    }

    /// Greedy: the sealed segment with the fewest live blocks whose moves have all been applied.
    fn pick(&self) -> Option<u32> {
        let _g = self.picker.lock().unwrap();
        let mut best = None;
        let mut best_live = u32::MAX;
        for s in 0..self.cfg.segs as usize {
            if self.state[s].load(Acquire) != SEALED || self.pending[s].load(Acquire) != 0 {
                continue;
            }
            let l = self.live[s].load(Relaxed);
            if l < best_live {
                best_live = l;
                best = Some(s as u32);
            }
        }
        let v = best?;
        if best_live >= self.cfg.seg_blocks {
            return None;
        }
        self.state[v as usize].store(CLEANING, Release);
        Some(v)
    }
}

/// An open segment a thread appends to.
struct Cursor {
    seg: u32,
    next: u32,
}

impl Cursor {
    fn none() -> Cursor {
        Cursor {
            seg: INVALID,
            next: 0,
        }
    }
}

/// Copies one victim's live blocks to `cold`. Inline and shared modes update the index here and free
/// the victim; owner mode returns the moves for the owner thread to apply.
fn clean_victim(log: &Log, v: u32, cold: &mut Cursor, mode: Mode) -> Option<Batch> {
    let sb = log.cfg.seg_blocks;
    let mut moves = Vec::new();
    for i in 0..sb {
        let p = v * sb + i;
        let l = log.rev[p as usize].load(Relaxed);
        if l == INVALID || log.index[l as usize].load(Acquire) != p {
            continue;
        }
        let src = log.disk.block(p);
        let c = log.crc[p as usize].load(Relaxed);
        assert_eq!(crc32c(src), c, "CRC mismatch at block {p}");
        if cold.seg == INVALID || cold.next == sb {
            if cold.seg != INVALID {
                log.seal(cold.seg);
            }
            cold.seg = loop {
                if let Some(s) = log.take_free(0) {
                    break s;
                }
                thread::yield_now();
            };
            cold.next = 0;
        }
        let q = cold.seg * sb + cold.next;
        cold.next += 1;
        log.disk.block(q).copy_from_slice(src);
        log.crc[q as usize].store(c, Relaxed);
        log.rev[q as usize].store(l, Release);
        let qs = log.seg(q);
        match mode {
            Mode::Inline => {
                log.index[l as usize].store(q, Release);
                log.live[qs].fetch_add(1, Relaxed);
                log.live[v as usize].fetch_sub(1, Relaxed);
                log.copies.fetch_add(1, Relaxed);
            }
            Mode::Shared => {
                log.live[qs].fetch_add(1, Relaxed);
                if log.index[l as usize]
                    .compare_exchange(p, q, AcqRel, Acquire)
                    .is_ok()
                {
                    log.live[v as usize].fetch_sub(1, Relaxed);
                    log.copies.fetch_add(1, Relaxed);
                } else {
                    log.live[qs].fetch_sub(1, Relaxed);
                    log.wasted.fetch_add(1, Relaxed);
                }
            }
            Mode::Owner => {
                log.pending[qs].fetch_add(1, Release);
                moves.push((l, p, q));
            }
        }
    }
    log.victims.fetch_add(1, Relaxed);
    if mode == Mode::Owner {
        return Some(Batch { victim: v, moves });
    }
    log.release(v);
    None
}

fn apply(log: &Log, b: Batch) {
    for (l, p, q) in b.moves {
        if log.index[l as usize].load(Relaxed) == p {
            log.index[l as usize].store(q, Release);
            log.live[log.seg(q)].fetch_add(1, Relaxed);
            log.live[b.victim as usize].fetch_sub(1, Relaxed);
            log.copies.fetch_add(1, Relaxed);
        } else {
            log.wasted.fetch_add(1, Relaxed);
        }
        log.pending[log.seg(q)].fetch_sub(1, Release);
    }
    log.release(b.victim);
}

fn worker(log: &Log, tx: Sender<Batch>) {
    let mut cold = Cursor::none();
    let mut base = None;
    while !log.stop.load(Acquire) {
        if base.is_none() && log.measuring.load(Acquire) {
            base = Some(thread_cpu_ns());
        }
        if log.free_n.load(Acquire) < log.cfg.free_low
            && let Some(v) = log.pick()
        {
            if let Some(b) = clean_victim(log, v, &mut cold, log.cfg.mode)
                && tx.send(b).is_err()
            {
                break;
            }
            continue;
        }
        thread::park_timeout(Duration::from_micros(200));
    }
    let used = thread_cpu_ns() - base.unwrap_or_else(thread_cpu_ns);
    log.worker_cpu.fetch_add(used, Relaxed);
}

struct Owner<'a> {
    log: &'a Log,
    hot: Cursor,
    cold: Cursor,
    rx: Receiver<Batch>,
    workers: Vec<Thread>,
    src: Vec<u8>,
    stall_ns: u64,
}

impl Owner<'_> {
    fn drain(&mut self) {
        while let Ok(b) = self.rx.try_recv() {
            apply(self.log, b);
        }
    }

    fn clean_inline(&mut self) {
        while self.log.free_n.load(Acquire) < self.log.cfg.free_low {
            let mut any = false;
            for _ in 0..VICTIMS_PER_PASS {
                if let Some(v) = self.log.pick() {
                    clean_victim(self.log, v, &mut self.cold, Mode::Inline);
                    any = true;
                }
            }
            if !any {
                break;
            }
        }
    }

    fn next_hot_segment(&mut self) {
        let log = self.log;
        if self.hot.seg != INVALID {
            log.seal(self.hot.seg);
        }
        if log.cfg.mode == Mode::Inline {
            self.clean_inline();
        } else if log.free_n.load(Acquire) < log.cfg.free_low {
            for w in &self.workers {
                w.unpark();
            }
        }
        let t0 = Instant::now();
        let mut stalled = false;
        self.hot.seg = loop {
            if let Some(s) = log.take_free(log.cfg.reserve) {
                break s;
            }
            stalled = true;
            self.drain();
            for w in &self.workers {
                w.unpark();
            }
            thread::yield_now();
        };
        if stalled {
            self.stall_ns += t0.elapsed().as_nanos() as u64;
        }
        self.hot.next = 0;
    }

    fn write(&mut self, l: u32, seq: u64) {
        let log = self.log;
        self.drain();
        if self.hot.seg == INVALID || self.hot.next == log.cfg.seg_blocks {
            self.next_hot_segment();
        }
        let p = self.hot.seg * log.cfg.seg_blocks + self.hot.next;
        self.hot.next += 1;
        self.src[..8].copy_from_slice(&(l as u64).to_le_bytes());
        self.src[8..16].copy_from_slice(&seq.to_le_bytes());
        let dst = log.disk.block(p);
        dst.copy_from_slice(&self.src);
        log.crc[p as usize].store(crc32c(dst), Relaxed);
        log.rev[p as usize].store(l, Release);
        log.live[log.seg(p)].fetch_add(1, Relaxed);
        let old = log.index[l as usize].swap(p, AcqRel);
        if old != INVALID {
            log.live[log.seg(old)].fetch_sub(1, Relaxed);
        }
    }
}

fn pct(v: &mut [u32], q: f64) -> f64 {
    let i = ((v.len() as f64 * q) as usize).min(v.len() - 1);
    v[i] as f64 / 1000.0
}

fn run(cfg: Cfg) {
    let blocks = cfg.segs as usize * cfg.seg_blocks as usize;
    let lbas = (cfg.u * blocks as f64) as usize;
    let warm = (cfg.warm * blocks as f64) as u64;
    let n = (cfg.measure * blocks as f64) as usize;
    let log = Log::new(cfg, lbas);
    let (tx, rx) = channel();
    let mut rng = Rng(log.cfg.seed);
    let (secs, owner_cpu, busy, stall, mut lat) = thread::scope(|sc| {
        let workers: Vec<Thread> = (0..log.cfg.workers)
            .map(|_| {
                let tx = tx.clone();
                let log = &log;
                sc.spawn(move || worker(log, tx)).thread().clone()
            })
            .collect();
        let mut o = Owner {
            log: &log,
            hot: Cursor::none(),
            cold: Cursor::none(),
            rx,
            workers,
            src: vec![0x5a; BLOCK],
            stall_ns: 0,
        };
        let mut seq = 0u64;
        for l in 0..lbas as u32 {
            o.write(l, seq);
            seq += 1;
        }
        for _ in 0..warm {
            o.write(rng.below(lbas as u64) as u32, seq);
            seq += 1;
        }
        log.copies.store(0, Relaxed);
        log.wasted.store(0, Relaxed);
        log.victims.store(0, Relaxed);
        o.stall_ns = 0;
        log.measuring.store(true, Release);
        for w in &o.workers {
            w.unpark();
        }
        let mut lat = vec![0u32; n];
        let period = if log.cfg.rate > 0.0 {
            1e9 / log.cfg.rate
        } else {
            0.0
        };
        let cpu0 = thread_cpu_ns();
        let t0 = Instant::now();
        let mut busy = 0u64;
        for (i, slot) in lat.iter_mut().enumerate() {
            if period > 0.0 {
                let due = Duration::from_nanos((i as f64 * period) as u64);
                while t0.elapsed() < due {
                    std::hint::spin_loop();
                }
            }
            let s = Instant::now();
            o.write(rng.below(lbas as u64) as u32, seq);
            let d = s.elapsed().as_nanos() as u64;
            busy += d;
            *slot = d.min(u32::MAX as u64) as u32;
            seq += 1;
        }
        let secs = t0.elapsed().as_secs_f64();
        let owner_cpu = thread_cpu_ns() - cpu0;
        log.stop.store(true, Release);
        for w in &o.workers {
            w.unpark();
        }
        (secs, owner_cpu, busy, o.stall_ns, lat)
    });
    drop(tx);
    report(&log, &mut lat, secs, owner_cpu, busy, stall, n);
}

fn report(log: &Log, lat: &mut [u32], secs: f64, owner_cpu: u64, busy: u64, stall: u64, n: usize) {
    let c = &log.cfg;
    let nf = n as f64;
    let copies = log.copies.load(Relaxed) as f64 / nf;
    let wasted = log.wasted.load(Relaxed) as f64 / nf;
    let wcpu = log.worker_cpu.load(Relaxed) as f64 / nf;
    let ocpu = owner_cpu as f64 / nf;
    lat.sort_unstable();
    println!(
        "RUN tag={} mode={:?} workers={} u={:.2} rate={} segs={} seg_blocks={} free_low={} reserve={} | \
         writes_per_s={:.0} owner_busy_ns={:.0} owner_work_ns={:.0} owner_cpu_ns={:.0} worker_cpu_ns={:.0} total_cpu_ns={:.0} \
         stall_ns={:.0} copies_per_user={:.3} wasted_per_user={:.4} wa={:.3} victims_per_user={:.4} \
         lat_p50_us={:.2} lat_p99_us={:.2} lat_p999_us={:.2} lat_max_us={:.1} secs={:.2}",
        c.tag,
        c.mode,
        c.workers,
        c.u,
        c.rate,
        c.segs,
        c.seg_blocks,
        c.free_low,
        c.reserve,
        nf / secs,
        busy as f64 / nf,
        (busy - stall) as f64 / nf,
        ocpu,
        wcpu,
        ocpu + wcpu,
        stall as f64 / nf,
        copies,
        wasted,
        1.0 + copies + wasted,
        log.victims.load(Relaxed) as f64 / nf,
        pct(lat, 0.50),
        pct(lat, 0.99),
        pct(lat, 0.999),
        *lat.last().unwrap() as f64 / 1000.0,
        secs,
    );
}

fn main() {
    assert!(
        std::arch::is_x86_feature_detected!("sse4.2"),
        "needs SSE4.2 for CRC32C"
    );
    let args: Vec<String> = std::env::args().skip(1).collect();
    run(Cfg::parse(&args));
}
