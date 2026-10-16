use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

pub struct CountingAlloc;

static LIVE: AtomicIsize = AtomicIsize::new(0);
static LARGE: AtomicIsize = AtomicIsize::new(0);
static LARGE_N: AtomicIsize = AtomicIsize::new(0);
const LARGE_MIN: usize = 16 * 1024;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        if l.size() >= LARGE_MIN {
            LARGE.fetch_add(l.size() as isize, Ordering::Relaxed);
            LARGE_N.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        if l.size() >= LARGE_MIN {
            LARGE.fetch_sub(l.size() as isize, Ordering::Relaxed);
            LARGE_N.fetch_sub(1, Ordering::Relaxed);
        }
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        LIVE.fetch_add(n as isize - l.size() as isize, Ordering::Relaxed);
        if l.size() >= LARGE_MIN {
            LARGE.fetch_sub(l.size() as isize, Ordering::Relaxed);
            LARGE_N.fetch_sub(1, Ordering::Relaxed);
        }
        if n >= LARGE_MIN {
            LARGE.fetch_add(n as isize, Ordering::Relaxed);
            LARGE_N.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(p, l, n)
    }
}

pub fn heap_bytes() -> i64 {
    LIVE.load(Ordering::Relaxed) as i64
}

/// Live bytes and count of allocations of 16 KiB or more.
pub fn large_allocs() -> (i64, i64) {
    (LARGE.load(Ordering::Relaxed) as i64, LARGE_N.load(Ordering::Relaxed) as i64)
}

pub fn rss_bytes() -> i64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap();
    for l in s.lines() {
        if let Some(v) = l.strip_prefix("VmRSS:") {
            let kb: i64 = v.trim().trim_end_matches("kB").trim().parse().unwrap();
            return kb * 1024;
        }
    }
    0
}

/// Total on-CPU time of all threads of this process, from schedstat (ns precision).
pub fn cpu_ns() -> u64 {
    let mut total = 0;
    for t in std::fs::read_dir("/proc/self/task").unwrap() {
        let p = t.unwrap().path().join("schedstat");
        if let Ok(s) = std::fs::read_to_string(p) {
            total += s.split_whitespace().next().unwrap().parse::<u64>().unwrap();
        }
    }
    total
}

pub fn threads() -> usize {
    std::fs::read_dir("/proc/self/task").unwrap().count()
}

pub fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg").unwrap().split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

pub struct CpuWindow {
    t0: Instant,
    c0: u64,
}

impl CpuWindow {
    pub fn start() -> Self {
        CpuWindow { t0: Instant::now(), c0: cpu_ns() }
    }
    /// (wall, cpu)
    pub fn stop(&self) -> (Duration, Duration) {
        (self.t0.elapsed(), Duration::from_nanos(cpu_ns() - self.c0))
    }
}

pub fn check(ok: bool, id: &str, what: &str, got: String, want: &str) {
    println!("[{}] {id} {what}: got={got} want={want}", if ok { "OK " } else { "FAIL" });
}

pub fn info(id: &str, what: &str, v: String) {
    println!("      {id} {what}: {v}");
}

pub fn kib(b: f64) -> String {
    format!("{:.1} KiB", b / 1024.0)
}

#[derive(Clone, Debug)]
pub struct Opts {
    pub groups: usize,
    pub hb_ms: u64,
    pub elect_min_ms: u64,
    pub elect_max_ms: u64,
    pub idle_s: u64,
    pub write_s: u64,
    pub inflight: usize,
    pub payload: usize,
    pub threads: usize,
    pub fsync_us: u64,
    pub init: String,
    pub seed: u64,
    pub sim_variant: String,
    pub kill: bool,
    pub tick_ms: u64,
    pub quiesce: bool,
}

impl Opts {
    pub fn parse(args: &[String]) -> Opts {
        let mut o = Opts {
            groups: 1000,
            hb_ms: 50,
            elect_min_ms: 150,
            elect_max_ms: 300,
            idle_s: 10,
            write_s: 0,
            inflight: 4,
            payload: 100,
            threads: 1,
            fsync_us: 0,
            init: "spread".into(),
            seed: 1,
            sim_variant: "plain".into(),
            kill: true,
            tick_ms: 0,
            quiesce: false,
        };
        let mut i = 0;
        while i < args.len() {
            let k = args[i].as_str();
            let v = args.get(i + 1).cloned().unwrap_or_default();
            match k {
                "--groups" => o.groups = v.parse().unwrap(),
                "--hb" => o.hb_ms = v.parse().unwrap(),
                "--elect" => {
                    let (a, b) = v.split_once('-').unwrap();
                    o.elect_min_ms = a.parse().unwrap();
                    o.elect_max_ms = b.parse().unwrap();
                }
                "--idle" => o.idle_s = v.parse().unwrap(),
                "--write" => o.write_s = v.parse().unwrap(),
                "--inflight" => o.inflight = v.parse().unwrap(),
                "--payload" => o.payload = v.parse().unwrap(),
                "--threads" => o.threads = v.parse().unwrap(),
                "--fsync-us" => o.fsync_us = v.parse().unwrap(),
                "--init" => o.init = v,
                "--tick" => o.tick_ms = v.parse().unwrap(),
                "--seed" => o.seed = v.parse().unwrap(),
                "--variant" => o.sim_variant = v,
                "--quiesce" => {
                    o.quiesce = true;
                    o.kill = false;
                    i += 1;
                    continue;
                }
                "--no-kill" => {
                    o.kill = false;
                    i += 1;
                    continue;
                }
                _ => panic!("unknown arg {k}"),
            }
            i += 2;
        }
        o
    }
}
