//! Measurement helpers and options, adapted from spikes/multi-raft/src/common.rs
//! for macOS: getrusage for CPU, proc_pidinfo for RSS, getloadavg for load.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicIsize, Ordering};
use std::time::{Duration, Instant};

pub struct CountingAlloc;

static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        LIVE.fetch_add(n as isize - l.size() as isize, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}

pub fn heap_bytes() -> i64 {
    LIVE.load(Ordering::Relaxed) as i64
}

fn taskinfo() -> libc::proc_taskinfo {
    let mut ti: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    let n = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            &mut ti as *mut _ as *mut libc::c_void,
            size,
        )
    };
    assert_eq!(n, size, "proc_pidinfo");
    ti
}

pub fn rss_bytes() -> i64 {
    taskinfo().pti_resident_size as i64
}

pub fn threads() -> i32 {
    taskinfo().pti_threadnum
}

/// User + system CPU time of this process (getrusage, µs resolution).
pub fn cpu_ns() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000_000 + t.tv_usec as u64 * 1000;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

pub fn loadavg() -> String {
    let mut l = [0f64; 3];
    unsafe { libc::getloadavg(l.as_mut_ptr(), 3) };
    format!("{:.2} {:.2} {:.2}", l[0], l[1], l[2])
}

pub struct CpuWindow {
    t0: Instant,
    c0: u64,
}

impl CpuWindow {
    pub fn start() -> Self {
        CpuWindow {
            t0: Instant::now(),
            c0: cpu_ns(),
        }
    }
    /// (wall, cpu)
    pub fn stop(&self) -> (Duration, Duration) {
        (self.t0.elapsed(), Duration::from_nanos(cpu_ns() - self.c0))
    }
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
    pub fsync_us: u64,
    pub seed: u64,
    pub rand_seed: Option<u64>,
    pub tick_ms: u64,
    pub settle_s: u64,
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
            inflight: 32,
            payload: 100,
            fsync_us: 0,
            seed: 1,
            rand_seed: None,
            tick_ms: 0,
            settle_s: 2,
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
                "--fsync-us" => o.fsync_us = v.parse().unwrap(),
                "--tick" => o.tick_ms = v.parse().unwrap(),
                "--seed" => o.seed = v.parse().unwrap(),
                "--rand-seed" => o.rand_seed = Some(v.parse().unwrap()),
                "--settle" => o.settle_s = v.parse().unwrap(),
                _ => panic!("unknown arg {k}"),
            }
            i += 2;
        }
        o
    }
}
