//! Rust baseline for the Go memtable spike: the stage-1 `MemTree` from `crates/core` loaded with
//! the same data file, measured the same way.
//!
//! Usage: memtable-rust-baseline DATA PROBES

use std::io::{BufReader, Read};
use std::time::Instant;

use rock_core::lsm::{MemTree, Tree};

#[repr(C)]
#[derive(Default)]
struct Mallinfo2 {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}

unsafe extern "C" {
    fn mallinfo2() -> Mallinfo2;
}

/// Bytes glibc malloc has handed out: in-use chunks of the main arena plus mmapped chunks.
fn malloc_in_use() -> usize {
    // SAFETY: mallinfo2 takes no arguments and returns a plain struct.
    let m = unsafe { mallinfo2() };
    m.uordblks + m.hblkhd
}

fn rss() -> usize {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap();
    s.split_whitespace().nth(1).unwrap().parse::<usize>().unwrap() * 4096
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (((z ^ (z >> 31)) as u128 * n as u128) >> 64) as usize
    }
}

fn pct(v: &mut [u64], q: f64) -> f64 {
    v.sort_unstable();
    v[((v.len() as f64 * q) as usize).min(v.len() - 1)] as f64 / 1000.0
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let probes_raw = std::fs::read(&args[2]).unwrap();
    let mut probes = Vec::new();
    let mut o = 0;
    while o < probes_raw.len() {
        let n = u16::from_le_bytes([probes_raw[o], probes_raw[o + 1]]) as usize;
        probes.push((o + 2, n));
        o += 2 + n;
    }
    let probe = |i: usize| &probes_raw[probes[i].0..probes[i].0 + probes[i].1];

    let (m0, r0) = (malloc_in_use(), rss());
    let mut tree = MemTree::new();
    let mut f = BufReader::with_capacity(1 << 20, std::fs::File::open(&args[1]).unwrap());
    let mut hdr = [0u8; 4];
    let start = Instant::now();
    while f.read_exact(&mut hdr).is_ok() {
        let kl = u16::from_le_bytes([hdr[0], hdr[1]]) as usize;
        let vl = u16::from_le_bytes([hdr[2], hdr[3]]) as usize;
        let mut k = vec![0u8; kl];
        let mut v = vec![0u8; vl];
        f.read_exact(&mut k).unwrap();
        f.read_exact(&mut v).unwrap();
        tree.insert(k, v);
    }
    let build = start.elapsed().as_secs_f64();
    let files = tree.len() as f64 / 2.09;
    println!("### variant=rust-memtree {}", env!("CARGO_PKG_NAME"));
    println!(
        "M1 build: entries={} {build:.2} s ({:.2} M entries/s)",
        tree.len(),
        tree.len() as f64 / build / 1e6
    );
    println!(
        "M1 memory: malloc in use {:.0} B/file, RSS {:.0} B/file (files = entries / 2.09)",
        (malloc_in_use() - m0) as f64 / files,
        (rss() - r0) as f64 / files
    );

    let mut rng = Rng(3);
    let mut lat = Vec::with_capacity(1_000_000);
    let mut miss = 0;
    for _ in 0..1_000_000 {
        let k = probe(rng.below(probes.len()));
        let s = Instant::now();
        let hit = tree.get(k).is_some();
        lat.push(s.elapsed().as_nanos() as u64);
        miss += usize::from(!hit);
    }
    let (p50, p99) = (pct(&mut lat, 0.5), pct(&mut lat, 0.99));
    println!("M4 get: p50 {p50:.2} us p99 {p99:.2} us (1000000 probes, {miss} misses)");

    let mut prefix = vec![0x02];
    prefix.extend_from_slice(&1u32.to_be_bytes());
    prefix.extend_from_slice(&((1u64 << 40) | 1).to_be_bytes());
    prefix.push(b'd');
    let mut lat = Vec::with_capacity(1000);
    let mut got = 0;
    for _ in 0..1000 {
        let s = Instant::now();
        got = tree.scan(&prefix).count();
        lat.push(s.elapsed().as_nanos() as u64);
    }
    println!("M4 scan: {got} entries, p50 {:.1} us", pct(&mut lat, 0.5));

    let s = Instant::now();
    for _ in 0..1_000_000 {
        let k = probe(rng.below(probes.len()));
        let mut v = tree.get(k).unwrap().to_vec();
        *v.last_mut().unwrap() ^= 0xff;
        tree.insert(k.to_vec(), v);
    }
    println!(
        "M4 update: 1000000 same-length overwrites, {:.2} us each",
        s.elapsed().as_secs_f64()
    );

    let m1 = malloc_in_use();
    let s = Instant::now();
    tree.begin_snapshot();
    let mut pieces = Vec::new();
    while let Some(p) = tree.next_piece(1 << 20) {
        pieces.push(p);
    }
    drop(tree);
    let mut restored = MemTree::new();
    restored.restore(&pieces).unwrap();
    drop(pieces);
    println!(
        "M7 restore through snapshot pieces: {:.2} s, malloc in use {:.0} B/file (was {:.0})",
        s.elapsed().as_secs_f64(),
        (malloc_in_use() - m0) as f64 / files,
        (m1 - m0) as f64 / files
    );
    std::hint::black_box(&restored);
}
