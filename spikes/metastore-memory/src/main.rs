//! Bytes per file of a MetaStore shard held in the stage-1 `MemTree`. See README.md.

use std::alloc::{GlobalAlloc, Layout as AllocLayout, System};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hint::black_box;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use rock_core::event::BLOCK_BYTES;
use rock_core::log::record::{BlobKind, Entry, Layout};
use rock_core::lsm::{MemTree, Tree};

// ---------------------------------------------------------------- allocator

struct Counting;

static REQ: AtomicUsize = AtomicUsize::new(0);
static ACT: AtomicUsize = AtomicUsize::new(0);
static GLIBC: AtomicUsize = AtomicUsize::new(0);
static JEM: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK_REQ: AtomicUsize = AtomicUsize::new(0);
static PEAK_ACT: AtomicUsize = AtomicUsize::new(0);

/// glibc ptmalloc chunk size: request + 8-byte header, 16-byte aligned, 32 B minimum;
/// mmap above 128 KiB (page rounded). A model, not a measurement.
fn glibc_chunk(n: usize) -> usize {
    if n >= 128 << 10 {
        (n + 16 + 4095) & !4095
    } else {
        ((n + 8 + 15) & !15).max(32)
    }
}

/// jemalloc 5 size class (4 classes per doubling above 128 B, no per-object header).
fn jemalloc_class(n: usize) -> usize {
    if n <= 8 {
        8
    } else if n <= 16 {
        16
    } else if n <= 128 {
        (n + 15) & !15
    } else {
        let lg = usize::BITS - 1 - (n - 1).leading_zeros();
        let step = 1usize << (lg - 2);
        (n + step - 1) & !(step - 1)
    }
}

fn add(req: usize, act: usize) {
    let r = REQ.fetch_add(req, Relaxed) + req;
    let a = ACT.fetch_add(act, Relaxed) + act;
    GLIBC.fetch_add(glibc_chunk(req), Relaxed);
    JEM.fetch_add(jemalloc_class(req), Relaxed);
    LIVE.fetch_add(1, Relaxed);
    PEAK_REQ.fetch_max(r, Relaxed);
    PEAK_ACT.fetch_max(a, Relaxed);
}

fn sub(req: usize, act: usize) {
    REQ.fetch_sub(req, Relaxed);
    ACT.fetch_sub(act, Relaxed);
    GLIBC.fetch_sub(glibc_chunk(req), Relaxed);
    JEM.fetch_sub(jemalloc_class(req), Relaxed);
    LIVE.fetch_sub(1, Relaxed);
}

fn msize(p: *const u8) -> usize {
    unsafe { libc::malloc_size(p.cast()) }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: AllocLayout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            add(l.size(), msize(p));
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: AllocLayout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() {
            add(l.size(), msize(p));
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: AllocLayout) {
        sub(l.size(), msize(p));
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: AllocLayout, new: usize) -> *mut u8 {
        let old = msize(p);
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            sub(l.size(), old);
            add(new, msize(q));
        }
        q
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy, Debug, Default)]
struct Mem {
    req: usize,
    act: usize,
    glibc: usize,
    jem: usize,
    allocs: usize,
}

impl Mem {
    fn now() -> Self {
        Self {
            req: REQ.load(Relaxed),
            act: ACT.load(Relaxed),
            glibc: GLIBC.load(Relaxed),
            jem: JEM.load(Relaxed),
            allocs: LIVE.load(Relaxed),
        }
    }
    fn since(self, base: Self) -> Self {
        Self {
            req: self.req - base.req,
            act: self.act - base.act,
            glibc: self.glibc - base.glibc,
            jem: self.jem - base.jem,
            allocs: self.allocs - base.allocs,
        }
    }
}

fn reset_peak() {
    PEAK_REQ.store(REQ.load(Relaxed), Relaxed);
    PEAK_ACT.store(ACT.load(Relaxed), Relaxed);
}

fn peak() -> (usize, usize) {
    (PEAK_REQ.load(Relaxed), PEAK_ACT.load(Relaxed))
}

/// Leading fields of `struct task_vm_info` (mach/task_info.h) through `phys_footprint` (rev 1).
#[repr(C)]
#[derive(Default)]
struct TaskVmInfo {
    virtual_size: u64,
    region_count: i32,
    page_size: i32,
    resident_size: u64,
    resident_size_peak: u64,
    device: u64,
    device_peak: u64,
    internal: u64,
    internal_peak: u64,
    external: u64,
    external_peak: u64,
    reusable: u64,
    reusable_peak: u64,
    purgeable_volatile_pmap: u64,
    purgeable_volatile_resident: u64,
    purgeable_volatile_virtual: u64,
    compressed: u64,
    compressed_peak: u64,
    compressed_lifetime: u64,
    phys_footprint: u64,
}

const TASK_VM_INFO: u32 = 22;

/// Resident, compressed and physical footprint bytes from mach `task_info(TASK_VM_INFO)`.
/// macOS compresses (and swaps) idle pages, so resident alone undercounts under memory pressure.
#[derive(Clone, Copy)]
struct Rss {
    resident: u64,
    compressed: u64,
    footprint: u64,
}

fn rss() -> Rss {
    let mut info = TaskVmInfo::default();
    let mut count = (size_of::<TaskVmInfo>() / size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
    let rc = unsafe {
        #[allow(deprecated)]
        let task = libc::mach_task_self();
        libc::task_info(task, TASK_VM_INFO, (&raw mut info).cast(), &mut count)
    };
    assert_eq!(rc, 0, "task_info");
    Rss {
        resident: info.resident_size,
        compressed: info.compressed,
        footprint: info.phys_footprint,
    }
}

// ---------------------------------------------------------------- rng, output

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
        self.next() % n
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = Vec::with_capacity(n);
        while v.len() < n {
            let x = self.next().to_le_bytes();
            v.extend_from_slice(&x[..(n - v.len()).min(8)]);
        }
        v
    }
}

fn check(name: &str, ok: bool, got: &str, want: &str) {
    println!(
        "{} {name}: got={got} want={want}",
        if ok { "[OK ]" } else { "[FAIL]" }
    );
}

fn us(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

fn pct(v: &mut [u64], p: usize) -> f64 {
    v.sort_unstable();
    v[(v.len() * p / 100).min(v.len() - 1)] as f64 / 1000.0
}

// ---------------------------------------------------------------- file model (ADR 0004)

const VOLUME: u32 = 1;
const TAG_FS: u8 = 0x02;
const TAG_ATTR: u8 = b'a';
const TAG_DIRENT: u8 = b'd';
const TAG_XATTR: u8 = b'x';
const TAG_SLOT: u8 = b'n';
const TYPE_REG: u8 = 1;
const TYPE_DIR: u8 = 2;
const ATTR_LEN: usize = 92;
/// Directory 1 holds 996 files and 4 subdirectories: the 1,000-entry directory F7 lists.
const BIG_DIR_ENTRIES: usize = 1000;
const BIG_DIR_FILES: u32 = 996;
const BIG_DIR: usize = 1;
const XATTRS: [(&[u8], usize); 3] = [
    (b"user.DOSATTRIB", 24),
    (b"com.apple.FinderInfo", 32),
    (b"com.apple.quarantine", 57),
];
const NAME_CHARS: &[u8; 64] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._";

fn ino(slot: usize, seq: u64) -> u64 {
    ((slot as u64) << 40) | seq
}

fn key(inode: u64, tag: u8, last: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(14 + last.len());
    k.push(TAG_FS);
    k.extend_from_slice(&VOLUME.to_be_bytes());
    k.extend_from_slice(&inode.to_be_bytes());
    k.push(tag);
    k.extend_from_slice(last);
    k
}

/// mode, uid, gid, nlink, flags (u32); size, alloc_end (u64); atime, mtime, ctime, btime
/// (i64 seconds + u32 ns); change counter (u64). 92 bytes.
fn attr_value(rng: &mut Rng, dir: bool, nlink: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(ATTR_LEN);
    let mode: u32 = if dir { 0o040_755 } else { 0o100_644 };
    let size = if dir { 0 } else { rng.below(1 << 24) };
    for x in [mode, 1000 + rng.below(4) as u32, 1000, nlink, 0] {
        v.extend_from_slice(&x.to_be_bytes());
    }
    v.extend_from_slice(&size.to_be_bytes());
    v.extend_from_slice(&size.next_multiple_of(4096).to_be_bytes());
    for _ in 0..4 {
        v.extend_from_slice(&(1_700_000_000 + rng.below(1 << 26) as i64).to_be_bytes());
        v.extend_from_slice(&(rng.below(1_000_000_000) as u32).to_be_bytes());
    }
    v.extend_from_slice(&rng.below(1 << 20).to_be_bytes());
    debug_assert_eq!(v.len(), ATTR_LEN);
    v
}

#[derive(Clone)]
enum Names {
    Fixed(usize),
    /// (length, cumulative count)
    Hist(Vec<(usize, u64)>),
}

impl Names {
    fn parse(arg: &str) -> Self {
        if let Ok(n) = arg.parse() {
            return Self::Fixed(n);
        }
        let text = std::fs::read_to_string(arg).expect("name histogram");
        let mut acc = 0;
        let hist = text
            .lines()
            .filter(|l| !l.starts_with('#'))
            .map(|l| {
                let mut it = l.split_whitespace();
                let len: usize = it.next().unwrap().parse().unwrap();
                acc += it.next().unwrap().parse::<u64>().unwrap();
                (len, acc)
            })
            .collect();
        Self::Hist(hist)
    }
    fn label(&self) -> String {
        match self {
            Self::Fixed(n) => format!("{n}B"),
            Self::Hist(h) => {
                let total = h.last().unwrap().1;
                let mut prev = 0;
                let sum: u64 = h
                    .iter()
                    .map(|&(l, c)| {
                        let n = c - prev;
                        prev = c;
                        l as u64 * n
                    })
                    .sum();
                format!("sampled-mean-{:.1}B", sum as f64 / total as f64)
            }
        }
    }
    fn draw(&self, rng: &mut Rng) -> usize {
        match self {
            Self::Fixed(n) => *n,
            Self::Hist(h) => {
                let x = rng.below(h.last().unwrap().1);
                h[h.partition_point(|&(_, c)| c <= x)].0
            }
        }
    }
}

struct Model {
    names: Names,
    xattrs: usize,
    seed: u64,
}

#[derive(Default, Clone, Copy)]
struct Stats {
    files: u64,
    dirs: u64,
    entries: u64,
    key_bytes: u64,
    val_bytes: u64,
}

/// Files per directory, 20 to 50 uniformly; directory 1 holds 996.
fn plan(files: u64, seed: u64) -> Vec<u32> {
    let mut rng = Rng(seed ^ 0x5eed);
    let mut plan = Vec::new();
    let mut total = 0u64;
    while total < files {
        let n = if plan.len() == BIG_DIR {
            BIG_DIR_FILES
        } else {
            rng.range(20, 50) as u32
        };
        let n = n.min((files - total) as u32);
        plan.push(n);
        total += u64::from(n);
    }
    plan
}

/// Emits every key in create order: directories breadth first (directory j's parent is
/// (j-1)/4), each directory's files and subdirectories in random name order.
fn generate(m: &Model, plan: &[u32], mut sink: impl FnMut(Vec<u8>, Vec<u8>)) -> Stats {
    let mut rng = Rng(m.seed);
    let mut st = Stats::default();
    let mut emit = |st: &mut Stats, k: Vec<u8>, v: Vec<u8>| {
        st.entries += 1;
        st.key_bytes += k.len() as u64;
        st.val_bytes += v.len() as u64;
        sink(k, v);
    };
    let xattrs = |rng: &mut Rng, st: &mut Stats, emit: &mut dyn FnMut(&mut Stats, Vec<u8>, Vec<u8>), i: u64| {
        for &(name, len) in &XATTRS[..m.xattrs] {
            emit(st, key(i, TAG_XATTR, name), rng.bytes(len));
        }
    };
    let d = plan.len();
    let subdirs = |j: usize| (4 * j + 1)..(4 * j + 5).min(d);
    let a = attr_value(&mut rng, true, 2 + subdirs(0).len() as u32);
    emit(&mut st, key(ino(0, 1), TAG_ATTR, &[]), a);
    xattrs(&mut rng, &mut st, &mut emit, ino(0, 1));
    st.dirs += 1;
    let mut names: HashSet<Vec<u8>> = HashSet::new();
    let name = |rng: &mut Rng, names: &mut HashSet<Vec<u8>>| loop {
        let mut len = m.names.draw(rng).max(1);
        for attempt in 0.. {
            if attempt > 0 && attempt % 64 == 0 {
                len += 1;
            }
            let n: Vec<u8> = (0..len)
                .map(|_| NAME_CHARS[rng.below(64) as usize])
                .collect();
            if names.insert(n.clone()) {
                return n;
            }
        }
    };
    for j in 0..d {
        let dir = ino(j, 1);
        names.clear();
        for k in 0..u64::from(plan[j]) {
            let f = ino(j, 2 + k);
            let n = name(&mut rng, &mut names);
            let mut v = Vec::with_capacity(9);
            v.extend_from_slice(&f.to_be_bytes());
            v.push(TYPE_REG);
            emit(&mut st, key(dir, TAG_DIRENT, &n), v);
            let a = attr_value(&mut rng, false, 1);
            emit(&mut st, key(f, TAG_ATTR, &[]), a);
            xattrs(&mut rng, &mut st, &mut emit, f);
            st.files += 1;
        }
        for c in subdirs(j) {
            let ci = ino(c, 1);
            let n = name(&mut rng, &mut names);
            let mut v = Vec::with_capacity(9);
            v.extend_from_slice(&ci.to_be_bytes());
            v.push(TYPE_DIR);
            emit(&mut st, key(dir, TAG_DIRENT, &n), v);
            let a = attr_value(&mut rng, true, 2 + subdirs(c).len() as u32);
            emit(&mut st, key(ci, TAG_ATTR, &[]), a);
            xattrs(&mut rng, &mut st, &mut emit, ci);
            st.dirs += 1;
        }
        let mut v = Vec::with_capacity(16);
        v.extend_from_slice(&(2 + u64::from(plan[j])).to_be_bytes());
        v.extend_from_slice(&rng.below(1 << 40).to_be_bytes());
        emit(&mut st, key(ino(j, 0), TAG_SLOT, &[]), v);
    }
    st
}

fn dirent_prefix(dir_slot: usize) -> Vec<u8> {
    key(ino(dir_slot, 1), TAG_DIRENT, &[])
}

/// Largest snapshot piece for the default `Config` (`max_record_bytes` 1 MiB), computed as
/// `GroupCommit`'s private `max_blob_bytes` does (crates/core/src/log/commit.rs).
fn max_blob_bytes() -> usize {
    let max_record_blocks = ((1usize << 20) / BLOCK_BYTES) as u32;
    let fits = |blocks: u32| {
        Layout::of(&[Entry::Blob {
            kind: BlobKind::SNAPSHOT,
            owner: 0,
            len: blocks * BLOCK_BYTES as u32,
        }])
        .record_blocks()
            <= max_record_blocks
    };
    (1..max_record_blocks).rev().find(|&b| fits(b)).unwrap() as usize * BLOCK_BYTES
}

/// Sum of malloc_size over every key and value buffer of `tree`.
fn kv_alloc_bytes(tree: &MemTree) -> usize {
    tree.scan(&[])
        .map(|(k, v)| msize(k.as_ptr()) + msize(v.as_ptr()))
        .sum()
}

// ---------------------------------------------------------------- measure (child of f1/f2/f3)

fn measure(args: &[String]) {
    let files: u64 = args[0].parse().unwrap();
    let names = Names::parse(&args[1]);
    let xattrs: usize = args[2].parse().unwrap();
    let order = args[3].as_str();
    let m = Model {
        names: names.clone(),
        xattrs,
        seed: 42,
    };
    let plan = plan(files, m.seed);
    let rss0 = rss();
    let base = Mem::now();
    let t = Instant::now();
    let mut tree = MemTree::new();
    let st = match order {
        "create" => generate(&m, &plan, |k, v| tree.insert(k, v)),
        "shuffled" => {
            let mut list = Vec::new();
            let st = generate(&m, &plan, |k, v| list.push((k, v)));
            let mut rng = Rng(7);
            for i in (1..list.len()).rev() {
                list.swap(i, rng.below(i as u64 + 1) as usize);
            }
            for (k, v) in list {
                tree.insert(k, v);
            }
            st
        }
        "restored" => {
            let mut orig = MemTree::new();
            let st = generate(&m, &plan, |k, v| orig.insert(k, v));
            let pieces = orig.checkpoint(max_blob_bytes());
            drop(orig);
            tree.restore(&pieces).unwrap();
            st
        }
        _ => panic!("order"),
    };
    let build = us(t);
    let mem = Mem::now().since(base);
    let rss1 = rss();
    assert_eq!(tree.len() as u64, st.entries);
    let kv = kv_alloc_bytes(&tree);
    let f = st.files as f64;
    println!(
        "RESULT files={} dirs={} entries={} names={} xattrs={xattrs} order={order} \
         logical={:.1} req={:.1} act={:.1} glibc={:.1} jem={:.1} allocs={:.2} \
         node_per_entry={:.1} rss={:.1} compressed={:.1} footprint={:.1} build_us={:.0}",
        st.files,
        st.dirs,
        st.entries,
        names.label(),
        (st.key_bytes + st.val_bytes) as f64 / f,
        mem.req as f64 / f,
        mem.act as f64 / f,
        mem.glibc as f64 / f,
        mem.jem as f64 / f,
        mem.allocs as f64 / f,
        (mem.act - kv) as f64 / st.entries as f64,
        (rss1.resident as f64 - rss0.resident as f64) / f,
        (rss1.compressed as f64 - rss0.compressed as f64) / f,
        (rss1.footprint as f64 - rss0.footprint as f64) / f,
        build
    );
}

struct Row(Vec<(String, String)>);

impl Row {
    fn get(&self, k: &str) -> &str {
        &self.0.iter().find(|(a, _)| a == k).unwrap().1
    }
    fn f(&self, k: &str) -> f64 {
        self.get(k).parse().unwrap()
    }
}

fn child(args: &[&str]) -> Row {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe).arg("measure").args(args).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let text = String::from_utf8(out.stdout).unwrap();
    let line = text.lines().find(|l| l.starts_with("RESULT ")).unwrap();
    println!("  {line}");
    let mut fields = Vec::new();
    for tok in line["RESULT ".len()..].split(' ') {
        let (k, v) = tok.split_once('=').unwrap();
        fields.push((k.to_string(), v.to_string()));
    }
    Row(fields)
}

fn header() {
    println!(
        "  columns, all per regular file unless noted: logical = key+value bytes; req = requested \
         heap; act = macOS malloc_size (measured); glibc/jem = modelled Linux allocators; allocs = \
         live allocations; node_per_entry = BTreeMap node bytes per entry (act minus key/value \
         buffers); rss / compressed / footprint = growth of resident, compressor-held and \
         phys_footprint bytes (task_info TASK_VM_INFO)"
    );
}

fn f1(hist: &str) {
    println!("\n## F1 bytes per file in MemTree, 1M files, sampled names, separate process each");
    header();
    let base = child(&["1000000", hist, "0", "create"]);
    let x1 = child(&["1000000", hist, "1", "create"]);
    let x3 = child(&["1000000", hist, "3", "create"]);
    let shuf = child(&["1000000", hist, "0", "shuffled"]);
    let rest = child(&["1000000", hist, "0", "restored"]);
    let b = base.f("act");
    check(
        "F1 attrs+dirent, create order, act B/file",
        (300.0..=400.0).contains(&b),
        &format!("{b:.1}"),
        "300..=400",
    );
    let d1 = x1.f("act") - b;
    let d3 = (x3.f("act") - b) / 3.0;
    check(
        "F1 +1 xattr (user.DOSATTRIB 24 B), added B/file",
        (100.0..=160.0).contains(&d1),
        &format!("{d1:.1} (total {:.1})", x1.f("act")),
        "100..=160 (own guess)",
    );
    check(
        "F1 +3 xattrs, added B per xattr",
        (100.0..=160.0).contains(&d3),
        &format!("{d3:.1} (total {:.1})", x3.f("act")),
        "100..=160 (own guess)",
    );
    println!(
        "  insert order, act B/file: create {:.1}, shuffled {:.1}, restored (checkpoint+restore) {:.1}; \
         node bytes per entry: {:.1} / {:.1} / {:.1}",
        b,
        shuf.f("act"),
        rest.f("act"),
        base.f("node_per_entry"),
        shuf.f("node_per_entry"),
        rest.f("node_per_entry")
    );
}

fn f2(hist: &str) {
    println!("\n## F2 scale, create order, sampled names, separate process each");
    header();
    let rows: Vec<Row> = ["1000000", "10000000", "30000000"]
        .iter()
        .map(|n| child(&[n, hist, "0", "create"]))
        .collect();
    for col in ["req", "act", "glibc", "jem", "rss", "compressed", "footprint"] {
        let v: Vec<f64> = rows.iter().map(|r| r.f(col)).collect();
        println!(
            "  {col:5} B/file  1M {:.1}  10M {:.1}  30M {:.1}",
            v[0], v[1], v[2]
        );
    }
    let a: Vec<f64> = rows.iter().map(|r| r.f("act")).collect();
    let ratio = a[2] / a[0];
    check(
        "F2 act B/file 30M vs 1M",
        (0.9..=1.1).contains(&ratio),
        &format!("{ratio:.3}x ({:.1} vs {:.1})", a[2], a[0]),
        "within 10%",
    );
    let r: Vec<f64> = rows.iter().map(|r| r.f("footprint")).collect();
    println!(
        "  phys_footprint growth / act heap: 1M {:.3}  10M {:.3}  30M {:.3}",
        r[0] / a[0],
        r[1] / a[1],
        r[2] / a[2]
    );
}

fn fit(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    let n = xs.len() as f64;
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let sxy: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let sxx: f64 = xs.iter().map(|x| (x - mx).powi(2)).sum();
    let slope = sxy / sxx;
    let icpt = my - slope * mx;
    let ss_res: f64 = xs
        .iter()
        .zip(ys)
        .map(|(x, y)| (y - (icpt + slope * x)).powi(2))
        .sum();
    let ss_tot: f64 = ys.iter().map(|y| (y - my).powi(2)).sum();
    (slope, icpt, 1.0 - ss_res / ss_tot)
}

fn f3(hist: &str) {
    println!("\n## F3 name length sweep, 1M files, create order, separate process each");
    header();
    let lens = [8.0, 24.0, 64.0];
    let rows: Vec<Row> = ["8", "24", "64"]
        .iter()
        .map(|l| child(&["1000000", l, "0", "create"]))
        .collect();
    let sampled = child(&["1000000", hist, "0", "create"]);
    for col in ["req", "act", "glibc", "jem"] {
        let ys: Vec<f64> = rows.iter().map(|r| r.f(col)).collect();
        let (s, i, r2) = fit(&lens, &ys);
        println!(
            "  {col:5} B/file  8B {:.1}  24B {:.1}  64B {:.1}  sampled {:.1}   fit {i:.1} + {s:.2} x len, R2 {r2:.4}",
            ys[0],
            ys[1],
            ys[2],
            sampled.f(col)
        );
        if col == "act" {
            check(
                "F3 act B/file linear in name length",
                r2 >= 0.95,
                &format!("R2={r2:.4} slope={s:.2} B per name byte"),
                "R2 >= 0.95",
            );
        }
    }
}

// ---------------------------------------------------------------- F4 representations

/// klen u16 | key | value, one allocation, ordered by key.
#[derive(PartialEq, Eq)]
struct OneAlloc(Box<[u8]>);

impl OneAlloc {
    fn new(k: &[u8], v: &[u8]) -> Self {
        let mut b = Vec::with_capacity(2 + k.len() + v.len());
        b.extend_from_slice(&(k.len() as u16).to_le_bytes());
        b.extend_from_slice(k);
        b.extend_from_slice(v);
        Self(b.into_boxed_slice())
    }
    fn key(&self) -> &[u8] {
        let n = u16::from_le_bytes([self.0[0], self.0[1]]) as usize;
        &self.0[2..2 + n]
    }
    fn value(&self) -> &[u8] {
        let n = u16::from_le_bytes([self.0[0], self.0[1]]) as usize;
        &self.0[2 + n..]
    }
}
impl Ord for OneAlloc {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.key().cmp(o.key())
    }
}
impl PartialOrd for OneAlloc {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl std::borrow::Borrow<[u8]> for OneAlloc {
    fn borrow(&self) -> &[u8] {
        self.key()
    }
}

/// Sorted, immutable: entries `klen u16 | vlen u16 | key | value` in one buffer plus offsets.
struct Packed {
    data: Vec<u8>,
    idx: Vec<u32>,
}

impl Packed {
    fn build(tree: &MemTree) -> Self {
        let (mut n, mut bytes) = (0, 0);
        for (k, v) in tree.scan(&[]) {
            n += 1;
            bytes += 4 + k.len() + v.len();
        }
        assert!(bytes < u32::MAX as usize);
        let mut data = Vec::with_capacity(bytes);
        let mut idx = Vec::with_capacity(n);
        for (k, v) in tree.scan(&[]) {
            idx.push(data.len() as u32);
            data.extend_from_slice(&(k.len() as u16).to_le_bytes());
            data.extend_from_slice(&(v.len() as u16).to_le_bytes());
            data.extend_from_slice(k);
            data.extend_from_slice(v);
        }
        Self { data, idx }
    }
    fn at(&self, off: u32) -> (&[u8], &[u8]) {
        let o = off as usize;
        let kl = u16::from_le_bytes([self.data[o], self.data[o + 1]]) as usize;
        let vl = u16::from_le_bytes([self.data[o + 2], self.data[o + 3]]) as usize;
        (
            &self.data[o + 4..o + 4 + kl],
            &self.data[o + 4 + kl..o + 4 + kl + vl],
        )
    }
    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.idx
            .binary_search_by(|&o| self.at(o).0.cmp(key))
            .ok()
            .map(|i| self.at(self.idx[i]).1)
    }
    fn scan(&self, prefix: &[u8], mut f: impl FnMut(&[u8], &[u8])) {
        let start = self.idx.partition_point(|&o| self.at(o).0 < prefix);
        for &o in &self.idx[start..] {
            let (k, v) = self.at(o);
            if !k.starts_with(prefix) {
                break;
            }
            f(k, v);
        }
    }
}

const RESTART: usize = 16;

fn put_varint(out: &mut Vec<u8>, mut x: usize) {
    while x >= 0x80 {
        out.push((x as u8) | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
}

fn get_varint(data: &[u8], pos: &mut usize) -> usize {
    let (mut x, mut shift) = (0, 0);
    loop {
        let b = data[*pos];
        *pos += 1;
        x |= ((b & 0x7f) as usize) << shift;
        if b < 0x80 {
            return x;
        }
        shift += 7;
    }
}

/// Sorted, immutable, LevelDB-style blocks: keys prefix-compressed against the previous key,
/// a full key every 16 entries, and the offsets of those restart points.
struct Prefixed {
    data: Vec<u8>,
    restarts: Vec<u32>,
}

impl Prefixed {
    fn build(tree: &MemTree) -> Self {
        let mut data = Vec::new();
        let mut restarts = Vec::new();
        let mut prev: Vec<u8> = Vec::new();
        for (i, (k, v)) in tree.scan(&[]).enumerate() {
            let shared = if i % RESTART == 0 {
                restarts.push(data.len() as u32);
                0
            } else {
                k.iter().zip(&prev).take_while(|(a, b)| a == b).count()
            };
            put_varint(&mut data, shared);
            put_varint(&mut data, k.len() - shared);
            put_varint(&mut data, v.len());
            data.extend_from_slice(&k[shared..]);
            data.extend_from_slice(v);
            prev.clear();
            prev.extend_from_slice(k);
        }
        data.shrink_to_fit();
        restarts.shrink_to_fit();
        assert!(data.len() < u32::MAX as usize);
        Self { data, restarts }
    }
    /// Full key at a restart point.
    fn restart_key(&self, off: u32) -> &[u8] {
        let mut p = off as usize;
        let shared = get_varint(&self.data, &mut p);
        debug_assert_eq!(shared, 0);
        let kl = get_varint(&self.data, &mut p);
        let _vl = get_varint(&self.data, &mut p);
        &self.data[p..p + kl]
    }
    /// Walks entries from the block that may hold `target`, calling `f` until it returns false.
    fn walk(&self, target: &[u8], mut f: impl FnMut(&[u8], &[u8]) -> bool) {
        let pp = self
            .restarts
            .partition_point(|&o| self.restart_key(o) <= target);
        let mut p = self.restarts[pp.saturating_sub(1)] as usize;
        let mut key = [0u8; 512];
        let mut kl;
        while p < self.data.len() {
            let shared = get_varint(&self.data, &mut p);
            let unshared = get_varint(&self.data, &mut p);
            let vl = get_varint(&self.data, &mut p);
            key[shared..shared + unshared].copy_from_slice(&self.data[p..p + unshared]);
            kl = shared + unshared;
            p += unshared;
            let v = &self.data[p..p + vl];
            p += vl;
            if !f(&key[..kl], v) {
                return;
            }
        }
    }
    fn get(&self, target: &[u8]) -> Option<&[u8]> {
        let mut found: Option<(usize, usize)> = None;
        let base = self.data.as_ptr() as usize;
        self.walk(target, |k, v| match k.cmp(target) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => {
                found = Some((v.as_ptr() as usize - base, v.len()));
                false
            }
            std::cmp::Ordering::Greater => false,
        });
        found.map(|(o, n)| &self.data[o..o + n])
    }
    fn scan(&self, prefix: &[u8], mut f: impl FnMut(&[u8], &[u8])) {
        self.walk(prefix, |k, v| {
            if k < prefix {
                return true;
            }
            if !k.starts_with(prefix) {
                return false;
            }
            f(k, v);
            true
        });
    }
}

// ---------------------------------------------------------------- 10M-file run: F7, F5, F6, F4, F8

struct Speed {
    get_p50: f64,
    get_p99: f64,
    attr_p50: f64,
    dirent_p50: f64,
    list_p50: f64,
    list_p99: f64,
}

fn bench(
    keys: &[(bool, Vec<u8>)],
    get: impl Fn(&[u8]) -> Option<usize>,
    list: impl Fn() -> usize,
) -> Speed {
    for (_, k) in keys.iter().take(10_000) {
        black_box(get(k));
    }
    let mut all = Vec::with_capacity(keys.len());
    let (mut attr, mut dirent) = (Vec::new(), Vec::new());
    for (is_attr, k) in keys {
        let t = Instant::now();
        let r = black_box(get(black_box(k)));
        let ns = t.elapsed().as_nanos() as u64;
        assert!(r.is_some(), "sampled key missing");
        all.push(ns);
        if *is_attr { attr.push(ns) } else { dirent.push(ns) }
    }
    for _ in 0..50 {
        black_box(list());
    }
    let mut lists = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let t = Instant::now();
        let n = black_box(list());
        lists.push(t.elapsed().as_nanos() as u64);
        assert_eq!(n, BIG_DIR_ENTRIES);
    }
    Speed {
        get_p50: pct(&mut all, 50),
        get_p99: pct(&mut all, 99),
        attr_p50: pct(&mut attr, 50),
        dirent_p50: pct(&mut dirent, 50),
        list_p50: pct(&mut lists, 50),
        list_p99: pct(&mut lists, 99),
    }
}

fn print_speed(name: &str, s: &Speed) {
    println!(
        "  {name}: get p50 {:.2} us p99 {:.2} us (attr p50 {:.2}, dirent p50 {:.2}); \
         1000-entry listing p50 {:.1} us p99 {:.1} us",
        s.get_p50, s.get_p99, s.attr_p50, s.dirent_p50, s.list_p50, s.list_p99
    );
}

fn sample_keys(tree: &MemTree, n: usize) -> Vec<(bool, Vec<u8>)> {
    let mut rng = Rng(99);
    let (mut attrs, mut dirents): (Vec<Vec<u8>>, Vec<Vec<u8>>) = (Vec::new(), Vec::new());
    let (mut seen_a, mut seen_d) = (0u64, 0u64);
    for (k, _) in tree.scan(&[]) {
        let (res, seen) = match k[13] {
            TAG_ATTR => (&mut attrs, &mut seen_a),
            TAG_DIRENT => (&mut dirents, &mut seen_d),
            _ => continue,
        };
        *seen += 1;
        if res.len() < n {
            res.push(k.to_vec());
        } else {
            let j = rng.below(*seen) as usize;
            if j < n {
                res[j] = k.to_vec();
            }
        }
    }
    let mut keys: Vec<(bool, Vec<u8>)> = attrs
        .into_iter()
        .map(|k| (true, k))
        .chain(dirents.into_iter().map(|k| (false, k)))
        .collect();
    for i in (1..keys.len()).rev() {
        keys.swap(i, rng.below(i as u64 + 1) as usize);
    }
    keys
}

fn alloc_overhead_ns() -> (f64, f64) {
    let l = AllocLayout::from_size_align(32, 8).unwrap();
    let n = 2_000_000;
    let mut ptrs = Vec::with_capacity(n);
    let t = Instant::now();
    for _ in 0..n {
        ptrs.push(unsafe { GLOBAL.alloc(l) });
    }
    for &p in &ptrs {
        unsafe { GLOBAL.dealloc(p, l) };
    }
    let counted = t.elapsed().as_nanos() as f64 / n as f64;
    ptrs.clear();
    let t = Instant::now();
    for _ in 0..n {
        ptrs.push(unsafe { System.alloc(l) });
    }
    for &p in &ptrs {
        unsafe { System.dealloc(p, l) };
    }
    (counted, t.elapsed().as_nanos() as f64 / n as f64)
}

/// Allocation size (requested -> malloc_size) of one leaf and one internal node: the heap growth
/// of a 1-entry and a 12-entry map (two leaves and a root) minus the key and value buffers.
fn node_sizes() {
    fn probe<T>(name: &str, make: impl Fn(u8) -> (T, Mem)) {
        let b = Mem::now();
        let (one, kv1) = make(1);
        let g1 = Mem::now().since(b);
        drop(one);
        let b = Mem::now();
        let (twelve, kv12) = make(12);
        let g12 = Mem::now().since(b);
        drop(twelve);
        let (leaf_req, leaf_act) = (g1.req - kv1.req, g1.act - kv1.act);
        println!(
            "  {name}: leaf {leaf_req} -> {leaf_act} B, internal {} -> {} B",
            g12.req - kv12.req - 2 * leaf_req,
            g12.act - kv12.act - 2 * leaf_act
        );
    }
    fn kv(bufs: &[&[u8]]) -> Mem {
        Mem {
            req: bufs.iter().map(|b| b.len()).sum(),
            act: bufs.iter().map(|b| msize(b.as_ptr())).sum(),
            ..Mem::default()
        }
    }
    probe("BTreeMap<Vec<u8>,Vec<u8>> (MemTree)", |n| {
        let m: BTreeMap<Vec<u8>, Vec<u8>> = (0..n).map(|i| (vec![i], Vec::new())).collect();
        let bufs: Vec<&[u8]> = m.keys().map(Vec::as_slice).collect();
        let k = kv(&bufs);
        drop(bufs);
        (m, k)
    });
    probe("BTreeMap<Box<[u8]>,Box<[u8]>>", |n| {
        let m: BTreeMap<Box<[u8]>, Box<[u8]>> =
            (0..n).map(|i| (vec![i].into_boxed_slice(), Box::default())).collect();
        let bufs: Vec<&[u8]> = m.keys().map(|k| &**k).collect();
        let k = kv(&bufs);
        drop(bufs);
        (m, k)
    });
    probe("BTreeSet<OneAlloc>", |n| {
        let m: BTreeSet<OneAlloc> = (0..n).map(|i| OneAlloc::new(&[i], &[])).collect();
        let bufs: Vec<&[u8]> = m.iter().map(|e| &*e.0).collect();
        let k = kv(&bufs);
        drop(bufs);
        (m, k)
    });
}

struct Repr {
    name: &'static str,
    act: f64,
    glibc: f64,
    /// Mutable and built by the same create-order inserts as the MemTree (bulk-built trees lose
    /// their packed nodes as soon as inserts split them).
    steady_mutable: bool,
}

fn tenm(args: &[String]) {
    let files: u64 = args[0].parse().unwrap();
    let names = Names::parse(&args[1]);
    let m = Model {
        names: names.clone(),
        xattrs: 0,
        seed: 42,
    };
    let (counted, plain) = alloc_overhead_ns();
    println!(
        "\n## {files} files, names {}, attrs + dirent, create order, one process",
        names.label()
    );
    println!(
        "  counting allocator cost: alloc+free of 32 B {counted:.1} ns, plain System {plain:.1} ns \
         (every timing below includes it)"
    );
    node_sizes();
    let plan = plan(files, m.seed);
    let base = Mem::now();
    let t = Instant::now();
    let mut tree = MemTree::new();
    let st = generate(&m, &plan, |k, v| tree.insert(k, v));
    let build = us(t);
    let tm = Mem::now().since(base);
    let f = st.files as f64;
    let e = st.entries as f64;
    println!(
        "  built in {:.0} us ({:.2} us/entry): {} files, {} dirs, {} entries; logical {:.1} B/file; \
         heap req {:.1} act {:.1} glibc {:.1} jem {:.1} B/file; resident {} MB, phys_footprint {} MB",
        build,
        build / e,
        st.files,
        st.dirs,
        st.entries,
        (st.key_bytes + st.val_bytes) as f64 / f,
        tm.req as f64 / f,
        tm.act as f64 / f,
        tm.glibc as f64 / f,
        tm.jem as f64 / f,
        rss().resident >> 20,
        rss().footprint >> 20
    );

    println!("\n## F7 point get and 1,000-entry listing on the MemTree");
    let keys = sample_keys(&tree, 100_000);
    let big = dirent_prefix(BIG_DIR);
    let s = bench(
        &keys,
        |k| tree.get(k).map(<[u8]>::len),
        || tree.scan(&big).count(),
    );
    print_speed("MemTree", &s);
    check(
        "F7 get p50 vs redb 7.2 us",
        s.get_p50 < 7.2,
        &format!("{:.2} us (p99 {:.2})", s.get_p50, s.get_p99),
        "< 7.2 us",
    );
    check(
        "F7 1000-entry listing p50 vs redb 196 us",
        s.list_p50 < 196.0,
        &format!("{:.1} us (p99 {:.1})", s.list_p50, s.list_p99),
        "< 196 us",
    );
    let mut plus = Vec::with_capacity(200);
    for _ in 0..200 {
        let t = Instant::now();
        let mut n = 0;
        for (_, v) in tree.scan(&big) {
            let child = u64::from_be_bytes(v[..8].try_into().unwrap());
            n += tree.get(&key(child, TAG_ATTR, &[])).unwrap().len();
        }
        black_box(n);
        plus.push(t.elapsed().as_nanos() as u64);
    }
    println!(
        "  readdirplus (listing + 1,000 attribute gets): p50 {:.1} us p99 {:.1} us",
        pct(&mut plus, 50),
        pct(&mut plus, 99)
    );
    let speed_memtree = s;

    println!("\n## F5/F6 checkpoint and restore");
    let max_piece = max_blob_bytes();
    let before = Mem::now();
    reset_peak();
    let t = Instant::now();
    let pieces = tree.checkpoint(max_piece);
    let ckpt = us(t);
    let (pk_req, pk_act) = peak();
    let pm = Mem::now().since(before);
    let ser: usize = pieces.iter().map(Vec::len).sum();
    let ratio_act = (tm.act + pk_act - before.act) as f64 / tm.act as f64;
    let ratio_req = (tm.req + pk_req - before.req) as f64 / tm.req as f64;
    println!(
        "  max_piece {max_piece} B (default max_record_bytes 1 MiB); {} pieces, {} B serialized = \
         {:.1} B/file, {:.1} B/entry; piece capacity {} B = {:.3}x their length; pieces hold act {} B ({:.1} B/file)",
        pieces.len(),
        ser,
        ser as f64 / f,
        ser as f64 / e,
        pieces.iter().map(Vec::capacity).sum::<usize>(),
        pieces.iter().map(Vec::capacity).sum::<usize>() as f64 / ser as f64,
        pm.act,
        pm.act as f64 / f
    );
    println!(
        "  F6 checkpoint {:.0} us = {:.0} ns/entry, {:.0} MB/s",
        ckpt,
        ckpt * 1000.0 / e,
        ser as f64 / ckpt
    );
    println!(
        "  derived: with pieces allocated at their final length the peak would be {:.3}x (1 + serialized / tree)",
        1.0 + ser as f64 / tm.act as f64
    );
    check(
        "F5 peak live heap during checkpoint / tree (act)",
        (1.8..=2.2).contains(&ratio_act),
        &format!("{ratio_act:.3}x (req {ratio_req:.3}x), peak above tree {:.1} B/file", (pk_act - before.act) as f64 / f),
        "1.8..=2.2x",
    );
    let mut restored = MemTree::new();
    let before = Mem::now();
    reset_peak();
    let t = Instant::now();
    restored.restore(&pieces).unwrap();
    let rst = us(t);
    let (_, pk_act) = peak();
    let rm = Mem::now().since(before);
    assert_eq!(restored.len(), tree.len());
    let rate = e / rst * 1e6;
    check(
        "F6 restore rate",
        rate > 1e6,
        &format!("{:.2}M entries/s ({:.0} us for {} entries, {:.0} ns/entry)", rate / 1e6, rst, st.entries, rst * 1000.0 / e),
        "> 1M entries/s",
    );
    println!(
        "  restored tree: act {:.1} B/file (create-order tree {:.1}, ratio {:.3}); req {:.1}; \
         peak during restore = original tree + pieces + new tree = {:.2}x the original tree",
        rm.act as f64 / f,
        tm.act as f64 / f,
        rm.act as f64 / tm.act as f64,
        rm.req as f64 / f,
        (pk_act - base.act) as f64 / tm.act as f64
    );
    let sr = bench(
        &keys,
        |k| restored.get(k).map(<[u8]>::len),
        || restored.scan(&big).count(),
    );
    print_speed("restored MemTree", &sr);
    let restored_act = rm.act as f64 / f;
    let restored_glibc = rm.glibc as f64 / f;
    drop(restored);
    drop(pieces);

    println!("\n## F4 representations of the same map (memory measured while the MemTree stays alive)");
    let mut reprs = vec![
        Repr {
            name: "MemTree, BTreeMap<Vec,Vec>, create order",
            act: tm.act as f64 / f,
            glibc: tm.glibc as f64 / f,
            steady_mutable: true,
        },
        Repr {
            name: "MemTree after checkpoint+restore (sorted inserts)",
            act: restored_act,
            glibc: restored_glibc,
            steady_mutable: false,
        },
    ];
    let line = |name: &str, build: f64, mem: Mem, s: &Speed| {
        println!(
            "  {name}: build {:.0} us; req {:.1} act {:.1} glibc {:.1} jem {:.1} B/file, {:.2} allocs/file",
            build,
            mem.req as f64 / f,
            mem.act as f64 / f,
            mem.glibc as f64 / f,
            mem.jem as f64 / f,
            mem.allocs as f64 / f
        );
        print_speed(name, s);
    };
    print_speed("MemTree (baseline, from F7)", &speed_memtree);

    {
        let b = Mem::now();
        let t = Instant::now();
        let map: BTreeMap<Vec<u8>, Vec<u8>> =
            tree.scan(&[]).map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        let build = us(t);
        let mem = Mem::now().since(b);
        let s = bench(&keys, |k| map.get(k).map(Vec::len), || map.range(big.clone()..).take_while(|(k, _)| k.starts_with(&big)).count());
        let name = "BTreeMap<Vec,Vec> bulk-built (FromIterator on sorted input)";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: false });
    }
    {
        let b = Mem::now();
        let t = Instant::now();
        let mut map: BTreeMap<Box<[u8]>, Box<[u8]>> = BTreeMap::new();
        generate(&m, &plan, |k, v| {
            map.insert(k.into_boxed_slice(), v.into_boxed_slice());
        });
        let build = us(t);
        let mem = Mem::now().since(b);
        let bp: Box<[u8]> = big.clone().into_boxed_slice();
        let s = bench(&keys, |k| map.get(k).map(|v| v.len()), || map.range(bp.clone()..).take_while(|(k, _)| k.starts_with(&big)).count());
        let name = "BTreeMap<Box<[u8]>,Box<[u8]>>, create order";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: true });
    }
    {
        let b = Mem::now();
        let t = Instant::now();
        let mut set: BTreeSet<OneAlloc> = BTreeSet::new();
        generate(&m, &plan, |k, v| {
            set.insert(OneAlloc::new(&k, &v));
        });
        let build = us(t);
        let mem = Mem::now().since(b);
        for (_, k) in &keys {
            assert_eq!(set.get(k.as_slice()).map(OneAlloc::value), tree.get(k));
        }
        let s = bench(
            &keys,
            |k| set.get(k).map(|e| e.value().len()),
            || {
                set.range::<[u8], _>((std::ops::Bound::Included(big.as_slice()), std::ops::Bound::Unbounded))
                    .take_while(|e| e.key().starts_with(&big))
                    .count()
            },
        );
        let name = "BTreeSet of one allocation per entry (klen|key|value), create order";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: true });
    }
    {
        let b = Mem::now();
        let t = Instant::now();
        let set: BTreeSet<OneAlloc> = tree.scan(&[]).map(|(k, v)| OneAlloc::new(k, v)).collect();
        let build = us(t);
        let mem = Mem::now().since(b);
        for (_, k) in &keys {
            assert_eq!(set.get(k.as_slice()).map(OneAlloc::value), tree.get(k));
        }
        let s = bench(
            &keys,
            |k| set.get(k).map(|e| e.value().len()),
            || {
                set.range::<[u8], _>((std::ops::Bound::Included(big.as_slice()), std::ops::Bound::Unbounded))
                    .take_while(|e| e.key().starts_with(&big))
                    .count()
            },
        );
        let name = "BTreeSet of one allocation per entry, bulk-built (FromIterator on sorted input)";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: false });
    }
    {
        let b = Mem::now();
        let t = Instant::now();
        let p = Packed::build(&tree);
        let build = us(t);
        let mem = Mem::now().since(b);
        for (_, k) in &keys {
            assert_eq!(p.get(k), tree.get(k));
        }
        let s = bench(&keys, |k| p.get(k).map(<[u8]>::len), || {
            let mut n = 0;
            p.scan(&big, |_, _| n += 1);
            n
        });
        let name = "Packed arena + sorted u32 offsets (immutable)";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: false });
    }
    {
        let b = Mem::now();
        let t = Instant::now();
        let p = Prefixed::build(&tree);
        let build = us(t);
        let mem = Mem::now().since(b);
        for (_, k) in &keys {
            assert_eq!(p.get(k), tree.get(k));
        }
        let mut listed = Vec::new();
        p.scan(&big, |k, v| listed.push((k.to_vec(), v.to_vec())));
        let want: Vec<(Vec<u8>, Vec<u8>)> = tree.scan(&big).map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        assert_eq!(listed, want);
        drop((listed, want));
        let s = bench(&keys, |k| p.get(k).map(<[u8]>::len), || {
            let mut n = 0;
            p.scan(&big, |_, _| n += 1);
            n
        });
        let name = "Prefix-compressed blocks of 16, restart offsets (immutable)";
        line(name, build, mem, &s);
        reprs.push(Repr { name, act: mem.act as f64 / f, glibc: mem.glibc as f64 / f, steady_mutable: false });
    }
    println!("  (every alternative returned the MemTree's value for all {} sampled keys)", keys.len());
    let mt = reprs[0].act;
    let best_mut = reprs[2..].iter().filter(|r| r.steady_mutable).min_by(|a, b| a.act.total_cmp(&b.act)).unwrap();
    let best_all = reprs[2..].iter().min_by(|a, b| a.act.total_cmp(&b.act)).unwrap();
    check(
        "F4 best mutable alternative built in create order / MemTree (act)",
        (0.5..1.0).contains(&(best_mut.act / mt)),
        &format!("{:.3} ({}: {:.1} vs {:.1} B/file)", best_mut.act / mt, best_mut.name, best_mut.act, mt),
        "0.5..1.0",
    );
    check(
        "F4 best of all / MemTree (act)",
        (0.5..1.0).contains(&(best_all.act / mt)),
        &format!("{:.3} ({}: {:.1} vs {:.1} B/file)", best_all.act / mt, best_all.name, best_all.act, mt),
        "0.5..1.0",
    );

    println!("\n## F8 projection: files per node by MetaStore RAM budget (bytes/file measured at {files} files)");
    println!(
        "  A node holds one full replica of each shard it is in; followers keep the same tree as the \
         leader. 'replicas' = file replicas that fit in the budget; 'distinct' = files per node of \
         cluster capacity with 3 replicas = replicas / 3. Not counted: Raft entries awaiting \
         checkpoint, replay records, caches, the transient checkpoint peak (shown separately)."
    );
    println!("  | Representation | B/file | 4 GiB replicas / distinct | 16 GiB | 64 GiB |");
    let gib = (1u64 << 30) as f64;
    let row = |name: &str, bpf: f64| {
        let cell = |g: f64| format!("{:.1}M / {:.1}M", g * gib / bpf / 1e6, g * gib / bpf / 3e6);
        println!("  | {name} | {bpf:.0} | {} | {} | {} |", cell(4.0), cell(16.0), cell(64.0));
    };
    row("MemTree, macOS malloc (measured)", mt);
    row("MemTree, glibc model", reprs[0].glibc);
    row(
        &format!("MemTree + checkpoint peak ({ratio_act:.2}x)"),
        mt * ratio_act,
    );
    row(&format!("{} (best mutable)", best_mut.name), best_mut.act);
    row(&format!("{} (best mutable), glibc model", best_mut.name), best_mut.glibc);
    row(&format!("{} (best)", best_all.name), best_all.act);
}

// ---------------------------------------------------------------- name length sampler

fn sample_names(args: &[String]) {
    let mut hist: BTreeMap<usize, u64> = BTreeMap::new();
    let mut stack: Vec<std::path::PathBuf> = args.iter().map(Into::into).collect();
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else { continue };
        for ent in rd.flatten() {
            *hist.entry(ent.file_name().len()).or_default() += 1;
            if ent.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push(ent.path());
            }
        }
    }
    let total: u64 = hist.values().sum();
    let mean = hist.iter().map(|(l, c)| *l as u64 * c).sum::<u64>() as f64 / total as f64;
    let mut acc = 0;
    let (mut p50, mut p99) = (0, 0);
    for (&l, &c) in &hist {
        acc += c;
        if p50 == 0 && acc * 2 >= total {
            p50 = l;
        }
        if p99 == 0 && acc * 100 >= total * 99 {
            p99 = l;
        }
    }
    println!(
        "# name lengths in bytes (entries of every kind, symlinks not followed) under {}; {total} names, mean {mean:.2}, p50 {p50}, p99 {p99}",
        args.join(" ")
    );
    println!("# length count");
    for (l, c) in hist {
        println!("{l} {c}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args[0].as_str() {
        "info" => println!("{}", args[1..].join(" ")),
        "sample-names" => sample_names(&args[1..]),
        "measure" => measure(&args[1..]),
        "f1" => f1(&args[1]),
        "f2" => f2(&args[1]),
        "f3" => f3(&args[1]),
        "big" => tenm(&args[1..]),
        _ => panic!("unknown command"),
    }
}
