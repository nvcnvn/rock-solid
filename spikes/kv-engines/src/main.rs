// Throwaway spike harness: redb vs fjall vs a plain append-only file for Raft logs,
// MetaStore shard state and the per-disk chunk index. See README.md.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fjall::{AbstractTree, KeyspaceCreateOptions, PersistMode, Readable};
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
use redb::{Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

const MIB: u64 = 1024 * 1024;
const LOG_T: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const KV_T: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

// ---------------------------------------------------------------- measurement

fn proc_io() -> (u64, u64, u64) {
    let s = fs::read_to_string("/proc/self/io").unwrap();
    let f = |name: &str| -> u64 {
        s.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap()
            .parse()
            .unwrap()
    };
    (
        f("read_bytes:"),
        f("write_bytes:"),
        f("cancelled_write_bytes:"),
    )
}

fn cpu_us() -> u64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as u64 * 1_000_000 + t.tv_usec as u64;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// (sectors written, flush requests completed) of the whole root NVMe; includes other processes.
fn diskstats() -> (u64, u64) {
    let s = fs::read_to_string("/proc/diskstats").unwrap();
    for l in s.lines() {
        let t: Vec<&str> = l.split_whitespace().collect();
        if t.len() > 18 && t[2] == "nvme0n1" {
            return (t[9].parse().unwrap(), t[18].parse().unwrap());
        }
    }
    (0, 0)
}

struct Meter {
    t0: Instant,
    io0: (u64, u64, u64),
    cpu0: u64,
    dev0: (u64, u64),
}

#[derive(Clone, Copy)]
struct Measured {
    secs: f64,
    wb: u64,
    rb: u64,
    cpu_us: u64,
    dev_wb: u64,
    dev_flush: u64,
}

impl Meter {
    fn start() -> Meter {
        Meter {
            t0: Instant::now(),
            io0: proc_io(),
            cpu0: cpu_us(),
            dev0: diskstats(),
        }
    }
    fn stop(&self) -> Measured {
        let io = proc_io();
        let d = diskstats();
        Measured {
            secs: self.t0.elapsed().as_secs_f64(),
            rb: io.0 - self.io0.0,
            wb: (io.1 - self.io0.1).saturating_sub(io.2 - self.io0.2),
            cpu_us: cpu_us() - self.cpu0,
            dev_wb: (d.0 - self.dev0.0) * 512,
            dev_flush: d.1 - self.dev0.1,
        }
    }
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn status_kb(field: &str) -> u64 {
    let s = fs::read_to_string("/proc/self/status").unwrap();
    s.lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .map(|v| v.parse().unwrap())
        .unwrap_or(0)
}

fn rss_mb() -> f64 {
    status_kb("VmRSS:") as f64 / 1024.0
}

fn hwm_mb() -> f64 {
    status_kb("VmHWM:") as f64 / 1024.0
}

fn fd_count() -> usize {
    fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

fn loadavg() -> String {
    let s = fs::read_to_string("/proc/loadavg").unwrap();
    s.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

fn walk(p: &Path, f: &mut dyn FnMut(&Path, &fs::Metadata)) {
    if let Ok(rd) = fs::read_dir(p) {
        for e in rd.flatten() {
            let path = e.path();
            if let Ok(m) = fs::symlink_metadata(&path) {
                if m.is_dir() {
                    walk(&path, f);
                } else if m.is_file() {
                    f(&path, &m);
                }
            }
        }
    } else if let Ok(m) = fs::metadata(p) {
        f(p, &m);
    }
}

/// Allocated bytes (st_blocks), which is what the disk really holds.
fn du(p: &Path) -> u64 {
    let mut t = 0;
    walk(p, &mut |_, m| t += m.blocks() * 512);
    t
}

/// Drop the page cache of every file under `p` (clean pages only; no root needed).
fn drop_cache(p: &Path) {
    walk(p, &mut |path, _| {
        if let Ok(f) = File::open(path) {
            use std::os::fd::AsRawFd;
            unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        }
    });
}

/// Allocated bytes in journal files (*.jnl) under `p`; the rest is tables and metadata.
fn jnl_bytes(p: &Path) -> u64 {
    let mut t = 0;
    walk(p, &mut |path, m| {
        if path.extension().is_some_and(|x| x == "jnl") {
            t += m.blocks() * 512
        }
    });
    t
}

fn fresh(dir: &Path) -> PathBuf {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();
    dir.to_path_buf()
}

fn header(test: &str) {
    println!("\n## {test}  loadavg={}  rss={:.0}MB", loadavg(), rss_mb());
}

fn check(id: &str, ok: bool, got: impl std::fmt::Display, want: &str) {
    println!(
        "[{}] {id}: got={got} want={want}",
        if ok { "OK " } else { "FAIL" }
    );
}

fn mb(b: u64) -> f64 {
    b as f64 / MIB as f64
}

fn fsync_dir(p: &Path) {
    File::open(p).unwrap().sync_all().unwrap();
}

// ---------------------------------------------------------------- engines

fn redb_open(path: &Path, cache: u64) -> redb::Database {
    let mut b = redb::Database::builder();
    b.set_cache_size(cache as usize);
    b.create(path).unwrap()
}

fn fjall_open(dir: &Path, cache: u64) -> fjall::Database {
    fjall::Database::builder(dir)
        .cache_size(cache)
        .open()
        .unwrap()
}

/// Wait until fjall has no flush or compaction in flight (checked a few times in a row).
fn fjall_settle(db: &fjall::Database) {
    let mut quiet = 0;
    while quiet < 5 {
        if db.outstanding_flushes() == 0 && db.active_compactions() == 0 {
            quiet += 1;
        } else {
            quiet = 0;
        }
        std::thread::sleep(Duration::from_millis(40));
    }
}

fn fjall_mode(durable: bool) -> Option<PersistMode> {
    if durable {
        Some(PersistMode::SyncData)
    } else {
        None
    }
}

// ---------------------------------------------------------------- K1: Raft log

const CRC_POLY: u32 = 0x82f6_3b78;

fn crc32c(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let t = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ CRC_POLY
                } else {
                    c >> 1
                };
            }
            *e = c;
        }
        t
    });
    let mut c = !0u32;
    for &b in data {
        c = t[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

/// Segmented append-only log: frames of [len u32][crc32c u32][index u64][payload].
struct FileLog {
    dir: PathBuf,
    seg_size: u64,
    prealloc: bool,
    segs: Vec<(u64, u64)>, // (first index, segment id)
    cur: Option<File>,
    cur_id: u64,
    cur_off: u64,
    next_id: u64,
    ready: Vec<u64>, // pre-zeroed segment ids not yet used
}

impl FileLog {
    fn new(dir: &Path, seg_size: u64, prealloc: bool) -> FileLog {
        FileLog {
            dir: dir.to_path_buf(),
            seg_size,
            prealloc,
            segs: vec![],
            cur: None,
            cur_id: 0,
            cur_off: 0,
            next_id: 0,
            ready: vec![],
        }
    }
    fn seg_path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("{id:08}.seg"))
    }
    /// Pre-zero enough segments for `bytes` of log, outside the measured loop (recycled segments).
    fn prepare(&mut self, bytes: u64) {
        if !self.prealloc {
            return;
        }
        let n = bytes / self.seg_size + 2;
        // 4 KiB writes keep page-cache folios small: a write into a large folio is accounted
        // as the whole folio in /proc/self/io.
        let zeros = vec![0u8; 4096];
        for _ in 0..n {
            let id = self.next_id;
            self.next_id += 1;
            let mut f = File::create(self.seg_path(id)).unwrap();
            for _ in 0..self.seg_size / 4096 {
                f.write_all(&zeros).unwrap();
            }
            f.sync_all().unwrap();
            self.ready.push(id);
        }
        self.ready.reverse();
        fsync_dir(&self.dir);
    }
    fn roll(&mut self, first_index: u64) {
        let id = if self.prealloc {
            self.ready.pop().expect("prepare() too small")
        } else {
            let id = self.next_id;
            self.next_id += 1;
            id
        };
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.seg_path(id))
            .unwrap();
        if !self.prealloc {
            fsync_dir(&self.dir);
        }
        self.cur = Some(f);
        self.cur_id = id;
        self.cur_off = 0;
        self.segs.push((first_index, id));
    }
    fn append(&mut self, first: u64, entries: &[&[u8]]) {
        let mut buf = Vec::with_capacity(entries.iter().map(|e| e.len() + 16).sum());
        for (i, e) in entries.iter().enumerate() {
            let idx = first + i as u64;
            let mut body = Vec::with_capacity(e.len() + 8);
            body.extend_from_slice(&idx.to_be_bytes());
            body.extend_from_slice(e);
            buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crc32c(&body).to_le_bytes());
            buf.extend_from_slice(&body);
        }
        if self.cur.is_none() || self.cur_off + buf.len() as u64 > self.seg_size {
            self.roll(first);
        }
        let f = self.cur.as_ref().unwrap();
        f.write_all_at(&buf, self.cur_off).unwrap();
        self.cur_off += buf.len() as u64;
        f.sync_data().unwrap();
    }
    /// Unlink every segment whose entries are all below `n`.
    fn truncate_below(&mut self, n: u64) {
        let mut removed = false;
        while self.segs.len() > 1 && self.segs[1].0 <= n {
            let (_, id) = self.segs.remove(0);
            fs::remove_file(self.seg_path(id)).unwrap();
            removed = true;
        }
        if removed {
            fsync_dir(&self.dir);
        }
    }
    /// Recovery: read every frame of every segment in order, stop at the first bad frame.
    fn recover(dir: &Path) -> Vec<(u64, Vec<u8>)> {
        let mut ids: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .collect();
        ids.sort();
        let mut out = vec![];
        for p in ids {
            let mut data = vec![];
            File::open(&p).unwrap().read_to_end(&mut data).unwrap();
            let mut off = 0usize;
            while off + 8 <= data.len() {
                let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap());
                if len < 8 || off + 8 + len > data.len() {
                    break;
                }
                let body = &data[off + 8..off + 8 + len];
                if crc32c(body) != crc {
                    break;
                }
                let idx = u64::from_be_bytes(body[..8].try_into().unwrap());
                out.push((idx, body[8..].to_vec()));
                off += 8 + len;
            }
        }
        out
    }
}

enum Log {
    Redb(redb::Database),
    Fjall(fjall::Database, fjall::Keyspace),
    File(FileLog),
}

impl Log {
    fn open(kind: &str, dir: &Path) -> Log {
        match kind {
            "redb" => Log::Redb(redb_open(&dir.join("log.redb"), 64 * MIB)),
            "fjall" => {
                let db = fjall_open(dir, 64 * MIB);
                let ks = db.keyspace("log", KeyspaceCreateOptions::default).unwrap();
                Log::Fjall(db, ks)
            }
            "file-append" => Log::File(FileLog::new(dir, 16 * MIB, false)),
            "file-prealloc" => Log::File(FileLog::new(dir, 16 * MIB, true)),
            _ => panic!("{kind}"),
        }
    }
    fn prepare(&mut self, bytes: u64) {
        if let Log::File(f) = self {
            f.prepare(bytes)
        }
    }
    fn append(&mut self, first: u64, entries: &[&[u8]]) {
        match self {
            Log::Redb(db) => {
                let tx = db.begin_write().unwrap();
                {
                    let mut t = tx.open_table(LOG_T).unwrap();
                    for (i, e) in entries.iter().enumerate() {
                        t.insert(first + i as u64, *e).unwrap();
                    }
                }
                tx.commit().unwrap();
            }
            Log::Fjall(db, ks) => {
                let mut b = db.batch().durability(fjall_mode(true));
                for (i, e) in entries.iter().enumerate() {
                    b.insert(ks, (first + i as u64).to_be_bytes(), *e);
                }
                b.commit().unwrap();
            }
            Log::File(f) => f.append(first, entries),
        }
    }
    /// Durable prefix truncation. `first` is the current first index (Raft knows it).
    fn truncate_below(&mut self, first: u64, n: u64, fjall_drop_range: bool) {
        match self {
            Log::Redb(db) => {
                let tx = db.begin_write().unwrap();
                {
                    let mut t = tx.open_table(LOG_T).unwrap();
                    t.retain_in(..n, |_, _| false).unwrap();
                }
                tx.commit().unwrap();
            }
            Log::Fjall(db, ks) => {
                if fjall_drop_range {
                    ks.tree.drop_range::<[u8; 8], _>(..n.to_be_bytes()).unwrap();
                } else {
                    let mut b = db.batch().durability(fjall_mode(true));
                    for i in first..n {
                        b.remove(ks, i.to_be_bytes());
                    }
                    b.commit().unwrap();
                }
            }
            Log::File(f) => f.truncate_below(n),
        }
    }
    fn settle(&self) {
        if let Log::Fjall(db, _) = self {
            fjall_settle(db)
        }
    }
    /// Reclaim space the engine keeps after truncation (redb compact, fjall major compaction).
    fn reclaim(&mut self) {
        match self {
            Log::Redb(db) => {
                db.compact().unwrap();
            }
            Log::Fjall(db, ks) => {
                ks.rotate_memtable_and_wait().unwrap();
                ks.major_compact().unwrap();
                fjall_settle(db);
            }
            Log::File(_) => {}
        }
    }
    fn flush_memtable(&self) {
        if let Log::Fjall(db, ks) = self {
            ks.rotate_memtable_and_wait().unwrap();
            fjall_settle(db);
        }
    }
    fn count(&self) -> u64 {
        match self {
            Log::Redb(db) => db
                .begin_read()
                .unwrap()
                .open_table(LOG_T)
                .unwrap()
                .len()
                .unwrap(),
            Log::Fjall(_, ks) => ks.iter().count() as u64,
            Log::File(_) => 0,
        }
    }
}

const LOG_KINDS: [&str; 4] = ["file-prealloc", "file-append", "redb", "fjall"];

fn payloads(n: usize, size: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut r = SmallRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let mut v = vec![0u8; size];
            r.fill_bytes(&mut v);
            v
        })
        .collect()
}

fn k1(work: &Path, quick: bool) {
    header("K1 Raft log append: durable commit per batch");
    println!(
        "payload = random bytes; key = u64 index; wamp = /proc/self/io write_bytes / payload bytes"
    );
    println!(
        "devwamp = whole-disk sectors written / payload (includes ext4 journal and other processes)"
    );
    let pool = payloads(256, 4096, 1);
    let mut summary: HashMap<(String, usize, usize), (u64, f64, f64)> = HashMap::new();
    for &size in &[256usize, 4096] {
        for &batch in &[1usize, 16, 128] {
            let commits = match (batch, quick) {
                (1, false) => 2000,
                (16, false) => 1000,
                (_, false) => 300,
                (_, true) => 100,
            };
            for kind in LOG_KINDS {
                let dir = fresh(&work.join("k1"));
                let mut log = Log::open(kind, &dir);
                log.prepare((commits * batch * (size + 16)) as u64);
                let entries: Vec<&[u8]> =
                    (0..batch).map(|i| &pool[i % pool.len()][..size]).collect();
                let mut lat = Vec::with_capacity(commits);
                let m = Meter::start();
                let mut idx = 1u64;
                for _ in 0..commits {
                    let t = Instant::now();
                    log.append(idx, &entries);
                    lat.push(t.elapsed().as_micros() as u64);
                    idx += batch as u64;
                }
                let commit_phase = m.stop();
                log.settle();
                let total = m.stop();
                let payload = (commits * batch * size) as u64;
                let p50 = pct(&mut lat, 0.5);
                let p99 = pct(&mut lat, 0.99);
                println!(
                    "K1 {kind:13} size={size:4} batch={batch:3} commits={commits} p50={p50}us p99={p99}us entries/s={:.0} \
                     wamp_commit={:.2} wamp_total={:.2} devwamp={:.2} dev_flushes={} cpu/entry={:.1}us disk={:.1}MB",
                    (commits * batch) as f64 / commit_phase.secs,
                    commit_phase.wb as f64 / payload as f64,
                    total.wb as f64 / payload as f64,
                    total.dev_wb as f64 / payload as f64,
                    total.dev_flush,
                    total.cpu_us as f64 / (commits * batch) as f64,
                    mb(du(&dir)),
                );
                summary.insert(
                    (kind.to_string(), size, batch),
                    (
                        p50,
                        total.wb as f64 / payload as f64,
                        (commits * batch) as f64 / commit_phase.secs,
                    ),
                );
                drop(log);
            }
        }
    }
    let g = |k: &str, s, b| summary[&(k.to_string(), s, b)];
    for (s, b) in [(256, 1), (4096, 16)] {
        let floor = g("file-prealloc", s, b).0 as f64;
        for k in ["redb", "fjall"] {
            let r = g(k, s, b).0 as f64 / floor;
            check(
                &format!("K1 {k} p50 vs file floor size={s} batch={b}"),
                r < 3.0,
                format!("{r:.2}x"),
                "< 3x (hypothesis)",
            );
        }
    }
    let w = g("redb", 256, 1).1;
    check(
        "K1 redb wamp size=256 batch=1",
        w > 10.0,
        format!("{w:.1}"),
        "> 10 (CoW rewrites leaf+branch pages per commit)",
    );
    let w = g("fjall", 256, 128).1;
    check(
        "K1 fjall wamp_total size=256 batch=128",
        w < 3.0,
        format!("{w:.2}"),
        "< 3 (journal + one memtable flush)",
    );

    header("K1b prefix truncation after snapshot");
    let n_entries: u64 = if quick { 20_000 } else { 200_000 };
    let keep = n_entries / 10;
    let pl = payloads(128, 256, 2);
    let entries: Vec<&[u8]> = pl.iter().map(|v| v.as_slice()).collect();
    for kind in ["file-prealloc", "redb", "fjall", "fjall-droprange"] {
        let dir = fresh(&work.join("k1b"));
        let base = if kind == "fjall-droprange" {
            "fjall"
        } else {
            kind
        };
        let mut log = Log::open(base, &dir);
        if let Log::File(f) = &mut log {
            f.seg_size = 4 * MIB;
        }
        log.prepare(n_entries * 280);
        let mut idx = 1;
        while idx <= n_entries {
            log.append(idx, &entries);
            idx += entries.len() as u64;
        }
        log.flush_memtable();
        let before = du(&dir);
        let cut = idx - keep;
        let m = Meter::start();
        log.truncate_below(1, cut, kind == "fjall-droprange");
        let tr = m.stop();
        log.settle();
        let after = du(&dir);
        let m2 = Meter::start();
        log.reclaim();
        let rc = m2.stop();
        let after_reclaim = du(&dir);
        if base == "fjall" {
            let mut files = vec![];
            walk(&dir, &mut |p, m| {
                if m.len() > 64 * 1024 {
                    files.push(format!(
                        "{}={:.1}MB",
                        p.strip_prefix(&dir).unwrap().display(),
                        mb(m.blocks() * 512)
                    ));
                }
            });
            files.sort();
            println!(
                "K1b {kind:15} files>64KB after reclaim: {}",
                files.join(" ")
            );
        }
        println!(
            "K1b {kind:15} entries={n_entries} truncate_below={cut} truncate={:.1}ms written={:.2}MB disk_before={:.1}MB \
             disk_after={:.1}MB reclaim(compact)={:.0}ms reclaim_written={:.1}MB disk_after_reclaim={:.1}MB live_entries={}",
            tr.secs * 1e3,
            mb(tr.wb),
            mb(before),
            mb(after),
            rc.secs * 1e3,
            mb(rc.wb),
            mb(after_reclaim),
            log.count(),
        );
    }

    header(
        "K1c steady state: append 128 x 256 B per commit, truncate every 100 commits to the last 20k entries",
    );
    let commits = if quick { 300 } else { 4000 };
    for kind in ["file-prealloc", "redb", "fjall", "fjall-droprange"] {
        let dir = fresh(&work.join("k1c"));
        let base = if kind == "fjall-droprange" {
            "fjall"
        } else {
            kind
        };
        let mut log = Log::open(base, &dir);
        if let Log::File(f) = &mut log {
            f.seg_size = 4 * MIB;
        }
        log.prepare((commits * 128 * 280) as u64);
        let mut lat = vec![];
        let mut tlat = vec![];
        let m = Meter::start();
        let mut idx = 1u64;
        let mut first = 1u64;
        let mut max_disk = 0;
        for c in 0..commits {
            let t = Instant::now();
            log.append(idx, &entries);
            lat.push(t.elapsed().as_micros() as u64);
            idx += 128;
            if c % 100 == 99 && idx > 20_000 {
                let t = Instant::now();
                log.truncate_below(first, idx - 20_000, kind == "fjall-droprange");
                tlat.push(t.elapsed().as_micros() as u64);
                first = idx - 20_000;
                max_disk = max_disk.max(du(&dir));
            }
        }
        log.settle();
        let tot = m.stop();
        let payload = (commits * 128 * 256) as u64;
        println!(
            "K1c {kind:15} commits={commits} append_p50={}us append_p99={}us truncate_p50={}us truncate_max={}us \
             wamp={:.2} cpu/entry={:.1}us disk_max={:.1}MB disk_end={:.1}MB live={:.1}MB",
            pct(&mut lat, 0.5),
            pct(&mut lat, 0.99),
            pct(&mut tlat, 0.5),
            pct(&mut tlat, 1.0),
            tot.wb as f64 / payload as f64,
            tot.cpu_us as f64 / (commits * 128) as f64,
            mb(max_disk),
            mb(du(&dir)),
            mb(20_000 * 256),
        );
    }
    let _ = fs::remove_dir_all(work.join("k1"));
    let _ = fs::remove_dir_all(work.join("k1b"));
    let _ = fs::remove_dir_all(work.join("k1c"));
}

// ---------------------------------------------------------------- generic KV (K2, K3, K7)

enum Kv {
    Redb {
        db: redb::Database,
        rt: Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>,
        quick_repair: bool,
    },
    Fjall {
        db: fjall::Database,
        ks: fjall::Keyspace,
    },
}

impl Kv {
    fn open(kind: &str, dir: &Path, cache: u64) -> Kv {
        match kind {
            "redb" | "redb-qr" => {
                fs::create_dir_all(dir).unwrap();
                let db = redb_open(&dir.join("db.redb"), cache);
                {
                    let tx = db.begin_write().unwrap();
                    tx.open_table(KV_T).unwrap();
                    tx.commit().unwrap();
                }
                Kv::Redb {
                    db,
                    rt: None,
                    quick_repair: kind == "redb-qr",
                }
            }
            "fjall" => {
                let db = fjall_open(dir, cache);
                let ks = db.keyspace("kv", KeyspaceCreateOptions::default).unwrap();
                Kv::Fjall { db, ks }
            }
            _ => panic!("{kind}"),
        }
    }
    fn write(&mut self, puts: &[(Vec<u8>, Vec<u8>)], dels: &[Vec<u8>], durable: bool) {
        match self {
            Kv::Redb {
                db,
                rt,
                quick_repair,
            } => {
                *rt = None;
                let mut tx = db.begin_write().unwrap();
                tx.set_durability(if durable {
                    Durability::Immediate
                } else {
                    Durability::None
                })
                .unwrap();
                tx.set_quick_repair(*quick_repair);
                {
                    let mut t = tx.open_table(KV_T).unwrap();
                    for k in dels {
                        t.remove(k.as_slice()).unwrap();
                    }
                    for (k, v) in puts {
                        t.insert(k.as_slice(), v.as_slice()).unwrap();
                    }
                }
                tx.commit().unwrap();
            }
            Kv::Fjall { db, ks } => {
                let mut b = db.batch().durability(fjall_mode(durable));
                for k in dels {
                    b.remove(ks, k.as_slice());
                }
                for (k, v) in puts {
                    b.insert(ks, k.as_slice(), v.as_slice());
                }
                b.commit().unwrap();
            }
        }
    }
    fn redb_table(
        db: &redb::Database,
        rt: &mut Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>,
    ) -> redb::ReadOnlyTable<&'static [u8], &'static [u8]> {
        if rt.is_none() {
            *rt = Some(db.begin_read().unwrap().open_table(KV_T).unwrap());
        }
        rt.take().unwrap()
    }
    fn get(&mut self, k: &[u8]) -> Option<usize> {
        match self {
            Kv::Redb { db, rt, .. } => {
                let t = Self::redb_table(db, rt);
                let r = t.get(k).unwrap().map(|v| v.value().len());
                *rt = Some(t);
                r
            }
            Kv::Fjall { ks, .. } => ks.get(k).unwrap().map(|v| v.len()),
        }
    }
    fn scan(&mut self, prefix: &[u8]) -> usize {
        let mut end = prefix.to_vec();
        *end.last_mut().unwrap() += 1;
        match self {
            Kv::Redb { db, rt, .. } => {
                let t = Self::redb_table(db, rt);
                let mut n = 0;
                for e in t.range::<&[u8]>(prefix..end.as_slice()).unwrap() {
                    let (k, v) = e.unwrap();
                    n += (k.value().len() + v.value().len() > 0) as usize;
                }
                *rt = Some(t);
                n
            }
            Kv::Fjall { ks, .. } => ks.prefix(prefix).map(|g| g.into_inner().unwrap()).count(),
        }
    }
    /// Last key <= k (the extent that covers a block).
    fn floor(&mut self, k: &[u8]) -> bool {
        match self {
            Kv::Redb { db, rt, .. } => {
                let t = Self::redb_table(db, rt);
                let r = t.range::<&[u8]>(..=k).unwrap().next_back().is_some();
                *rt = Some(t);
                r
            }
            Kv::Fjall { ks, .. } => ks.range(..=k).next_back().is_some(),
        }
    }
    fn settle(&self) {
        if let Kv::Fjall { db, .. } = self {
            fjall_settle(db)
        }
    }
    /// fjall: move the memtable into an SST so that reads after reopen hit SSTs, not the journal.
    fn flush(&self) {
        if let Kv::Fjall { db, ks } = self {
            ks.rotate_memtable_and_wait().unwrap();
            fjall_settle(db)
        }
    }
    fn len(&mut self) -> u64 {
        match self {
            Kv::Redb { db, rt, .. } => {
                let t = Self::redb_table(db, rt);
                let n = t.len().unwrap();
                *rt = Some(t);
                n
            }
            Kv::Fjall { ks, .. } => ks.len().unwrap() as u64,
        }
    }
}

// ---------------------------------------------------------------- K2: MetaStore shard

fn tag_key(vol: u32, ino: u64, tag: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    k.push(b'F');
    k.extend_from_slice(&vol.to_be_bytes());
    k.extend_from_slice(&ino.to_be_bytes());
    k.push(tag);
    k
}

fn ino(slot: u64, seq: u64) -> u64 {
    (slot << 40) | seq
}

fn attr_val(r: &mut SmallRng) -> Vec<u8> {
    let mut v = vec![0u8; r.gen_range(100..=500)];
    r.fill_bytes(&mut v);
    v
}

fn k2(engine: &str, work: &Path, quick: bool) {
    let dirs: u64 = if quick { 50 } else { 500 };
    let files: u64 = 1000;
    let cache = 64 * MIB;
    header(&format!(
        "K2 MetaStore shard: {engine}, {dirs} dirs x {files} files (dentry + attr keys), cache {}MB",
        cache / MIB
    ));
    let dir = fresh(&work.join(format!("k2-{engine}")));
    let mut kv = Kv::open(engine, &dir, cache);
    let mut r = SmallRng::seed_from_u64(7);
    let mut order: Vec<u64> = (0..dirs).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, r.gen_range(0..=i));
    }
    let mut logical: u64 = 0;
    let mut batch: Vec<(Vec<u8>, Vec<u8>)> = vec![];
    let m = Meter::start();
    for &d in &order {
        let slot = d + 1;
        let dino = ino(slot, 1);
        let a = attr_val(&mut r);
        batch.push((tag_key(1, dino, b'a'), a));
        for f in 0..files {
            let fino = ino(slot, 2 + f);
            let mut dk = tag_key(1, dino, b'd');
            dk.extend_from_slice(format!("file-{f:06}-{:x}", r.gen_range(0..u32::MAX)).as_bytes());
            let mut dv = fino.to_be_bytes().to_vec();
            dv.extend_from_slice(&[1u8; 8]);
            batch.push((dk, dv));
            batch.push((tag_key(1, fino, b'a'), attr_val(&mut r)));
            if batch.len() >= 1024 {
                logical += batch
                    .iter()
                    .map(|(k, v)| (k.len() + v.len()) as u64)
                    .sum::<u64>();
                kv.write(&batch, &[], false);
                batch.clear();
            }
        }
    }
    logical += batch
        .iter()
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum::<u64>();
    kv.write(&batch, &[], true);
    batch.clear();
    kv.settle();
    let load = m.stop();
    let nkeys = kv.len();
    let disk_load = du(&dir);
    println!(
        "K2 {engine:6} load keys={nkeys} logical={:.1}MB time={:.1}s keys/s={:.0} wamp={:.2} disk={:.1}MB space_amp={:.2} rss={:.0}MB cpu/key={:.1}us",
        mb(logical),
        load.secs,
        nkeys as f64 / load.secs,
        load.wb as f64 / logical as f64,
        mb(disk_load),
        disk_load as f64 / logical as f64,
        rss_mb(),
        load.cpu_us as f64 / nkeys as f64
    );

    let rand_key = |r: &mut SmallRng| -> Vec<u8> {
        let slot = r.gen_range(0..dirs) + 1;
        tag_key(1, ino(slot, 2 + r.gen_range(0..files)), b'a')
    };
    for (mode, durable) in [("durable", true), ("apply-nosync+ckpt/100", false)] {
        let rounds = if quick { 300 } else { 3000 };
        let mut get_lat = vec![];
        let mut commit_lat = vec![];
        let mut put_bytes = 0u64;
        let mut ops = 0u64;
        let m = Meter::start();
        let mut miss = 0;
        for rd in 0..rounds {
            let mut puts = vec![];
            for _ in 0..64 {
                let k = rand_key(&mut r);
                if r.gen_bool(0.7) {
                    let t = Instant::now();
                    if kv.get(&k).is_none() {
                        miss += 1;
                    }
                    get_lat.push(t.elapsed().as_nanos() as u64);
                } else {
                    let v = attr_val(&mut r);
                    put_bytes += (k.len() + v.len()) as u64;
                    puts.push((k, v));
                }
                ops += 1;
            }
            let t = Instant::now();
            kv.write(&puts, &[], durable || rd % 100 == 99);
            commit_lat.push(t.elapsed().as_micros() as u64);
        }
        kv.settle();
        let mm = m.stop();
        println!(
            "K2 {engine:6} mixed70/30 {mode:22} rounds={rounds} ops/s={:.0} get_p50={:.1}us get_p99={:.1}us commit_p50={}us commit_p99={}us \
             wamp={:.2} devwamp={:.2} cpu/op={:.1}us misses={miss} rss={:.0}MB disk={:.1}MB",
            ops as f64 / mm.secs,
            pct(&mut get_lat, 0.5) as f64 / 1e3,
            pct(&mut get_lat, 0.99) as f64 / 1e3,
            pct(&mut commit_lat, 0.5),
            pct(&mut commit_lat, 0.99),
            mm.wb as f64 / put_bytes as f64,
            mm.dev_wb as f64 / put_bytes as f64,
            mm.cpu_us as f64 / ops as f64,
            rss_mb(),
            mb(du(&dir))
        );
    }

    let mut scan_lat = vec![];
    let mut n_ok = 0;
    for _ in 0..500 {
        let slot = r.gen_range(0..dirs) + 1;
        let p = tag_key(1, ino(slot, 1), b'd');
        let t = Instant::now();
        let n = kv.scan(&p);
        scan_lat.push(t.elapsed().as_micros() as u64);
        n_ok += (n == files as usize) as usize;
    }
    println!(
        "K2 {engine:6} readdir 1000 entries warm: p50={}us p99={}us",
        pct(&mut scan_lat, 0.5),
        pct(&mut scan_lat, 0.99)
    );
    check(
        &format!("K2 {engine} readdir returns 1000 entries"),
        n_ok == 500,
        format!("{n_ok}/500"),
        "500/500",
    );
    let disk_end = du(&dir);
    println!(
        "K2 {engine:6} disk_end={:.1}MB (journal {:.1}MB) space_amp={:.2} hwm={:.0}MB",
        mb(disk_end),
        mb(jnl_bytes(&dir)),
        disk_end as f64 / logical as f64,
        hwm_mb()
    );

    kv.settle();
    drop(kv);
    drop_cache(&dir);
    let sub_ms = open_in_subprocess(engine, &dir);
    drop_cache(&dir);
    let t = Instant::now();
    let kv = Kv::open(engine, &dir, cache);
    let open_unflushed_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "K2 {engine:6} reopen with unflushed journal (page cache dropped): open={open_unflushed_ms:.1}ms fresh_process_open={sub_ms:.1}ms disk_journal_included={:.1}MB",
        mb(du(&dir))
    );
    kv.flush();
    kv.settle();
    drop(kv);
    drop_cache(&dir);
    let sub_ms = open_in_subprocess(engine, &dir);
    drop_cache(&dir);
    let t = Instant::now();
    let mut kv = Kv::open(engine, &dir, cache);
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let mut cold = vec![];
    for _ in 0..200 {
        let k = rand_key(&mut r);
        let t = Instant::now();
        kv.get(&k);
        cold.push(t.elapsed().as_micros() as u64);
    }
    let slot = r.gen_range(0..dirs) + 1;
    let t = Instant::now();
    let n = kv.scan(&tag_key(1, ino(slot, 1), b'd'));
    let cold_scan = t.elapsed().as_micros();
    println!(
        "K2 {engine:6} cold open after memtable flush={open_ms:.1}ms fresh_process_open={sub_ms:.1}ms first-200-gets p50={}us p99={}us cold_readdir={cold_scan}us ({n} entries) rss_after_open={:.0}MB",
        pct(&mut cold, 0.5),
        pct(&mut cold, 0.99),
        rss_mb()
    );
    drop(kv);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- K3: chunk index

struct ChunkSet {
    h: Vec<u64>,
    id: Vec<[u8; 16]>,
}

const EXTENTS_PER_CHUNK: u64 = 10;

fn chunk_key(h: u64, id: &[u8; 16], version: u64, block: u16) -> Vec<u8> {
    let mut k = Vec::with_capacity(38);
    k.extend_from_slice(&1u32.to_be_bytes());
    k.extend_from_slice(&h.to_be_bytes());
    k.extend_from_slice(id);
    k.extend_from_slice(&version.to_be_bytes());
    k.extend_from_slice(&block.to_be_bytes());
    k
}

fn chunk_val(r: &mut SmallRng) -> Vec<u8> {
    let mut v = vec![0u8; 16];
    v[..4].copy_from_slice(&r.gen_range(0u32..60_000).to_be_bytes());
    v[4..8].copy_from_slice(&r.gen_range(0u32..16384).to_be_bytes());
    v[8..10].copy_from_slice(&r.gen_range(1u16..=100).to_be_bytes());
    v[12..16].copy_from_slice(&r.next_u32().to_be_bytes());
    v
}

fn chunkset(n_chunks: u64) -> ChunkSet {
    let mut r = SmallRng::seed_from_u64(11);
    let step = u64::MAX / n_chunks;
    let mut cs = ChunkSet {
        h: Vec::with_capacity(n_chunks as usize),
        id: Vec::with_capacity(n_chunks as usize),
    };
    for i in 0..n_chunks {
        cs.h.push(i * step + r.gen_range(0..step));
        let mut id = [0u8; 16];
        id[..4].copy_from_slice(&r.gen_range(1u32..64).to_be_bytes());
        id[4..12].copy_from_slice(&r.next_u64().to_be_bytes());
        id[12..].copy_from_slice(&r.gen_range(0u32..4096).to_be_bytes());
        cs.id.push(id);
    }
    cs
}

fn k3(engine: &str, work: &Path, quick: bool) {
    let n_chunks: u64 = if quick { 100_000 } else { 1_000_000 };
    let n = n_chunks * EXTENTS_PER_CHUNK;
    let cache = 64 * MIB;
    header(&format!(
        "K3 chunk index: {engine}, {n} entries (38 B key, 16 B value), cache {}MB",
        cache / MIB
    ));
    let dir = fresh(&work.join(format!("k3-{engine}")));
    let cs = chunkset(n_chunks);
    let mut kv = Kv::open(engine, &dir, cache);
    let mut r = SmallRng::seed_from_u64(12);
    let logical = n * 54;
    let m = Meter::start();
    let mut batch = Vec::with_capacity(100_000);
    for j in 0..n_chunks as usize {
        for e in 0..EXTENTS_PER_CHUNK {
            batch.push((
                chunk_key(cs.h[j], &cs.id[j], 1, (e * 100) as u16),
                chunk_val(&mut r),
            ));
        }
        if batch.len() >= 100_000 {
            kv.write(&batch, &[], false);
            batch.clear();
        }
    }
    kv.write(&batch, &[], true);
    batch.clear();
    kv.settle();
    let load = m.stop();
    let disk = du(&dir);
    println!(
        "K3 {engine:6} sorted bulk load (100k/commit) time={:.1}s entries/s={:.0} wamp={:.2} disk={:.1}MB space_amp={:.2} rss={:.0}MB cpu/entry={:.2}us",
        load.secs,
        n as f64 / load.secs,
        load.wb as f64 / logical as f64,
        mb(disk),
        disk as f64 / logical as f64,
        rss_mb(),
        load.cpu_us as f64 / n as f64
    );

    let mut version = 2u64;
    let total_updates: usize = if quick { 20_000 } else { 200_000 };
    for &bs in &[1_000usize, 10_000, 100_000] {
        if bs > total_updates {
            continue;
        }
        let commits = total_updates / bs;
        let mut lat = vec![];
        let m = Meter::start();
        for _ in 0..commits {
            let mut puts = Vec::with_capacity(bs);
            let mut dels = Vec::with_capacity(bs);
            for _ in 0..bs {
                let j = r.gen_range(0..n_chunks as usize);
                puts.push((
                    chunk_key(cs.h[j], &cs.id[j], version, r.gen_range(0..1024)),
                    chunk_val(&mut r),
                ));
                dels.push(chunk_key(
                    cs.h[j],
                    &cs.id[j],
                    1,
                    (r.gen_range(0..EXTENTS_PER_CHUNK) * 100) as u16,
                ));
            }
            version += 1;
            let t = Instant::now();
            kv.write(&puts, &dels, true);
            lat.push(t.elapsed().as_micros() as u64);
        }
        let commit_phase = m.stop();
        kv.settle();
        let tot = m.stop();
        let lb = (commits * bs * 54) as u64;
        println!(
            "K3 {engine:6} random updates batch={bs:6} (insert+delete per entry) commits={commits} commit_p50={:.1}ms commit_p99={:.1}ms \
             entries/s={:.0} wamp_commit={:.1} wamp_total={:.1} devwamp={:.1} written={:.0}MB cpu/entry={:.2}us",
            pct(&mut lat, 0.5) as f64 / 1e3,
            pct(&mut lat, 0.99) as f64 / 1e3,
            (commits * bs) as f64 / commit_phase.secs,
            commit_phase.wb as f64 / lb as f64,
            tot.wb as f64 / lb as f64,
            tot.dev_wb as f64 / lb as f64,
            mb(tot.wb),
            tot.cpu_us as f64 / (commits * bs) as f64
        );
    }
    let disk = du(&dir);
    let entries = kv.len();
    println!(
        "K3 {engine:6} after updates entries={entries} disk={:.1}MB (journal {:.1}MB) bytes/entry={:.1} rss={:.0}MB",
        mb(disk),
        mb(jnl_bytes(&dir)),
        disk as f64 / entries as f64,
        rss_mb()
    );

    let lookups = |kv: &mut Kv, r: &mut SmallRng, cnt: usize, label: &str| {
        let (mut hit, mut miss, mut flo) = (vec![], vec![], vec![]);
        let mut found = 0;
        for _ in 0..cnt {
            let j = r.gen_range(0..n_chunks as usize);
            let k = chunk_key(
                cs.h[j],
                &cs.id[j],
                1,
                (r.gen_range(0..EXTENTS_PER_CHUNK) * 100) as u16,
            );
            let t = Instant::now();
            found += kv.get(&k).is_some() as usize;
            hit.push(t.elapsed().as_nanos() as u64);
            let k = chunk_key(cs.h[j], &cs.id[j], 0, 7);
            let t = Instant::now();
            kv.get(&k);
            miss.push(t.elapsed().as_nanos() as u64);
            let k = chunk_key(cs.h[j], &cs.id[j], 1, r.gen_range(0..1024));
            let t = Instant::now();
            kv.floor(&k);
            flo.push(t.elapsed().as_nanos() as u64);
        }
        println!(
            "K3 {engine:6} lookups {label:5} n={cnt} get_hit p50={:.1}us p99={:.1}us  get_miss p50={:.1}us p99={:.1}us  floor_seek p50={:.1}us p99={:.1}us found={:.1}%",
            pct(&mut hit, 0.5) as f64 / 1e3,
            pct(&mut hit, 0.99) as f64 / 1e3,
            pct(&mut miss, 0.5) as f64 / 1e3,
            pct(&mut miss, 0.99) as f64 / 1e3,
            pct(&mut flo, 0.5) as f64 / 1e3,
            pct(&mut flo, 0.99) as f64 / 1e3,
            found as f64 * 100.0 / cnt as f64
        );
    };
    lookups(&mut kv, &mut r, 100_000, "warm");
    println!(
        "K3 {engine:6} rss_after_lookups={:.0}MB hwm={:.0}MB",
        rss_mb(),
        hwm_mb()
    );
    kv.settle();
    drop(kv);
    drop_cache(&dir);
    let sub_ms = open_in_subprocess(engine, &dir);
    drop_cache(&dir);
    let t = Instant::now();
    let kv = Kv::open(engine, &dir, cache);
    println!(
        "K3 {engine:6} reopen with unflushed journal (page cache dropped) open={:.1}ms fresh_process_open={sub_ms:.1}ms disk={:.1}MB",
        t.elapsed().as_secs_f64() * 1e3,
        mb(du(&dir))
    );
    kv.flush();
    kv.settle();
    drop(kv);
    drop_cache(&dir);
    let sub_ms = open_in_subprocess(engine, &dir);
    drop_cache(&dir);
    let t = Instant::now();
    let mut kv = Kv::open(engine, &dir, cache);
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    println!(
        "K3 {engine:6} reopen after memtable flush (page cache dropped) open={open_ms:.1}ms fresh_process_open={sub_ms:.1}ms rss={:.0}MB",
        rss_mb()
    );
    lookups(&mut kv, &mut r, 2_000, "cold");
    drop(kv);

    header(&format!(
        "K5b crash recovery time on the K3 index ({engine}): kill -9 during durable 1k-entry commits"
    ));
    let variants: Vec<&str> = if engine == "redb" {
        vec!["redb", "redb-qr"]
    } else {
        vec![engine]
    };
    for v in variants {
        for it in 0..3 {
            let exe = std::env::current_exe().unwrap();
            let mut child = Command::new(exe)
                .args(["k3child", v, dir.to_str().unwrap(), &n_chunks.to_string()])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut out = BufReader::new(child.stdout.take().unwrap());
            let mut line = String::new();
            let mut acks = 0;
            let t = Instant::now();
            while acks < 5 {
                line.clear();
                if out.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                acks += line.starts_with('A') as usize;
            }
            let child_open_and_5_commits = t.elapsed().as_millis();
            std::thread::sleep(Duration::from_millis(r.gen_range(1..20)));
            child.kill().unwrap();
            child.wait().unwrap();
            drop_cache(&dir);
            let m = Meter::start();
            let kv = Kv::open(v, &dir, cache);
            let rec = m.stop();
            println!(
                "K5b {v:7} iter={it} child(open+5 commits)={child_open_and_5_commits}ms reopen_after_kill={:.0}ms read={:.0}MB written={:.1}MB",
                rec.secs * 1e3,
                mb(rec.rb),
                mb(rec.wb)
            );
            drop(kv);
        }
    }
    if std::env::var("KV_KEEP").is_err() {
        let _ = fs::remove_dir_all(&dir);
    }
}

/// Open time in a fresh process, so that no background work of an earlier instance overlaps it.
fn open_in_subprocess(engine: &str, dir: &Path) -> f64 {
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["open", engine, dir.to_str().unwrap()])
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim()
        .strip_prefix("open=")
        .and_then(|v| v.strip_suffix("ms"))
        .map(|v| v.parse().unwrap())
        .unwrap_or(-1.0)
}

fn k3child(engine: &str, dir: &Path, n_chunks: u64) {
    let cs = chunkset(n_chunks);
    let mut kv = Kv::open(engine, dir, 64 * MIB);
    let mut r = SmallRng::seed_from_u64(cpu_us());
    let mut version = 1_000_000;
    loop {
        let mut puts = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let j = r.gen_range(0..n_chunks as usize);
            puts.push((
                chunk_key(cs.h[j], &cs.id[j], version, r.gen_range(0..1024)),
                chunk_val(&mut r),
            ));
        }
        version += 1;
        kv.write(&puts, &[], true);
        println!("A {version}");
        std::io::stdout().flush().unwrap();
    }
}

// ---------------------------------------------------------------- K4: group commit across shards

fn k4(work: &Path, quick: bool) {
    header("K4 group commit across N=100 shards: one 256 B log entry per shard per round");
    let shards = 100usize;
    let entry = payloads(1, 256, 3).pop().unwrap();
    let rounds_one = if quick { 50 } else { 300 };
    let rounds_sep = if quick { 5 } else { 30 };
    let mut results: HashMap<&str, f64> = HashMap::new();
    let report = |name: &'static str,
                  lat: &mut Vec<u64>,
                  m: Measured,
                  rounds: usize,
                  results: &mut HashMap<&str, f64>| {
        let p50 = pct(lat, 0.5);
        println!(
            "K4 {name:28} rounds={rounds} round_p50={p50}us round_p99={}us shard-appends/s={:.0} written/round={:.1}KB cpu/round={:.0}us",
            pct(lat, 0.99),
            (rounds * shards) as f64 / m.secs,
            m.wb as f64 / rounds as f64 / 1024.0,
            m.cpu_us as f64 / rounds as f64
        );
        results.insert(name, p50 as f64);
    };

    // redb, one file, 100 tables
    {
        let dir = fresh(&work.join("k4"));
        let db = redb_open(&dir.join("db.redb"), 64 * MIB);
        let defs: Vec<String> = (0..shards).map(|s| format!("log{s}")).collect();
        let tdef = |s: usize| TableDefinition::<u64, &[u8]>::new(&defs[s]);
        for (name, one) in [
            ("redb one txn, 100 tables", true),
            ("redb 100 txns, 1 file", false),
        ] {
            let rounds = if one { rounds_one } else { rounds_sep };
            let mut lat = vec![];
            let m = Meter::start();
            for rd in 0..rounds as u64 {
                let t = Instant::now();
                if one {
                    let tx = db.begin_write().unwrap();
                    for s in 0..shards {
                        tx.open_table(tdef(s))
                            .unwrap()
                            .insert(rd, entry.as_slice())
                            .unwrap();
                    }
                    tx.commit().unwrap();
                } else {
                    for s in 0..shards {
                        let tx = db.begin_write().unwrap();
                        tx.open_table(tdef(s))
                            .unwrap()
                            .insert(rd, entry.as_slice())
                            .unwrap();
                        tx.commit().unwrap();
                    }
                }
                lat.push(t.elapsed().as_micros() as u64);
            }
            report(name, &mut lat, m.stop(), rounds, &mut results);
        }
    }
    // redb, 100 files, 100 threads committing in parallel
    {
        let dir = fresh(&work.join("k4"));
        let dbs: Vec<redb::Database> = (0..shards)
            .map(|s| redb_open(&dir.join(format!("s{s}.redb")), 4 * MIB))
            .collect();
        let lat = par_rounds(shards, rounds_sep, |s, rd| {
            let tx = dbs[s].begin_write().unwrap();
            tx.open_table(LOG_T)
                .unwrap()
                .insert(rd, entry.as_slice())
                .unwrap();
            tx.commit().unwrap();
        });
        report(
            "redb 100 files, 100 threads",
            &mut lat.0.clone(),
            lat.1,
            rounds_sep,
            &mut results,
        );
    }
    // fjall, one database, 100 keyspaces
    {
        let dir = fresh(&work.join("k4"));
        let db = fjall_open(&dir, 64 * MIB);
        let kss: Vec<fjall::Keyspace> = (0..shards)
            .map(|s| {
                db.keyspace(&format!("log{s}"), KeyspaceCreateOptions::default)
                    .unwrap()
            })
            .collect();
        for (name, one) in [
            ("fjall one batch, 100 keyspaces", true),
            ("fjall 100 batches, 1 db", false),
        ] {
            let rounds = if one { rounds_one } else { rounds_sep };
            let mut lat = vec![];
            let m = Meter::start();
            for rd in 0..rounds as u64 {
                let t = Instant::now();
                if one {
                    let mut b = db.batch().durability(fjall_mode(true));
                    for ks in &kss {
                        b.insert(ks, rd.to_be_bytes(), entry.as_slice());
                    }
                    b.commit().unwrap();
                } else {
                    for ks in &kss {
                        let mut b = db.batch().durability(fjall_mode(true));
                        b.insert(ks, rd.to_be_bytes(), entry.as_slice());
                        b.commit().unwrap();
                    }
                }
                lat.push(t.elapsed().as_micros() as u64);
            }
            report(name, &mut lat, m.stop(), rounds, &mut results);
        }
        let lat = par_rounds(shards, rounds_sep, |s, rd| {
            let mut b = db.batch().durability(fjall_mode(true));
            b.insert(&kss[s], (rd + 1_000_000).to_be_bytes(), entry.as_slice());
            b.commit().unwrap();
        });
        report(
            "fjall 100 threads, 1 db",
            &mut lat.0.clone(),
            lat.1,
            rounds_sep,
            &mut results,
        );
    }
    // plain files
    {
        let dir = fresh(&work.join("k4"));
        let mut one = FileLog::new(&dir.join("one"), 16 * MIB, true);
        fs::create_dir_all(&one.dir).unwrap();
        one.prepare((rounds_one * shards * 290) as u64);
        let entries: Vec<&[u8]> = (0..shards).map(|_| entry.as_slice()).collect();
        let mut lat = vec![];
        let m = Meter::start();
        for rd in 0..rounds_one as u64 {
            let t = Instant::now();
            one.append(rd * shards as u64, &entries);
            lat.push(t.elapsed().as_micros() as u64);
        }
        report(
            "file one fdatasync (shared)",
            &mut lat,
            m.stop(),
            rounds_one,
            &mut results,
        );
        let logs: Vec<std::sync::Mutex<FileLog>> = (0..shards)
            .map(|s| {
                let mut l = FileLog::new(&dir.join(format!("s{s}")), MIB, true);
                fs::create_dir_all(&l.dir).unwrap();
                l.prepare((rounds_sep * 290) as u64);
                std::sync::Mutex::new(l)
            })
            .collect();
        let lat = par_rounds(shards, rounds_sep, |s, rd| {
            logs[s].lock().unwrap().append(rd, &[entry.as_slice()]);
        });
        report(
            "file 100 files, 100 threads",
            &mut lat.0.clone(),
            lat.1,
            rounds_sep,
            &mut results,
        );
    }
    let r1 = results["redb 100 txns, 1 file"] / results["redb one txn, 100 tables"];
    check(
        "K4 redb: 100 separate commits vs one combined",
        r1 > 20.0,
        format!("{r1:.0}x slower"),
        "> 20x (each commit is one fsync)",
    );
    let r2 = results["fjall 100 batches, 1 db"] / results["fjall one batch, 100 keyspaces"];
    check(
        "K4 fjall: 100 separate batches vs one combined",
        r2 > 20.0,
        format!("{r2:.0}x slower"),
        "> 20x",
    );
    let r3 = results["fjall 100 threads, 1 db"] / results["fjall one batch, 100 keyspaces"];
    check(
        "K4 fjall: 100 concurrent SyncData batches are not coalesced into one fsync",
        r3 > 20.0,
        format!("{r3:.1}x of one batch"),
        "> 20x (journal mutex held across each fsync; hypothesis)",
    );
    let _ = fs::remove_dir_all(work.join("k4"));

    header("K4b 1000 shards in one process: open cost, RSS, file descriptors");
    for kind in [
        "redb 1 file 1000 tables",
        "redb 1000 files",
        "fjall 1 db 1000 keyspaces",
    ] {
        let dir = fresh(&work.join("k4b"));
        let rss0 = rss_mb();
        let fd0 = fd_count();
        let t = Instant::now();
        let n = 1000;
        let names: Vec<String> = (0..n).map(|s| format!("log{s}")).collect();
        let reopen_ms;
        match kind {
            "redb 1 file 1000 tables" => {
                {
                    let db = redb_open(&dir.join("db.redb"), 64 * MIB);
                    let tx = db.begin_write().unwrap();
                    for nm in &names {
                        let mut t = tx
                            .open_table(TableDefinition::<u64, &[u8]>::new(nm))
                            .unwrap();
                        for i in 0..10u64 {
                            t.insert(i, entry.as_slice()).unwrap();
                        }
                    }
                    tx.commit().unwrap();
                    println!(
                        "K4b {kind:26} create+10 entries each={:.0}ms rss_delta={:.0}MB fds_delta={}",
                        t.elapsed().as_secs_f64() * 1e3,
                        rss_mb() - rss0,
                        fd_count() - fd0
                    );
                }
                let t = Instant::now();
                let db = redb_open(&dir.join("db.redb"), 64 * MIB);
                let rt = db.begin_read().unwrap();
                let mut c = 0;
                for nm in &names {
                    c += rt
                        .open_table(TableDefinition::<u64, &[u8]>::new(nm))
                        .unwrap()
                        .len()
                        .unwrap();
                }
                reopen_ms = t.elapsed().as_secs_f64() * 1e3;
                assert_eq!(c, 10_000);
            }
            "redb 1000 files" => {
                {
                    let dbs: Vec<redb::Database> = names
                        .iter()
                        .map(|nm| redb_open(&dir.join(nm), 4 * MIB))
                        .collect();
                    for db in &dbs {
                        let tx = db.begin_write().unwrap();
                        let mut t = tx.open_table(LOG_T).unwrap();
                        for i in 0..10u64 {
                            t.insert(i, entry.as_slice()).unwrap();
                        }
                        drop(t);
                        tx.commit().unwrap();
                    }
                    println!(
                        "K4b {kind:26} create+10 entries each={:.0}ms rss_delta={:.0}MB fds_delta={}",
                        t.elapsed().as_secs_f64() * 1e3,
                        rss_mb() - rss0,
                        fd_count() - fd0
                    );
                }
                let t = Instant::now();
                let dbs: Vec<redb::Database> = names
                    .iter()
                    .map(|nm| redb_open(&dir.join(nm), 4 * MIB))
                    .collect();
                reopen_ms = t.elapsed().as_secs_f64() * 1e3;
                drop(dbs);
            }
            _ => {
                {
                    let db = fjall_open(&dir, 64 * MIB);
                    let kss: Vec<fjall::Keyspace> = names
                        .iter()
                        .map(|nm| db.keyspace(nm, KeyspaceCreateOptions::default).unwrap())
                        .collect();
                    let mut b = db.batch().durability(fjall_mode(true));
                    for ks in &kss {
                        for i in 0..10u64 {
                            b.insert(ks, i.to_be_bytes(), entry.as_slice());
                        }
                    }
                    b.commit().unwrap();
                    println!(
                        "K4b {kind:26} create+10 entries each={:.0}ms rss_delta={:.0}MB fds_delta={}",
                        t.elapsed().as_secs_f64() * 1e3,
                        rss_mb() - rss0,
                        fd_count() - fd0
                    );
                }
                let t = Instant::now();
                let db = fjall_open(&dir, 64 * MIB);
                let kss: Vec<fjall::Keyspace> = names
                    .iter()
                    .map(|nm| db.keyspace(nm, KeyspaceCreateOptions::default).unwrap())
                    .collect();
                reopen_ms = t.elapsed().as_secs_f64() * 1e3;
                let c: usize = kss.iter().map(|k| k.len().unwrap()).sum();
                assert_eq!(c, 10_000);
            }
        }
        println!(
            "K4b {kind:26} reopen={reopen_ms:.0}ms disk={:.1}MB",
            mb(du(&dir))
        );
    }
    let _ = fs::remove_dir_all(work.join("k4b"));
}

/// `shards` threads, `rounds` rounds; in each round every thread runs `f(shard, round)` once,
/// and the round's latency is from the barrier to the last thread finishing.
fn par_rounds(shards: usize, rounds: usize, f: impl Fn(usize, u64) + Sync) -> (Vec<u64>, Measured) {
    let start = Barrier::new(shards + 1);
    let end = Barrier::new(shards + 1);
    let mut lat = vec![];
    let m = Meter::start();
    std::thread::scope(|sc| {
        for s in 0..shards {
            let (start, end, f) = (&start, &end, &f);
            sc.spawn(move || {
                for rd in 0..rounds as u64 {
                    start.wait();
                    f(s, rd);
                    end.wait();
                }
            });
        }
        for _ in 0..rounds {
            start.wait();
            let t = Instant::now();
            end.wait();
            lat.push(t.elapsed().as_micros() as u64);
        }
    });
    (lat, m.stop())
}

// ---------------------------------------------------------------- K5: kill -9 loop

const K5_BATCH: u16 = 64;

fn k5_key(b: u64, i: u16) -> Vec<u8> {
    let mut k = b.to_be_bytes().to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn k5_val(b: u64, i: u16) -> Vec<u8> {
    let mut r = SmallRng::seed_from_u64(b.wrapping_mul(65_537) ^ i as u64);
    let mut v = vec![0u8; 256];
    r.fill_bytes(&mut v);
    v
}

/// Every (batch, entries) found in the store, with each entry checked against its expected value.
fn k5_scan(engine: &str, dir: &Path) -> (HashMap<u64, (u16, u16)>, f64) {
    let t = Instant::now();
    let mut found: HashMap<u64, (u16, u16)> = HashMap::new();
    let mut add = |k: &[u8], v: &[u8]| {
        let b = u64::from_be_bytes(k[..8].try_into().unwrap());
        let i = u16::from_be_bytes(k[8..10].try_into().unwrap());
        let e = found.entry(b).or_default();
        e.0 += 1;
        e.1 += (v == k5_val(b, i).as_slice()) as u16;
    };
    let open_ms;
    match engine {
        "file" => {
            let recs = FileLog::recover(dir);
            open_ms = t.elapsed().as_secs_f64() * 1e3;
            for (idx, v) in recs {
                add(
                    &k5_key(idx / K5_BATCH as u64, (idx % K5_BATCH as u64) as u16),
                    &v,
                );
            }
        }
        _ => {
            let mut kv = Kv::open(engine, dir, 16 * MIB);
            open_ms = t.elapsed().as_secs_f64() * 1e3;
            match &mut kv {
                Kv::Redb { db, .. } => {
                    let rt = db.begin_read().unwrap().open_table(KV_T).unwrap();
                    for e in rt.iter().unwrap() {
                        let (k, v) = e.unwrap();
                        add(k.value(), v.value());
                    }
                }
                Kv::Fjall { ks, .. } => {
                    for g in ks.iter() {
                        let (k, v) = g.into_inner().unwrap();
                        add(&k, &v);
                    }
                }
            }
        }
    }
    (found, open_ms)
}

fn k5child(engine: &str, dir: &Path) {
    let (found, _) = k5_scan(engine, dir);
    let mut b = found.keys().max().map(|m| m + 1).unwrap_or(0);
    let mut kv = if engine == "file" {
        None
    } else {
        Some(Kv::open(engine, dir, 16 * MIB))
    };
    let mut flog = FileLog::new(dir, 64 * MIB, false);
    flog.next_id = fs::read_dir(dir).unwrap().count() as u64 + 1;
    loop {
        let puts: Vec<(Vec<u8>, Vec<u8>)> = (0..K5_BATCH)
            .map(|i| (k5_key(b, i), k5_val(b, i)))
            .collect();
        match &mut kv {
            Some(kv) => kv.write(&puts, &[], true),
            None => {
                if flog.cur.is_none() {
                    flog.roll(b * K5_BATCH as u64);
                }
                let es: Vec<&[u8]> = puts.iter().map(|(_, v)| v.as_slice()).collect();
                flog.append(b * K5_BATCH as u64, &es);
            }
        }
        println!("A {b}");
        std::io::stdout().flush().unwrap();
        b += 1;
    }
}

fn k5(work: &Path, quick: bool) {
    header(
        "K5 crash safety: kill -9 a writer of 64-entry durable batches, reopen, verify (process crash only, not power loss)",
    );
    let iters = if quick { 10 } else { 50 };
    for engine in ["redb", "fjall", "file"] {
        let dir = fresh(&work.join(format!("k5-{engine}")));
        let mut r = SmallRng::seed_from_u64(5);
        let mut max_acked: i64 = -1;
        let (mut bad_ack, mut bad_atomic, mut bad_value, mut bad_future) = (0, 0, 0, 0);
        let mut opens = vec![];
        let mut unacked_present = 0;
        let mut max_present: i64 = -1;
        for _ in 0..iters {
            let start = max_present + 1;
            let exe = std::env::current_exe().unwrap();
            let mut child = Command::new(exe)
                .args(["k5child", engine, dir.to_str().unwrap()])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let out = child.stdout.take().unwrap();
            let last = Arc::new(AtomicU64::new(0));
            let l2 = last.clone();
            let reader = std::thread::spawn(move || {
                for line in BufReader::new(out).lines() {
                    let line = line.unwrap_or_default();
                    if let Some(b) = line.strip_prefix("A ") {
                        l2.store(b.parse::<u64>().unwrap() + 1, Ordering::SeqCst);
                    }
                }
            });
            std::thread::sleep(Duration::from_millis(r.gen_range(30..400)));
            child.kill().unwrap();
            child.wait().unwrap();
            reader.join().unwrap();
            let acked = last.load(Ordering::SeqCst) as i64 - 1;
            max_acked = max_acked.max(acked);
            // The child starts at `start`; at most the one batch after its last ack may be
            // committed without an ack.
            let allowed = acked.max(start - 1) + 1;
            let (found, open_ms) = k5_scan(engine, &dir);
            opens.push((open_ms * 1000.0) as u64);
            for b in 0..=max_acked.max(-1) {
                if b >= 0 && found.get(&(b as u64)).map(|e| e.0) != Some(K5_BATCH) {
                    bad_ack += 1;
                }
            }
            for (&b, &(n, ok)) in &found {
                if n != K5_BATCH {
                    bad_atomic += 1;
                }
                if ok != n {
                    bad_value += 1;
                }
                if b as i64 > allowed {
                    bad_future += 1;
                }
                if b as i64 == allowed && b as i64 > acked {
                    unacked_present += 1;
                }
                max_present = max_present.max(b as i64);
            }
        }
        println!(
            "K5 {engine:5} iterations={iters} batches_acked={} reopen p50={:.1}ms max={:.1}ms committed-but-unacked batches seen={unacked_present}",
            max_acked + 1,
            pct(&mut opens, 0.5) as f64 / 1e3,
            pct(&mut opens, 1.0) as f64 / 1e3
        );
        check(
            &format!("K5 {engine} every acknowledged batch present"),
            bad_ack == 0,
            format!("{bad_ack} missing"),
            "0",
        );
        check(
            &format!("K5 {engine} no partial batch visible"),
            bad_atomic == 0,
            format!("{bad_atomic} partial"),
            "0",
        );
        check(
            &format!("K5 {engine} values intact"),
            bad_value == 0,
            format!("{bad_value} corrupt"),
            "0",
        );
        check(
            &format!("K5 {engine} nothing beyond the in-flight batch"),
            bad_future == 0,
            format!("{bad_future}"),
            "0",
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

// ---------------------------------------------------------------- K5c: fjall open time vs journal size

fn k5c(work: &Path, quick: bool) {
    header(
        "K5c fjall open time (fresh process, page cache dropped) vs journal bytes; random 54 B index entries, non-durable 10k batches",
    );
    let targets: &[u64] = if quick { &[16] } else { &[16, 32, 64] };
    for &target in targets {
        let dir = fresh(&work.join("k5c"));
        let db = fjall_open(&dir, 64 * MIB);
        let ks = db.keyspace("kv", KeyspaceCreateOptions::default).unwrap();
        let mut r = SmallRng::seed_from_u64(target);
        let mut n = 0u64;
        while jnl_bytes(&dir) < target * MIB && n < 4_000_000 {
            let mut b = db.batch();
            for _ in 0..10_000 {
                let mut k = [0u8; 38];
                r.fill_bytes(&mut k);
                let mut v = [0u8; 16];
                r.fill_bytes(&mut v);
                b.insert(&ks, k, v);
            }
            b.commit().unwrap();
            n += 10_000;
        }
        db.persist(PersistMode::SyncAll).unwrap();
        fjall_settle(&db);
        let jnl = jnl_bytes(&dir);
        let journals = db.journal_count();
        drop(ks);
        drop(db);
        drop_cache(&dir);
        let cold = open_in_subprocess("fjall-raw", &dir);
        let warm = open_in_subprocess("fjall-raw", &dir);
        println!(
            "K5c fjall entries={n} journal={:.0}MB in {journals} files tables={:.0}MB open_cold={cold:.0}ms open_warm={warm:.0}ms",
            mb(jnl),
            mb(du(&dir) - jnl)
        );
    }
    let _ = fs::remove_dir_all(work.join("k5c"));
}

// ---------------------------------------------------------------- K7: long reader

fn k7(work: &Path, quick: bool) {
    header(
        "K7 long-lived reader during 100 durable commits of 1000 overwrites (100k keys x 200 B)",
    );
    let nkeys = 100_000u64;
    let commits = if quick { 50 } else { 100 };
    for engine in ["redb", "fjall"] {
        for with_reader in [false, true] {
            let dir = fresh(&work.join("k7"));
            let mut kv = Kv::open(engine, &dir, 64 * MIB);
            let mut r = SmallRng::seed_from_u64(9);
            let key = |i: u64| i.to_be_bytes().to_vec();
            let val = |r: &mut SmallRng| {
                let mut v = vec![0u8; 200];
                r.fill_bytes(&mut v);
                v
            };
            let mut probe_val = vec![];
            for c in 0..(nkeys / 10_000) {
                let puts: Vec<_> = (c * 10_000..(c + 1) * 10_000)
                    .map(|i| (key(i), val(&mut r)))
                    .collect();
                if c == 0 {
                    probe_val = puts[0].1.clone();
                }
                kv.write(&puts, &[], true);
            }
            kv.settle();
            let base = du(&dir);
            enum Reader {
                Redb(redb::ReadTransaction),
                Fjall(fjall::Snapshot),
            }
            let reader = if with_reader {
                Some(match &kv {
                    Kv::Redb { db, .. } => Reader::Redb(db.begin_read().unwrap()),
                    Kv::Fjall { db, .. } => Reader::Fjall(db.snapshot()),
                })
            } else {
                None
            };
            let mut lat = vec![];
            let mut max_disk = 0;
            for c in 0..commits {
                let puts: Vec<_> = (0..1000)
                    .map(|_| (key(r.gen_range(0..nkeys)), val(&mut r)))
                    .collect();
                let t = Instant::now();
                kv.write(&puts, &[], true);
                lat.push(t.elapsed().as_micros() as u64);
                if c % 50 == 49 {
                    max_disk = max_disk.max(du(&dir));
                }
            }
            kv.settle();
            let with = du(&dir);
            let still_old = match (&reader, &kv) {
                (Some(Reader::Redb(rt)), _) => rt
                    .open_table(KV_T)
                    .unwrap()
                    .get(key(0).as_slice())
                    .unwrap()
                    .map(|v| v.value() == probe_val.as_slice()),
                (Some(Reader::Fjall(s)), Kv::Fjall { ks, .. }) => s
                    .get(ks, key(0))
                    .unwrap()
                    .map(|v| v.as_ref() == probe_val.as_slice()),
                _ => None,
            };
            drop(reader);
            for _ in 0..50 {
                let puts: Vec<_> = (0..1000)
                    .map(|_| (key(r.gen_range(0..nkeys)), val(&mut r)))
                    .collect();
                kv.write(&puts, &[], true);
            }
            kv.settle();
            let after_release = du(&dir);
            let compacted = match &mut kv {
                Kv::Redb { db, rt, .. } => {
                    *rt = None;
                    db.compact().unwrap();
                    du(&dir)
                }
                Kv::Fjall { db, ks } => {
                    ks.major_compact().unwrap();
                    fjall_settle(db);
                    du(&dir)
                }
            };
            println!(
                "K7 {engine:5} reader={with_reader:5} commit_p50={}us commit_p99={}us disk: before={:.1}MB max_during={:.1}MB end={:.1}MB \
                 after_release+50={:.1}MB after_compact={:.1}MB (journal {:.1}MB) reader_sees_old_value={still_old:?}",
                pct(&mut lat, 0.5),
                pct(&mut lat, 0.99),
                mb(base),
                mb(max_disk),
                mb(with),
                mb(after_release),
                mb(compacted),
                mb(jnl_bytes(&dir))
            );
            drop(kv);
        }
    }
    let _ = fs::remove_dir_all(work.join("k7"));
}

// ---------------------------------------------------------------- main

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("help");
    let quick = std::env::var("KV_QUICK").is_ok();
    let work = PathBuf::from(std::env::var("KV_WORK").unwrap_or_else(|_| "work".into()));
    fs::create_dir_all(&work).unwrap();
    match cmd {
        "info" => {
            let k = fs::read_to_string("/proc/sys/kernel/osrelease").unwrap();
            println!(
                "# kernel {} redb 4.4.0 fjall 3.1.12 (lsm-tree 3.1.10) {} quick={quick} loadavg={}",
                k.trim(),
                args[2..].join(" "),
                loadavg()
            );
        }
        "k1" => k1(&work, quick),
        "k2" => k2(&args[2], &work, quick),
        "k3" => k3(&args[2], &work, quick),
        "k3child" => k3child(&args[2], Path::new(&args[3]), args[4].parse().unwrap()),
        "k4" => k4(&work, quick),
        "k5" => k5(&work, quick),
        "k5child" => k5child(&args[2], Path::new(&args[3])),
        "k7" => k7(&work, quick),
        "open" if args[2] == "fjall-raw" => {
            let t = Instant::now();
            let db = fjall_open(Path::new(&args[3]), 64 * MIB);
            let ks = db.keyspace("kv", KeyspaceCreateOptions::default).unwrap();
            println!("open={:.1}ms", t.elapsed().as_secs_f64() * 1e3);
            drop(ks);
            drop(db);
        }
        "k5c" => k5c(&work, quick),
        "idle" => {
            header(
                "K0 background: whole-disk writes and flushes from other processes while this harness idles 10 s",
            );
            let m = Meter::start();
            std::thread::sleep(Duration::from_secs(10));
            let d = m.stop();
            println!(
                "K0 idle 10s: device written={:.1}MB ({:.0}KB/s) flushes={} ({:.0}/s)",
                mb(d.dev_wb),
                d.dev_wb as f64 / 10.0 / 1024.0,
                d.dev_flush,
                d.dev_flush as f64 / 10.0
            );
        }
        "open" => {
            let t = Instant::now();
            let kv = Kv::open(&args[2], Path::new(&args[3]), 64 * MIB);
            println!("open={:.1}ms", t.elapsed().as_secs_f64() * 1e3);
            drop(kv);
        }
        _ => eprintln!("usage: kv-engines-spike info|k1|k2 <redb|fjall>|k3 <redb|fjall>|k4|k5|k7"),
    }
}
