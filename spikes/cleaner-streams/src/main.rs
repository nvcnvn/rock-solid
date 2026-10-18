//! Write-amplification simulator for the per-disk log, extended from spikes/cleaner-wa with
//! more streams (Q14), chunk-sized slots for whole chunks (Q13), and the log's own metadata
//! traffic: Raft entries, sorted runs, compaction, manifests and the chunk-index entries the
//! cleaner causes (Q15).
//!
//! Usage: cleaner-streams key=value ...   (see `Cfg::parse` for keys)

use std::collections::HashMap;
use std::time::Instant;

const CHUNK_BLOCKS: u32 = 1024;
const INVALID: u32 = u32::MAX;
const BLOCK_BYTES: usize = 4096;
const REC_HDR_BYTES: usize = 64;
const CLEAN_REC_BLOCKS: usize = 256;
const MAX_REC_BLOCKS: usize = 256;
const BLOB_BASE: u32 = 0xC000_0000;
const ELL_WINDOW: usize = 16;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Policy {
    Greedy,
    CostBenefit,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Sep {
    Base,
    Moves,
    SepBit,
    SepBitUser,
    SepBitGc,
    Oracle,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Free,
    Open,
    Sealed,
    Cleaning,
    Slots,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Raft,
    Run,
    Manifest,
    SlotHdr,
}

#[derive(Clone, Copy, PartialEq)]
enum Src {
    User,
    Clean,
    Meta,
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
    fn f64(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn shuffle(&mut self, v: &mut [u32]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            v.swap(i, j);
        }
    }
}

struct Cfg {
    wl: String,
    u: f64,
    policy: Policy,
    streams: usize,
    sep: Sep,
    seg_mb: u32,
    segs: u32,
    rec_kb: u32,
    io_kb: u32,
    headers: bool,
    entry_b: usize,
    crc_b: usize,
    warm: f64,
    windows: u32,
    win: f64,
    free_low: usize,
    batch: usize,
    age_cap: f64,
    hot_w: f64,
    hot_f: f64,
    theta: f64,
    seed: u64,
    tag: String,
    // Q13
    slots: bool,
    slot_hdr_in_log: bool,
    whole_f: f64,
    whole_w: f64,
    whole_stream: usize,
    file_chunks: u32,
    writers: u32,
    // Q15
    meta: bool,
    raft_ratio: f64,
    meta_state_ratio: f64,
    meta_live: f64,
    idx_entry_b: f64,
    ckpt_mb: f64,
    man_row_b: f64,
    small_run_kb: f64,
    raft_stream: usize,
    run_stream: usize,
    meta_reserve: f64,
    compaction: String,
}

impl Cfg {
    fn parse(args: &[String]) -> Cfg {
        let mut kv: HashMap<String, String> = HashMap::new();
        for a in args {
            let (k, v) = a.split_once('=').unwrap_or_else(|| panic!("bad arg {a}"));
            kv.insert(k.to_string(), v.to_string());
        }
        let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
        let f = |k: &str, d: &str| get(k, d).parse::<f64>().unwrap();
        let seg_mb: u32 = get("seg_mb", "4").parse().unwrap();
        let segs: u32 = match kv.get("segs") {
            Some(s) => s.parse().unwrap(),
            None => (f("disk_gb", "16") * 1024.0 / seg_mb as f64) as u32,
        };
        let known = [
            "wl", "u", "pol", "streams", "sep", "seg_mb", "segs", "disk_gb", "rec_kb", "io_kb", "headers", "entry_b",
            "crc_b", "warm", "windows", "win", "free_low", "batch", "age_cap", "hot_w", "hot_f", "theta", "seed", "tag",
            "slots", "slot_hdr_in_log", "whole_f", "whole_w", "whole_stream", "file_chunks", "writers", "meta",
            "raft_ratio", "meta_state_ratio", "meta_live", "idx_entry_b", "ckpt_mb", "man_row_b", "small_run_kb",
            "raft_stream", "run_stream", "meta_reserve", "compaction",
        ];
        for k in kv.keys() {
            assert!(known.contains(&k.as_str()), "unknown key {k}");
        }
        Cfg {
            wl: get("wl", "uniform"),
            u: f("u", "0.8"),
            policy: match get("pol", "cb").as_str() {
                "greedy" => Policy::Greedy,
                "cb" => Policy::CostBenefit,
                p => panic!("pol {p}"),
            },
            streams: get("streams", "2").parse().unwrap(),
            sep: match get("sep", "base").as_str() {
                "base" => Sep::Base,
                "moves" => Sep::Moves,
                "sepbit" => Sep::SepBit,
                "sepbit_user" => Sep::SepBitUser,
                "sepbit_gc" => Sep::SepBitGc,
                "oracle" => Sep::Oracle,
                s => panic!("sep {s}"),
            },
            seg_mb,
            segs,
            rec_kb: get("rec_kb", "64").parse().unwrap(),
            io_kb: get("io_kb", "4").parse().unwrap(),
            headers: get("headers", "1") == "1",
            entry_b: get("entry_b", "32").parse().unwrap(),
            crc_b: get("crc_b", "4").parse().unwrap(),
            warm: f("warm", "6"),
            windows: get("windows", "4").parse().unwrap(),
            win: f("win", "1"),
            free_low: get("free_low", "8").parse().unwrap(),
            batch: get("batch", "4").parse().unwrap(),
            age_cap: f("age_cap", "1"),
            hot_w: f("hot_w", "0.8"),
            hot_f: f("hot_f", "0.2"),
            theta: f("theta", "0.99"),
            seed: get("seed", "1").parse().unwrap(),
            tag: get("tag", "-"),
            slots: get("slots", "0") == "1",
            slot_hdr_in_log: get("slot_hdr_in_log", "0") == "1",
            whole_f: f("whole_f", "0.5"),
            whole_w: f("whole_w", "0.5"),
            whole_stream: get("whole_stream", "0").parse().unwrap(),
            file_chunks: get("file_chunks", "16").parse().unwrap(),
            writers: get("writers", "1").parse().unwrap(),
            meta: get("meta", "0") == "1",
            raft_ratio: f("raft_ratio", "0"),
            meta_state_ratio: f("meta_state_ratio", "0.5"),
            meta_live: f("meta_live", "0"),
            idx_entry_b: f("idx_entry_b", "40"),
            ckpt_mb: f("ckpt_mb", "4"),
            man_row_b: f("man_row_b", "1.0625"),
            small_run_kb: f("small_run_kb", "64"),
            raft_stream: get("raft_stream", "0").parse().unwrap(),
            run_stream: get("run_stream", "1").parse().unwrap(),
            meta_reserve: f("meta_reserve", "0"),
            compaction: get("compaction", "tiered"),
        }
    }
}

enum Workload {
    Uniform,
    HotCold { w: f64, hot: Vec<u32>, cold: Vec<u32> },
    Zipf(Box<Zipf>),
    ChunkRand,
    Mixed { nw: u32, p_whole: f64 },
    Files { n: u32, files: u32, active: Vec<(u32, u32)>, turn: usize },
}

/// YCSB's scrambled Zipfian generator (Gray et al., SIGMOD 1994), ranks permuted over blocks.
struct Zipf {
    n: f64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    perm: Vec<u32>,
}

impl Zipf {
    fn new(n: u32, theta: f64, rng: &mut Rng) -> Zipf {
        let zetan: f64 = (1..=n as u64).map(|i| 1.0 / (i as f64).powf(theta)).sum();
        let zeta2 = 1.0 + 1.0 / 2f64.powf(theta);
        let nf = n as f64;
        let eta = (1.0 - (2.0 / nf).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        let mut perm: Vec<u32> = (0..n).collect();
        rng.shuffle(&mut perm);
        Zipf { n: nf, theta, alpha: 1.0 / (1.0 - theta), zetan, eta, perm }
    }
    fn next(&self, rng: &mut Rng) -> u32 {
        let u = rng.f64();
        let uz = u * self.zetan;
        let rank = if uz < 1.0 {
            0
        } else if uz < 1.0 + 0.5f64.powf(self.theta) {
            1
        } else {
            ((self.n * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as usize).min(self.perm.len() - 1)
        };
        self.perm[rank]
    }
}

#[derive(Clone, Copy, Default)]
struct Counters {
    user: u64,
    user_hdr: u64,
    clean: u64,
    clean_hdr: u64,
    clean_blob: u64,
    raft: u64,
    run_flush: u64,
    run_compact: u64,
    manifest: u64,
    slot_hdr: u64,
    meta_hdr: u64,
    idx_user_entries: u64,
    idx_clean_entries: u64,
    victims: u64,
    victim_live: u64,
    passes: u64,
    free_sum: u64,
    free_samples: u64,
    slot_segs_sum: u64,
    live_sum: u64,
}

impl Counters {
    fn minus(&self, o: &Counters) -> Counters {
        Counters {
            user: self.user - o.user,
            user_hdr: self.user_hdr - o.user_hdr,
            clean: self.clean - o.clean,
            clean_hdr: self.clean_hdr - o.clean_hdr,
            clean_blob: self.clean_blob - o.clean_blob,
            raft: self.raft - o.raft,
            run_flush: self.run_flush - o.run_flush,
            run_compact: self.run_compact - o.run_compact,
            manifest: self.manifest - o.manifest,
            slot_hdr: self.slot_hdr - o.slot_hdr,
            meta_hdr: self.meta_hdr - o.meta_hdr,
            idx_user_entries: self.idx_user_entries - o.idx_user_entries,
            idx_clean_entries: self.idx_clean_entries - o.idx_clean_entries,
            victims: self.victims - o.victims,
            victim_live: self.victim_live - o.victim_live,
            passes: self.passes - o.passes,
            free_sum: self.free_sum - o.free_sum,
            free_samples: self.free_samples - o.free_samples,
            live_sum: self.live_sum - o.live_sum,
            slot_segs_sum: self.slot_segs_sum - o.slot_segs_sum,
        }
    }
    fn total(&self) -> u64 {
        self.user
            + self.user_hdr
            + self.clean
            + self.clean_hdr
            + self.clean_blob
            + self.raft
            + self.run_flush
            + self.run_compact
            + self.manifest
            + self.slot_hdr
            + self.meta_hdr
    }
    fn wa(&self) -> f64 {
        self.total() as f64 / self.user as f64
    }
}

struct Blob {
    ids: Vec<u32>,
    kind: Kind,
}

struct Lsm {
    runs: Vec<(f64, u32)>,
}

struct Sim {
    b: u32,
    s: u32,
    l: u32,
    headers: bool,
    entry_b: usize,
    crc_b: usize,
    live: Vec<u32>,
    fill: Vec<u32>,
    smtime: Vec<u32>,
    sopen: Vec<u32>,
    sclass: Vec<u8>,
    srecs: Vec<u32>,
    raftlive: Vec<u32>,
    state: Vec<State>,
    free: Vec<u32>,
    open: Vec<u32>,
    fwd: Vec<u32>,
    rev: Vec<u32>,
    bmtime: Vec<u32>,
    bmoves: Vec<u8>,
    temp: Vec<u8>,
    now: u32,
    policy: Policy,
    sep: Sep,
    nst: usize,
    free_low: usize,
    batch: usize,
    age_cap: f64,
    ells: Vec<f64>,
    ell: f64,
    c: Counters,
    // blobs
    bb_slot: Vec<u32>,
    bb_blob: Vec<u32>,
    bb_mt: Vec<u32>,
    bb_free: Vec<u32>,
    blobs: Vec<Option<Blob>>,
    blob_free: Vec<u32>,
    // slots
    slots: bool,
    slot_blocks: u32,
    spp: u32,
    slot_live: Vec<u16>,
    seg_free_slots: Vec<u8>,
    slot_segs: u32,
    slot_hdr_in_log: bool,
    chunk_hdr_blob: Vec<u32>,
    whole_chunks: u32,
    whole_stream: usize,
    // meta
    meta: bool,
    raft_stream: usize,
    run_stream: usize,
    raft_acc: f64,
    raft_ratio: f64,
    meta_state_ratio: f64,
    idx_entry_b: f64,
    ckpt_blocks: u64,
    since_ckpt: u64,
    raft_blobs: Vec<u32>,
    manifest_blob: u32,
    man_row_b: f64,
    small_run_b: f64,
    idx: Lsm,
    mst: Lsm,
    meta_live_b: f64,
    in_ckpt: bool,
    idx_mark: u64,
    live_total: u64,
    universal: bool,
    tiered: bool,
}

impl Sim {
    fn new(cfg: &Cfg, l: u32, nst: usize, whole_chunks: u32, temp: Vec<u8>) -> Sim {
        let b = cfg.seg_mb * 256;
        let s = cfg.segs;
        let n = b as usize * s as usize;
        assert!(n < BLOB_BASE as usize);
        let slot_blocks = if cfg.slot_hdr_in_log { CHUNK_BLOCKS } else { CHUNK_BLOCKS + 4 };
        let spp = if cfg.slot_hdr_in_log { b / slot_blocks } else { (b - 1) / slot_blocks };
        assert!(!cfg.slots || (spp >= 1 && spp <= 255));
        Sim {
            b,
            s,
            l,
            headers: cfg.headers,
            entry_b: cfg.entry_b,
            crc_b: cfg.crc_b,
            live: vec![0; s as usize],
            fill: vec![0; s as usize],
            smtime: vec![0; s as usize],
            sopen: vec![0; s as usize],
            sclass: vec![0; s as usize],
            srecs: vec![0; s as usize],
            raftlive: vec![0; s as usize],
            state: vec![State::Free; s as usize],
            free: (0..s).rev().collect(),
            open: vec![INVALID; nst],
            fwd: vec![INVALID; l as usize],
            rev: vec![INVALID; n],
            bmtime: vec![0; l as usize],
            bmoves: vec![0; l as usize],
            temp,
            now: 0,
            policy: cfg.policy,
            sep: cfg.sep,
            nst,
            free_low: cfg.free_low,
            batch: cfg.batch,
            age_cap: if cfg.age_cap > 0.0 { cfg.age_cap * n as f64 } else { f64::INFINITY },
            ells: Vec::new(),
            ell: f64::INFINITY,
            c: Counters::default(),
            bb_slot: Vec::new(),
            bb_blob: Vec::new(),
            bb_mt: Vec::new(),
            bb_free: Vec::new(),
            blobs: Vec::new(),
            blob_free: Vec::new(),
            slots: cfg.slots,
            slot_blocks,
            spp,
            slot_live: if cfg.slots { vec![0; s as usize * spp as usize] } else { Vec::new() },
            seg_free_slots: vec![0; s as usize],
            slot_segs: 0,
            slot_hdr_in_log: cfg.slot_hdr_in_log,
            chunk_hdr_blob: vec![INVALID; whole_chunks as usize],
            whole_chunks,
            whole_stream: cfg.whole_stream,
            meta: cfg.meta,
            raft_stream: cfg.raft_stream,
            run_stream: cfg.run_stream,
            raft_acc: 0.0,
            raft_ratio: cfg.raft_ratio,
            meta_state_ratio: cfg.meta_state_ratio,
            idx_entry_b: cfg.idx_entry_b,
            ckpt_blocks: (cfg.ckpt_mb * 256.0) as u64,
            since_ckpt: 0,
            raft_blobs: Vec::new(),
            manifest_blob: INVALID,
            man_row_b: cfg.man_row_b,
            small_run_b: cfg.small_run_kb * 1024.0,
            idx: Lsm { runs: Vec::new() },
            mst: Lsm { runs: Vec::new() },
            meta_live_b: cfg.meta_live * n as f64 * BLOCK_BYTES as f64,
            in_ckpt: false,
            idx_mark: 0,
            live_total: 0,
            universal: cfg.compaction == "universal",
            tiered: cfg.compaction == "tiered",
        }
    }

    fn is_whole(&self, lba: u32) -> bool {
        lba / CHUNK_BLOCKS < self.whole_chunks
    }

    fn open_seg(&mut self, st: usize) -> u32 {
        if self.open[st] != INVALID {
            return self.open[st];
        }
        let seg = self.free.pop().expect("out of free segments");
        let i = seg as usize;
        self.state[i] = State::Open;
        self.fill[i] = 0;
        self.live[i] = 0;
        self.smtime[i] = 0;
        self.sopen[i] = self.now;
        self.sclass[i] = st as u8;
        self.srecs[i] = 0;
        self.open[st] = seg;
        seg
    }

    fn seal(&mut self, st: usize) {
        self.state[self.open[st] as usize] = State::Sealed;
        self.open[st] = INVALID;
    }

    /// Invalidate the block stored at `old`, freeing its slot and slot segment when they empty.
    fn kill_slot(&mut self, old: u32, id: u32) {
        let seg = (old / self.b) as usize;
        self.rev[old as usize] = INVALID;
        self.live[seg] -= 1;
        self.live_total -= 1;
        if id >= BLOB_BASE {
            let bl = self.bb_blob[(id - BLOB_BASE) as usize];
            if self.blobs[bl as usize].as_ref().unwrap().kind == Kind::Raft {
                self.raftlive[seg] -= 1;
            }
        }
        if self.state[seg] == State::Slots {
            let off = old % self.b - if self.slot_hdr_in_log { 0 } else { 1 };
            let k = seg * self.spp as usize + (off / self.slot_blocks) as usize;
            self.slot_live[k] -= 1;
            if self.slot_live[k] == 0 {
                self.seg_free_slots[seg] += 1;
                if self.seg_free_slots[seg] as u32 == self.spp {
                    self.state[seg] = State::Free;
                    self.slot_segs -= 1;
                    self.free.push(seg as u32);
                }
            }
        }
    }

    fn place(&mut self, id: u32, slot: u32, user: bool) {
        let b = self.b;
        let mt;
        if id < self.l {
            let lba = id as usize;
            let old = self.fwd[lba];
            if user {
                self.now = self.now.checked_add(1).expect("clock overflow");
                self.bmtime[lba] = self.now;
                self.bmoves[lba] = 0;
                mt = self.now;
            } else {
                mt = self.bmtime[lba];
                self.bmoves[lba] = self.bmoves[lba].saturating_add(1);
            }
            if old != INVALID {
                self.kill_slot(old, id);
            }
            self.fwd[lba] = slot;
        } else {
            let i = (id - BLOB_BASE) as usize;
            let old = self.bb_slot[i];
            if user {
                self.bb_mt[i] = self.now;
            }
            mt = self.bb_mt[i];
            if old != INVALID {
                self.kill_slot(old, id);
            }
            self.bb_slot[i] = slot;
            let bl = self.bb_blob[i];
            if self.blobs[bl as usize].as_ref().unwrap().kind == Kind::Raft {
                self.raftlive[(slot / b) as usize] += 1;
            }
        }
        let seg = (slot / b) as usize;
        self.rev[slot as usize] = id;
        self.live[seg] += 1;
        self.live_total += 1;
        if mt > self.smtime[seg] {
            self.smtime[seg] = mt;
        }
    }

    fn count_blocks(&mut self, id: u32, src: Src) {
        if id < self.l {
            match src {
                Src::User => self.c.user += 1,
                _ => self.c.clean += 1,
            }
        } else {
            if src == Src::Clean {
                self.c.clean_blob += 1;
                return;
            }
            let bl = self.bb_blob[(id - BLOB_BASE) as usize] as usize;
            match self.blobs[bl].as_ref().unwrap().kind {
                Kind::Raft => self.c.raft += 1,
                Kind::Run => {
                    if self.in_ckpt {
                        self.c.run_flush += 1
                    } else {
                        self.c.run_compact += 1
                    }
                }
                Kind::Manifest => self.c.manifest += 1,
                Kind::SlotHdr => self.c.slot_hdr += 1,
            }
        }
    }

    /// Append `blocks` to stream `st` as records; `hdr` false writes them without record headers.
    fn append(&mut self, st: usize, blocks: &[(u32, bool)], src: Src, hdr: bool) {
        let hdr_on = self.headers && hdr;
        let hdr_min = usize::from(hdr_on);
        let user = src != Src::Clean;
        let mut i = 0;
        while i < blocks.len() {
            let seg = self.open_seg(st) as usize;
            let remaining = (self.b - self.fill[seg]) as usize;
            if remaining < hdr_min + 1 {
                self.seal(st);
                continue;
            }
            let mut n = 0;
            let mut bytes = REC_HDR_BYTES;
            while i + n < blocks.len() && n < MAX_REC_BLOCKS.max(if hdr { 0 } else { usize::MAX }) {
                let start = n == 0 || blocks[i + n].1;
                let nb = bytes + if start { self.entry_b } else { 0 } + self.crc_b;
                let h = if hdr_on { nb.div_ceil(BLOCK_BYTES) } else { 0 };
                if h + n + 1 > remaining {
                    break;
                }
                bytes = nb;
                n += 1;
            }
            let h = if hdr_on { bytes.div_ceil(BLOCK_BYTES) } else { 0 };
            let base = seg as u32 * self.b + self.fill[seg];
            for k in 0..h as u32 {
                self.rev[(base + k) as usize] = INVALID;
            }
            self.fill[seg] += h as u32;
            if h > 0 {
                self.srecs[seg] += 1;
            }
            for k in 0..n {
                let slot = seg as u32 * self.b + self.fill[seg];
                self.fill[seg] += 1;
                self.place(blocks[i + k].0, slot, user);
                self.count_blocks(blocks[i + k].0, src);
            }
            match src {
                Src::User => self.c.user_hdr += h as u64,
                Src::Clean => self.c.clean_hdr += h as u64,
                Src::Meta => self.c.meta_hdr += h as u64,
            }
            self.since_ckpt += (h + n) as u64;
            i += n;
            if self.fill[seg] == self.b {
                self.seal(st);
            }
        }
    }

    // ---- blobs ----

    fn new_blob(&mut self, kind: Kind, nblocks: u32) -> (u32, Vec<(u32, bool)>) {
        let mut ids = Vec::with_capacity(nblocks as usize);
        let bl = match self.blob_free.pop() {
            Some(x) => x,
            None => {
                self.blobs.push(None);
                (self.blobs.len() - 1) as u32
            }
        };
        for _ in 0..nblocks {
            let i = match self.bb_free.pop() {
                Some(x) => x,
                None => {
                    self.bb_slot.push(INVALID);
                    self.bb_blob.push(0);
                    self.bb_mt.push(0);
                    (self.bb_slot.len() - 1) as u32
                }
            };
            self.bb_slot[i as usize] = INVALID;
            self.bb_blob[i as usize] = bl;
            assert!(i < u32::MAX - BLOB_BASE);
            ids.push(BLOB_BASE + i);
        }
        let recs: Vec<(u32, bool)> = ids.iter().enumerate().map(|(k, &id)| (id, k == 0)).collect();
        self.blobs[bl as usize] = Some(Blob { ids, kind });
        (bl, recs)
    }

    fn kill_blob(&mut self, bl: u32) {
        let blob = self.blobs[bl as usize].take().unwrap();
        // put it back temporarily so kill_slot can see its kind
        self.blobs[bl as usize] = Some(Blob { ids: Vec::new(), kind: blob.kind });
        for &id in &blob.ids {
            let i = (id - BLOB_BASE) as usize;
            let s = self.bb_slot[i];
            if s != INVALID {
                self.kill_slot(s, id);
            }
            self.bb_slot[i] = INVALID;
            self.bb_free.push(i as u32);
        }
        self.blobs[bl as usize] = None;
        self.blob_free.push(bl);
    }

    // ---- stream choice ----

    fn user_stream(&self, lba: u32) -> usize {
        if self.whole_chunks > 0 && self.is_whole(lba) {
            return self.whole_stream;
        }
        match self.sep {
            Sep::Base | Sep::Moves | Sep::SepBitGc => 0,
            Sep::SepBit | Sep::SepBitUser => {
                let mt = self.bmtime[lba as usize];
                if self.fwd[lba as usize] == INVALID || ((self.now - mt) as f64) < self.ell { 0 } else { 1 }
            }
            Sep::Oracle => {
                if self.temp[lba as usize / CHUNK_BLOCKS as usize] == 0 {
                    0
                } else {
                    1
                }
            }
        }
    }

    fn gc_stream(&self, id: u32, victim: usize) -> usize {
        if id >= BLOB_BASE {
            return self.run_stream;
        }
        let lba = id as usize;
        let age = (self.now - self.bmtime[lba]) as f64;
        match self.sep {
            Sep::Base => usize::from(self.nst >= 2),
            Sep::Moves => (1 + self.bmoves[lba] as usize).min(self.nst - 1),
            Sep::SepBitUser => 2,
            Sep::SepBit => {
                if self.sclass[victim] == 0 {
                    2
                } else if age < 4.0 * self.ell {
                    3
                } else if age < 16.0 * self.ell {
                    4
                } else {
                    5
                }
            }
            Sep::SepBitGc => {
                if age < 4.0 * self.ell {
                    1
                } else if age < 16.0 * self.ell {
                    2
                } else {
                    3
                }
            }
            Sep::Oracle => {
                if self.temp[lba / CHUNK_BLOCKS as usize] == 0 {
                    2
                } else {
                    3
                }
            }
        }
    }

    // ---- cleaner ----

    fn copy_overhead(&self, live: f64) -> f64 {
        // worst case: every block its own extent, records of CLEAN_REC_BLOCKS, headers rounded up to blocks
        if self.headers {
            let per_rec = (REC_HDR_BYTES + CLEAN_REC_BLOCKS * (self.entry_b + self.crc_b)).div_ceil(BLOCK_BYTES) as f64;
            (live / CLEAN_REC_BLOCKS as f64).ceil() * per_rec + 2.0
        } else {
            0.0
        }
    }

    fn pick_victims(&self) -> Vec<u32> {
        let k = self.batch;
        let mut best: Vec<(f64, u32)> = Vec::with_capacity(k + 1);
        let bf = self.b as f64;
        for seg in 0..self.s as usize {
            if self.state[seg] != State::Sealed || self.raftlive[seg] > 0 {
                continue;
            }
            let lv = self.live[seg] as f64;
            if bf - lv <= self.copy_overhead(lv) {
                continue;
            }
            let score = match self.policy {
                Policy::Greedy => -lv,
                Policy::CostBenefit => {
                    let u = lv / bf;
                    let age = ((self.now - self.smtime[seg]) as f64 + 1.0).min(self.age_cap);
                    (1.0 - u) * age / (1.0 + u)
                }
            };
            if best.len() < k || score > best[best.len() - 1].0 {
                let pos = best.partition_point(|&(s, _)| s >= score);
                best.insert(pos, (score, seg as u32));
                best.truncate(k);
            }
        }
        best.into_iter().map(|(_, s)| s).collect()
    }

    fn clean_pass(&mut self) {
        let victims = self.pick_victims();
        let live_sum: u64 = victims.iter().map(|&v| self.live[v as usize] as u64).sum();
        assert!(
            !victims.is_empty() && live_sum < (victims.len() as u64) * (self.b as u64 - 2),
            "cleaner cannot make progress: victims are full (live {:.4} of disk, chunk-index runs {} holding {:.4} of disk)",
            self.live_total as f64 / (self.b as f64 * self.s as f64),
            self.idx.runs.len(),
            self.idx.runs.iter().map(|r| r.0).sum::<f64>() / BLOCK_BYTES as f64 / (self.b as f64 * self.s as f64)
        );
        let mut per: Vec<Vec<(u64, u32)>> = vec![Vec::new(); self.nst];
        for &v in &victims {
            let vi = v as usize;
            self.state[vi] = State::Cleaning;
            self.c.victims += 1;
            self.c.victim_live += self.live[vi] as u64;
            self.c.idx_clean_entries += self.srecs[vi] as u64;
            if self.sclass[vi] == 0 {
                self.ells.push((self.now - self.sopen[vi]) as f64);
                if self.ells.len() > ELL_WINDOW {
                    self.ells.remove(0);
                }
                self.ell = self.ells.iter().sum::<f64>() / self.ells.len() as f64;
            }
            let base = v * self.b;
            for off in 0..self.fill[vi] {
                let id = self.rev[(base + off) as usize];
                if id == INVALID {
                    continue;
                }
                let key = if id < self.l {
                    (((id / CHUNK_BLOCKS) as u64) << 11) | (id % CHUNK_BLOCKS) as u64
                } else {
                    (1u64 << 63) | ((self.bb_blob[(id - BLOB_BASE) as usize] as u64) << 31) | (id - BLOB_BASE) as u64
                };
                let st = self.gc_stream(id, vi);
                per[st].push((key, id));
            }
        }
        for st in 0..self.nst {
            if per[st].is_empty() {
                continue;
            }
            let mut v = std::mem::take(&mut per[st]);
            v.sort_unstable();
            let mut blocks: Vec<(u32, bool)> = Vec::with_capacity(v.len());
            let mut prev: Option<u32> = None;
            for &(_, id) in &v {
                let contiguous = match prev {
                    Some(pl) if id < self.l => pl < self.l && id == pl + 1 && id % CHUNK_BLOCKS != 0,
                    Some(pl) if pl >= BLOB_BASE => {
                        self.bb_blob[(id - BLOB_BASE) as usize] == self.bb_blob[(pl - BLOB_BASE) as usize]
                    }
                    _ => false,
                };
                blocks.push((id, !contiguous));
                if !contiguous && id < self.l {
                    self.c.idx_clean_entries += 1;
                }
                prev = Some(id);
            }
            for rec in blocks.chunks(CLEAN_REC_BLOCKS) {
                self.c.idx_clean_entries += 1;
                self.append(st, rec, Src::Clean, true);
            }
        }
        for &v in &victims {
            let vi = v as usize;
            debug_assert_eq!(self.live[vi], 0);
            self.state[vi] = State::Free;
            self.fill[vi] = 0;
            self.smtime[vi] = 0;
            self.free.push(v);
        }
        self.c.passes += 1;
    }

    fn ensure_free(&mut self) {
        let mut stalled = 0;
        while self.free.len() < self.free_low {
            let before = self.free.len();
            self.clean_pass();
            stalled = if self.free.len() > before { 0 } else { stalled + 1 };
            if stalled >= 1000 {
                let rb: f64 = self.idx.runs.iter().map(|r| r.0).sum();
                panic!(
                    "cleaner livelock: 1000 passes without gaining a free segment (live {:.4} of disk, idx runs {} of {:.0} blocks, mst runs {}, raft blobs {})",
                    self.live_total as f64 / (self.b as f64 * self.s as f64),
                    self.idx.runs.len(),
                    rb / BLOCK_BYTES as f64,
                    self.mst.runs.len(),
                    self.raft_blobs.len()
                );
            }
        }
    }

    // ---- slots ----

    fn slot_write(&mut self, chunk: u32) {
        if self.slot_hdr_in_log {
            let old = self.chunk_hdr_blob[chunk as usize];
            if old != INVALID {
                self.kill_blob(old);
            }
        }
        // pick the fullest slot segment with a free slot
        let mut best = INVALID;
        let mut best_free = u8::MAX;
        for seg in 0..self.s as usize {
            if self.state[seg] == State::Slots && self.seg_free_slots[seg] > 0 && self.seg_free_slots[seg] < best_free {
                best_free = self.seg_free_slots[seg];
                best = seg as u32;
            }
        }
        if best == INVALID {
            self.ensure_free();
            let seg = self.free.pop().expect("out of free segments");
            let i = seg as usize;
            self.state[i] = State::Slots;
            self.live[i] = 0;
            self.fill[i] = self.b;
            self.seg_free_slots[i] = self.spp as u8;
            self.slot_segs += 1;
            for k in 0..self.spp as usize {
                self.slot_live[i * self.spp as usize + k] = 0;
            }
            best = seg;
        }
        let seg = best as usize;
        let k = (0..self.spp as usize).find(|&k| self.slot_live[seg * self.spp as usize + k] == 0).unwrap();
        // the slot counts each block before it is placed, so freeing the chunk's old copy, which
        // may sit in this segment, never frees the segment under the new one
        self.seg_free_slots[seg] -= 1;
        let mut slot = seg as u32 * self.b + if self.slot_hdr_in_log { 0 } else { 1 } + k as u32 * self.slot_blocks;
        let base_lba = chunk * CHUNK_BLOCKS;
        let hdr_per = if self.slot_hdr_in_log { 0 } else { 1 };
        for part in 0..(CHUNK_BLOCKS as usize / MAX_REC_BLOCKS) as u32 {
            for _ in 0..hdr_per {
                self.rev[slot as usize] = INVALID;
                slot += 1;
                self.c.user_hdr += 1;
            }
            for j in 0..MAX_REC_BLOCKS as u32 {
                let lba = base_lba + part * MAX_REC_BLOCKS as u32 + j;
                self.slot_live[seg * self.spp as usize + k] += 1;
                self.place(lba, slot, true);
                slot += 1;
                self.c.user += 1;
            }
        }
        self.c.idx_user_entries += 2 * (CHUNK_BLOCKS as u64 / MAX_REC_BLOCKS as u64);
        if self.slot_hdr_in_log {
            let (bl, recs) = self.new_blob(Kind::SlotHdr, 4);
            self.chunk_hdr_blob[chunk as usize] = bl;
            let st = self.whole_stream;
            self.append(st, &recs, Src::User, false);
        }
    }

    // ---- user path ----

    fn user_record(&mut self, blocks: &[(u32, bool)]) {
        self.ensure_free();
        self.c.free_sum += self.free.len() as u64;
        self.c.free_samples += 1;
        self.c.live_sum += self.live_total;
        self.c.slot_segs_sum += self.slot_segs as u64;
        if self.slots && self.is_whole(blocks[0].0) {
            debug_assert!(blocks.len() == CHUNK_BLOCKS as usize);
            self.slot_write(blocks[0].0 / CHUNK_BLOCKS);
            if self.meta && self.raft_ratio > 0.0 {
                // the round's record and its Raft entries go to the log; the chunk's data to its slot
                self.raft_acc += self.raft_ratio * CHUNK_BLOCKS as f64 * BLOCK_BYTES as f64;
                let nb = (self.raft_acc / BLOCK_BYTES as f64) as u32;
                if nb > 0 {
                    self.raft_acc -= nb as f64 * BLOCK_BYTES as f64;
                    let (bl, recs) = self.new_blob(Kind::Raft, nb);
                    self.raft_blobs.push(bl);
                    let st = self.raft_stream;
                    self.append(st, &recs, Src::Meta, true);
                }
            }
        } else {
            let writes = blocks.iter().filter(|b| b.1).count() as u64;
            let mut per: Vec<Vec<(u32, bool)>> = vec![Vec::new(); self.nst];
            for &(id, start) in blocks {
                let st = self.user_stream(id);
                let first = per[st].is_empty();
                per[st].push((id, start || first));
            }
            let mut nrec = 0;
            for st in 0..self.nst {
                if !per[st].is_empty() {
                    let v = std::mem::take(&mut per[st]);
                    for part in v.chunks(MAX_REC_BLOCKS) {
                        nrec += 1;
                        let mut p = part.to_vec();
                        p[0].1 = true;
                        // Raft entries share the round's record when they go to the same stream
                        if self.meta && self.raft_ratio > 0.0 && st == self.raft_stream {
                            self.raft_acc += self.raft_ratio * part.len() as f64 * BLOCK_BYTES as f64;
                            let nb = (self.raft_acc / BLOCK_BYTES as f64) as u32;
                            if nb > 0 {
                                self.raft_acc -= nb as f64 * BLOCK_BYTES as f64;
                                let (bl, recs) = self.new_blob(Kind::Raft, nb);
                                self.raft_blobs.push(bl);
                                p.extend_from_slice(&recs);
                            }
                        }
                        self.append(st, &p, Src::User, true);
                    }
                }
            }
            if self.meta && self.raft_ratio > 0.0 && self.raft_stream >= self.nst_chunk_user() {
                let n = blocks.len();
                self.raft_acc += self.raft_ratio * n as f64 * BLOCK_BYTES as f64;
                let nb = (self.raft_acc / BLOCK_BYTES as f64) as u32;
                if nb > 0 {
                    self.raft_acc -= nb as f64 * BLOCK_BYTES as f64;
                    let (bl, recs) = self.new_blob(Kind::Raft, nb);
                    self.raft_blobs.push(bl);
                    let st = self.raft_stream;
                    self.append(st, &recs, Src::Meta, true);
                }
            }
            self.c.idx_user_entries += 2 * writes + nrec;
        }
        if self.meta {
            if self.since_ckpt >= self.ckpt_blocks {
                self.checkpoint();
            }
        }
    }

    /// Streams that user chunk writes may use; a raft stream at or past it is dedicated.
    fn nst_chunk_user(&self) -> usize {
        match self.sep {
            Sep::SepBit | Sep::SepBitUser | Sep::Oracle => 2,
            _ => 1,
        }
        .max(if self.whole_chunks > 0 && !self.slots { self.whole_stream + 1 } else { 1 })
    }

    // ---- checkpoints and LSM ----

    fn write_run(&mut self, bytes: f64) -> u32 {
        let nb = ((bytes / BLOCK_BYTES as f64).ceil() as u32).max(1);
        let (bl, recs) = self.new_blob(Kind::Run, nb);
        let st = self.run_stream;
        for part in recs.chunks(MAX_REC_BLOCKS) {
            let mut p = part.to_vec();
            p[0].1 = true;
            self.ensure_free();
            self.append(st, &p, Src::Meta, true);
        }
        bl
    }

    /// Pick runs to merge: Cassandra-style buckets over runs adjacent in age (the design's rule),
    /// or RocksDB universal compaction's size-ratio rule with a run-count trigger of 4.
    fn pick_merge(&self, runs: &[(f64, u32)]) -> Option<(usize, usize)> {
        if self.tiered {
            return None;
        }
        if self.universal {
            if runs.len() <= 4 {
                return None;
            }
            for start in (0..runs.len()).rev() {
                let mut sum = runs[start].0;
                let mut k = start;
                while k > 0 && start - k + 1 < 32 && runs[k - 1].0 <= 1.01 * sum {
                    sum += runs[k - 1].0;
                    k -= 1;
                }
                if start - k + 1 >= 2 {
                    return Some((k, start + 1));
                }
            }
            return Some((3, runs.len()));
        }
        let mut best: Option<(f64, usize, usize)> = None;
        for i in 0..runs.len() {
            let mut j = i + 1;
            let mut sum = runs[i].0;
            while j < runs.len() && j - i < 32 {
                let avg = sum / (j - i) as f64;
                let s = runs[j].0;
                let fits = (s >= 0.5 * avg && s <= 1.5 * avg) || (s < self.small_run_b && avg < self.small_run_b);
                if !fits {
                    break;
                }
                sum += s;
                j += 1;
            }
            if j - i >= 4 {
                let avg = sum / (j - i) as f64;
                if best.is_none_or(|b| avg < b.0) {
                    best = Some((avg, i, j));
                }
            }
        }
        best.map(|(_, i, j)| (i, j))
    }

    /// Size-tiered by size class (factor 4, everything under small_run_b in one class), merging
    /// 4 to 32 runs of one class whatever their age: what a size-tiered index costs when merges
    /// may take any runs of similar size.
    fn compact_tiered(&mut self, which: usize, live_cap: f64, dead: &mut Vec<u32>) {
        loop {
            let runs = if which == 0 { &self.idx.runs } else { &self.mst.runs };
            let class = |s: f64| if s < self.small_run_b { 0 } else { 1 + (s / self.small_run_b).log(4.0).floor() as i32 };
            let mut by: std::collections::BTreeMap<i32, Vec<usize>> = std::collections::BTreeMap::new();
            for (i, r) in runs.iter().enumerate() {
                by.entry(class(r.0)).or_default().push(i);
            }
            let Some(pick) = by.values().find(|v| v.len() >= 4) else { break };
            let pick: Vec<usize> = pick.iter().take(32).cloned().collect();
            let sum: f64 = pick.iter().map(|&i| runs[i].0).sum();
            let out = sum.min(live_cap.max(1.0));
            let inputs: Vec<u32> = pick.iter().map(|&i| runs[i].1).collect();
            let bl = self.write_run(out);
            let runs = if which == 0 { &mut self.idx.runs } else { &mut self.mst.runs };
            let first = pick[0];
            for &i in pick.iter().rev() {
                runs.remove(i);
            }
            runs.insert(first, (out, bl));
            dead.extend(inputs);
        }
    }

    fn compact(&mut self, which: usize, live_cap: f64, dead: &mut Vec<u32>) {
        if self.tiered {
            return self.compact_tiered(which, live_cap, dead);
        }
        loop {
            let runs = if which == 0 { &self.idx.runs } else { &self.mst.runs };
            let Some((i, j)) = self.pick_merge(runs) else { break };
            let sum: f64 = runs[i..j].iter().map(|r| r.0).sum();
            let out = sum.min(live_cap.max(1.0));
            let inputs: Vec<u32> = runs[i..j].iter().map(|r| r.1).collect();
            let bl = self.write_run(out);
            let runs = if which == 0 { &mut self.idx.runs } else { &mut self.mst.runs };
            runs.splice(i..j, [(out, bl)]);
            dead.extend(inputs);
        }
    }

    fn checkpoint(&mut self) {
        self.in_ckpt = true;
        self.since_ckpt = 0;
        let entries = self.c.idx_user_entries + self.c.idx_clean_entries;
        let idx_bytes = self.idx_entry_b * (entries - self.idx_mark) as f64;
        self.idx_mark = entries;
        let raft_bytes: f64 = self.raft_blobs.iter().map(|&b| self.blobs[b as usize].as_ref().unwrap().ids.len() as f64).sum::<f64>()
            * BLOCK_BYTES as f64;
        let mut dead = Vec::new();
        if idx_bytes > 0.0 {
            let bl = self.write_run(idx_bytes);
            self.idx.runs.push((idx_bytes, bl));
        }
        if raft_bytes > 0.0 {
            let mb = raft_bytes * self.meta_state_ratio;
            let bl = self.write_run(mb);
            self.mst.runs.push((mb, bl));
        }
        self.in_ckpt = false;
        let live_idx = self.idx_entry_b * self.live_extents();
        self.compact(0, live_idx, &mut dead);
        let ml = self.meta_live_b;
        self.compact(1, ml, &mut dead);
        self.in_ckpt = true;
        let used = (0..self.s as usize).filter(|&i| self.state[i] != State::Free).count() as f64;
        let nb = ((used * self.man_row_b + 4096.0) / BLOCK_BYTES as f64).ceil() as u32;
        let (bl, recs) = self.new_blob(Kind::Manifest, nb);
        let st = self.run_stream;
        self.ensure_free();
        self.append(st, &recs, Src::Meta, true);
        self.in_ckpt = false;
        if self.manifest_blob != INVALID {
            let m = self.manifest_blob;
            self.kill_blob(m);
        }
        self.manifest_blob = bl;
        for d in dead {
            self.kill_blob(d);
        }
        let rb = std::mem::take(&mut self.raft_blobs);
        for b in rb {
            self.kill_blob(b);
        }
    }

    fn live_extents(&self) -> f64 {
        // one extent per live block of chunks overwritten in small writes, four per whole chunk
        let small = (self.l - self.whole_chunks * CHUNK_BLOCKS) as f64;
        small + 4.0 * self.whole_chunks as f64
    }
}

fn run(cfg: &Cfg) {
    let t0 = Instant::now();
    let b = cfg.seg_mb * 256;
    let n = b as u64 * cfg.segs as u64;
    let l = (((cfg.u - cfg.meta_reserve) * n as f64) as u32 / CHUNK_BLOCKS) * CHUNK_BLOCKS;
    let nchunks = l / CHUNK_BLOCKS;
    let mut rng = Rng(cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x1234_5678);
    let io = (cfg.io_kb / 4).max(1);
    assert!(CHUNK_BLOCKS % io == 0);
    let writes_per_rec = ((cfg.rec_kb / 4) / io).max(1);
    let mut temp = vec![1u8; nchunks as usize];
    let mut whole_chunks = 0;
    let mut wl = match cfg.wl.as_str() {
        "uniform" => Workload::Uniform,
        "hotcold" => {
            let mut chunks: Vec<u32> = (0..nchunks).collect();
            rng.shuffle(&mut chunks);
            let nh = ((nchunks as f64 * cfg.hot_f).round() as usize).max(1);
            let cold = chunks.split_off(nh);
            for &c in &chunks {
                temp[c as usize] = 0;
            }
            Workload::HotCold { w: cfg.hot_w, hot: chunks, cold }
        }
        "zipf" => {
            assert_eq!(io, 1);
            Workload::Zipf(Box::new(Zipf::new(l, cfg.theta, &mut rng)))
        }
        "chunkrand" => {
            assert_eq!(io, CHUNK_BLOCKS);
            whole_chunks = nchunks;
            Workload::ChunkRand
        }
        "files" => {
            assert_eq!(io, CHUNK_BLOCKS);
            whole_chunks = nchunks;
            let n = cfg.file_chunks;
            let files = nchunks / n;
            Workload::Files { n, files, active: Vec::new(), turn: 0 }
        }
        "mixed" => {
            let nw = (nchunks as f64 * cfg.whole_f).round() as u32;
            whole_chunks = nw;
            let small = (cfg.rec_kb / 4) as f64;
            let w = cfg.whole_w;
            let p_whole = w * small / (CHUNK_BLOCKS as f64 * (1.0 - w) + w * small);
            Workload::Mixed { nw, p_whole }
        }
        w => panic!("wl {w}"),
    };
    if cfg.wl == "zipf" && cfg.sep == Sep::Oracle {
        panic!("zipf has no chunk-level oracle");
    }
    let nst = cfg.streams.max(match cfg.sep {
        Sep::Base => 1,
        Sep::Moves => 2,
        Sep::SepBit => 6,
        Sep::SepBitUser => 3,
        Sep::SepBitGc => 4,
        Sep::Oracle => 4,
    })
    .max(cfg.whole_stream + 1)
    .max(if cfg.meta { cfg.raft_stream.max(cfg.run_stream) + 1 } else { 1 });
    let mut sim = Sim::new(cfg, l, nst, whole_chunks, temp);

    // initial fill, whole chunks in write order
    let mut rec: Vec<(u32, bool)> = Vec::new();
    for chunk in 0..nchunks {
        if sim.slots && sim.is_whole(chunk * CHUNK_BLOCKS) {
            rec.clear();
            for k in 0..CHUNK_BLOCKS {
                rec.push((chunk * CHUNK_BLOCKS + k, k == 0));
            }
            sim.user_record(&rec);
            continue;
        }
        for part in 0..(CHUNK_BLOCKS as usize / CLEAN_REC_BLOCKS) as u32 {
            rec.clear();
            let s0 = chunk * CHUNK_BLOCKS + part * CLEAN_REC_BLOCKS as u32;
            for k in 0..CLEAN_REC_BLOCKS as u32 {
                rec.push((s0 + k, k == 0));
            }
            sim.user_record(&rec);
        }
    }

    let small_io = 1u32;
    let small_per_rec = cfg.rec_kb / 4;
    let mut write_until = |sim: &mut Sim, rng: &mut Rng, target: u64| {
        while sim.c.user < target {
            rec.clear();
            let mut whole: Option<u32> = None;
            match &mut wl {
                Workload::ChunkRand => whole = Some(rng.below(nchunks as u64) as u32),
                Workload::Files { n, files, active, turn } => {
                    while active.len() < cfg.writers as usize {
                        let f = rng.below(*files as u64) as u32;
                        if !active.iter().any(|a| a.0 == f) {
                            active.push((f, 0));
                        }
                    }
                    let t = *turn % active.len();
                    *turn += 1;
                    let (f, k) = active[t];
                    whole = Some(f * *n + k);
                    if k + 1 == *n {
                        active.remove(t);
                    } else {
                        active[t].1 += 1;
                    }
                }
                Workload::Mixed { nw, p_whole } => {
                    if rng.f64() < *p_whole {
                        whole = Some(rng.below(*nw as u64) as u32);
                    } else {
                        for _ in 0..small_per_rec {
                            let c = *nw + rng.below((nchunks - *nw) as u64) as u32;
                            let s0 = c * CHUNK_BLOCKS + rng.below((CHUNK_BLOCKS / small_io) as u64) as u32 * small_io;
                            rec.push((s0, true));
                        }
                    }
                }
                _ => {
                    for _ in 0..writes_per_rec {
                        let s0 = match &mut wl {
                            Workload::Uniform => rng.below((l / io) as u64) as u32 * io,
                            Workload::HotCold { w, hot, cold } => {
                                let set = if rng.f64() < *w { &*hot } else { &*cold };
                                let c = set[rng.below(set.len() as u64) as usize];
                                c * CHUNK_BLOCKS + rng.below((CHUNK_BLOCKS / io) as u64) as u32 * io
                            }
                            Workload::Zipf(z) => z.next(rng),
                            _ => unreachable!(),
                        };
                        for k in 0..io {
                            rec.push((s0 + k, k == 0));
                        }
                    }
                }
            }
            if let Some(c) = whole {
                for k in 0..CHUNK_BLOCKS {
                    rec.push((c * CHUNK_BLOCKS + k, k == 0));
                }
            }
            sim.user_record(&rec);
        }
    };

    let fill_user = sim.c.user;
    write_until(&mut sim, &mut rng, fill_user + (cfg.warm * n as f64) as u64);
    let start = sim.c;
    let mut wins = Vec::new();
    let mut prev = sim.c;
    for _ in 0..cfg.windows {
        let target = sim.c.user + (cfg.win * n as f64) as u64;
        write_until(&mut sim, &mut rng, target);
        wins.push(sim.c.minus(&prev).wa());
        prev = sim.c;
    }
    let m = sim.c.minus(&start);
    let wa = m.wa();
    let u = m.user as f64;
    let chunk_wa = (m.user + m.user_hdr + m.clean + m.clean_hdr + m.slot_hdr) as f64 / u;
    let victim_u = m.victim_live as f64 / (m.victims as f64 * b as f64);
    let avg_free = m.free_sum as f64 / m.free_samples as f64;
    let slot_segs = m.slot_segs_sum as f64 / m.free_samples as f64;
    let slot_occ = if slot_segs > 0.0 { sim.whole_chunks as f64 / (slot_segs * sim.spp as f64) } else { 0.0 };
    let wmin = wins.iter().cloned().fold(f64::MAX, f64::min);
    let wmax = wins.iter().cloned().fold(0.0, f64::max);
    let spread = (wmax - wmin) / wa;
    let wins_s: Vec<String> = wins.iter().map(|w| format!("{w:.3}")).collect();
    let runs_idx = sim.idx.runs.len();
    let runs_mst = sim.mst.runs.len();
    println!(
        "RUN tag={} wl={} u={:.3} pol={} sep={:?} streams={} seg_mb={} segs={} disk_gb={:.0} rec_kb={} io_kb={} slots={} slot_hdr_in_log={} whole_f={} whole_w={} whole_stream={} file_chunks={} writers={} meta={} raft_ratio={} meta_live={} raft_stream={} run_stream={} compaction={} hot={}/{} | wa={:.4} chunk_wa={:.4} copies_per_user={:.4} hdr_per_user={:.4} raft_per_user={:.4} runflush_per_user={:.4} runcompact_per_user={:.4} manifest_per_user={:.4} blobmove_per_user={:.4} slothdr_per_user={:.4} idx_user_entries_per_write={:.4} idx_clean_entries_per_user={:.4} victim_u={:.4} u_data={:.4} u_total={:.4} avg_free={:.1} slot_segs={:.1} slot_occ={:.4} ell={:.0} runs={}/{} windows=[{}] spread={:.4} steady={} secs={:.1}",
        cfg.tag,
        cfg.wl,
        cfg.u,
        match cfg.policy {
            Policy::Greedy => "greedy",
            Policy::CostBenefit => "cb",
        },
        cfg.sep,
        sim.nst,
        cfg.seg_mb,
        cfg.segs,
        n as f64 * 4096.0 / (1u64 << 30) as f64,
        cfg.rec_kb,
        cfg.io_kb,
        u8::from(cfg.slots),
        u8::from(cfg.slot_hdr_in_log),
        cfg.whole_f,
        cfg.whole_w,
        cfg.whole_stream,
        cfg.file_chunks,
        cfg.writers,
        u8::from(cfg.meta),
        cfg.raft_ratio,
        cfg.meta_live,
        cfg.raft_stream,
        cfg.run_stream,
        cfg.compaction,
        cfg.hot_w,
        cfg.hot_f,
        wa,
        chunk_wa,
        m.clean as f64 / u,
        (m.user_hdr + m.clean_hdr + m.meta_hdr) as f64 / u,
        m.raft as f64 / u,
        m.run_flush as f64 / u,
        m.run_compact as f64 / u,
        m.manifest as f64 / u,
        m.clean_blob as f64 / u,
        m.slot_hdr as f64 / u,
        m.idx_user_entries as f64 / u,
        m.idx_clean_entries as f64 / u,
        victim_u,
        l as f64 / n as f64,
        m.live_sum as f64 / m.free_samples as f64 / n as f64,
        avg_free,
        slot_segs,
        slot_occ,
        sim.ell.min(1e15),
        runs_idx,
        runs_mst,
        wins_s.join(","),
        spread,
        if spread < 0.03 { "OK" } else { "NO" },
        t0.elapsed().as_secs_f64()
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    run(&Cfg::parse(&args));
}
