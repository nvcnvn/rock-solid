//! Write-amplification simulator for a log-structured ChunkStore with a segment cleaner.
//!
//! Usage: cleaner-wa key=value ...   (see `Cfg::parse` for keys)
//!        cleaner-wa bench           (copy-loop CPU micro-benchmark)

use std::collections::HashMap;
use std::time::Instant;

const CHUNK_BLOCKS: u32 = 1024;
const INVALID: u32 = u32::MAX;
const BLOCK_BYTES: usize = 4096;
const REC_HDR_BYTES: usize = 64;
const CLEAN_REC_BLOCKS: usize = 256;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Policy {
    Greedy,
    CostBenefit,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum CopyOrder {
    Chunk,
    Age,
    Victim,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    Free,
    Open,
    Sealed,
    Cleaning,
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
    order: CopyOrder,
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
    snap: f64,
    age_cap: f64,
    hot_w: f64,
    hot_f: f64,
    theta: f64,
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
        let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
        let f = |k: &str, d: &str| get(k, d).parse::<f64>().unwrap();
        let seg_mb: u32 = get("seg_mb", "4").parse().unwrap();
        let segs: u32 = match kv.get("segs") {
            Some(s) => s.parse().unwrap(),
            None => (f("disk_gb", "16") * 1024.0 / seg_mb as f64) as u32,
        };
        let known = [
            "wl", "u", "pol", "streams", "order", "seg_mb", "segs", "disk_gb", "rec_kb", "io_kb",
            "headers", "entry_b", "crc_b", "warm", "windows", "win", "free_low", "batch", "snap", "age_cap", "hot_w", "hot_f",
            "theta", "seed", "tag",
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
            order: match get("order", "chunk").as_str() {
                "chunk" => CopyOrder::Chunk,
                "age" => CopyOrder::Age,
                "victim" => CopyOrder::Victim,
                o => panic!("order {o}"),
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
            snap: f("snap", "0"),
            age_cap: f("age_cap", "0"),
            hot_w: f("hot_w", "0.8"),
            hot_f: f("hot_f", "0.2"),
            theta: f("theta", "0.99"),
            seed: get("seed", "1").parse().unwrap(),
            tag: get("tag", "-"),
        }
    }
}

enum Workload {
    Uniform,
    HotCold { w: f64, hot: Vec<u32>, cold: Vec<u32> },
    Zipf(Box<Zipf>),
    ChunkRand,
    Fifo { cursor: u32 },
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
            ((self.n * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as usize)
                .min(self.perm.len() - 1)
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
    victims: u64,
    victim_live: u64,
    passes: u64,
    free_sum: u64,
    free_samples: u64,
}

impl Counters {
    fn minus(&self, o: &Counters) -> Counters {
        Counters {
            user: self.user - o.user,
            user_hdr: self.user_hdr - o.user_hdr,
            clean: self.clean - o.clean,
            clean_hdr: self.clean_hdr - o.clean_hdr,
            victims: self.victims - o.victims,
            victim_live: self.victim_live - o.victim_live,
            passes: self.passes - o.passes,
            free_sum: self.free_sum - o.free_sum,
            free_samples: self.free_samples - o.free_samples,
        }
    }
    fn wa(&self) -> f64 {
        (self.user + self.user_hdr + self.clean + self.clean_hdr) as f64 / self.user as f64
    }
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
    state: Vec<State>,
    free: Vec<u32>,
    open: [u32; 2],
    fwd: Vec<u32>,
    rev: Vec<u32>,
    bmtime: Vec<u32>,
    pending: Vec<u64>,
    pin_slot: Vec<u32>,
    pin_lba: Vec<u32>,
    pin_mtime: Vec<u32>,
    now: u32,
    policy: Policy,
    streams: usize,
    order: CopyOrder,
    free_low: usize,
    batch: usize,
    age_cap: f64,
    c: Counters,
    scratch: Vec<(u64, u32)>,
    cblocks: Vec<(u32, bool)>,
}

impl Sim {
    fn new(cfg: &Cfg, l: u32) -> Sim {
        let b = cfg.seg_mb * 256;
        let s = cfg.segs;
        let n = b as usize * s as usize;
        assert!(n < INVALID as usize);
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
            state: vec![State::Free; s as usize],
            free: (0..s).rev().collect(),
            open: [INVALID; 2],
            fwd: vec![INVALID; l as usize],
            rev: vec![INVALID; n],
            bmtime: vec![0; l as usize],
            pending: vec![0; (l as usize).div_ceil(64)],
            pin_slot: Vec::new(),
            pin_lba: Vec::new(),
            pin_mtime: Vec::new(),
            now: 0,
            policy: cfg.policy,
            streams: cfg.streams,
            order: cfg.order,
            free_low: cfg.free_low,
            batch: cfg.batch,
            age_cap: if cfg.age_cap > 0.0 { cfg.age_cap * n as f64 } else { f64::INFINITY },
            c: Counters::default(),
            scratch: Vec::new(),
            cblocks: Vec::new(),
        }
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
        self.open[st] = seg;
        seg
    }

    fn seal(&mut self, st: usize) {
        self.state[self.open[st] as usize] = State::Sealed;
        self.open[st] = INVALID;
    }

    fn place(&mut self, id: u32, slot: u32, user: bool) {
        let b = self.b;
        let mt;
        if id < self.l {
            let lba = id as usize;
            let old = self.fwd[lba];
            if user {
                let prev_mt = self.bmtime[lba];
                self.now = self.now.checked_add(1).expect("clock overflow");
                self.bmtime[lba] = self.now;
                mt = self.now;
                if old != INVALID {
                    let (w, bit) = (lba / 64, 1u64 << (lba % 64));
                    if self.pending[w] & bit != 0 {
                        self.pending[w] &= !bit;
                        let pid = self.pin_slot.len() as u32;
                        self.pin_slot.push(old);
                        self.pin_lba.push(id);
                        self.pin_mtime.push(prev_mt);
                        self.rev[old as usize] = self.l + pid;
                    } else {
                        self.rev[old as usize] = INVALID;
                        self.live[(old / b) as usize] -= 1;
                    }
                }
            } else {
                mt = self.bmtime[lba];
                self.rev[old as usize] = INVALID;
                self.live[(old / b) as usize] -= 1;
            }
            self.fwd[lba] = slot;
        } else {
            let pid = (id - self.l) as usize;
            let old = self.pin_slot[pid];
            self.rev[old as usize] = INVALID;
            self.live[(old / b) as usize] -= 1;
            self.pin_slot[pid] = slot;
            mt = self.pin_mtime[pid];
        }
        let seg = (slot / b) as usize;
        self.rev[slot as usize] = id;
        self.live[seg] += 1;
        if mt > self.smtime[seg] {
            self.smtime[seg] = mt;
        }
    }

    fn append(&mut self, st: usize, blocks: &[(u32, bool)], user: bool) {
        let hdr_min = usize::from(self.headers);
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
            while i + n < blocks.len() {
                let start = n == 0 || blocks[i + n].1;
                let nb = bytes + if start { self.entry_b } else { 0 } + self.crc_b;
                let h = if self.headers { nb.div_ceil(BLOCK_BYTES) } else { 0 };
                if h + n + 1 > remaining {
                    break;
                }
                bytes = nb;
                n += 1;
            }
            let h = if self.headers { bytes.div_ceil(BLOCK_BYTES) } else { 0 };
            let base = seg as u32 * self.b + self.fill[seg];
            for k in 0..h as u32 {
                self.rev[(base + k) as usize] = INVALID;
            }
            self.fill[seg] += h as u32;
            for k in 0..n {
                let slot = seg as u32 * self.b + self.fill[seg];
                self.fill[seg] += 1;
                self.place(blocks[i + k].0, slot, user);
            }
            if user {
                self.c.user_hdr += h as u64;
                self.c.user += n as u64;
            } else {
                self.c.clean_hdr += h as u64;
                self.c.clean += n as u64;
            }
            i += n;
            if self.fill[seg] == self.b {
                self.seal(st);
            }
        }
    }

    fn lba_ver(&self, id: u32) -> (u32, u32) {
        if id < self.l { (id, 0) } else { (self.pin_lba[(id - self.l) as usize], 1) }
    }

    /// Header blocks a copy of `live` scattered blocks costs, plus one split record: a victim
    /// that frees no more than this loses space when cleaned.
    fn copy_overhead(&self, live: f64) -> f64 {
        if self.headers {
            live * (self.entry_b + self.crc_b) as f64 / BLOCK_BYTES as f64 + 2.0
        } else {
            0.0
        }
    }

    fn pick_victims(&self) -> Vec<u32> {
        let k = self.batch;
        let mut best: Vec<(f64, u32)> = Vec::with_capacity(k + 1);
        let bf = self.b as f64;
        for seg in 0..self.s as usize {
            if self.state[seg] != State::Sealed {
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
            "cleaner cannot make progress: victims are full"
        );
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        for &v in &victims {
            let vi = v as usize;
            self.state[vi] = State::Cleaning;
            self.c.victims += 1;
            self.c.victim_live += self.live[vi] as u64;
            let base = v * self.b;
            for off in 0..self.fill[vi] {
                let id = self.rev[(base + off) as usize];
                if id == INVALID {
                    continue;
                }
                let key = match self.order {
                    CopyOrder::Chunk => {
                        let (lba, ver) = self.lba_ver(id);
                        (((lba / CHUNK_BLOCKS) as u64) << 11)
                            | ((ver as u64) << 10)
                            | (lba % CHUNK_BLOCKS) as u64
                    }
                    CopyOrder::Age => {
                        if id < self.l {
                            self.bmtime[id as usize] as u64
                        } else {
                            self.pin_mtime[(id - self.l) as usize] as u64
                        }
                    }
                    CopyOrder::Victim => 0,
                };
                scratch.push((key, id));
            }
        }
        if self.order != CopyOrder::Victim {
            scratch.sort_unstable();
        }
        let mut blocks = std::mem::take(&mut self.cblocks);
        blocks.clear();
        let mut prev: Option<(u32, u32)> = None;
        for &(_, id) in &scratch {
            let (lba, ver) = self.lba_ver(id);
            let contiguous = matches!(prev, Some((pl, pv)) if pv == ver && lba == pl + 1 && lba % CHUNK_BLOCKS != 0);
            blocks.push((id, !contiguous));
            prev = Some((lba, ver));
        }
        let st = if self.streams == 2 { 1 } else { 0 };
        for rec in blocks.chunks(CLEAN_REC_BLOCKS) {
            self.append(st, rec, false);
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
        self.scratch = scratch;
        self.cblocks = blocks;
    }

    fn user_record(&mut self, blocks: &[(u32, bool)]) {
        let mut stalled = 0;
        while self.free.len() < self.free_low {
            let before = self.free.len();
            self.clean_pass();
            stalled = if self.free.len() > before { 0 } else { stalled + 1 };
            assert!(stalled < 1000, "cleaner livelock: 1000 passes without gaining a free segment");
        }
        self.c.free_sum += self.free.len() as u64;
        self.c.free_samples += 1;
        self.append(0, blocks, true);
    }
}

fn run(cfg: &Cfg) {
    let t0 = Instant::now();
    let b = cfg.seg_mb * 256;
    let n = b as u64 * cfg.segs as u64;
    let l = ((cfg.u * n as f64 / (1.0 + cfg.snap)) as u32 / CHUNK_BLOCKS) * CHUNK_BLOCKS;
    let nchunks = l / CHUNK_BLOCKS;
    let mut rng = Rng(cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x1234_5678);
    let io = (cfg.io_kb / 4).max(1);
    assert!(CHUNK_BLOCKS % io == 0);
    let writes_per_rec = ((cfg.rec_kb / 4) / io).max(1);
    let mut wl = match cfg.wl.as_str() {
        "uniform" => Workload::Uniform,
        "hotcold" => {
            let mut chunks: Vec<u32> = (0..nchunks).collect();
            rng.shuffle(&mut chunks);
            let nh = ((nchunks as f64 * cfg.hot_f).round() as usize).max(1);
            let cold = chunks.split_off(nh);
            Workload::HotCold { w: cfg.hot_w, hot: chunks, cold }
        }
        "zipf" => {
            assert_eq!(io, 1);
            Workload::Zipf(Box::new(Zipf::new(l, cfg.theta, &mut rng)))
        }
        "chunkrand" => {
            assert_eq!(io, CHUNK_BLOCKS);
            Workload::ChunkRand
        }
        "fifo" => {
            assert_eq!(io, CHUNK_BLOCKS);
            Workload::Fifo { cursor: 0 }
        }
        w => panic!("wl {w}"),
    };
    let mut sim = Sim::new(cfg, l);

    let mut rec: Vec<(u32, bool)> = Vec::new();
    for chunk in 0..nchunks {
        for part in 0..(CHUNK_BLOCKS as usize / CLEAN_REC_BLOCKS) as u32 {
            rec.clear();
            let s0 = chunk * CHUNK_BLOCKS + part * CLEAN_REC_BLOCKS as u32;
            for k in 0..CLEAN_REC_BLOCKS as u32 {
                rec.push((s0 + k, k == 0));
            }
            sim.user_record(&rec);
        }
    }
    if cfg.snap > 0.0 {
        let mut chunks: Vec<u32> = (0..nchunks).collect();
        rng.shuffle(&mut chunks);
        let ns = (nchunks as f64 * cfg.snap).round() as usize;
        for &c in &chunks[..ns.min(chunks.len())] {
            for lba in c * CHUNK_BLOCKS..(c + 1) * CHUNK_BLOCKS {
                sim.pending[lba as usize / 64] |= 1u64 << (lba % 64);
            }
        }
    }

    let mut next_write = |rng: &mut Rng| -> u32 {
        match &mut wl {
            Workload::Uniform => rng.below((l / io) as u64) as u32 * io,
            Workload::HotCold { w, hot, cold } => {
                let set = if rng.f64() < *w { &*hot } else { &*cold };
                let c = set[rng.below(set.len() as u64) as usize];
                c * CHUNK_BLOCKS + rng.below((CHUNK_BLOCKS / io) as u64) as u32 * io
            }
            Workload::Zipf(z) => z.next(rng),
            Workload::ChunkRand => rng.below(nchunks as u64) as u32 * CHUNK_BLOCKS,
            Workload::Fifo { cursor } => {
                let c = *cursor;
                *cursor = (*cursor + 1) % nchunks;
                c * CHUNK_BLOCKS
            }
        }
    };

    let mut write_until = |sim: &mut Sim, rng: &mut Rng, target: u64| {
        while sim.c.user < target {
            rec.clear();
            for _ in 0..writes_per_rec {
                let s0 = next_write(rng);
                for k in 0..io {
                    rec.push((s0 + k, k == 0));
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
    let wa_data = (m.user + m.clean) as f64 / m.user as f64;
    let victim_u = m.victim_live as f64 / (m.victims as f64 * b as f64);
    let avg_free = m.free_sum as f64 / m.free_samples as f64;
    let pinned = sim.pin_slot.len() as u64;
    let live_total = l as u64 + pinned;
    let u_live = live_total as f64 / n as f64;
    let u_eff = live_total as f64 / ((cfg.segs as f64 - avg_free) * b as f64);
    let wmin = wins.iter().cloned().fold(f64::MAX, f64::min);
    let wmax = wins.iter().cloned().fold(0.0, f64::max);
    let spread = (wmax - wmin) / wa;
    let wins_s: Vec<String> = wins.iter().map(|w| format!("{w:.3}")).collect();
    println!(
        "RUN tag={} wl={} u={:.3} pol={} streams={} order={:?} seg_mb={} segs={} disk_gb={:.0} rec_kb={} io_kb={} headers={} entry_b={} snap={} age_cap={} hot={}/{} theta={} free_low={} batch={} | wa={:.4} wa_data={:.4} hdr_per_user={:.4} uhdr_per_user={:.4} chdr_per_copy={:.4} copies_per_user={:.4} segread_per_user={:.4} victim_u={:.4} u_live={:.4} u_eff={:.4} avg_free={:.1} pinned_frac={:.4} passes_per_gb={:.2} windows=[{}] spread={:.4} steady={} secs={:.1}",
        cfg.tag,
        cfg.wl,
        cfg.u,
        match cfg.policy {
            Policy::Greedy => "greedy",
            Policy::CostBenefit => "cb",
        },
        cfg.streams,
        cfg.order,
        cfg.seg_mb,
        cfg.segs,
        n as f64 * 4096.0 / (1u64 << 30) as f64,
        cfg.rec_kb,
        cfg.io_kb,
        u8::from(cfg.headers),
        cfg.entry_b,
        cfg.snap,
        cfg.age_cap,
        cfg.hot_w,
        cfg.hot_f,
        cfg.theta,
        cfg.free_low,
        cfg.batch,
        wa,
        wa_data,
        (m.user_hdr + m.clean_hdr) as f64 / m.user as f64,
        m.user_hdr as f64 / m.user as f64,
        m.clean_hdr as f64 / m.clean.max(1) as f64,
        m.clean as f64 / m.user as f64,
        m.victims as f64 * b as f64 / m.user as f64,
        victim_u,
        u_live,
        u_eff,
        avg_free,
        pinned as f64 / l as f64,
        m.passes as f64 / (m.user as f64 * 4096.0 / (1u64 << 30) as f64),
        wins_s.join(","),
        spread,
        if spread < 0.03 { "OK" } else { "NO" },
        t0.elapsed().as_secs_f64()
    );
}

#[cfg(target_arch = "x86_64")]
fn crc32c(data: &[u8]) -> u32 {
    use std::arch::x86_64::_mm_crc32_u64;
    let mut c: u64 = 0xFFFF_FFFF;
    for w in data.chunks_exact(8) {
        let v = u64::from_le_bytes(w.try_into().unwrap());
        // SAFETY: bench() checks for SSE4.2 before calling.
        c = unsafe { _mm_crc32_u64(c, v) };
    }
    !(c as u32)
}

fn bench() {
    assert!(std::arch::is_x86_feature_detected!("sse4.2"));
    let nblk = 16384usize;
    let mut rng = Rng(7);
    let src: Vec<u8> = (0..nblk * BLOCK_BYTES).map(|_| rng.next() as u8).collect();
    let mut dst = vec![0u8; nblk * BLOCK_BYTES];
    let mut order: Vec<u32> = (0..nblk as u32).collect();
    rng.shuffle(&mut order);
    let reps = 40;
    let time = |f: &mut dyn FnMut() -> u64| {
        let t = Instant::now();
        let mut sink = 0u64;
        for _ in 0..reps {
            sink = sink.wrapping_add(f());
        }
        (t.elapsed().as_nanos() as f64 / (reps * nblk) as f64, sink)
    };
    let (copy_ns, s1) = time(&mut || {
        for (i, &o) in order.iter().enumerate() {
            let o = o as usize;
            dst[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES]
                .copy_from_slice(&src[o * BLOCK_BYTES..(o + 1) * BLOCK_BYTES]);
        }
        dst[17] as u64
    });
    let (crc_ns, s2) = time(&mut || {
        let mut x = 0u64;
        for &o in &order {
            let o = o as usize;
            x ^= crc32c(&src[o * BLOCK_BYTES..(o + 1) * BLOCK_BYTES]) as u64;
        }
        x
    });
    let (both_ns, s3) = time(&mut || {
        let mut x = 0u64;
        for (i, &o) in order.iter().enumerate() {
            let o = o as usize;
            let blk = &src[o * BLOCK_BYTES..(o + 1) * BLOCK_BYTES];
            x ^= crc32c(blk) as u64;
            dst[i * BLOCK_BYTES..(i + 1) * BLOCK_BYTES].copy_from_slice(blk);
        }
        x
    });
    let mut idx: HashMap<u64, u64> = HashMap::with_capacity(1 << 22);
    for i in 0..(1u64 << 22) {
        idx.insert(i.wrapping_mul(0x9E37_79B9_7F4A_7C15), i);
    }
    let t = Instant::now();
    let nupd = 1u64 << 22;
    for i in 0..nupd {
        let k = (rng.below(1 << 22)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        *idx.get_mut(&k).unwrap() = i;
    }
    let idx_ns = t.elapsed().as_nanos() as f64 / nupd as f64;
    println!(
        "BENCH copy4k_ns={copy_ns:.0} crc32c4k_ns={crc_ns:.0} copy+crc4k_ns={both_ns:.0} hashmap_index_update_ns={idx_ns:.0} (sink {})",
        s1 ^ s2 ^ s3
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(|s| s.as_str()) == Some("bench") {
        bench();
        return;
    }
    run(&Cfg::parse(&args));
}
