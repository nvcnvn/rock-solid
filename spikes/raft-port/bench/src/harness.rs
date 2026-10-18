//! One synchronous loop driving 3 simulated nodes x N groups, shared by both
//! libraries. Adapted from spikes/multi-raft/src/rr.rs: the loop owns time
//! (ticks), the network (a VecDeque) and storage. Each round it ticks every
//! replica, delivers all queued messages, collects every dirty group's Ready,
//! persists all of them, does one (simulated) fsync, then advances.

use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::common::*;

/// What the loop needs from a message.
pub trait Msg {
    fn from(&self) -> u64;
    fn to(&self) -> u64;
    fn kind(&self) -> i32;
    fn term(&self) -> u64;
    fn index(&self) -> u64;
    fn is_heartbeat(&self) -> bool;
    fn append_entries(&self) -> Option<usize>;
}

/// Per-Ready counters reported back to the loop.
#[derive(Default)]
pub struct ReadyOut {
    pub persisted_entries: u64,
    pub must_sync: bool,
    pub ops: u64,
}

/// One replica of one group.
pub trait Replica {
    type M: Msg;
    type Pending;
    fn new(id: u64, group: usize, o: &Opts, tick_ms: u64) -> Self;
    fn tick(&mut self);
    fn step(&mut self, m: Self::M);
    fn campaign(&mut self);
    fn propose(&mut self, data: Vec<u8>) -> bool;
    fn is_leader(&self) -> bool;
    fn term(&self) -> u64;
    fn has_ready(&self) -> bool;
    /// Takes the Ready, persists it (not yet synced) and applies committed entries.
    fn ready_persist(&mut self, send: &mut Vec<Self::M>, out: &mut ReadyOut) -> Self::Pending;
    /// Called after the shared fsync. Returns true if the replica has more work.
    fn after_sync(&mut self, p: Self::Pending, send: &mut Vec<Self::M>, out: &mut ReadyOut)
    -> bool;
}

struct Env<M> {
    group: u32,
    msg: M,
}

#[derive(Default, Clone, Copy)]
pub struct Stats {
    pub msgs: u64,
    pub heartbeats: u64,
    pub appends: u64,
    pub append_entries: u64,
    pub rounds: u64,
    pub fsyncs: u64,
    pub persisted_entries: u64,
    pub readies: u64,
    pub ticks: u64,
}

pub struct Cluster<R: Replica> {
    pub nodes: Vec<Vec<R>>,
    queue: VecDeque<Env<R::M>>,
    dirty: Vec<(usize, usize)>,
    pub dead: [bool; 3],
    pub st: Stats,
    outstanding: Vec<usize>,
    pub ops: u64,
    payload: Vec<u8>,
    writing: bool,
    inflight: usize,
    fsync_us: u64,
    sim_rng: Option<StdRng>,
    trace: Option<DefaultHasher>,
    vtick: u64,
}

impl<R: Replica> Cluster<R> {
    pub fn new(o: &Opts, tick_ms: u64) -> Cluster<R> {
        let mut nodes = Vec::new();
        for id in 1..=3u64 {
            let mut v = Vec::with_capacity(o.groups);
            for g in 0..o.groups {
                v.push(R::new(id, g, o, tick_ms));
            }
            nodes.push(v);
        }
        Cluster {
            nodes,
            queue: VecDeque::new(),
            dirty: Vec::new(),
            dead: [false; 3],
            st: Stats::default(),
            outstanding: vec![0; o.groups],
            ops: 0,
            payload: vec![7u8; o.payload],
            writing: false,
            inflight: o.inflight,
            fsync_us: o.fsync_us,
            sim_rng: None,
            trace: None,
            vtick: 0,
        }
    }

    fn groups(&self) -> usize {
        self.nodes[0].len()
    }

    pub fn tick_all(&mut self) {
        self.st.ticks += 1;
        for n in 0..3 {
            if self.dead[n] {
                continue;
            }
            for g in 0..self.groups() {
                self.nodes[n][g].tick();
                self.dirty.push((n, g));
            }
        }
    }

    fn send(&mut self, group: usize, msgs: &mut Vec<R::M>) {
        for m in msgs.drain(..) {
            self.st.msgs += 1;
            if m.is_heartbeat() {
                self.st.heartbeats += 1;
            }
            if let Some(n) = m.append_entries() {
                self.st.appends += 1;
                self.st.append_entries += n as u64;
            }
            self.queue.push_back(Env {
                group: group as u32,
                msg: m,
            });
        }
    }

    fn deliver_batch(&mut self) {
        let mut batch: Vec<Env<R::M>> = std::mem::take(&mut self.queue).into();
        if let Some(rng) = self.sim_rng.as_mut() {
            for i in (1..batch.len()).rev() {
                batch.swap(i, rng.gen_range(0..=i));
            }
        }
        self.st.rounds += 1;
        for e in batch {
            let (from, to) = (e.msg.from() as usize - 1, e.msg.to() as usize - 1);
            if self.dead[from] || self.dead[to] {
                continue;
            }
            if let Some(rng) = self.sim_rng.as_mut() {
                if rng.gen_bool(0.1) {
                    continue;
                }
            }
            let g = e.group as usize;
            if let Some(h) = self.trace.as_mut() {
                (
                    self.vtick,
                    g,
                    from,
                    to,
                    e.msg.kind(),
                    e.msg.term(),
                    e.msg.index(),
                )
                    .hash(h);
            }
            self.nodes[to][g].step(e.msg);
            self.dirty.push((to, g));
        }
    }

    fn on_ops(&mut self, g: usize, ops: u64) {
        self.ops += ops;
        self.outstanding[g] = self.outstanding[g].saturating_sub(ops as usize);
    }

    /// Collects every dirty group's Ready, persists all of them, fsyncs once, then advances.
    fn process_ready(&mut self) {
        let mut dirty = std::mem::take(&mut self.dirty);
        dirty.sort_unstable();
        dirty.dedup();
        let mut stash = Vec::new();
        let mut must_sync = false;
        let mut send = Vec::new();
        for (n, g) in dirty {
            if self.dead[n] || !self.nodes[n][g].has_ready() {
                continue;
            }
            self.st.readies += 1;
            let mut out = ReadyOut::default();
            let p = self.nodes[n][g].ready_persist(&mut send, &mut out);
            self.send(g, &mut send);
            self.st.persisted_entries += out.persisted_entries;
            must_sync |= out.must_sync;
            self.on_ops(g, out.ops);
            stash.push((n, g, p));
        }
        if must_sync {
            self.st.fsyncs += 1;
            if self.fsync_us > 0 {
                std::thread::sleep(Duration::from_micros(self.fsync_us));
            }
        }
        for (n, g, p) in stash {
            let mut out = ReadyOut::default();
            let more = self.nodes[n][g].after_sync(p, &mut send, &mut out);
            self.send(g, &mut send);
            self.on_ops(g, out.ops);
            if more {
                self.dirty.push((n, g));
            }
        }
    }

    fn propose_all(&mut self) {
        for g in 0..self.groups() {
            if self.outstanding[g] >= self.inflight {
                continue;
            }
            let Some(l) = self.leader_of(g) else { continue };
            while self.outstanding[g] < self.inflight {
                if !self.nodes[l][g].propose(self.payload.clone()) {
                    break;
                }
                self.outstanding[g] += 1;
            }
            self.dirty.push((l, g));
        }
    }

    pub fn leader_of(&self, g: usize) -> Option<usize> {
        (0..3).find(|&n| !self.dead[n] && self.nodes[n][g].is_leader())
    }

    pub fn leaders(&self) -> usize {
        (0..self.groups())
            .filter(|&g| self.leader_of(g).is_some())
            .count()
    }

    fn leaders_on(&self, n: usize) -> usize {
        (0..self.groups())
            .filter(|&g| self.nodes[n][g].is_leader())
            .count()
    }

    pub fn settle_network(&mut self) {
        self.process_ready();
        while !self.queue.is_empty() || !self.dirty.is_empty() {
            self.deliver_batch();
            self.process_ready();
        }
    }

    /// Wall-clock driver. Runs until `until` or `stop` returns true (checked once per round).
    fn run(
        &mut self,
        until: Instant,
        tick: Duration,
        next_tick: &mut Instant,
        mut stop: impl FnMut(&Self) -> bool,
    ) -> bool {
        loop {
            let now = Instant::now();
            if now >= until {
                return false;
            }
            if now >= *next_tick {
                self.tick_all();
                *next_tick += tick;
                if *next_tick < now {
                    *next_tick = now + tick;
                }
            }
            if self.writing {
                self.propose_all();
            }
            self.settle_network();
            if stop(self) {
                return true;
            }
            if !self.writing {
                let wake = (*next_tick).min(until);
                let now = Instant::now();
                if wake > now {
                    std::thread::sleep(wake - now);
                }
            }
        }
    }
}

pub fn bench<R: Replica>(lib: &str, o: &Opts) {
    let tick_ms = if o.tick_ms > 0 { o.tick_ms } else { o.hb_ms };
    let tick = Duration::from_millis(tick_ms);
    println!(
        "=== {lib} groups={} tick={}ms hb={}ms election={}-{}ms write={}s inflight={} fsync_us={} loadavg={}",
        o.groups,
        tick_ms,
        o.hb_ms,
        o.elect_min_ms,
        o.elect_max_ms,
        o.write_s,
        o.inflight,
        o.fsync_us,
        loadavg()
    );
    let rss0 = rss_bytes();
    let heap0 = heap_bytes();
    let mut c: Cluster<R> = Cluster::new(o, tick_ms);
    let heap_created = (heap_bytes() - heap0) as f64 / o.groups as f64;
    let t0 = Instant::now();
    for g in 0..c.groups() {
        c.nodes[g % 3][g].campaign();
        c.dirty.push((g % 3, g));
    }
    let mut next_tick = Instant::now() + tick;
    let n = c.groups();
    let done = c.run(t0 + Duration::from_secs(60), tick, &mut next_tick, |c| {
        c.leaders() == n
    });
    let el = t0.elapsed();
    info(
        "P5",
        &format!("{lib} all groups elected"),
        format!(
            "{} ms ({} leaders, done={done})",
            el.as_millis(),
            c.leaders()
        ),
    );
    info(
        "P5",
        &format!("{lib} leaders per node"),
        format!(
            "{} {} {}",
            c.leaders_on(0),
            c.leaders_on(1),
            c.leaders_on(2)
        ),
    );
    c.run(
        Instant::now() + Duration::from_secs(o.settle_s),
        tick,
        &mut next_tick,
        |_| false,
    );
    let rss_group = (rss_bytes() - rss0) as f64 / n as f64;
    let heap_group = (heap_bytes() - heap0) as f64 / n as f64;
    println!(
        "      P5 {lib} memory per group (3 replicas) after election + {}s: live heap {} (at creation {}), RSS {} (total RSS {} MiB, threads {})",
        o.settle_s,
        kib(heap_group),
        kib(heap_created),
        kib(rss_group),
        rss_bytes() >> 20,
        threads()
    );
    println!(
        "RESULT lib={lib} metric=heap_kib_per_group value={:.2}",
        heap_group / 1024.0
    );
    println!(
        "RESULT lib={lib} metric=rss_kib_per_group value={:.2}",
        rss_group / 1024.0
    );

    if o.write_s > 0 {
        write_phase(&mut c, lib, o, tick, &mut next_tick);
        return;
    }

    let s0 = c.st;
    let w = CpuWindow::start();
    c.run(
        Instant::now() + Duration::from_secs(o.idle_s),
        tick,
        &mut next_tick,
        |_| false,
    );
    let (wall, cpu) = w.stop();
    let s1 = c.st;
    let secs = wall.as_secs_f64();
    let msgs = (s1.msgs - s0.msgs) as f64;
    let pct = cpu.as_secs_f64() / secs * 100.0;
    println!(
        "      P6 {lib} idle {}s: {:.2}% of one core, {:.0} msgs/s ({:.0} heartbeats/s), {:.2} us CPU per message, {:.1} msgs per group per heartbeat, loadavg {}",
        o.idle_s,
        pct,
        msgs / secs,
        (s1.heartbeats - s0.heartbeats) as f64 / secs,
        cpu.as_secs_f64() * 1e6 / msgs.max(1.0),
        msgs / n as f64 / (secs * 1000.0 / o.hb_ms as f64),
        loadavg()
    );
    println!("RESULT lib={lib} metric=idle_cpu_pct value={pct:.3}");
    println!(
        "RESULT lib={lib} metric=idle_msgs_per_s value={:.0}",
        msgs / secs
    );
    println!(
        "RESULT lib={lib} metric=idle_us_per_msg value={:.3}",
        cpu.as_secs_f64() * 1e6 / msgs.max(1.0)
    );
    println!(
        "      P5 {lib} after the idle window: live heap {} RSS {} per group",
        kib((heap_bytes() - heap0) as f64 / n as f64),
        kib((rss_bytes() - rss0) as f64 / n as f64)
    );
}

fn write_phase<R: Replica>(
    c: &mut Cluster<R>,
    lib: &str,
    o: &Opts,
    tick: Duration,
    next_tick: &mut Instant,
) {
    c.writing = true;
    let s0 = c.st;
    let ops0 = c.ops;
    let w = CpuWindow::start();
    c.run(
        Instant::now() + Duration::from_secs(o.write_s),
        tick,
        next_tick,
        |_| false,
    );
    let (wall, cpu) = w.stop();
    let s1 = c.st;
    let ops = (c.ops - ops0) as f64;
    let secs = wall.as_secs_f64();
    let msgs = (s1.msgs - s0.msgs) as f64;
    println!(
        "      P7 {lib} {}B proposals, {} in flight per group, {} groups, fsync {} us: {:.0} ops/s, {:.2} us CPU per op, {:.0} msgs/s, {:.2} us CPU per message, CPU {:.0}% of one core",
        o.payload,
        o.inflight,
        o.groups,
        o.fsync_us,
        ops / secs,
        cpu.as_secs_f64() * 1e6 / ops.max(1.0),
        msgs / secs,
        cpu.as_secs_f64() * 1e6 / msgs.max(1.0),
        cpu.as_secs_f64() / secs * 100.0
    );
    println!(
        "      P7 {lib} batching: {:.2} entries per MsgApp, {:.2} entries persisted per Ready, {:.0} fsyncs/s, {:.1} entries per fsync, {:.1} Readys per fsync, {:.0} rounds/s, loadavg {}",
        (s1.append_entries - s0.append_entries) as f64 / (s1.appends - s0.appends).max(1) as f64,
        (s1.persisted_entries - s0.persisted_entries) as f64
            / (s1.readies - s0.readies).max(1) as f64,
        (s1.fsyncs - s0.fsyncs) as f64 / secs,
        (s1.persisted_entries - s0.persisted_entries) as f64
            / (s1.fsyncs - s0.fsyncs).max(1) as f64,
        (s1.readies - s0.readies) as f64 / (s1.fsyncs - s0.fsyncs).max(1) as f64,
        (s1.rounds - s0.rounds) as f64 / secs,
        loadavg()
    );
    println!(
        "RESULT lib={lib} metric=write_ops_per_s value={:.0}",
        ops / secs
    );
    println!(
        "RESULT lib={lib} metric=write_us_per_op value={:.3}",
        cpu.as_secs_f64() * 1e6 / ops.max(1.0)
    );
}

/// P4: virtual time, seeded message shuffle and 10% drop, node 1 stops at tick 150.
pub fn sim<R: Replica>(lib: &str, o: &Opts) {
    let tick_ms = o.hb_ms;
    let mut c: Cluster<R> = Cluster::new(o, tick_ms);
    c.sim_rng = Some(StdRng::seed_from_u64(o.seed));
    c.trace = Some(DefaultHasher::new());
    for t in 0..300 {
        c.vtick = t;
        if t == 150 {
            c.dead[0] = true;
        }
        c.tick_all();
        c.settle_network();
    }
    let mut h = c.trace.take().unwrap();
    let mut leaders = Vec::new();
    for g in 0..c.groups() {
        let l = c.leader_of(g).map(|n| n + 1).unwrap_or(0);
        let term = c.nodes[1][g].term();
        (l, term).hash(&mut h);
        leaders.push(format!("{l}@{term}"));
    }
    println!(
        "SIM {lib} seed={} rand_seed={} groups={} msgs={} trace={:016x} leaders={}",
        o.seed,
        o.rand_seed.unwrap_or(o.seed),
        o.groups,
        c.st.msgs,
        h.finish(),
        leaders.join(",")
    );
}
