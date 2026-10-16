//! raft-rs (tikv/raft 0.7.0) harness: 3 simulated nodes x N groups driven by one
//! synchronous loop. The loop owns time (ticks), the network (a VecDeque) and storage.

use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use raft::prelude::*;
use raft::storage::MemStorage;
use raft::StateRole;
use slog::{o, Discard, Logger};

use crate::common::*;

struct Env {
    group: u32,
    msg: Message,
}

#[derive(Default, Clone, Copy)]
struct Stats {
    msgs: u64,
    heartbeats: u64,
    appends: u64,
    append_entries: u64,
    pair_batches: u64,
    rounds: u64,
    fsyncs: u64,
    persisted_entries: u64,
    readies: u64,
    ticks: u64,
}

pub struct Cluster {
    nodes: Vec<Vec<RawNode<MemStorage>>>,
    queue: VecDeque<Env>,
    dirty: Vec<(usize, usize)>,
    dead: [bool; 3],
    st: Stats,
    outstanding: Vec<usize>,
    ops: u64,
    payload: Vec<u8>,
    writing: bool,
    inflight: usize,
    fsync_us: u64,
    // simulation-only
    sim_rng: Option<StdRng>,
    override_rng: Option<StdRng>,
    trace: Option<DefaultHasher>,
    vtick: u64,
    elect_ticks: (usize, usize),
    quiesced: bool,
}

impl Cluster {
    pub fn new(o: &Opts, tick_ms: u64) -> Cluster {
        let logger = Logger::root(Discard, o!());
        let et_min = (o.elect_min_ms / tick_ms) as usize;
        let et_max = (o.elect_max_ms / tick_ms) as usize;
        let mut nodes = Vec::new();
        for id in 1..=3u64 {
            let mut v = Vec::with_capacity(o.groups);
            for _ in 0..o.groups {
                let cfg = Config {
                    id,
                    heartbeat_tick: (o.hb_ms / tick_ms) as usize,
                    election_tick: et_min,
                    min_election_tick: et_min,
                    max_election_tick: et_max,
                    check_quorum: true,
                    pre_vote: true,
                    max_size_per_msg: 1 << 20,
                    max_inflight_msgs: 256,
                    ..Default::default()
                };
                let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
                v.push(RawNode::new(&cfg, store, &logger).unwrap());
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
            override_rng: None,
            trace: None,
            vtick: 0,
            elect_ticks: (et_min, et_max),
            quiesced: false,
        }
    }

    fn groups(&self) -> usize {
        self.nodes[0].len()
    }

    fn reroll_if_reset(&mut self, n: usize, g: usize, before: (u64, StateRole)) {
        if let Some(rng) = self.override_rng.as_mut() {
            let r = &mut self.nodes[n][g].raft;
            if (r.term, r.state) != before {
                let t = rng.gen_range(self.elect_ticks.0..self.elect_ticks.1);
                r.set_randomized_election_timeout(t);
            }
        }
    }

    fn snap(&self, n: usize, g: usize) -> (u64, StateRole) {
        let r = &self.nodes[n][g].raft;
        (r.term, r.state)
    }

    pub fn tick_all(&mut self) {
        if self.quiesced {
            return;
        }
        self.st.ticks += 1;
        for n in 0..3 {
            if self.dead[n] {
                continue;
            }
            for g in 0..self.groups() {
                let before = self.snap(n, g);
                self.nodes[n][g].tick();
                self.reroll_if_reset(n, g, before);
                self.dirty.push((n, g));
            }
        }
    }

    fn send(&mut self, group: usize, msgs: Vec<Message>) {
        for m in msgs {
            self.st.msgs += 1;
            match m.msg_type {
                MessageType::MsgHeartbeat => self.st.heartbeats += 1,
                MessageType::MsgAppend => {
                    self.st.appends += 1;
                    self.st.append_entries += m.entries.len() as u64;
                }
                _ => {}
            }
            self.queue.push_back(Env { group: group as u32, msg: m });
        }
    }

    fn deliver_batch(&mut self) {
        let mut batch: Vec<Env> = std::mem::take(&mut self.queue).into();
        if let Some(rng) = self.sim_rng.as_mut() {
            for i in (1..batch.len()).rev() {
                batch.swap(i, rng.gen_range(0..=i));
            }
        }
        let mut pairs = 0u16;
        for e in &batch {
            pairs |= 1 << ((e.msg.from - 1) * 3 + (e.msg.to - 1));
        }
        self.st.pair_batches += pairs.count_ones() as u64;
        self.st.rounds += 1;
        for e in batch {
            let (from, to) = (e.msg.from as usize - 1, e.msg.to as usize - 1);
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
                (self.vtick, g, from, to, e.msg.msg_type as i32, e.msg.term, e.msg.index).hash(h);
            }
            let before = self.snap(to, g);
            let _ = self.nodes[to][g].step(e.msg);
            self.reroll_if_reset(to, g, before);
            self.dirty.push((to, g));
        }
    }

    fn on_committed(&mut self, n: usize, g: usize, ents: Vec<Entry>) {
        if ents.is_empty() {
            return;
        }
        let leader = self.nodes[n][g].raft.state == StateRole::Leader;
        for e in ents {
            if e.entry_type == EntryType::EntryNormal && !e.data.is_empty() && leader {
                self.ops += 1;
                self.outstanding[g] = self.outstanding[g].saturating_sub(1);
            }
        }
    }

    /// Collects every dirty group's Ready, persists all of them, fsyncs once, then advances.
    fn process_ready(&mut self) {
        let mut dirty = std::mem::take(&mut self.dirty);
        dirty.sort_unstable();
        dirty.dedup();
        let mut stash = Vec::new();
        let mut must_sync = false;
        for (n, g) in dirty {
            if !self.nodes[n][g].has_ready() {
                continue;
            }
            self.st.readies += 1;
            let mut rd = self.nodes[n][g].ready();
            let msgs = rd.take_messages();
            self.send(g, msgs);
            let committed = rd.take_committed_entries();
            self.on_committed(n, g, committed);
            let store = self.nodes[n][g].store().clone();
            if !rd.entries().is_empty() {
                self.st.persisted_entries += rd.entries().len() as u64;
                store.wl().append(rd.entries()).unwrap();
            }
            if let Some(hs) = rd.hs() {
                store.wl().set_hardstate(hs.clone());
            }
            must_sync |= rd.must_sync();
            stash.push((n, g, rd));
        }
        if must_sync {
            self.st.fsyncs += 1;
            if self.fsync_us > 0 {
                std::thread::sleep(Duration::from_micros(self.fsync_us));
            }
        }
        for (n, g, mut rd) in stash {
            let pm = rd.take_persisted_messages();
            self.send(g, pm);
            let before = self.snap(n, g);
            let mut light = self.nodes[n][g].advance(rd);
            if let Some(c) = light.commit_index() {
                self.nodes[n][g].store().wl().mut_hard_state().set_commit(c);
            }
            let lm = light.take_messages();
            self.send(g, lm);
            let lc = light.take_committed_entries();
            self.on_committed(n, g, lc);
            self.nodes[n][g].advance_apply();
            self.reroll_if_reset(n, g, before);
        }
    }

    fn propose_all(&mut self) {
        for g in 0..self.groups() {
            if self.outstanding[g] >= self.inflight {
                continue;
            }
            let Some(l) = self.leader_of(g) else { continue };
            while self.outstanding[g] < self.inflight {
                if self.nodes[l][g].propose(vec![], self.payload.clone()).is_err() {
                    break;
                }
                self.outstanding[g] += 1;
            }
            self.dirty.push((l, g));
        }
    }

    fn leader_of(&self, g: usize) -> Option<usize> {
        (0..3).find(|&n| !self.dead[n] && self.nodes[n][g].raft.state == StateRole::Leader)
    }

    pub fn leaders(&self) -> usize {
        (0..self.groups()).filter(|&g| self.leader_of(g).is_some()).count()
    }

    fn leaders_on(&self, n: usize) -> usize {
        (0..self.groups()).filter(|&g| self.nodes[n][g].raft.state == StateRole::Leader).count()
    }

    fn settle_network(&mut self) {
        self.process_ready();
        while !self.queue.is_empty() {
            self.deliver_batch();
            self.process_ready();
        }
    }

    /// Wall-clock driver. Runs until `until` or `stop` returns true (checked once per round).
    fn run(&mut self, until: Instant, tick: Duration, next_tick: &mut Instant, mut stop: impl FnMut(&Self) -> bool) -> bool {
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

pub fn bench(o: &Opts) {
    let tick_ms = if o.tick_ms > 0 { o.tick_ms } else { o.hb_ms };
    let tick = Duration::from_millis(tick_ms);
    let lib = "raft-rs";
    println!(
        "=== raft-rs groups={} tick={}ms hb={}ms election={}-{}ms init={} write={}s inflight={} fsync_us={} loadavg={}",
        o.groups, tick_ms, o.hb_ms, o.elect_min_ms, o.elect_max_ms, o.init, o.write_s, o.inflight, o.fsync_us, loadavg()
    );
    let rss0 = rss_bytes();
    let heap0 = heap_bytes();
    let t_create = Instant::now();
    let mut c = Cluster::new(o, tick_ms);
    info("M1", "create 3 x groups RawNodes", format!("{} us", t_create.elapsed().as_micros()));
    let t0 = Instant::now();
    if o.init == "spread" {
        for g in 0..c.groups() {
            let _ = c.nodes[g % 3][g].campaign();
            c.dirty.push((g % 3, g));
        }
    }
    let mut next_tick = Instant::now() + tick;
    let n = c.groups();
    let done = c.run(t0 + Duration::from_secs(60), tick, &mut next_tick, |c| c.leaders() == n);
    let el = t0.elapsed();
    check(
        done && el.as_millis() as u64 <= 2 * o.elect_max_ms + o.hb_ms,
        "M3a",
        &format!("{lib} all groups elected from bootstrap ({})", o.init),
        format!("{} ms ({} leaders)", el.as_millis(), c.leaders()),
        &format!("<= {} ms", 2 * o.elect_max_ms + o.hb_ms),
    );
    info("M3a", "leaders per node", format!("{} {} {}", c.leaders_on(0), c.leaders_on(1), c.leaders_on(2)));
    let settle_until = Instant::now() + Duration::from_secs(2);
    c.run(settle_until, tick, &mut next_tick, |_| false);
    let per_group = (rss_bytes() - rss0) as f64 / n as f64;
    let heap_group = (heap_bytes() - heap0) as f64 / n as f64;
    check(
        per_group < 100.0 * 1024.0,
        "M1",
        &format!("{lib} RSS per group (3 replicas)"),
        format!(
            "{} (heap {} of which {} in {} allocations >= 16 KiB; total RSS {} MiB, threads {})",
            kib(per_group),
            kib(heap_group),
            kib(large_allocs().0 as f64 / n as f64),
            large_allocs().1,
            rss_bytes() >> 20,
            threads()
        ),
        "< 100 KiB",
    );

    if o.write_s > 0 {
        write_phase(&mut c, o, tick, &mut next_tick);
        return;
    }

    c.quiesced = o.quiesce;
    let s0 = c.st;
    let w = CpuWindow::start();
    c.run(Instant::now() + Duration::from_secs(o.idle_s), tick, &mut next_tick, |_| false);
    let (wall, cpu) = w.stop();
    let s1 = c.st;
    let secs = wall.as_secs_f64();
    let pct = cpu.as_secs_f64() / secs * 100.0;
    check(
        pct / 3.0 < 5.0,
        "M2",
        &format!("{lib} idle CPU, 3 nodes in one thread{}", if o.quiesce { ", quiesced (no ticks)" } else { "" }),
        format!("{:.2}% of one core total, {:.2}% per simulated node", pct, pct / 3.0),
        "< 5% per node",
    );
    info(
        "M2",
        "idle traffic",
        format!(
            "{:.0} raft msgs/s ({:.0} heartbeats/s), {:.0} node-pair batches/s, {:.0} rounds/s, {:.1} msgs per group per heartbeat interval, loadavg {}",
            (s1.msgs - s0.msgs) as f64 / secs,
            (s1.heartbeats - s0.heartbeats) as f64 / secs,
            (s1.pair_batches - s0.pair_batches) as f64 / secs,
            (s1.rounds - s0.rounds) as f64 / secs,
            (s1.msgs - s0.msgs) as f64 / n as f64 / (secs * 1000.0 / o.hb_ms as f64),
            loadavg()
        ),
    );
    info(
        "M1",
        "after the idle window",
        format!("RSS per group {} (heap {})", kib((rss_bytes() - rss0) as f64 / n as f64), kib((heap_bytes() - heap0) as f64 / n as f64)),
    );
    if s1.ticks > s0.ticks {
        info("M2", "cpu per tick of all 3 x groups replicas", format!("{:.0} us", cpu.as_micros() as f64 / (s1.ticks - s0.ticks) as f64));
    }

    if o.kill {
        let on_node1 = c.leaders_on(0);
        c.dead[0] = true;
        let t0 = Instant::now();
        let done = c.run(t0 + Duration::from_secs(60), tick, &mut next_tick, |c| c.leaders() == n);
        let el = t0.elapsed();
        let want = o.elect_max_ms + 2 * o.hb_ms;
        check(
            done && el.as_millis() as u64 <= want,
            "M3b",
            &format!("{lib} re-elect after node 1 stops ({on_node1} groups had their leader there)"),
            format!("{} ms", el.as_millis()),
            &format!("<= {want} ms"),
        );
    }
}

fn write_phase(c: &mut Cluster, o: &Opts, tick: Duration, next_tick: &mut Instant) {
    let lib = "raft-rs";
    c.writing = true;
    let s0 = c.st;
    let ops0 = c.ops;
    let heap0 = heap_bytes();
    let w = CpuWindow::start();
    c.run(Instant::now() + Duration::from_secs(o.write_s), tick, next_tick, |_| false);
    let (wall, cpu) = w.stop();
    let s1 = c.st;
    let ops = c.ops - ops0;
    let secs = wall.as_secs_f64();
    check(
        ops as f64 / secs > 100_000.0,
        "M4",
        &format!("{lib} {}B proposals, {} in flight per group, {} groups, one thread", o.payload, o.inflight, o.groups),
        format!(
            "{:.0} ops/s, {:.2} us CPU per op, CPU {:.0}% of one core",
            ops as f64 / secs,
            cpu.as_micros() as f64 / ops as f64,
            cpu.as_secs_f64() / secs * 100.0
        ),
        "> 100000 ops/s",
    );
    info(
        "M4",
        "batching",
        format!(
            "{:.2} entries per MsgAppend, {:.2} entries persisted per Ready, {:.0} fsyncs/s, {:.1} entries per fsync, {:.1} Readys per fsync, heap growth {} MiB, loadavg {}",
            (s1.append_entries - s0.append_entries) as f64 / (s1.appends - s0.appends).max(1) as f64,
            (s1.persisted_entries - s0.persisted_entries) as f64 / (s1.readies - s0.readies).max(1) as f64,
            (s1.fsyncs - s0.fsyncs) as f64 / secs,
            (s1.persisted_entries - s0.persisted_entries) as f64 / (s1.fsyncs - s0.fsyncs).max(1) as f64,
            (s1.readies - s0.readies) as f64 / (s1.fsyncs - s0.fsyncs).max(1) as f64,
            (heap_bytes() - heap0) >> 20,
            loadavg()
        ),
    );
}

/// M7: virtual time, seeded message shuffle and 10% drop, node 1 stops at tick 150.
pub fn sim(o: &Opts) {
    let tick_ms = o.hb_ms;
    let mut c = Cluster::new(o, tick_ms);
    c.sim_rng = Some(StdRng::seed_from_u64(o.seed));
    c.trace = Some(DefaultHasher::new());
    if o.sim_variant == "seeded" {
        c.override_rng = Some(StdRng::seed_from_u64(o.seed ^ 0xabcd));
        for n in 0..3 {
            for g in 0..c.groups() {
                let t = c.override_rng.as_mut().unwrap().gen_range(c.elect_ticks.0..c.elect_ticks.1);
                c.nodes[n][g].raft.set_randomized_election_timeout(t);
            }
        }
    }
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
        let term = c.nodes[1][g].raft.term;
        (l, term).hash(&mut h);
        leaders.push(format!("{l}@{term}"));
    }
    println!(
        "SIM raft-rs variant={} seed={} groups={} msgs={} trace={:016x} leaders={}",
        o.sim_variant,
        o.seed,
        o.groups,
        c.st.msgs,
        h.finish(),
        leaders.join(",")
    );
}
