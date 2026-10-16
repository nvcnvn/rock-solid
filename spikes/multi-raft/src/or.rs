//! openraft 0.9.25 harness: 3 simulated nodes x N groups, each replica a `Raft` with its own
//! tasks on one tokio runtime; RPCs are direct calls into the target `Raft` handle.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::async_runtime::TokioOneShotSender;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::impls::OneshotResponder;
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    AsyncRuntime, BasicNode, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogReader, RaftNetwork,
    RaftNetworkFactory, RaftSnapshotBuilder, RaftTypeConfig, ServerState, Snapshot, SnapshotMeta, SnapshotPolicy,
    StorageError, StoredMembership, TokioInstant, Vote,
};
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};
use tokio::time::Instant;

use crate::common::*;

pub trait Cfg:
    RaftTypeConfig<
    D = Vec<u8>,
    R = (),
    NodeId = u64,
    Node = BasicNode,
    Entry = Entry<Self>,
    SnapshotData = Cursor<Vec<u8>>,
    Responder = OneshotResponder<Self>,
>
{
}
impl<T> Cfg for T where
    T: RaftTypeConfig<
        D = Vec<u8>,
        R = (),
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<T>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = OneshotResponder<T>,
    >
{
}

openraft::declare_raft_types!(pub TC: D = Vec<u8>, R = ());
openraft::declare_raft_types!(pub TCS: D = Vec<u8>, R = (), AsyncRuntime = SeededRuntime);

// ---------- seeded runtime (M7): tokio, except election-timeout randomness ----------

static SEED: AtomicU64 = AtomicU64::new(0);
thread_local! {
    static RNG: RefCell<StdRng> = RefCell::new(StdRng::seed_from_u64(SEED.load(Relaxed)));
}

pub struct SeededRng;
impl RngCore for SeededRng {
    fn next_u32(&mut self) -> u32 {
        RNG.with(|r| r.borrow_mut().next_u32())
    }
    fn next_u64(&mut self) -> u64 {
        RNG.with(|r| r.borrow_mut().next_u64())
    }
    fn fill_bytes(&mut self, d: &mut [u8]) {
        RNG.with(|r| r.borrow_mut().fill_bytes(d))
    }
    fn try_fill_bytes(&mut self, d: &mut [u8]) -> Result<(), rand::Error> {
        RNG.with(|r| r.borrow_mut().try_fill_bytes(d))
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SeededRuntime;
impl AsyncRuntime for SeededRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle<T: Send + 'static> = tokio::task::JoinHandle<T>;
    type Sleep = tokio::time::Sleep;
    type Instant = TokioInstant;
    type TimeoutError = tokio::time::error::Elapsed;
    type Timeout<R, T: std::future::Future<Output = R> + Send> = tokio::time::Timeout<T>;
    type ThreadLocalRng = SeededRng;
    type OneshotSender<T: Send> = TokioOneShotSender<T>;
    type OneshotReceiver<T: Send> = tokio::sync::oneshot::Receiver<T>;
    type OneshotReceiverError = tokio::sync::oneshot::error::RecvError;
    fn spawn<T>(f: T) -> Self::JoinHandle<T::Output>
    where
        T: std::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        tokio::task::spawn(f)
    }
    fn sleep(d: Duration) -> Self::Sleep {
        tokio::time::sleep(d)
    }
    fn sleep_until(d: Self::Instant) -> Self::Sleep {
        tokio::time::sleep_until(d)
    }
    fn timeout<R, F: std::future::Future<Output = R> + Send>(d: Duration, f: F) -> Self::Timeout<R, F> {
        tokio::time::timeout(d, f)
    }
    fn timeout_at<R, F: std::future::Future<Output = R> + Send>(d: Self::Instant, f: F) -> Self::Timeout<R, F> {
        tokio::time::timeout_at(d, f)
    }
    fn is_panic(e: &Self::JoinError) -> bool {
        e.is_panic()
    }
    fn thread_rng() -> Self::ThreadLocalRng {
        SeededRng
    }
    fn oneshot<T: Send>() -> (Self::OneshotSender<T>, Self::OneshotReceiver<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (TokioOneShotSender(tx), rx)
    }
}

// ---------- router (in-memory network) ----------

#[derive(Default)]
pub struct Stats {
    rpcs: AtomicU64,
    votes: AtomicU64,
    appends: AtomicU64,
    append_entries: AtomicU64,
    heartbeats: AtomicU64,
    log_appends: AtomicU64,
    log_append_entries: AtomicU64,
    fsyncs: AtomicU64,
    fsync_appends: AtomicU64,
    fsync_entries: AtomicU64,
}

struct SimTrace {
    h: DefaultHasher,
    rng: StdRng,
    t0: Instant,
}

pub struct Router<C: Cfg> {
    rafts: RwLock<Vec<Vec<Option<Raft<C>>>>>,
    dead: AtomicU8,
    st: Stats,
    sim: Mutex<Option<SimTrace>>,
}

impl<C: Cfg> Router<C> {
    fn get(&self, node: u64, g: usize) -> Raft<C> {
        self.rafts.read().unwrap()[node as usize - 1][g].clone().unwrap()
    }
    fn is_dead(&self, n: u64) -> bool {
        self.dead.load(Relaxed) & (1 << (n - 1)) != 0
    }
    /// Returns false if the RPC is dropped.
    fn pass(&self, g: usize, from: u64, to: u64, kind: u8, term: u64) -> bool {
        self.st.rpcs.fetch_add(1, Relaxed);
        if self.is_dead(from) || self.is_dead(to) {
            return false;
        }
        let mut s = self.sim.lock().unwrap();
        if let Some(s) = s.as_mut() {
            (s.t0.elapsed().as_millis() as u64, g, from, to, kind, term).hash(&mut s.h);
            if s.rng.gen_bool(0.1) {
                return false;
            }
        }
        true
    }
    fn leader_of(&self, g: usize) -> Option<u64> {
        let r = self.rafts.read().unwrap();
        (1..=3u64).find(|&n| !self.is_dead(n) && r[n as usize - 1][g].as_ref().unwrap().metrics().borrow().state == ServerState::Leader)
    }
    fn leaders(&self, groups: usize) -> usize {
        (0..groups).filter(|&g| self.leader_of(g).is_some()).count()
    }
    fn leaders_on(&self, n: u64, groups: usize) -> usize {
        let r = self.rafts.read().unwrap();
        (0..groups).filter(|&g| r[n as usize - 1][g].as_ref().unwrap().metrics().borrow().state == ServerState::Leader).count()
    }
}

fn unreachable<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("dropped")))
}

pub struct NetFactory<C: Cfg> {
    router: Arc<Router<C>>,
    group: usize,
    from: u64,
}

pub struct Conn<C: Cfg> {
    router: Arc<Router<C>>,
    group: usize,
    from: u64,
    to: u64,
}

impl<C: Cfg> RaftNetworkFactory<C> for NetFactory<C> {
    type Network = Conn<C>;
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Conn<C> {
        Conn { router: self.router.clone(), group: self.group, from: self.from, to: target }
    }
}

impl<C: Cfg> RaftNetwork<C> for Conn<C> {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let st = &self.router.st;
        st.appends.fetch_add(1, Relaxed);
        st.append_entries.fetch_add(rpc.entries.len() as u64, Relaxed);
        if rpc.entries.is_empty() {
            st.heartbeats.fetch_add(1, Relaxed);
        }
        if !self.router.pass(self.group, self.from, self.to, 1, rpc.vote.leader_id().term) {
            return Err(unreachable());
        }
        let r = self.router.get(self.to, self.group);
        r.append_entries(rpc).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _o: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
        if !self.router.pass(self.group, self.from, self.to, 3, rpc.vote.leader_id().term) {
            return Err(unreachable());
        }
        let r = self.router.get(self.to, self.group);
        r.install_snapshot(rpc).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _o: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.router.st.votes.fetch_add(1, Relaxed);
        if !self.router.pass(self.group, self.from, self.to, 2, rpc.vote.leader_id().term) {
            return Err(unreachable());
        }
        let r = self.router.get(self.to, self.group);
        r.vote(rpc).await.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))
    }
}

// ---------- log storage, optionally behind one shared WAL task ----------

type WalTx<C> = tokio::sync::mpsc::UnboundedSender<(u64, LogFlushed<C>)>;

struct LogInner<C: Cfg> {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    log: BTreeMap<u64, Entry<C>>,
    last_purged: Option<LogId<u64>>,
}

pub struct LogStore<C: Cfg> {
    inner: Arc<Mutex<LogInner<C>>>,
    router: Arc<Router<C>>,
    wal: Option<WalTx<C>>,
}

impl<C: Cfg> Clone for LogStore<C> {
    fn clone(&self) -> Self {
        LogStore { inner: self.inner.clone(), router: self.router.clone(), wal: self.wal.clone() }
    }
}

impl<C: Cfg> RaftLogReader<C> for LogStore<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl<C: Cfg> RaftLogStorage<C> for LogStore<C> {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<u64>> {
        let i = self.inner.lock().unwrap();
        let last = i.log.values().next_back().map(|e| e.log_id).or(i.last_purged);
        Ok(LogState { last_purged_log_id: i.last_purged, last_log_id: last })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, v: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().unwrap().vote = Some(*v);
        Ok(())
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().vote)
    }
    async fn save_committed(&mut self, c: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.inner.lock().unwrap().committed = c;
        Ok(())
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().unwrap().committed)
    }
    async fn append<I>(&mut self, entries: I, cb: LogFlushed<C>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut n = 0;
        {
            let mut i = self.inner.lock().unwrap();
            for e in entries {
                n += 1;
                i.log.insert(e.log_id.index, e);
            }
        }
        self.router.st.log_appends.fetch_add(1, Relaxed);
        self.router.st.log_append_entries.fetch_add(n, Relaxed);
        match &self.wal {
            Some(tx) => tx.send((n, cb)).map_err(|_| ()).unwrap(),
            None => cb.log_io_completed(Ok(())),
        }
        Ok(())
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().unwrap().log.split_off(&id.index);
        Ok(())
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut i = self.inner.lock().unwrap();
        i.log = i.log.split_off(&(id.index + 1));
        i.last_purged = Some(id);
        Ok(())
    }
}

async fn wal_task<C: Cfg>(mut rx: tokio::sync::mpsc::UnboundedReceiver<(u64, LogFlushed<C>)>, fsync_us: u64, router: Arc<Router<C>>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(x) = rx.try_recv() {
            batch.push(x);
        }
        if fsync_us > 0 {
            tokio::task::spawn_blocking(move || std::thread::sleep(Duration::from_micros(fsync_us))).await.unwrap();
        }
        let st = &router.st;
        st.fsyncs.fetch_add(1, Relaxed);
        st.fsync_appends.fetch_add(batch.len() as u64, Relaxed);
        st.fsync_entries.fetch_add(batch.iter().map(|b| b.0).sum::<u64>(), Relaxed);
        for (_, cb) in batch {
            cb.log_io_completed(Ok(()));
        }
    }
}

// ---------- state machine ----------

pub struct Sm<C: Cfg> {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    applied: u64,
    _c: std::marker::PhantomData<C>,
}

pub struct SnapB {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
}

impl<C: Cfg> RaftSnapshotBuilder<C> for SnapB {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u64>> {
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.membership.clone(),
                snapshot_id: format!("{:?}", self.last_applied),
            },
            snapshot: Box::new(Cursor::new(vec![])),
        })
    }
}

impl<C: Cfg> RaftStateMachine<C> for Sm<C> {
    type SnapshotBuilder = SnapB;
    async fn applied_state(&mut self) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        Ok((self.last_applied, self.membership.clone()))
    }
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        for e in entries {
            self.last_applied = Some(e.log_id);
            match e.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(_) => self.applied += 1,
                EntryPayload::Membership(m) => self.membership = StoredMembership::new(Some(e.log_id), m),
            }
            out.push(());
        }
        Ok(out)
    }
    async fn get_snapshot_builder(&mut self) -> SnapB {
        SnapB { last_applied: self.last_applied, membership: self.membership.clone() }
    }
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(vec![])))
    }
    async fn install_snapshot(&mut self, meta: &SnapshotMeta<u64, BasicNode>, _s: Box<Cursor<Vec<u8>>>) -> Result<(), StorageError<u64>> {
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        Ok(())
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u64>> {
        Ok(None)
    }
}

// ---------- cluster ----------

fn config(o: &Opts) -> Arc<Config> {
    Arc::new(
        Config {
            heartbeat_interval: o.hb_ms,
            election_timeout_min: o.elect_min_ms,
            election_timeout_max: o.elect_max_ms,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

async fn build<C: Cfg>(o: &Opts) -> Arc<Router<C>> {
    let router = Arc::new(Router::<C> {
        rafts: RwLock::new(vec![vec![None; o.groups]; 3]),
        dead: AtomicU8::new(0),
        st: Stats::default(),
        sim: Mutex::new(None),
    });
    let wal = if o.fsync_us > 0 {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(wal_task(rx, o.fsync_us, router.clone()));
        Some(tx)
    } else {
        None
    };
    let cfg = config(o);
    for g in 0..o.groups {
        for id in 1..=3u64 {
            let log = LogStore {
                inner: Arc::new(Mutex::new(LogInner { vote: None, committed: None, log: BTreeMap::new(), last_purged: None })),
                router: router.clone(),
                wal: wal.clone(),
            };
            let sm = Sm::<C> { last_applied: None, membership: Default::default(), applied: 0, _c: Default::default() };
            let net = NetFactory { router: router.clone(), group: g, from: id };
            let raft = Raft::<C>::new(id, cfg.clone(), net, log, sm).await.unwrap();
            router.rafts.write().unwrap()[id as usize - 1][g] = Some(raft);
        }
    }
    router
}

async fn initialize<C: Cfg>(router: &Router<C>, o: &Opts) {
    let members: BTreeSet<u64> = [1, 2, 3].into();
    let mut set = tokio::task::JoinSet::new();
    for g in 0..o.groups {
        let nodes: Vec<u64> = if o.init == "all" { vec![1, 2, 3] } else { vec![(g % 3) as u64 + 1] };
        for n in nodes {
            let r = router.get(n, g);
            let m = members.clone();
            set.spawn(async move { r.initialize(m).await.unwrap() });
        }
    }
    set.join_all().await;
}

async fn wait_leaders<C: Cfg>(router: &Router<C>, n: usize, limit: Duration) -> (bool, Duration) {
    let t0 = Instant::now();
    loop {
        if router.leaders(n) == n {
            return (true, t0.elapsed());
        }
        if t0.elapsed() > limit {
            return (false, t0.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn tasks() -> usize {
    tokio::runtime::Handle::current().metrics().num_alive_tasks()
}

pub fn bench(o: &Opts) {
    let rt = if o.threads <= 1 {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    } else {
        tokio::runtime::Builder::new_multi_thread().worker_threads(o.threads).enable_all().build().unwrap()
    };
    rt.block_on(bench_async(o.clone()));
}

async fn bench_async(o: Opts) {
    let lib = format!("openraft(threads={})", o.threads);
    println!(
        "=== openraft 0.9.25 groups={} hb={}ms election={}-{}ms init={} threads={} write={}s inflight={} fsync_us={} loadavg={}",
        o.groups, o.hb_ms, o.elect_min_ms, o.elect_max_ms, o.init, o.threads, o.write_s, o.inflight, o.fsync_us, loadavg()
    );
    let n = o.groups;
    let rss0 = rss_bytes();
    let heap0 = heap_bytes();
    let tasks0 = tasks();
    let t_create = std::time::Instant::now();
    let router = build::<TC>(&o).await;
    info("M1", "create 3 x groups Raft instances", format!("{} us", t_create.elapsed().as_micros()));
    info("M2", "tokio tasks per replica before initialize", format!("{:.2}", (tasks() - tasks0) as f64 / (3 * n) as f64));
    let t0 = Instant::now();
    initialize(&router, &o).await;
    let (done, _) = wait_leaders(&router, n, Duration::from_secs(60)).await;
    let el = t0.elapsed();
    check(
        done && el.as_millis() as u64 <= 2 * o.elect_max_ms + o.hb_ms,
        "M3a",
        &format!("{lib} all groups elected from bootstrap ({})", o.init),
        format!("{} ms ({} leaders)", el.as_millis(), router.leaders(n)),
        &format!("<= {} ms", 2 * o.elect_max_ms + o.hb_ms),
    );
    info("M3a", "leaders per node", format!("{} {} {}", router.leaders_on(1, n), router.leaders_on(2, n), router.leaders_on(3, n)));
    tokio::time::sleep(Duration::from_secs(2)).await;
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
    let t = (tasks() - tasks0) as f64;
    check(
        (t / n as f64) <= 11.5,
        "M2",
        &format!("{lib} tokio tasks per group (3 replicas, 1 leader)"),
        format!("{:.2} ({} tasks)", t / n as f64, tasks() - tasks0),
        "<= 11",
    );

    if o.write_s > 0 {
        write_phase(&router, &o, &lib).await;
        return;
    }

    if o.quiesce {
        for row in router.rafts.read().unwrap().iter() {
            for r in row.iter().flatten() {
                r.runtime_config().tick(false);
            }
        }
    }
    let st = &router.st;
    let (r0, h0) = (st.rpcs.load(Relaxed), st.heartbeats.load(Relaxed));
    let w = CpuWindow::start();
    tokio::time::sleep(Duration::from_secs(o.idle_s)).await;
    let (wall, cpu) = w.stop();
    let secs = wall.as_secs_f64();
    let (r1, h1) = (st.rpcs.load(Relaxed), st.heartbeats.load(Relaxed));
    let pct = cpu.as_secs_f64() / secs * 100.0;
    check(
        pct / 3.0 < 5.0,
        "M2",
        &format!("{lib} idle CPU, 3 nodes in one process{}", if o.quiesce { ", quiesced (enable_tick(false))" } else { "" }),
        format!("{:.2}% of one core total, {:.2}% per simulated node", pct, pct / 3.0),
        "< 5% per node",
    );
    info(
        "M2",
        "idle traffic",
        format!(
            "{:.0} RPCs/s = {:.0} raft msgs/s counting responses ({:.0} heartbeats/s), {:.1} msgs per group per heartbeat interval, loadavg {}",
            (r1 - r0) as f64 / secs,
            2.0 * (r1 - r0) as f64 / secs,
            (h1 - h0) as f64 / secs,
            2.0 * (r1 - r0) as f64 / n as f64 / (secs * 1000.0 / o.hb_ms as f64),
            loadavg()
        ),
    );
    info(
        "M1",
        "after the idle window",
        format!("RSS per group {} (heap {})", kib((rss_bytes() - rss0) as f64 / n as f64), kib((heap_bytes() - heap0) as f64 / n as f64)),
    );

    if o.kill {
        let on1 = router.leaders_on(1, n);
        router.dead.store(1, Relaxed);
        let t0 = Instant::now();
        let (done, _) = wait_leaders(&router, n, Duration::from_secs(60)).await;
        let el = t0.elapsed();
        let want = o.elect_max_ms + 2 * o.hb_ms;
        check(
            done && el.as_millis() as u64 <= want,
            "M3b",
            &format!("{lib} re-elect after node 1 stops ({on1} groups had their leader there)"),
            format!("{} ms ({} leaders)", el.as_millis(), router.leaders(n)),
            &format!("<= {want} ms"),
        );
    }
    std::process::exit(0);
}

async fn write_phase<C: Cfg>(router: &Arc<Router<C>>, o: &Opts, lib: &str) {
    let st = &router.st;
    let ld = |a: &AtomicU64| a.load(Relaxed);
    let s0 = [ld(&st.appends), ld(&st.append_entries), ld(&st.log_appends), ld(&st.log_append_entries), ld(&st.fsyncs), ld(&st.fsync_appends), ld(&st.fsync_entries), ld(&st.heartbeats)];
    let ops = Arc::new(AtomicU64::new(0));
    let heap0 = heap_bytes();
    let w = CpuWindow::start();
    let deadline = Instant::now() + Duration::from_secs(o.write_s);
    let mut hs = Vec::new();
    for g in 0..o.groups {
        let Some(l) = router.leader_of(g) else { continue };
        for _ in 0..o.inflight {
            let r = router.get(l, g);
            let ops = ops.clone();
            let p = vec![7u8; o.payload];
            hs.push(tokio::spawn(async move {
                while Instant::now() < deadline {
                    if r.client_write(p.clone()).await.is_err() {
                        break;
                    }
                    ops.fetch_add(1, Relaxed);
                }
            }));
        }
    }
    for h in hs {
        h.await.unwrap();
    }
    let (wall, cpu) = w.stop();
    let secs = wall.as_secs_f64();
    let ops = ops.load(Relaxed);
    let s1 = [ld(&st.appends), ld(&st.append_entries), ld(&st.log_appends), ld(&st.log_append_entries), ld(&st.fsyncs), ld(&st.fsync_appends), ld(&st.fsync_entries), ld(&st.heartbeats)];
    let d: Vec<f64> = (0..8).map(|i| (s1[i] - s0[i]) as f64).collect();
    check(
        ops as f64 / secs > 100_000.0,
        "M4",
        &format!("{lib} {}B proposals, {} in flight per group, {} groups", o.payload, o.inflight, o.groups),
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
            "{:.2} entries per AppendEntries RPC ({:.0} of {:.0} RPCs empty), {:.2} entries per RaftLogStorage::append, {:.0} fsyncs/s, {:.1} entries per fsync, {:.1} append calls per fsync, heap growth {} MiB, loadavg {}",
            d[1] / d[0].max(1.0),
            d[7],
            d[0],
            d[3] / d[2].max(1.0),
            d[4] / secs,
            d[6] / d[4].max(1.0),
            d[5] / d[4].max(1.0),
            (heap_bytes() - heap0) >> 20,
            loadavg()
        ),
    );
    std::process::exit(0);
}

/// M7: tokio current-thread runtime with the clock paused (auto-advance), seeded 10% RPC drop,
/// node 1 stops at virtual 7.5 s, stop at 15 s.
pub fn sim(o: &Opts) {
    SEED.store(o.seed ^ 0xabcd, Relaxed);
    let mut b = tokio::runtime::Builder::new_current_thread();
    b.enable_all().start_paused(true);
    #[cfg(tokio_unstable)]
    if o.sim_variant == "seeded-tokio" {
        b.rng_seed(tokio::runtime::RngSeed::from_bytes(&o.seed.to_le_bytes()));
    }
    let rt = b.build().unwrap();
    match o.sim_variant.as_str() {
        "plain" => rt.block_on(sim_async::<TC>(o.clone())),
        _ => rt.block_on(sim_async::<TCS>(o.clone())),
    }
}

async fn sim_async<C: Cfg>(o: Opts) {
    let router = build::<C>(&o).await;
    *router.sim.lock().unwrap() = Some(SimTrace { h: DefaultHasher::new(), rng: StdRng::seed_from_u64(o.seed), t0: Instant::now() });
    initialize(&router, &o).await;
    tokio::time::sleep(Duration::from_millis(7500)).await;
    router.dead.store(1, Relaxed);
    tokio::time::sleep(Duration::from_millis(7500)).await;
    let mut leaders = Vec::new();
    let mut h = router.sim.lock().unwrap().take().unwrap().h;
    for g in 0..o.groups {
        let l = router.leader_of(g).unwrap_or(0);
        let term = router.get(2, g).metrics().borrow().current_term;
        (l, term).hash(&mut h);
        leaders.push(format!("{l}@{term}"));
    }
    println!(
        "SIM openraft variant={} seed={} groups={} rpcs={} trace={:016x} leaders={}",
        o.sim_variant,
        o.seed,
        o.groups,
        router.st.rpcs.load(Relaxed),
        h.finish(),
        leaders.join(",")
    );
    std::process::exit(0);
}
