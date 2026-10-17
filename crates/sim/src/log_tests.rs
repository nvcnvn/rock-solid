//! The disk log on the simulated disk: group commit, checkpoints, crashes and recovery.
//!
//! Cases named `rocksdb_*` are ported from `RocksDB` `db/log_test.cc`
//! (a3b0f187ff4b501c7fe4a67dbcc4ad6d433a454c), which keeps the `LevelDB` log tests; `etcd_*` from etcd
//! `server/storage/wal/wal_test.go` (b25719d10cdc41fb707b0d9f819c2529dec5d590); `par_*` from
//! Alagappan et al., "Protocol-Aware Recovery for Consensus-Based Storage", FAST 2018, which
//! separates a torn tail (a crash) from a bad record with later records after it (corruption).

use std::collections::{BTreeSet, VecDeque};

use crate::disk::SimDisk;
use crate::pool::HeapPool;
use rock_core::buf::BufPool;
use rock_core::chunk_index::{ChunkIndex, ExtentValue};
use rock_core::event::{Action, BLOCK_BYTES, Context, Event, IoId, Lba, Tick};
use rock_core::id::{ChunkId, InodeId, PoolId, VolumeId};
use rock_core::log::commit::{Config, GroupCommit, Item, LogEvent, Ticket};
use rock_core::log::record::{BlobKind, Entry, Extent};
use rock_core::log::recovery::{Mode, Recovered, Recovery, RecoveryError, Step};
use rock_core::log::segment::{Geometry, Superblock};
use rock_core::log::{LogRecord, Stream};
use rock_core::lsm::{MemTree, Tree};
use rock_core::rng::Rng;

const INDEX: u64 = 1;
const POOL_BYTES: usize = 64 << 20;
const GIB_BLOCKS: u64 = 1 << 18;

struct Node {
    disk: SimDisk,
    pool: HeapPool,
    log: GroupCommit,
    index: ChunkIndex,
    actions: VecDeque<Action>,
    acked: BTreeSet<Ticket>,
    failed: BTreeSet<Ticket>,
    durable: Vec<LogRecord>,
    checkpoints: usize,
    checkpoint_failed: bool,
}

impl Node {
    fn format(disk: SimDisk, config: Config, log_id: u64) -> Self {
        let mut pool = HeapPool::new(POOL_BYTES);
        let superblock = Superblock {
            log_id,
            geometry: Geometry::for_disk(disk.blocks()).unwrap(),
        };
        let (log, actions) = GroupCommit::format(
            &mut Context::new(Tick(0), &mut pool),
            config,
            superblock,
            IoId(1),
        )
        .unwrap();
        let mut node = Self::new(disk, pool, log, ChunkIndex::default());
        node.actions.extend(actions);
        node.run();
        node
    }

    fn open(mut disk: SimDisk, config: Config) -> Result<(Self, Recovered), RecoveryError> {
        let mut pool = HeapPool::new(POOL_BYTES);
        let recovered = recover(&mut disk, &mut pool, Mode::Replay)?;
        let index = index_of(&recovered);
        let log = GroupCommit::resume(config, recovered.resume.clone(), IoId(1_000_000));
        Ok((Self::new(disk, pool, log, index), recovered))
    }

    fn new(disk: SimDisk, pool: HeapPool, log: GroupCommit, index: ChunkIndex) -> Self {
        Self {
            disk,
            pool,
            log,
            index,
            actions: VecDeque::new(),
            acked: BTreeSet::new(),
            failed: BTreeSet::new(),
            durable: Vec::new(),
            checkpoints: 0,
            checkpoint_failed: false,
        }
    }

    fn handle(&mut self, actions: Vec<Action>, events: Vec<LogEvent>) {
        self.actions.extend(actions);
        for event in events {
            match event {
                LogEvent::Durable { record, tickets } => {
                    self.index.apply(&record);
                    self.acked.extend(tickets);
                    self.durable.push(record);
                }
                LogEvent::Failed { tickets, .. } => self.failed.extend(tickets),
                LogEvent::CheckpointDone(_) => self.checkpoints += 1,
                LogEvent::CheckpointFailed(_) => self.checkpoint_failed = true,
            }
        }
        if self.log.checkpoint_due() && !self.checkpoint_failed {
            let pieces = self.index.tree().checkpoint(self.log.max_blob_bytes());
            let out = self
                .log
                .begin_checkpoint(
                    &mut Context::new(Tick(0), &mut self.pool),
                    vec![(INDEX, pieces)],
                )
                .unwrap();
            self.handle(out.actions, out.events);
        }
    }

    fn submit(&mut self, stream: Stream, item: Item) -> Ticket {
        let (ticket, out) = self
            .log
            .submit(&mut Context::new(Tick(0), &mut self.pool), stream, item)
            .unwrap();
        self.handle(out.actions, out.events);
        ticket
    }

    fn write(&mut self, chunk: u32, version: u64, first_block: u16, blocks: u16) -> Ticket {
        let extent = extent(chunk, version, first_block, blocks);
        let bytes: Vec<u8> = (0..blocks).flat_map(|b| pattern(&extent, b)).collect();
        let data = self.pool.alloc_from_slice(&bytes).unwrap();
        self.submit(Stream::Hot, Item::Write { extent, data })
    }

    fn blob(&mut self, stream: Stream, owner: u64, len: usize) -> Ticket {
        let bytes = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
        self.submit(
            stream,
            Item::Blob {
                kind: BlobKind(9),
                owner,
                bytes,
            },
        )
    }

    /// Executes the oldest pending action; false when there is none.
    fn step(&mut self) -> bool {
        let Some(action) = self.actions.pop_front() else {
            return false;
        };
        if let Some(event) = self.disk.execute(&mut self.pool, &action) {
            let out = self
                .log
                .on_event(&mut Context::new(Tick(0), &mut self.pool), event);
            self.handle(out.actions, out.events);
        }
        true
    }

    fn run(&mut self) {
        while self.step() {}
    }

    /// Runs until a round's write is on the disk, unflushed, and its flush is next.
    fn run_to_unflushed_write(&mut self) {
        while !matches!(self.actions.front(), Some(Action::DiskFsync(_))) {
            assert!(self.step(), "no round to write");
        }
        assert!(self.disk.unflushed_blocks() > 0);
    }

    fn crash_with(mut self, landed: impl FnMut(usize, usize) -> bool) -> SimDisk {
        self.disk.crash_with(landed);
        self.disk
    }
}

fn recover(
    disk: &mut SimDisk,
    pool: &mut HeapPool,
    mode: Mode,
) -> Result<Recovered, RecoveryError> {
    let (mut recovery, mut step) = Recovery::start(&mut Context::new(Tick(0), pool), mode, IoId(1));
    loop {
        match step {
            Step::Done(result) => return result,
            Step::Io(actions) => {
                let [action] = actions.as_slice() else {
                    panic!("one read at a time: {actions:?}");
                };
                let event = disk.execute(pool, action).unwrap();
                step = recovery.on_event(&mut Context::new(Tick(0), pool), event);
            }
        }
    }
}

fn index_of(recovered: &Recovered) -> ChunkIndex {
    let mut tree = MemTree::new();
    if let Some((_, pieces)) = recovered
        .snapshots
        .iter()
        .find(|(owner, _)| *owner == INDEX)
    {
        tree.restore(pieces).unwrap();
    }
    let mut index = ChunkIndex::new(tree);
    for record in &recovered.records {
        index.apply(record);
    }
    index
}

fn rebuilt(disk: &SimDisk) -> ChunkIndex {
    let recovered = recover(
        &mut disk.clone(),
        &mut HeapPool::new(POOL_BYTES),
        Mode::Rebuild,
    )
    .unwrap();
    assert_eq!(recovered.snapshots, Vec::new());
    index_of(&recovered)
}

fn extent(chunk: u32, version: u64, first_block: u16, blocks: u16) -> Extent {
    Extent {
        pool: PoolId(1),
        chunk: ChunkId::new(VolumeId(1), InodeId(1), chunk),
        version,
        first_block,
        blocks,
    }
}

fn pattern(extent: &Extent, block: u16) -> Vec<u8> {
    let seed = extent.chunk.hash()
        ^ extent.version.rotate_left(17)
        ^ u64::from(extent.first_block + block);
    seed.to_le_bytes().repeat(BLOCK_BYTES / 8)
}

/// Every data extent the index maps reads back as the bytes that were written.
fn assert_readable(disk: &SimDisk, index: &ChunkIndex, chunks: u32) {
    for chunk in 0..chunks {
        let id = ChunkId::new(VolumeId(1), InodeId(1), chunk);
        for (key, value) in index.extents(PoolId(1), id) {
            if let ExtentValue::Data { blocks, data, .. } = value {
                let written = extent(chunk, key.version, key.first_block, blocks);
                for b in 0..blocks {
                    assert_eq!(
                        disk.read_block(data.advance(u64::from(b))),
                        pattern(&written, b),
                        "{key:?} block {b}"
                    );
                }
            }
        }
    }
}

fn lsns(records: &[LogRecord]) -> Vec<u64> {
    records.iter().map(|r| r.pos.lsn).collect()
}

fn small_records() -> Config {
    Config {
        max_record_bytes: 4 * BLOCK_BYTES,
        ..Config::default()
    }
}

#[test]
fn rocksdb_empty() {
    let node = Node::format(SimDisk::new(GIB_BLOCKS), Config::default(), 1);
    let (mut node, recovered) = Node::open(node.disk, Config::default()).unwrap();
    assert!(recovered.records.is_empty() && recovered.snapshots.is_empty());
    node.write(0, 1, 0, 1);
    node.run();
    assert_eq!(lsns(&node.durable), [1]);
}

#[test]
fn rocksdb_not_formatted_disk_is_reported() {
    let mut disk = SimDisk::new(GIB_BLOCKS);
    assert!(matches!(
        recover(&mut disk, &mut HeapPool::new(POOL_BYTES), Mode::Replay),
        Err(RecoveryError::NotFormatted(_))
    ));
}

/// Covers `ReadWrite`, `ManyBlocks` and `Fragmentation`: records spanning many segments of both streams.
#[test]
fn rocksdb_read_write_many_segments() {
    let mut node = Node::format(SimDisk::new(1 << 16), small_records(), 2);
    for i in 0..200u32 {
        node.write(
            i % 8,
            u64::from(i) + 1,
            u16::try_from(i % 4).unwrap(),
            1 + u16::try_from(i % 3).unwrap(),
        );
        if i % 5 == 0 {
            let len = (100 * i as usize).min(node.log.max_blob_bytes());
            node.blob(Stream::Cold, 7, len);
        }
        if i % 3 == 0 {
            node.run();
        }
    }
    node.run();
    assert!(node.failed.is_empty());
    let segments_used = 4095 - node.log.free_segments().len();
    assert!(segments_used > 20, "{segments_used}");

    let live = node.index.clone();
    let durable = node.durable.clone();
    let (reopened, recovered) = Node::open(node.disk, small_records()).unwrap();
    assert_eq!(recovered.records, durable);
    assert_eq!(reopened.index, live);
    assert_eq!(rebuilt(&reopened.disk), live);
    assert_readable(&reopened.disk, &reopened.index, 8);
}

fn node_with_records(records: u32) -> Node {
    let mut node = Node::format(SimDisk::new(GIB_BLOCKS), Config::default(), 3);
    for i in 0..records {
        node.write(i, 1, 0, 2);
        node.run();
    }
    node
}

#[test]
fn rocksdb_truncated_trailing_record_is_ignored() {
    for landed_blocks in 0..3 {
        let mut node = node_with_records(3);
        let live = node.index.clone();
        node.write(9, 1, 0, 2);
        node.run_to_unflushed_write();
        let disk = node.crash_with(|_, block| block < landed_blocks);
        let (node, recovered) = Node::open(disk, Config::default()).unwrap();
        assert_eq!(lsns(&recovered.records), [1, 2, 3], "{landed_blocks}");
        assert_eq!(node.index, live);
    }
}

#[test]
fn rocksdb_partial_last_is_ignored() {
    let mut node = node_with_records(3);
    node.write(9, 1, 0, 2);
    node.run_to_unflushed_write();
    let disk = node.crash_with(|_, block| block != 2);
    let (_, recovered) = Node::open(disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1, 2, 3]);
}

#[test]
fn rocksdb_unflushed_but_whole_last_record_is_kept() {
    let mut node = node_with_records(3);
    node.write(9, 1, 0, 2);
    node.run_to_unflushed_write();
    let disk = node.crash_with(|_, _| true);
    let (_, recovered) = Node::open(disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1, 2, 3, 4]);
}

/// A bad length or garbage in a middle record must not join its neighbours into one record.
#[test]
fn rocksdb_error_joins_records() {
    let node = node_with_records(4);
    let mut disk = node.disk;
    let middle = node.durable[1].lba;
    for byte in 0..BLOCK_BYTES {
        disk.flip(middle, byte, 0x5A);
    }
    let result = recover(&mut disk, &mut HeapPool::new(POOL_BYTES), Mode::Replay);
    assert_eq!(result.unwrap_err(), RecoveryError::Corrupt { lsn: 2 });
}

/// Recycle: a disk formatted again holds the old log's segments; none of their records come back.
#[test]
fn rocksdb_recycle() {
    let mut node = node_with_records(6);
    let old_records = node.durable.len();
    assert_eq!(old_records, 6);
    node.run();
    let reformatted = Node::format(node.disk, Config::default(), 4);
    let mut node = reformatted;
    node.write(1, 5, 0, 1);
    node.run();
    let (_, recovered) = Node::open(node.disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1]);
    assert_eq!(recovered.records[0].header.log_id, 4);
    assert_eq!(recovered.resume.free.len(), 4094);
}

/// `TestOpenOnTornWrite`: after recovery drops a torn tail, new records overwrite it and every
/// record reads back on the next recovery.
#[test]
fn etcd_open_on_torn_write() {
    let mut node = node_with_records(10);
    node.write(20, 1, 0, 3);
    node.run_to_unflushed_write();
    let disk = node.crash_with(|_, block| block % 2 == 0);
    let (mut node, recovered) = Node::open(disk, Config::default()).unwrap();
    assert_eq!(recovered.records.len(), 10);
    for i in 0..5 {
        node.write(30 + i, 1, 0, 1);
        node.run();
    }
    let live = node.index.clone();
    let (node, recovered) = Node::open(node.disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), (1..=15).collect::<Vec<_>>());
    assert_eq!(node.index, live);
    assert_readable(&node.disk, &node.index, 40);
}

/// `TestTailWriteNoSlackSpace`: a segment filled to its last block, reopened, keeps appending.
#[test]
fn etcd_tail_write_no_slack_space() {
    let mut node = Node::format(SimDisk::new(GIB_BLOCKS), Config::default(), 5);
    for i in 0..21 {
        node.write(i, 1, 0, 2);
        node.run();
    }
    assert_eq!(
        node.durable.last().map(|r| (r.pos.segment, r.pos.block)),
        Some((1, 61))
    );
    let (mut node, _) = Node::open(node.disk, Config::default()).unwrap();
    for i in 21..30 {
        node.write(i, 1, 0, 2);
        node.run();
    }
    let (node, recovered) = Node::open(node.disk, Config::default()).unwrap();
    assert_eq!(recovered.records.len(), 30);
    assert_eq!(recovered.records[21].pos.segment, 2);
    assert_readable(&node.disk, &node.index, 30);
}

/// `TestRecoverAfterCut`: a crash in the round that opens a new segment, whichever of the segment
/// header and the record landed.
#[test]
fn etcd_recover_after_cut() {
    for landed in [[false, false], [true, false], [false, true], [true, true]] {
        let mut node = Node::format(SimDisk::new(GIB_BLOCKS), Config::default(), 6);
        for i in 0..21 {
            node.write(i, 1, 0, 2);
            node.run();
        }
        node.write(21, 1, 0, 2);
        node.run_to_unflushed_write();
        let disk = node.crash_with(|_, block| if block == 0 { landed[0] } else { landed[1] });
        let (mut node, recovered) = Node::open(disk, Config::default()).unwrap();
        let expected = if landed == [true, true] { 22 } else { 21 };
        assert_eq!(recovered.records.len(), expected, "{landed:?}");
        node.write(40, 1, 0, 1);
        node.run();
        let (_, recovered) = Node::open(node.disk, Config::default()).unwrap();
        assert_eq!(
            lsns(&recovered.records),
            (1..=expected as u64 + 1).collect::<Vec<_>>()
        );
    }
}

#[test]
fn par_corrupted_data_in_a_middle_record_is_reported_not_truncated() {
    let node = node_with_records(5);
    let mut disk = node.disk;
    disk.flip(node.durable[2].lba.advance(1), 100, 0x01);
    let result = recover(&mut disk, &mut HeapPool::new(POOL_BYTES), Mode::Replay);
    assert_eq!(result.unwrap_err(), RecoveryError::Corrupt { lsn: 3 });
}

#[test]
fn par_corrupted_header_of_a_middle_record_is_reported_not_truncated() {
    let node = node_with_records(5);
    let mut disk = node.disk;
    disk.flip(node.durable[2].lba, 30, 0x80);
    let result = recover(&mut disk, &mut HeapPool::new(POOL_BYTES), Mode::Replay);
    assert_eq!(result.unwrap_err(), RecoveryError::Corrupt { lsn: 3 });
}

/// The last record's damage cannot be told from a torn write, so it reads as a crash.
#[test]
fn par_corrupted_last_record_reads_as_a_crash() {
    let node = node_with_records(5);
    let mut disk = node.disk;
    disk.flip(node.durable[4].lba.advance(2), 7, 0x10);
    let (_, recovered) = Node::open(disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1, 2, 3, 4]);
}

/// A torn record keeps its header in one stream while its LSN is reused in the other; recovery
/// picks the whole record and does not report the leftover as corruption.
#[test]
fn torn_record_left_behind_in_the_other_stream() {
    let mut node = node_with_records(2);
    node.blob(Stream::Cold, 8, 3 * BLOCK_BYTES);
    node.run_to_unflushed_write();
    let disk = node.crash_with(|_, block| block < 2);
    let (mut node, recovered) = Node::open(disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1, 2]);
    node.write(5, 1, 0, 1);
    node.run();
    assert_eq!(node.durable[0].pos.lsn, 3);
    assert_eq!(node.durable[0].pos.segment, 1);
    let (node, recovered) = Node::open(node.disk, Config::default()).unwrap();
    assert_eq!(lsns(&recovered.records), [1, 2, 3]);
    assert_eq!(rebuilt(&node.disk), node.index);
}

#[test]
fn checkpoints_bound_replay_and_rebuild_matches_the_manifest() {
    let config = Config {
        max_replay_bytes: 40 * BLOCK_BYTES as u64,
        ..Config::default()
    };
    let mut node = Node::format(SimDisk::new(GIB_BLOCKS), config, 7);
    for i in 0..120u32 {
        node.write(i % 16, u64::from(i), 0, 1 + u16::try_from(i % 4).unwrap());
        if i % 7 == 0 {
            node.submit(
                Stream::Hot,
                Item::Punch(extent(i % 16, u64::from(i) + 1000, 0, 1024)),
            );
        }
        node.run();
    }
    assert!(node.checkpoints >= 5, "{}", node.checkpoints);
    let live = node.index.clone();
    let (node, recovered) = Node::open(node.disk, config).unwrap();
    assert_eq!(recovered.snapshots.len(), 1);
    assert!(recovered.records.len() < 40, "{}", recovered.records.len());
    let replayed: u64 = recovered
        .records
        .iter()
        .map(|r| u64::from(r.header.layout().record_blocks()) * BLOCK_BYTES as u64)
        .sum();
    let segment_bytes = 64 * BLOCK_BYTES as u64;
    assert!(
        replayed <= config.max_replay_bytes + segment_bytes + (1 << 20),
        "{replayed}"
    );
    assert_eq!(node.index, live);
    assert_eq!(rebuilt(&node.disk), live);
    assert_readable(&node.disk, &node.index, 16);
}

fn random_item(node: &mut Node, rng: &mut Rng, version: &mut u64) {
    *version += 1;
    let chunk = u32::try_from(rng.below(6)).unwrap();
    let max_blob = node.log.max_blob_bytes() as u64;
    match rng.below(10) {
        0..=5 => {
            let max_blocks = (max_blob / BLOCK_BYTES as u64).min(3);
            let blocks = u16::try_from(rng.range_inclusive(1, max_blocks)).unwrap();
            let first = u16::try_from(rng.below(1024 - u64::from(blocks))).unwrap();
            node.write(chunk, *version, first, blocks);
        }
        6 => {
            let item = Item::Punch(extent(chunk, *version, 0, 1024));
            node.submit(Stream::Hot, item);
        }
        7 | 8 => {
            let len = rng.below(max_blob.min(3000));
            node.blob(
                Stream::Hot,
                100 + rng.below(4),
                usize::try_from(len).unwrap(),
            );
        }
        _ => {
            let len = rng.below(max_blob.min(6000));
            node.blob(Stream::Cold, 200, usize::try_from(len).unwrap());
        }
    }
}

/// Random workloads, crashed at random points with random torn blocks, recovered over and over:
/// recovery returns exactly the acknowledged records plus possibly the round in flight, the chunk
/// index recovered from the manifest equals a rebuild from every record header, and every
/// extent it maps reads back.
#[test]
fn random_crashes_keep_acknowledged_records() {
    let (mut kept, mut dropped, mut from_manifest, mut no_space) = (0, 0, 0, 0);
    for seed in 0..50 {
        let mut rng = Rng::from_seed(seed);
        let disk_blocks = [1024, 4096][usize::try_from(rng.below(2)).unwrap()];
        let config = Config {
            max_record_bytes: usize::try_from(rng.range_inclusive(2, 32)).unwrap() * BLOCK_BYTES,
            max_replay_bytes: rng.range_inclusive(4, 200) * BLOCK_BYTES as u64,
            ..Config::default()
        };
        let mut node = Node::format(SimDisk::new(disk_blocks), config, rng.next_u64());
        let mut version = 0;
        for cycle in 0..4 {
            for _ in 0..rng.below(300) {
                if rng.chance(2, 5) {
                    for _ in 0..rng.range_inclusive(1, 4) {
                        random_item(&mut node, &mut rng, &mut version);
                    }
                } else {
                    node.step();
                }
            }
            let live = node.index.clone();
            let durable_lsn = node.log.durable_lsn();
            let in_flight = node.log.in_flight();
            no_space += usize::from(!node.failed.is_empty());
            let mut disk = node.disk;
            disk.crash(&mut rng);
            let context = format!("seed {seed} cycle {cycle}");

            let (reopened, recovered) =
                Node::open(disk, config).unwrap_or_else(|e| panic!("{context}: {e:?}"));
            let recovered_lsn = reopened.log.durable_lsn();
            let mut expected = live;
            if recovered_lsn == durable_lsn + 1 {
                let in_flight = recovered.records.last().unwrap();
                assert_eq!(in_flight.pos.lsn, recovered_lsn, "{context}");
                expected.apply(in_flight);
                kept += 1;
            } else {
                assert_eq!(recovered_lsn, durable_lsn, "{context}");
                dropped += usize::from(in_flight);
            }
            from_manifest += usize::from(!recovered.snapshots.is_empty());
            assert_eq!(reopened.index, expected, "{context}");
            assert_eq!(rebuilt(&reopened.disk), expected, "{context}");
            assert_readable(&reopened.disk, &reopened.index, 6);
            for record in &recovered.records {
                for (entry, blob) in record
                    .header
                    .entries
                    .iter()
                    .filter(|e| matches!(e, Entry::Blob { .. }))
                    .zip(&record.blobs)
                {
                    let Entry::Blob { len, .. } = entry else {
                        unreachable!()
                    };
                    assert_eq!(blob.len(), *len as usize, "{context}");
                }
            }
            node = reopened;
        }
    }
    assert!(
        kept > 5 && dropped > 5 && from_manifest > 20 && no_space > 0,
        "kept {kept} dropped {dropped} from_manifest {from_manifest} no_space {no_space}"
    );
}

#[test]
fn recovery_reads_fail_cleanly() {
    let node = node_with_records(2);
    let mut disk = node.disk;
    let mut pool = HeapPool::new(POOL_BYTES);
    let (mut recovery, step) =
        Recovery::start(&mut Context::new(Tick(0), &mut pool), Mode::Replay, IoId(1));
    let Step::Io(actions) = step else { panic!() };
    let Action::DiskRead(io, Lba(0), buf) = actions[0] else {
        panic!()
    };
    disk.execute(&mut pool, &actions[0]);
    let _ = buf;
    let step = recovery.on_event(
        &mut Context::new(Tick(0), &mut pool),
        Event::DiskDone(io, Err(rock_core::event::DiskError::Io)),
    );
    assert_eq!(
        step,
        Step::Done(Err(RecoveryError::Disk(rock_core::event::DiskError::Io)))
    );
    assert_eq!(pool.live_buffers(), 0);
}
